// WS client that mimics frontend MBO logic, compares with server /book endpoint
import WebSocket from 'ws';

const WS_URL = process.argv[2] || 'ws://localhost:8081/ws';
const HTTP_BASE = WS_URL.replace(/^ws(s?):\/\//, 'http$1://').replace(/\/ws\/?$/, '');
const SYMBOL = process.argv[3] || 'WINZ25';
const CHECK_INTERVAL = parsePositiveInt(process.env.CHECK_INTERVAL_MS || '5000', 5000);
const RUN_SECONDS = parseNonNegativeInt(process.env.RUN_SECONDS || '0', 0);

const MSG = {
  SUBSCRIBE: 0x0001, SUBSCRIBE_OK: 0x0010, SUBSCRIBE_ERROR: 0x0011,
  BOOK_SNAPSHOT: 0x0020, ORDER_ADDED: 0x0030, ORDER_UPDATED: 0x0031,
  ORDER_DELETED: 0x0032, TRADE: 0x0033, BOOK_CLEARED: 0x0034,
  INFO_SNAPSHOT: 0x0021, UNSUBSCRIBED: 0x0012, SERVER_STATUS: 0x0050,
};

const orders = new Map();
let securityId = null;
let ready = false;
let subscribeSent = false;
let msgCount = 0, addCount = 0, updateCount = 0, deleteCount = 0, snapshotCount = 0, clearCount = 0;
let deleteNotInMap = 0;
let dupAddCount = 0, ghostUpdateCount = 0;
let socketError = null;
let closing = false;
let checkTimer = null;
let runTimer = null;
const dupAddSamples = [], ghostUpdateSamples = [], delNotInMapSamples = [];

function buildSubscribe(symbol) {
  const symBytes = Buffer.from(symbol, 'utf8');
  const buf = Buffer.alloc(8 + 4 + 1 + symBytes.length);
  buf.writeUInt32LE(buf.length, 0);
  buf.writeUInt16LE(MSG.SUBSCRIBE, 4);
  buf.writeUInt16LE(0, 6);
  buf.writeUInt32LE(0x03, 8); // flags: Book+Info
  buf.writeUInt8(symBytes.length, 12);
  symBytes.copy(buf, 13);
  return buf;
}

function processFrame(data) {
  let offset = 0;
  while (offset + 8 <= data.length) {
    const len = data.readUInt32LE(offset);
    if (len < 8 || offset + len > data.length) break;
    const type = data.readUInt16LE(offset + 4);
    const headerFlags = data.readUInt16LE(offset + 6);
    if (headerFlags !== 0) break;
    const payload = data.subarray(offset + 8, offset + len);
    processMessage(type, payload);
    offset += len;
  }
}

function processMessage(type, p) {
  msgCount++;
  switch (type) {
    case MSG.SERVER_STATUS: {
      ready = p.length > 0 && p.readUInt8(0) === 1;
      console.log(`ServerStatus: ready=${ready ? 1 : 0}`);
      if (ready)
        sendSubscribe('server-ready');
      break;
    }
    case MSG.SUBSCRIBE_OK: {
      securityId = p.readBigUInt64LE(0);
      const flags = p.readUInt32LE(8);
      const sLen = p.readUInt8(12);
      const sym = p.subarray(13, 13 + sLen).toString('utf8');
      console.log(`SubscribeOk: ${sym} id=${securityId} flags=${flags}`);
      break;
    }
    case MSG.SUBSCRIBE_ERROR: {
      const code = p.readUInt8(0);
      const sLen = p.readUInt8(1);
      const sym = p.subarray(2, 2 + sLen).toString('utf8');
      socketError = `subscribe error ${code} for ${sym}`;
      console.log(`SubscribeError: ${sym} code=${code}`);
      requestShutdown('subscribe-error');
      break;
    }
    case MSG.BOOK_SNAPSHOT: {
      snapshotCount++;
      orders.clear();
      // Don't parse MBP levels — server sends MBO snapshot (reset + individual adds)
      break;
    }
    case MSG.ORDER_ADDED:
    case MSG.ORDER_UPDATED: {
      const sid = p.readBigUInt64LE(0);
      const oid = p.readBigUInt64LE(8).toString();
      const price = Number(p.readBigInt64LE(16)) / 10000;
      const qty = Number(p.readBigInt64LE(24));
      const side = p.readUInt8(32);
      if (type === MSG.ORDER_ADDED) {
        addCount++;
        if (orders.has(oid)) {
          dupAddCount++;
          if (dupAddSamples.length < 5) dupAddSamples.push({ oid, existing: orders.get(oid), new: { side, price, qty }, msg: msgCount });
        }
      } else {
        updateCount++;
        if (!orders.has(oid)) {
          ghostUpdateCount++;
          if (ghostUpdateSamples.length < 5) ghostUpdateSamples.push({ oid, side, price, qty, msg: msgCount });
        }
      }
      orders.set(oid, { side, price, qty });
      break;
    }
    case MSG.ORDER_DELETED: {
      const oid = p.readBigUInt64LE(8).toString();
      if (!orders.has(oid)) {
        deleteNotInMap++;
        if (delNotInMapSamples.length < 5) delNotInMapSamples.push({ oid, msg: msgCount });
      }
      orders.delete(oid);
      deleteCount++;
      break;
    }
    case MSG.BOOK_CLEARED: {
      clearCount++;
      const clearSide = p.length > 8 ? p.readUInt8(8) : 0;
      if (clearSide === 0) {
        orders.clear();
      } else {
        const orderSide = clearSide - 1;
        for (const [oid, order] of orders) {
          if (order.side === orderSide) orders.delete(oid);
        }
      }
      break;
    }
  }
}

function computeMBP() {
  const bidMap = new Map(), askMap = new Map();
  for (const [, order] of orders) {
    const map = order.side === 0 ? bidMap : askMap;
    const existing = map.get(order.price);
    if (existing) { existing.qty += order.qty; existing.count++; }
    else map.set(order.price, { qty: order.qty, count: 1 });
  }
  const bids = [...bidMap.entries()].sort((a, b) => b[0] - a[0]);
  const asks = [...askMap.entries()].sort((a, b) => a[0] - b[0]);
  return {
    bidOrders: [...bidMap.values()].reduce((s, l) => s + l.count, 0),
    askOrders: [...askMap.values()].reduce((s, l) => s + l.count, 0),
    bidLevels: bids.length,
    askLevels: asks.length,
    bestBid: bids.length > 0 ? bids[0][0] : 0,
    bestAsk: asks.length > 0 ? asks[0][0] : 0,
  };
}

async function checkServer() {
  const local = computeMBP();
  const prefix = `[check] ${SYMBOL}`;
  try {
    const resp = await fetch(`${HTTP_BASE}/book/${SYMBOL}`);
    if (!resp.ok) {
      console.log(`${prefix} local=${formatLocalSummary(local)} | server-check unavailable (${resp.status})`);
      return;
    }
    const server = await resp.json();

    const match = local.bidOrders === server.bidOrders && local.askOrders === server.askOrders;
    const crossed = local.bestBid > 0 && local.bestAsk > 0 && local.bestBid >= local.bestAsk;

    const tag = match ? '✅ MATCH' : '❌ MISMATCH';
    console.log(`${tag} | local: ${local.bidOrders}b/${local.askOrders}a ${local.bidLevels}lv/${local.askLevels}lv bid=${local.bestBid} ask=${local.bestAsk} crossed=${crossed} | server: ${server.bidOrders}b/${server.askOrders}a bid=${server.bestBid} ask=${server.bestAsk} crossed=${server.crossed}`);
    if (!match) {
      console.log(`  Δ bids=${local.bidOrders - server.bidOrders} asks=${local.askOrders - server.askOrders} totalOrders=${orders.size} vs ${server.bidOrders + server.askOrders}`);
    }
    console.log(`  msgs=${msgCount} adds=${addCount} upd=${updateCount} dels=${deleteCount} snaps=${snapshotCount} clears=${clearCount} delNotInMap=${deleteNotInMap} dupAdd=${dupAddCount} ghostUpd=${ghostUpdateCount}`);
    if (dupAddSamples.length > 0 && !match) console.log(`  dupAdd samples:`, JSON.stringify(dupAddSamples.slice(0, 3)));
    if (ghostUpdateSamples.length > 0 && !match) console.log(`  ghostUpd samples:`, JSON.stringify(ghostUpdateSamples.slice(0, 3)));
    if (delNotInMapSamples.length > 0 && !match) console.log(`  delNotInMap samples:`, JSON.stringify(delNotInMapSamples.slice(0, 3)));
  } catch (e) {
    console.log(`${prefix} local=${formatLocalSummary(local)} | server-check failed: ${e.message}`);
  }
}

function formatLocalSummary(local) {
  const crossed = local.bestBid > 0 && local.bestAsk > 0 && local.bestBid >= local.bestAsk;
  return `${local.bidOrders}b/${local.askOrders}a ${local.bidLevels}lv/${local.askLevels}lv bid=${local.bestBid} ask=${local.bestAsk} crossed=${crossed}`;
}

function printSummary(reason) {
  console.log(`[summary] ${reason} | symbol=${SYMBOL} securityId=${securityId ?? '?'} socketError=${socketError ?? 'none'}`);
  console.log(`[summary] local=${formatLocalSummary(computeMBP())} orders=${orders.size} msgs=${msgCount} adds=${addCount} upd=${updateCount} dels=${deleteCount} snaps=${snapshotCount} clears=${clearCount} delNotInMap=${deleteNotInMap} dupAdd=${dupAddCount} ghostUpd=${ghostUpdateCount}`);
}

function requestShutdown(reason) {
  if (closing)
    return;

  closing = true;
  if (checkTimer) {
    clearInterval(checkTimer);
    checkTimer = null;
  }
  if (runTimer) {
    clearTimeout(runTimer);
    runTimer = null;
  }

  console.log(`Shutdown requested (${reason})`);
  ws.close();
  setTimeout(() => {
    if (ws.readyState !== WebSocket.CLOSED)
      ws.terminate();
  }, 1000).unref();
}

function sendSubscribe(reason) {
  if (subscribeSent || ws.readyState !== WebSocket.OPEN)
    return;

  subscribeSent = true;
  console.log(`Sending subscribe (${reason}) symbol=${SYMBOL}`);
  ws.send(buildSubscribe(SYMBOL));
}

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function parseNonNegativeInt(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : fallback;
}

// Connect
const ws = new WebSocket(WS_URL);
ws.binaryType = 'arraybuffer';

ws.on('open', () => {
  console.log(`Connected to ${WS_URL}`);
  setTimeout(() => sendSubscribe('open-delay'), 250).unref();
  checkTimer = setInterval(checkServer, CHECK_INTERVAL);
  if (RUN_SECONDS > 0) {
    runTimer = setTimeout(() => {
      console.log(`RUN_SECONDS=${RUN_SECONDS} reached; requesting graceful shutdown.`);
      requestShutdown('timer');
    }, RUN_SECONDS * 1000);
  }
});

ws.on('message', (data) => {
  processFrame(Buffer.from(data));
});

ws.on('close', () => {
  printSummary('socket closed');
  console.log('Disconnected');
  process.exit(socketError ? 1 : 0);
});
ws.on('error', (e) => {
  socketError = e.message;
  console.log('WS error:', e.message);
});

process.on('SIGINT', () => requestShutdown('SIGINT'));
process.on('SIGTERM', () => requestShutdown('SIGTERM'));
