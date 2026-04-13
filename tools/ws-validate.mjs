// WS client that mimics frontend MBO logic, compares with server /book endpoint
import WebSocket from 'ws';

const WS_URL = process.argv[2] || 'ws://localhost:8081/ws';
const HTTP_BASE = WS_URL.replace(/^ws(s?):\/\//, 'http$1://').replace(/\/ws\/?$/, '');
const SYMBOL = process.argv[3] || 'WINZ25';
const CHECK_INTERVAL = 5000;

const MSG = {
  SUBSCRIBE: 0x0001, SUBSCRIBE_OK: 0x0010, SUBSCRIBE_ERROR: 0x0011,
  BOOK_SNAPSHOT: 0x0020, ORDER_ADDED: 0x0030, ORDER_UPDATED: 0x0031,
  ORDER_DELETED: 0x0032, TRADE: 0x0033, BOOK_CLEARED: 0x0034,
  INFO_SNAPSHOT: 0x0021, UNSUBSCRIBED: 0x0012,
};

const orders = new Map();
let securityId = null;
let msgCount = 0, addCount = 0, updateCount = 0, deleteCount = 0, snapshotCount = 0, clearCount = 0;
let deleteNotInMap = 0;

function buildSubscribe(symbol) {
  const symBytes = Buffer.from(symbol, 'utf8');
  const buf = Buffer.alloc(4 + 1 + 1 + symBytes.length);
  buf.writeUInt16LE(buf.length, 0);
  buf.writeUInt16LE(MSG.SUBSCRIBE, 2);
  buf.writeUInt8(0x03, 4); // flags: Book+Info
  buf.writeUInt8(symBytes.length, 5);
  symBytes.copy(buf, 6);
  return buf;
}

function processFrame(data) {
  let offset = 0;
  while (offset + 4 <= data.length) {
    const len = data.readUInt16LE(offset);
    if (len < 4 || offset + len > data.length) break;
    const type = data.readUInt16LE(offset + 2);
    const payload = data.subarray(offset + 4, offset + len);
    processMessage(type, payload);
    offset += len;
  }
}

function processMessage(type, p) {
  msgCount++;
  switch (type) {
    case MSG.SUBSCRIBE_OK: {
      securityId = p.readBigUInt64LE(0);
      const flags = p.readUInt8(8);
      const sLen = p.readUInt8(9);
      const sym = p.subarray(10, 10 + sLen).toString('utf8');
      console.log(`SubscribeOk: ${sym} id=${securityId} flags=${flags}`);
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
      const side = p.readUInt8(16);
      const price = Number(p.readBigInt64LE(17));
      const qty = Number(p.readBigInt64LE(25));
      orders.set(oid, { side, price, qty });
      if (type === MSG.ORDER_ADDED) addCount++;
      else updateCount++;
      break;
    }
    case MSG.ORDER_DELETED: {
      const oid = p.readBigUInt64LE(8).toString();
      if (!orders.has(oid)) deleteNotInMap++;
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
  try {
    const resp = await fetch(`${HTTP_BASE}/book/${SYMBOL}`);
    if (!resp.ok) { console.log('Server /book not ready'); return; }
    const server = await resp.json();
    const local = computeMBP();

    const match = local.bidOrders === server.bidOrders && local.askOrders === server.askOrders;
    const crossed = local.bestBid > 0 && local.bestAsk > 0 && local.bestBid >= local.bestAsk;

    const tag = match ? '✅ MATCH' : '❌ MISMATCH';
    console.log(`${tag} | local: ${local.bidOrders}b/${local.askOrders}a ${local.bidLevels}lv/${local.askLevels}lv bid=${local.bestBid} ask=${local.bestAsk} crossed=${crossed} | server: ${server.bidOrders}b/${server.askOrders}a bid=${server.bestBid} ask=${server.bestAsk} crossed=${server.crossed}`);
    if (!match) {
      console.log(`  Δ bids=${local.bidOrders - server.bidOrders} asks=${local.askOrders - server.askOrders} totalOrders=${orders.size} vs ${server.bidOrders + server.askOrders}`);
    }
    console.log(`  msgs=${msgCount} adds=${addCount} upd=${updateCount} dels=${deleteCount} snaps=${snapshotCount} clears=${clearCount} delNotInMap=${deleteNotInMap}`);
  } catch (e) {
    console.log('Server check failed:', e.message);
  }
}

// Connect
const ws = new WebSocket(WS_URL);
ws.binaryType = 'arraybuffer';

ws.on('open', () => {
  console.log(`Connected to ${WS_URL}`);
  ws.send(buildSubscribe(SYMBOL));
  setInterval(checkServer, CHECK_INTERVAL);
});

ws.on('message', (data) => {
  processFrame(Buffer.from(data));
});

ws.on('close', () => { console.log('Disconnected'); process.exit(0); });
ws.on('error', (e) => console.log('WS error:', e.message));
