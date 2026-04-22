#!/usr/bin/env node
// WebSocket diagnostic CLI — subscribe & inspect every message from the server.
// Usage: node tools/ws-diag.mjs [url] [symbol] [--json] [--no-orders]
//
// Examples:
//   node tools/ws-diag.mjs                           # defaults: ws://localhost:8080/ws WINV25
//   node tools/ws-diag.mjs ws://host:8080/ws DOLV25  # custom url + symbol
//   node tools/ws-diag.mjs --no-orders                # suppress order-level noise
//   node tools/ws-diag.mjs --json                     # machine-readable output

import WebSocket from 'ws';
import { createInterface } from 'readline';

// ── Args ──
const args = process.argv.slice(2);
const flags = new Set(args.filter(a => a.startsWith('--')));
const positional = args.filter(a => !a.startsWith('--'));
const WS_URL = positional[0] || 'ws://localhost:8080/ws';
const SYMBOL = (positional[1] || 'WINV25').toUpperCase();
const JSON_MODE = flags.has('--json');
const SHOW_ORDERS = !flags.has('--no-orders');

// ── Protocol constants ──
const MSG = {
  SUBSCRIBE: 0x0001, UNSUBSCRIBE: 0x0002, GET: 0x0003,
  SUBSCRIBE_OK: 0x0010, SUBSCRIBE_ERROR: 0x0011, UNSUBSCRIBED: 0x0012,
  BOOK_SNAPSHOT: 0x0020, INFO_SNAPSHOT: 0x0021,
  ORDER_ADDED: 0x0030, ORDER_UPDATED: 0x0031, ORDER_DELETED: 0x0032,
  TRADE: 0x0033, BOOK_CLEARED: 0x0034,
  RANKINGS_UPDATE: 0x0040,
  SERVER_STATUS: 0x0050,
  CANDLE_SNAPSHOT: 0x0060, CANDLE_UPDATE: 0x0061,
};
const MSG_NAME = Object.fromEntries(Object.entries(MSG).map(([k, v]) => [v, k]));

const INFO_FIELDS = [
  'OpeningPrice', 'ClosingPrice', 'HighPrice', 'LowPrice',
  'LastTradePrice', 'LastTradeSize', 'SettlementPrice', 'TheoreticalOpeningPrice',
  'TheoreticalOpeningSize', 'AuctionImbalanceSize', 'TradeVolume', 'VwapPrice',
  'NetChange', 'NumberOfTrades', 'OpenInterest', 'PriceBandLow',
  'PriceBandHigh', 'TradingReferencePrice', 'AvgDailyTradedQty', 'MaxTradeVol',
  'TradingStatus', 'TradingEvent',
];

const PRICE_FIELDS = new Set([
  'OpeningPrice', 'ClosingPrice', 'HighPrice', 'LowPrice',
  'LastTradePrice', 'SettlementPrice', 'TheoreticalOpeningPrice',
  'VwapPrice', 'NetChange', 'PriceBandLow', 'PriceBandHigh', 'TradingReferencePrice',
]);

// ── State ──
let ws = null;
let serverReady = false;
let securityId = null;
let subscribedSymbol = null;
const counters = {
  frames: 0, messages: 0,
  subscribeOk: 0, bookSnapshot: 0, infoSnapshot: 0,
  orderAdded: 0, orderUpdated: 0, orderDeleted: 0,
  trade: 0, bookCleared: 0, candleSnapshot: 0, candleUpdate: 0,
  rankingsUpdate: 0, serverStatus: 0,
};
let candleTotalReceived = 0;

// ── Helpers ──
const ts = () => new Date().toISOString().slice(11, 23);
const fmtPrice = (v) => (Number(v) / 100).toFixed(2);

function log(tag, msg, data) {
  if (JSON_MODE) {
    console.log(JSON.stringify({ ts: ts(), tag, msg, ...data }));
  } else {
    const extra = data ? ' ' + JSON.stringify(data) : '';
    console.log(`[${ts()}] ${tag.padEnd(16)} ${msg}${extra}`);
  }
}

// ── Wire helpers ──
function buildSubscribe(symbol, msgType = MSG.SUBSCRIBE) {
  const sym = Buffer.from(symbol, 'utf8');
  const buf = Buffer.alloc(4 + 1 + 1 + sym.length);
  buf.writeUInt16LE(buf.length, 0);
  buf.writeUInt16LE(msgType, 2);
  buf.writeUInt8(0x03, 4); // flags ALL
  buf.writeUInt8(sym.length, 5);
  sym.copy(buf, 6);
  return buf;
}

function buildUnsubscribe(secId) {
  const buf = Buffer.alloc(12);
  buf.writeUInt16LE(12, 0);
  buf.writeUInt16LE(MSG.UNSUBSCRIBE, 2);
  buf.writeBigUInt64LE(secId, 4);
  return buf;
}

// ── Message parser ──
function parseMessage(buf, offset, len) {
  const type = buf.readUInt16LE(offset + 2);
  const p = offset + 4; // payload start
  const name = MSG_NAME[type] || `UNKNOWN_0x${type.toString(16).padStart(4, '0')}`;

  switch (type) {
    case MSG.SERVER_STATUS: {
      const ready = buf.readUInt8(p) === 1;
      return { name, ready };
    }
    case MSG.SUBSCRIBE_OK: {
      const secId = buf.readBigUInt64LE(p);
      const flags = buf.readUInt8(p + 8);
      const sLen = buf.readUInt8(p + 9);
      const symbol = buf.subarray(p + 10, p + 10 + sLen).toString('utf8');
      return { name, secId, flags, symbol };
    }
    case MSG.SUBSCRIBE_ERROR: {
      const code = buf.readUInt8(p);
      const sLen = buf.readUInt8(p + 1);
      const symbol = buf.subarray(p + 2, p + 2 + sLen).toString('utf8');
      const codes = { 1: 'UnknownSymbol', 2: 'NotReady' };
      return { name, code, error: codes[code] || `Code${code}`, symbol };
    }
    case MSG.UNSUBSCRIBED: {
      return { name, secId: buf.readBigUInt64LE(p) };
    }
    case MSG.BOOK_SNAPSHOT: {
      const secId = buf.readBigUInt64LE(p);
      const seqNum = buf.readUInt32LE(p + 8);
      const bidLevels = buf.readUInt16LE(p + 12);
      const askLevels = buf.readUInt16LE(p + 14);
      return { name, secId, seqNum, bidLevels, askLevels, totalBytes: len };
    }
    case MSG.INFO_SNAPSHOT: {
      const secId = buf.readBigUInt64LE(p);
      const mask = buf.readUInt32LE(p + 8);
      const fields = {};
      let o = p + 12;
      for (let i = 0; i < INFO_FIELDS.length; i++) {
        if (mask & (1 << i)) {
          const raw = buf.readBigInt64LE(o); o += 8;
          const fname = INFO_FIELDS[i];
          fields[fname] = PRICE_FIELDS.has(fname) ? fmtPrice(raw) : Number(raw);
        }
      }
      return { name, secId, fieldCount: Object.keys(fields).length, fields };
    }
    case MSG.ORDER_ADDED:
    case MSG.ORDER_UPDATED: {
      const secId = buf.readBigUInt64LE(p);
      const orderId = buf.readBigUInt64LE(p + 8);
      const side = buf.readUInt8(p + 16) === 0 ? 'BID' : 'ASK';
      const price = fmtPrice(buf.readBigInt64LE(p + 17));
      const qty = Number(buf.readBigInt64LE(p + 25));
      return { name, secId, orderId, side, price, qty };
    }
    case MSG.ORDER_DELETED: {
      const secId = buf.readBigUInt64LE(p);
      const orderId = buf.readBigUInt64LE(p + 8);
      const side = buf.readUInt8(p + 16) === 0 ? 'BID' : 'ASK';
      return { name, secId, orderId, side };
    }
    case MSG.TRADE: {
      const secId = buf.readBigUInt64LE(p);
      const price = fmtPrice(buf.readBigInt64LE(p + 8));
      const qty = Number(buf.readBigInt64LE(p + 16));
      const tradeId = buf.readBigUInt64LE(p + 24);
      return { name, secId, price, qty, tradeId };
    }
    case MSG.BOOK_CLEARED: {
      const secId = buf.readBigUInt64LE(p);
      const side = buf.readUInt8(p + 8);
      const sideStr = side === 0 ? 'BOTH' : side === 1 ? 'BID' : 'ASK';
      return { name, secId, side: sideStr };
    }
    case MSG.CANDLE_SNAPSHOT: {
      const secId = buf.readBigUInt64LE(p);
      const resolution = buf.readUInt16LE(p + 8);
      const flags = buf.readUInt8(p + 10);
      const count = buf.readUInt16LE(p + 11);
      const isFirst = !!(flags & 0x01);
      const candles = [];
      let o = p + 13;
      for (let i = 0; i < count && i < 3; i++) {
        candles.push({
          time: Number(buf.readBigInt64LE(o)),
          open: fmtPrice(buf.readBigInt64LE(o + 8)),
          high: fmtPrice(buf.readBigInt64LE(o + 16)),
          low: fmtPrice(buf.readBigInt64LE(o + 24)),
          close: fmtPrice(buf.readBigInt64LE(o + 32)),
          volume: Number(buf.readBigInt64LE(o + 40)),
        });
        o += 48;
      }
      return { name, secId, resolution, flags, isFirst, count, totalBytes: len, preview: candles };
    }
    case MSG.CANDLE_UPDATE: {
      const secId = buf.readBigUInt64LE(p);
      const resolution = buf.readUInt16LE(p + 8);
      const time = Number(buf.readBigInt64LE(p + 10));
      const open = fmtPrice(buf.readBigInt64LE(p + 18));
      const high = fmtPrice(buf.readBigInt64LE(p + 26));
      const low = fmtPrice(buf.readBigInt64LE(p + 34));
      const close = fmtPrice(buf.readBigInt64LE(p + 42));
      const volume = Number(buf.readBigInt64LE(p + 50));
      return { name, secId, resolution, time, open, high, low, close, volume };
    }
    case MSG.RANKINGS_UPDATE: {
      return { name, totalBytes: len };
    }
    default:
      return { name, totalBytes: len };
  }
}

// ── Frame processor ──
function processFrame(data) {
  const buf = Buffer.from(data);
  counters.frames++;
  let offset = 0;

  while (offset + 4 <= buf.length) {
    const len = buf.readUInt16LE(offset);
    if (len < 4 || offset + len > buf.length) {
      log('WARN', `bad framing at offset ${offset}, len=${len}, remaining=${buf.length - offset}`);
      break;
    }

    const msg = parseMessage(buf, offset, len);
    counters.messages++;
    offset += len;

    // Update counters
    switch (msg.name) {
      case 'SERVER_STATUS':
        counters.serverStatus++;
        serverReady = msg.ready;
        log('STATUS', serverReady ? '🟢 Server ready' : '🔴 Server initializing');
        break;

      case 'SUBSCRIBE_OK':
        counters.subscribeOk++;
        securityId = msg.secId;
        subscribedSymbol = msg.symbol;
        log('SUB_OK', `${msg.symbol} secId=${msg.secId} flags=${msg.flags}`);
        break;

      case 'SUBSCRIBE_ERROR':
        log('SUB_ERR', `${msg.symbol}: ${msg.error}`);
        break;

      case 'UNSUBSCRIBED':
        log('UNSUB', `secId=${msg.secId}`);
        break;

      case 'BOOK_SNAPSHOT':
        counters.bookSnapshot++;
        log('BOOK_SNAP', `secId=${msg.secId} seq=${msg.seqNum} bids=${msg.bidLevels} asks=${msg.askLevels} (${msg.totalBytes}B)`);
        break;

      case 'INFO_SNAPSHOT':
        counters.infoSnapshot++;
        log('INFO_SNAP', `${msg.fieldCount} fields`, msg.fields);
        break;

      case 'ORDER_ADDED':
        counters.orderAdded++;
        if (SHOW_ORDERS) log('ORDER_ADD', `${msg.side} ${msg.price} x${msg.qty} oid=${msg.orderId}`);
        break;

      case 'ORDER_UPDATED':
        counters.orderUpdated++;
        if (SHOW_ORDERS) log('ORDER_UPD', `${msg.side} ${msg.price} x${msg.qty} oid=${msg.orderId}`);
        break;

      case 'ORDER_DELETED':
        counters.orderDeleted++;
        if (SHOW_ORDERS) log('ORDER_DEL', `${msg.side} oid=${msg.orderId}`);
        break;

      case 'TRADE':
        counters.trade++;
        log('TRADE', `${msg.price} x${msg.qty} tid=${msg.tradeId}`);
        break;

      case 'BOOK_CLEARED':
        counters.bookCleared++;
        log('BOOK_CLR', `side=${msg.side}`);
        break;

      case 'CANDLE_SNAPSHOT':
        counters.candleSnapshot++;
        candleTotalReceived += msg.count;
        log('CANDLE_SNAP', `🕯️  res=${msg.resolution}s count=${msg.count} first=${msg.isFirst} total=${candleTotalReceived} (${msg.totalBytes}B)`, { preview: msg.preview });
        break;

      case 'CANDLE_UPDATE':
        counters.candleUpdate++;
        log('CANDLE_UPD', `🕯️  res=${msg.resolution}s t=${msg.time} O=${msg.open} H=${msg.high} L=${msg.low} C=${msg.close} V=${msg.volume}`);
        break;

      case 'RANKINGS_UPDATE':
        counters.rankingsUpdate++;
        break;

      default:
        log('UNKNOWN', msg.name, { totalBytes: msg.totalBytes });
    }
  }
}

// ── Summary ──
function printSummary() {
  console.log('\n── Summary ──');
  console.log(`  Frames: ${counters.frames}  Messages: ${counters.messages}`);
  console.log(`  SubscribeOk: ${counters.subscribeOk}  BookSnapshot: ${counters.bookSnapshot}  InfoSnapshot: ${counters.infoSnapshot}`);
  console.log(`  Orders: +${counters.orderAdded} ~${counters.orderUpdated} -${counters.orderDeleted}  Cleared: ${counters.bookCleared}`);
  console.log(`  Trades: ${counters.trade}`);
  console.log(`  CandleSnapshot: ${counters.candleSnapshot} (${candleTotalReceived} candles)  CandleUpdate: ${counters.candleUpdate}`);
  console.log(`  Rankings: ${counters.rankingsUpdate}  Status: ${counters.serverStatus}`);
}

// ── Connect ──
function connect() {
  log('CONNECT', `${WS_URL} (symbol=${SYMBOL})`);
  ws = new WebSocket(WS_URL);
  ws.binaryType = 'arraybuffer';

  ws.on('open', () => {
    log('OPEN', 'WebSocket connected');
    // Auto-subscribe after short delay (wait for ServerStatus)
    setTimeout(() => {
      log('SEND', `SUBSCRIBE ${SYMBOL} flags=ALL`);
      ws.send(buildSubscribe(SYMBOL));
    }, 200);
  });

  ws.on('message', processFrame);

  ws.on('close', (code, reason) => {
    log('CLOSE', `code=${code} reason=${reason || 'none'}`);
    printSummary();
  });

  ws.on('error', (e) => log('ERROR', e.message));
}

// ── Interactive commands ──
function startRepl() {
  const rl = createInterface({ input: process.stdin, output: process.stdout, prompt: '' });

  rl.on('line', (line) => {
    const parts = line.trim().split(/\s+/);
    const cmd = parts[0]?.toLowerCase();

    switch (cmd) {
      case 'sub':
      case 'subscribe': {
        const sym = (parts[1] || SYMBOL).toUpperCase();
        log('SEND', `SUBSCRIBE ${sym}`);
        ws.send(buildSubscribe(sym));
        break;
      }
      case 'get': {
        const sym = (parts[1] || subscribedSymbol || SYMBOL).toUpperCase();
        log('SEND', `GET ${sym}`);
        ws.send(buildSubscribe(sym, MSG.GET));
        break;
      }
      case 'unsub':
      case 'unsubscribe': {
        if (securityId) {
          log('SEND', `UNSUBSCRIBE secId=${securityId}`);
          ws.send(buildUnsubscribe(securityId));
        } else {
          log('WARN', 'No active subscription');
        }
        break;
      }
      case 'resub':
      case 'resubscribe': {
        const sym = subscribedSymbol || SYMBOL;
        if (securityId) {
          log('SEND', `UNSUBSCRIBE secId=${securityId}`);
          ws.send(buildUnsubscribe(securityId));
        }
        setTimeout(() => {
          log('SEND', `SUBSCRIBE ${sym}`);
          ws.send(buildSubscribe(sym));
        }, 100);
        break;
      }
      case 'stats':
      case 'summary':
        printSummary();
        break;
      case 'quit':
      case 'exit':
        ws.close();
        setTimeout(() => process.exit(0), 200);
        break;
      case 'help':
      case '?':
        console.log('Commands: sub [SYM], get [SYM], unsub, resub, stats, quit');
        break;
      default:
        if (cmd) console.log(`Unknown command: ${cmd}. Type "help" for commands.`);
    }
  });

  rl.on('close', () => {
    ws.close();
    setTimeout(() => process.exit(0), 200);
  });
}

connect();
startRepl();

// Graceful shutdown
process.on('SIGINT', () => {
  printSummary();
  ws?.close();
  setTimeout(() => process.exit(0), 300);
});
