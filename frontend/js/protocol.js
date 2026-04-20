// Wire protocol constants, message builders, and parser.
// Pure module — no DOM or state dependencies.

export const MSG = {
  SUBSCRIBE: 0x0001, UNSUBSCRIBE: 0x0002, GET: 0x0003,
  SUBSCRIBE_OK: 0x0010, SUBSCRIBE_ERROR: 0x0011, UNSUBSCRIBED: 0x0012,
  BOOK_SNAPSHOT: 0x0020, INFO_SNAPSHOT: 0x0021,
  ORDER_ADDED: 0x0030, ORDER_UPDATED: 0x0031, ORDER_DELETED: 0x0032,
  TRADE: 0x0033, BOOK_CLEARED: 0x0034,
  RANKINGS_UPDATE: 0x0040,
  SERVER_STATUS: 0x0050,
  CANDLE_SNAPSHOT: 0x0060,
  CANDLE_UPDATE: 0x0061,
};

export const MSG_NAMES = Object.fromEntries(Object.entries(MSG).map(([k, v]) => [v, k]));

export const DATA_FLAGS = { BOOK: 0x01, INFO: 0x02, ALL: 0x03 };
export const CANDLE_FLAGS = { FIRST: 0x01, LAST: 0x02 };

export const INFO_FIELDS = [
  'OpeningPrice', 'ClosingPrice', 'HighPrice', 'LowPrice',
  'LastTradePrice', 'LastTradeSize', 'SettlementPrice', 'TheoreticalOpeningPrice',
  'TheoreticalOpeningSize', 'AuctionImbalanceSize', 'TradeVolume', 'VwapPrice',
  'NetChange', 'NumberOfTrades', 'OpenInterest', 'PriceBandLow',
  'PriceBandHigh', 'TradingReferencePrice', 'AvgDailyTradedQty', 'MaxTradeVol',
  'TradingStatus', 'TradingEvent',
];

// Decimal places per price field, derived from SBE schema exponents.
// Price/PriceOptional → -4 (4 decimals), Price8/PriceOffset8Optional/Fixed8 → -8 (8 decimals).
export const FIELD_DECIMALS = {
  OpeningPrice: 4,
  ClosingPrice: 8,
  HighPrice: 4,
  LowPrice: 4,
  LastTradePrice: 4,
  SettlementPrice: 4,
  TheoreticalOpeningPrice: 4,
  VwapPrice: 4,
  NetChange: 8,
  PriceBandLow: 4,
  PriceBandHigh: 4,
  TradingReferencePrice: 8,
};

export const PRICE_FIELDS = new Set(Object.keys(FIELD_DECIMALS));

export function flagsStr(f) {
  const parts = [];
  if (f & DATA_FLAGS.BOOK) parts.push('Book');
  if (f & DATA_FLAGS.INFO) parts.push('Info');
  return parts.join('+') || 'None';
}

// ── Message builders (client → server) ──

const encoder = new TextEncoder();

export function buildSubscribeOrGet(msgType, symbol, flags) {
  const symBytes = encoder.encode(symbol.toUpperCase().trim());
  const totalLen = 4 + 1 + 1 + symBytes.length;
  const buf = new ArrayBuffer(totalLen);
  const v = new DataView(buf);
  v.setUint16(0, totalLen, true);
  v.setUint16(2, msgType, true);
  v.setUint8(4, flags);
  v.setUint8(5, symBytes.length);
  new Uint8Array(buf, 6).set(symBytes);
  return buf;
}

export function buildUnsubscribe(securityId) {
  const buf = new ArrayBuffer(12);
  const v = new DataView(buf);
  v.setUint16(0, 12, true);
  v.setUint16(2, MSG.UNSUBSCRIBE, true);
  v.setBigUint64(4, securityId, true);
  return buf;
}

// ── Message parser (server → client) ──
// Zero-copy: parses directly from coalesced ArrayBuffer at given offset.

const decoder = new TextDecoder();

export function parseMessage(buf, baseOffset, msgLen) {
  if (msgLen < 4) return null;
  const v = new DataView(buf, baseOffset, msgLen);
  const type = v.getUint16(2, true);
  let o = 4;

  switch (type) {
    case MSG.SUBSCRIBE_OK: {
      const securityId = v.getBigUint64(o, true); o += 8;
      const flags = v.getUint8(o); o += 1;
      const sLen = v.getUint8(o); o += 1;
      const symbol = decoder.decode(new Uint8Array(buf, baseOffset + o, sLen));
      return { type: 'SubscribeOk', securityId, flags, symbol };
    }
    case MSG.SUBSCRIBE_ERROR: {
      const errorCode = v.getUint8(o); o += 1;
      const sLen = v.getUint8(o); o += 1;
      const symbol = decoder.decode(new Uint8Array(buf, baseOffset + o, sLen));
      const errorNames = { 1: 'UnknownSymbol', 2: 'NotReady' };
      return { type: 'SubscribeError', errorCode, errorName: errorNames[errorCode] || "Code " + errorCode, symbol };
    }
    case MSG.UNSUBSCRIBED: {
      const securityId = v.getBigUint64(o, true);
      return { type: 'Unsubscribed', securityId };
    }
    case MSG.BOOK_SNAPSHOT: {
      const securityId = v.getBigUint64(o, true);
      return { type: 'BookSnapshot', securityId };
    }
    case MSG.INFO_SNAPSHOT: {
      const securityId = v.getBigUint64(o, true); o += 8;
      const mask = v.getUint32(o, true); o += 4;
      const fields = {};
      for (let i = 0; i < INFO_FIELDS.length; i++) {
        if (mask & (1 << i)) {
          fields[INFO_FIELDS[i]] = Number(v.getBigInt64(o, true));
          o += 8;
        }
      }
      return { type: 'InfoSnapshot', securityId, fields };
    }
    case MSG.ORDER_ADDED:
    case MSG.ORDER_UPDATED: {
      const securityId = v.getBigUint64(o, true); o += 8;
      const orderId = v.getBigUint64(o, true); o += 8;
      const side = v.getUint8(o); o += 1;
      const price = Number(v.getBigInt64(o, true)); o += 8;
      const qty = Number(v.getBigInt64(o, true));
      const typeName = type === MSG.ORDER_ADDED ? 'OrderAdded' : 'OrderUpdated';
      return { type: typeName, securityId, orderId, side, price, qty };
    }
    case MSG.ORDER_DELETED: {
      const securityId = v.getBigUint64(o, true); o += 8;
      const orderId = v.getBigUint64(o, true); o += 8;
      const side = v.getUint8(o);
      return { type: 'OrderDeleted', securityId, orderId, side };
    }
    case MSG.TRADE: {
      const securityId = v.getBigUint64(o, true); o += 8;
      const price = Number(v.getBigInt64(o, true)); o += 8;
      const qty = Number(v.getBigInt64(o, true)); o += 8;
      const tradeId = Number(v.getBigInt64(o, true));
      return { type: 'Trade', securityId, price, qty, tradeId };
    }
    case MSG.BOOK_CLEARED: {
      const securityId = v.getBigUint64(o, true); o += 8;
      const side = o < msgLen ? v.getUint8(o) : 0;
      return { type: 'BookCleared', securityId, side };
    }
    case MSG.RANKINGS_UPDATE: {
      const categories = [];
      for (let c = 0; c < 3; c++) {
        const count = v.getUint8(o); o += 1;
        const entries = [];
        for (let i = 0; i < count; i++) {
          const securityId = v.getBigUint64(o, true); o += 8;
          const value = Number(v.getBigInt64(o, true)); o += 8;
          const sLen = v.getUint8(o); o += 1;
          const symbol = decoder.decode(new Uint8Array(buf, baseOffset + o, sLen)); o += sLen;
          entries.push({ securityId, value, symbol });
        }
        categories.push(entries);
      }
      return { type: 'RankingsUpdate', volume: categories[0], gainers: categories[1], losers: categories[2] };
    }
    case MSG.SERVER_STATUS: {
      const ready = v.getUint8(o) === 1;
      return { type: 'ServerStatus', ready };
    }
    case MSG.CANDLE_SNAPSHOT: {
      const securityId = v.getBigUint64(o, true); o += 8;
      const resolution = v.getUint16(o, true); o += 2;
      const flags = v.getUint8(o); o += 1;
      const count = v.getUint16(o, true); o += 2;
      const candles = [];
      for (let i = 0; i < count; i++) {
        const time = Number(v.getBigInt64(o, true)); o += 8;
        const open = Number(v.getBigInt64(o, true)); o += 8;
        const high = Number(v.getBigInt64(o, true)); o += 8;
        const low = Number(v.getBigInt64(o, true)); o += 8;
        const close = Number(v.getBigInt64(o, true)); o += 8;
        const volume = Number(v.getBigInt64(o, true)); o += 8;
        candles.push({ time, open, high, low, close, volume });
      }
      const isFirst = !!(flags & CANDLE_FLAGS.FIRST);
      const isLast = !!(flags & CANDLE_FLAGS.LAST);
      return { type: 'CandleSnapshot', securityId, resolution, candles, isFirst, isLast };
    }
    case MSG.CANDLE_UPDATE: {
      const securityId = v.getBigUint64(o, true); o += 8;
      const resolution = v.getUint16(o, true); o += 2;
      const time = Number(v.getBigInt64(o, true)); o += 8;
      const open = Number(v.getBigInt64(o, true)); o += 8;
      const high = Number(v.getBigInt64(o, true)); o += 8;
      const low = Number(v.getBigInt64(o, true)); o += 8;
      const close = Number(v.getBigInt64(o, true)); o += 8;
      const volume = Number(v.getBigInt64(o, true));
      return { type: 'CandleUpdate', securityId, resolution, candle: { time, open, high, low, close, volume } };
    }
    default:
      return null;
  }
}
