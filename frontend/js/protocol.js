// Wire protocol constants, message builders, and parser.
// Pure module — no DOM or state dependencies.

export const MSG = {
  SUBSCRIBE: 0x0001, UNSUBSCRIBE: 0x0002, GET: 0x0003,
  SUBSCRIBE_OK: 0x0010, SUBSCRIBE_ERROR: 0x0011, UNSUBSCRIBED: 0x0012,
  BOOK_SNAPSHOT: 0x0020, INFO_SNAPSHOT: 0x0021,
  LEVEL_SNAPSHOT: 0x0022,
  ORDER_ADDED: 0x0030, ORDER_UPDATED: 0x0031, ORDER_DELETED: 0x0032,
  TRADE: 0x0033, BOOK_CLEARED: 0x0034,
  TRADE_BUST: 0x0035, MARKET_TIER_UPDATE: 0x0036,
  LEVEL_UPDATE: 0x0037, LEVEL_DELETED: 0x0038,
  RANKINGS_UPDATE: 0x0040,
  SERVER_STATUS: 0x0050,
  CANDLE_SNAPSHOT: 0x0060,
  CANDLE_UPDATE: 0x0061,
  SYMBOL_STALE_STATUS: 0x0070,
  RECOVERY_PROGRESS: 0x0080,
  SYMBOL_DELISTED: 0x0071,
  SERVER_HELLO: 0x00A0,
};

// Bitfield positions for ServerHello.capabilities. Mirror of the server's
// `ServerCapabilities` enum. Unknown bits MUST be ignored — never gate behaviour
// on the absence of a known bit (forward compatibility).
export const SERVER_CAPABILITIES = {
  SNAPSHOT_ON_SUBSCRIBE: 0x0001,
  SYMBOL_DELISTED_NOTIFICATION: 0x0002,
};

export const MSG_NAMES = Object.fromEntries(Object.entries(MSG).map(([k, v]) => [v, k]));

export const DATA_FLAGS = { BOOK: 0x01, INFO: 0x02, NEWS: 0x04, MBP: 0x08, TRADES: 0x10, ALL: 0x1F };
export const CANDLE_FLAGS = { FIRST: 0x01, LAST: 0x02 };

export const INFO_FIELDS = [
  'OpeningPrice', 'ClosingPrice', 'HighPrice', 'LowPrice',
  'LastTradePrice', 'LastTradeSize', 'SettlementPrice', 'TheoreticalOpeningPrice',
  'TheoreticalOpeningSize', 'AuctionImbalanceSize', 'TradeVolume', 'VwapPrice',
  'NetChange', 'NumberOfTrades', 'OpenInterest', 'PriceBandLow',
  'PriceBandHigh', 'TradingReferencePrice', 'AvgDailyTradedQty', 'MaxTradeVol',
  'TradingStatus', 'TradingEvent', 'PriceLimitType', 'MinPriceIncrement',
  'AuctionImbalanceCondition',
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
  if (f & DATA_FLAGS.NEWS) parts.push('News');
  if (f & DATA_FLAGS.MBP) parts.push('Mbp');
  if (f & DATA_FLAGS.TRADES) parts.push('Trades');
  return parts.join('+') || 'None';
}

// ── Message builders (client → server) ──

const encoder = new TextEncoder();

export function buildSubscribeOrGet(msgType, symbol, flags) {
  const symBytes = encoder.encode(symbol.toUpperCase().trim());
  // v2: [len u32][type u16][headerFlags u16][flags u32][symLen u8][symbol]
  const totalLen = 8 + 4 + 1 + symBytes.length;
  const buf = new ArrayBuffer(totalLen);
  const v = new DataView(buf);
  v.setUint32(0, totalLen, true);
  v.setUint16(4, msgType, true);
  v.setUint32(8, flags >>> 0, true);
  v.setUint8(12, symBytes.length);
  new Uint8Array(buf, 13).set(symBytes);
  return buf;
}

export function buildUnsubscribe(securityId) {
  const buf = new ArrayBuffer(16);
  const v = new DataView(buf);
  v.setUint32(0, 16, true);
  v.setUint16(4, MSG.UNSUBSCRIBE, true);
  v.setBigUint64(8, securityId, true);
  return buf;
}

// ── Message parser (server → client) ──
// Zero-copy: parses directly from coalesced ArrayBuffer at given offset.

const decoder = new TextDecoder();

export function parseMessage(buf, baseOffset, msgLen) {
  if (msgLen < 8) return null;
  const v = new DataView(buf, baseOffset, msgLen);
  const type = v.getUint16(4, true);
  let o = 8;

  switch (type) {
    case MSG.SUBSCRIBE_OK: {
      const securityId = v.getBigUint64(o, true); o += 8;
      const flags = v.getUint32(o, true); o += 4;
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
      // v2 layout: secId, orderId, price, qty, side (side moved to the end).
      const securityId = v.getBigUint64(o, true); o += 8;
      const orderId = v.getBigUint64(o, true); o += 8;
      const price = Number(v.getBigInt64(o, true)); o += 8;
      const qty = Number(v.getBigInt64(o, true)); o += 8;
      const side = v.getUint8(o);
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
      const tradeId = Number(v.getBigInt64(o, true)); o += 8;
      // Trailing flags byte. Min-length rule: tolerate a shorter frame that
      // omits it by treating absence as 0 (no flags).
      const flags = o < msgLen ? v.getUint8(o) : 0;
      const auctionPrint = (flags & 0x01) !== 0;
      return { type: 'Trade', securityId, price, qty, tradeId, flags, auctionPrint };
    }
    case MSG.BOOK_CLEARED: {
      const securityId = v.getBigUint64(o, true); o += 8;
      const side = o < msgLen ? v.getUint8(o) : 0;
      return { type: 'BookCleared', securityId, side };
    }
    case MSG.MARKET_TIER_UPDATE: {
      // v2 layout: secId, totalQty, orderCount(u32), side(u8).
      const securityId = v.getBigUint64(o, true); o += 8;
      const qty = Number(v.getBigInt64(o, true)); o += 8;
      const count = v.getUint32(o, true); o += 4;
      const side = v.getUint8(o);
      return { type: 'MarketTierUpdate', securityId, side, qty, count };
    }
    case MSG.LEVEL_SNAPSHOT: {
      // Format: securityId(8) + bidCount(2) + askCount(2) + bid entries + ask entries
      // Each entry: price(8) + totalQty(8) + orderCount(4) = 20 bytes.
      const securityId = v.getBigUint64(o, true); o += 8;
      const bidCount = v.getUint16(o, true); o += 2;
      const askCount = v.getUint16(o, true); o += 2;
      const bids = new Array(bidCount);
      for (let i = 0; i < bidCount; i++) {
        const price = Number(v.getBigInt64(o, true)); o += 8;
        const qty = Number(v.getBigInt64(o, true)); o += 8;
        const count = v.getUint32(o, true); o += 4;
        bids[i] = { price, qty, count };
      }
      const asks = new Array(askCount);
      for (let i = 0; i < askCount; i++) {
        const price = Number(v.getBigInt64(o, true)); o += 8;
        const qty = Number(v.getBigInt64(o, true)); o += 8;
        const count = v.getUint32(o, true); o += 4;
        asks[i] = { price, qty, count };
      }
      return { type: 'LevelSnapshot', securityId, bids, asks };
    }
    case MSG.LEVEL_UPDATE: {
      // v2 layout: secId, price, totalQty, orderCount(u32), side(u8).
      const securityId = v.getBigUint64(o, true); o += 8;
      const price = Number(v.getBigInt64(o, true)); o += 8;
      const qty = Number(v.getBigInt64(o, true)); o += 8;
      const count = v.getUint32(o, true); o += 4;
      const side = v.getUint8(o);
      return { type: 'LevelUpdate', securityId, side, price, qty, count };
    }
    case MSG.LEVEL_DELETED: {
      // v2 layout: secId, price, side(u8).
      const securityId = v.getBigUint64(o, true); o += 8;
      const price = Number(v.getBigInt64(o, true)); o += 8;
      const side = v.getUint8(o);
      return { type: 'LevelDeleted', securityId, side, price };
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
        const avg = Number(v.getBigInt64(o, true)); o += 8;
        candles.push({ time, open, high, low, close, volume, avg });
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
      const volume = Number(v.getBigInt64(o, true)); o += 8;
      const avg = Number(v.getBigInt64(o, true));
      return { type: 'CandleUpdate', securityId, resolution, candle: { time, open, high, low, close, volume, avg } };
    }
    case MSG.SYMBOL_STALE_STATUS: {
      const securityId = v.getBigUint64(o, true); o += 8;
      const isStale = v.getUint8(o) === 1;
      return { type: 'SymbolStaleStatus', securityId, isStale };
    }
    case MSG.RECOVERY_PROGRESS: {
      const totalSymbols = v.getUint32(o, true); o += 4;
      const totalStaleSymbols = v.getUint32(o, true); o += 4;
      const kindCount = v.getUint8(o); o += 1;
      const staleByKind = {};
      for (let i = 0; i < kindCount; i++) {
        const kindId = v.getUint8(o); o += 1;
        const count = v.getUint32(o, true); o += 4;
        staleByKind[kindId] = count;
      }
      return { type: 'RecoveryProgress', totalSymbols, totalStaleSymbols, staleByKind };
    }
    case MSG.SERVER_HELLO: {
      const protocolVersion = v.getUint32(o, true); o += 4;
      const capabilities = v.getUint32(o, true); o += 4;
      const buildLen = v.getUint8(o); o += 1;
      const buildVersion = decoder.decode(new Uint8Array(buf, baseOffset + o, buildLen));
      return { type: 'ServerHello', protocolVersion, capabilities, buildVersion };
    }
    case MSG.SYMBOL_DELISTED: {
      const securityId = v.getBigUint64(o, true);
      return { type: 'SymbolDelisted', securityId };
    }
    default:
      return null;
  }
}
