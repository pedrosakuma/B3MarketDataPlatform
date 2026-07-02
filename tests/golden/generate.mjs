// Standalone golden-vector generator for the v2 wire protocol.
//
// This encoder is intentionally INDEPENDENT of the production decoders
// (frontend/js/protocol.js and the C# WireFormat/WireProtocol). It writes the
// bytes by hand from the normative v2 layout so that both decoders can be
// tested against a common, committed set of fixtures.
//
// Run:  node tests/golden/generate.mjs > tests/golden/wire-v2-vectors.json
//
// Header (8 bytes): [u32 length][u16 type][u16 headerFlags=0], little-endian.
// Payload begins at offset 8. Hot fixed frames are largest-first (side/orderCount
// moved to the end); cold/variable frames keep their historical field order.

import { Buffer } from 'node:buffer';

const MSG = {
  SUBSCRIBE_OK: 0x0010,
  ORDER_ADDED: 0x0030,
  TRADE: 0x0033,
  MARKET_TIER_UPDATE: 0x0036,
  LEVEL_UPDATE: 0x0037,
  LEVEL_DELETED: 0x0038,
  SERVER_STATUS: 0x0050,
  INFO_SNAPSHOT: 0x0021,
  SERVER_HELLO: 0x00A0,
};

// A small growable writer that always prefixes the 8-byte v2 header on finish.
class FrameWriter {
  constructor(type) {
    this.type = type;
    this.bytes = [];
  }
  u8(x) { this.bytes.push(x & 0xff); }
  u16(x) { this.u8(x); this.u8(x >>> 8); }
  u32(x) { this.u16(x & 0xffff); this.u16((x >>> 16) & 0xffff); }
  i64(x) {
    let v = BigInt.asUintN(64, BigInt(x));
    for (let i = 0; i < 8; i++) { this.u8(Number(v & 0xffn)); v >>= 8n; }
  }
  u64(x) { this.i64(x); }
  str(s) {
    const b = Buffer.from(s, 'utf8');
    this.u8(b.length);
    for (const c of b) this.u8(c);
  }
  finish(extraTrailing = 0) {
    const payload = this.bytes;
    const total = 8 + payload.length + extraTrailing;
    const buf = Buffer.alloc(total);
    buf.writeUInt32LE(total, 0);
    buf.writeUInt16LE(this.type, 4);
    buf.writeUInt16LE(0, 6); // headerFlags
    Buffer.from(payload).copy(buf, 8);
    // extraTrailing bytes remain zero — exercises the min-length skip rule.
    return buf.toString('hex');
  }
}

const vectors = [];
function add(name, type, hex, expected) {
  vectors.push({ name, type, hex, expected });
}

// SubscribeOk: [secId u64][flags u32][symLen u8][symbol]
{
  const w = new FrameWriter(MSG.SUBSCRIBE_OK);
  w.u64(12345n); w.u32(0x1f); w.str('PETR4');
  add('SubscribeOk', 'SubscribeOk', w.finish(),
    { securityId: '12345', flags: 0x1f, symbol: 'PETR4' });
}

// OrderAdded (hot, reordered): [secId][orderId][price][qty][side u8]
{
  const w = new FrameWriter(MSG.ORDER_ADDED);
  w.u64(99n); w.u64(42n); w.i64(10000); w.i64(500); w.u8(1);
  add('OrderAdded', 'OrderAdded', w.finish(),
    { securityId: '99', orderId: '42', price: 10000, qty: 500, side: 1 });
}

// Trade (hot): [secId][price][qty][tradeId][flags u8]
{
  const w = new FrameWriter(MSG.TRADE);
  w.u64(1001n); w.i64(55000); w.i64(100); w.i64(9876); w.u8(0x01);
  add('Trade', 'Trade', w.finish(),
    { securityId: '1001', price: 55000, qty: 100, tradeId: 9876, flags: 0x01, auctionPrint: true });
}

// Trade with EXTRA trailing bytes — min-length rule: decoder ignores the tail.
{
  const w = new FrameWriter(MSG.TRADE);
  w.u64(2002n); w.i64(60000); w.i64(200); w.i64(1234); w.u8(0x00);
  add('TradeForwardCompatTrailing', 'Trade', w.finish(16),
    { securityId: '2002', price: 60000, qty: 200, tradeId: 1234, flags: 0x00, auctionPrint: false });
}

// MarketTierUpdate (hot, reordered): [secId][totalQty][orderCount u32][side u8]
{
  const w = new FrameWriter(MSG.MARKET_TIER_UPDATE);
  w.u64(501n); w.i64(12345); w.u32(7); w.u8(0);
  add('MarketTierUpdate', 'MarketTierUpdate', w.finish(),
    { securityId: '501', qty: 12345, count: 7, side: 0 });
}

// LevelUpdate (hot, reordered): [secId][price][totalQty][orderCount u32][side u8]
{
  const w = new FrameWriter(MSG.LEVEL_UPDATE);
  w.u64(42n); w.i64(1234567); w.i64(9876); w.u32(2); w.u8(1);
  add('LevelUpdate', 'LevelUpdate', w.finish(),
    { securityId: '42', price: 1234567, qty: 9876, count: 2, side: 1 });
}

// LevelDeleted (hot, reordered): [secId][price][side u8]
{
  const w = new FrameWriter(MSG.LEVEL_DELETED);
  w.u64(99n); w.i64(-5); w.u8(0);
  add('LevelDeleted', 'LevelDeleted', w.finish(),
    { securityId: '99', price: -5, side: 0 });
}

// ServerStatus: [ready u8]
{
  const w = new FrameWriter(MSG.SERVER_STATUS);
  w.u8(1);
  add('ServerStatus', 'ServerStatus', w.finish(), { ready: true });
}

// InfoSnapshot (cold, kept order): [secId][mask u32][i64 values in bit order]
// Bits: OpeningPrice(0)=10000, HighPrice(2)=12000, LowPrice(3)=9500.
{
  const w = new FrameWriter(MSG.INFO_SNAPSHOT);
  const mask = (1 << 0) | (1 << 2) | (1 << 3);
  w.u64(77n); w.u32(mask); w.i64(10000); w.i64(12000); w.i64(9500);
  add('InfoSnapshot', 'InfoSnapshot', w.finish(),
    { securityId: '77', fields: { OpeningPrice: 10000, HighPrice: 12000, LowPrice: 9500 } });
}

// ServerHello (cold): [protocolVersion u32][capabilities u32][buildLen u8][build]
{
  const w = new FrameWriter(MSG.SERVER_HELLO);
  w.u32(2); w.u32(0x03); w.str('test-build');
  add('ServerHello', 'ServerHello', w.finish(),
    { protocolVersion: 2, capabilities: 0x03, buildVersion: 'test-build' });
}

process.stdout.write(JSON.stringify({ protocolVersion: 2, headerSize: 8, vectors }, null, 2) + '\n');
