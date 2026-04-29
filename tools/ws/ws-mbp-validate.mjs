// E2E validator: subscribes to a symbol with both Book and Mbp flags, builds
// the price ladder two ways in parallel:
//   (a) MBO-derived: aggregate live orders into per-(side,price) buckets
//   (b) MBP-direct:  apply LevelSnapshot/LevelUpdate/LevelDeleted directly
// After each WebSocket message (which corresponds to a coalesced broadcaster
// batch on the server), the two ladders MUST be identical for every level.
// Reports any divergence with line/level details.
//
//   node tools/ws/ws-mbp-validate.mjs ws://localhost:8081/ws SYMBOL [SYMBOL2 ...]

import WebSocket from 'ws';

const WS_URL = process.argv[2] || 'ws://localhost:8081/ws';
const SYMBOLS = process.argv.slice(3);
if (SYMBOLS.length === 0) {
  console.error('usage: node ws-mbp-validate.mjs <wsUrl> <symbol> [<symbol2> ...]');
  process.exit(1);
}

const MSG = {
  SUBSCRIBE: 0x0001, SUBSCRIBE_OK: 0x0010, SUBSCRIBE_ERROR: 0x0011,
  BOOK_SNAPSHOT: 0x0020, INFO_SNAPSHOT: 0x0021, LEVEL_SNAPSHOT: 0x0022,
  ORDER_ADDED: 0x0030, ORDER_UPDATED: 0x0031, ORDER_DELETED: 0x0032,
  TRADE: 0x0033, BOOK_CLEARED: 0x0034,
  TRADE_BUST: 0x0035, MARKET_TIER_UPDATE: 0x0036,
  LEVEL_UPDATE: 0x0037, LEVEL_DELETED: 0x0038,
  SERVER_STATUS: 0x0050,
};

// flags: Book(0x01) | Mbp(0x08)
const FLAGS = 0x09;

const ws = new WebSocket(WS_URL);

// Per security state
const state = new Map(); // symbol → { secId, mboOrders, mboLevels, mbpLevels, divergences, snapshots, batches }

function buildSubscribe(symbol) {
  const symBytes = Buffer.from(symbol, 'utf8');
  const buf = Buffer.alloc(4 + 1 + 1 + symBytes.length);
  buf.writeUInt16LE(buf.length, 0);
  buf.writeUInt16LE(MSG.SUBSCRIBE, 2);
  buf.writeUInt8(FLAGS, 4);
  buf.writeUInt8(symBytes.length, 5);
  symBytes.copy(buf, 6);
  return buf;
}

function getOrInitBySecId(secId) {
  for (const s of state.values()) if (s.secId === secId) return s;
  return null;
}

function getOrInitBySymbol(symbol) {
  let s = state.get(symbol);
  if (!s) {
    s = {
      symbol, secId: null,
      mboOrders: new Map(),
      mboLevels: new Map(),  // key: `${side}|${price}` → {qty, count}
      mbpLevels: new Map(),
      divergences: 0,
      mboFrames: 0, mbpFrames: 0,
    };
    state.set(symbol, s);
  }
  return s;
}

function levelKey(side, price) { return `${side}|${price}`; }

function mboLevelAdd(s, side, price, qty) {
  const k = levelKey(side, price);
  const lvl = s.mboLevels.get(k);
  if (lvl) { lvl.qty += qty; lvl.count++; }
  else s.mboLevels.set(k, { qty, count: 1 });
}

function mboLevelRemove(s, side, price, qty) {
  const k = levelKey(side, price);
  const lvl = s.mboLevels.get(k);
  if (!lvl) return;
  lvl.qty -= qty;
  lvl.count--;
  if (lvl.count <= 0) s.mboLevels.delete(k);
}

function processFrame(data) {
  let o = 0;
  while (o + 4 <= data.length) {
    const len = data.readUInt16LE(o);
    if (len < 4 || o + len > data.length) break;
    const type = data.readUInt16LE(o + 2);
    const p = data.subarray(o + 4, o + len);
    processMessage(type, p);
    o += len;
  }
  // After each WS message (coalesced batch boundary), check consistency for
  // every symbol that's seen at least one frame this turn.
  for (const s of state.values()) {
    if (s.secId == null) continue;
    checkConsistency(s);
  }
}

function processMessage(type, p) {
  switch (type) {
    case MSG.SUBSCRIBE_OK: {
      const secId = p.readBigUInt64LE(0);
      const sLen = p.readUInt8(9);
      const sym = p.subarray(10, 10 + sLen).toString('utf8');
      const s = getOrInitBySymbol(sym);
      s.secId = secId;
      console.log(`SubscribeOk: ${sym} secId=${secId}`);
      break;
    }
    case MSG.SUBSCRIBE_ERROR: {
      const errCode = p.readUInt8(0);
      const sLen = p.readUInt8(1);
      const sym = p.subarray(2, 2 + sLen).toString('utf8');
      console.error(`SubscribeError: ${sym} code=${errCode}`);
      break;
    }
    case MSG.BOOK_SNAPSHOT: {
      const secId = p.readBigUInt64LE(0);
      const s = getOrInitBySecId(secId);
      if (!s) break;
      s.mboOrders.clear();
      s.mboLevels.clear();
      break;
    }
    case MSG.LEVEL_SNAPSHOT: {
      const secId = p.readBigUInt64LE(0);
      const bidCount = p.readUInt16LE(8);
      const askCount = p.readUInt16LE(10);
      const s = getOrInitBySecId(secId);
      if (!s) break;
      s.mbpLevels.clear();
      let off = 12;
      for (let i = 0; i < bidCount; i++) {
        const price = p.readBigInt64LE(off); off += 8;
        const qty = p.readBigInt64LE(off); off += 8;
        const cnt = p.readUInt32LE(off); off += 4;
        s.mbpLevels.set(levelKey(0, price), { qty, count: cnt });
      }
      for (let i = 0; i < askCount; i++) {
        const price = p.readBigInt64LE(off); off += 8;
        const qty = p.readBigInt64LE(off); off += 8;
        const cnt = p.readUInt32LE(off); off += 4;
        s.mbpLevels.set(levelKey(1, price), { qty, count: cnt });
      }
      break;
    }
    case MSG.ORDER_ADDED:
    case MSG.ORDER_UPDATED: {
      const secId = p.readBigUInt64LE(0);
      const orderId = p.readBigUInt64LE(8);
      const side = p.readUInt8(16);
      const price = p.readBigInt64LE(17);
      const qty = p.readBigInt64LE(25);
      const s = getOrInitBySecId(secId);
      if (!s) break;
      s.mboFrames++;
      const prev = s.mboOrders.get(orderId);
      if (prev) mboLevelRemove(s, prev.side, prev.price, prev.qty);
      s.mboOrders.set(orderId, { side, price, qty });
      mboLevelAdd(s, side, price, qty);
      break;
    }
    case MSG.ORDER_DELETED: {
      const secId = p.readBigUInt64LE(0);
      const orderId = p.readBigUInt64LE(8);
      const s = getOrInitBySecId(secId);
      if (!s) break;
      s.mboFrames++;
      const prev = s.mboOrders.get(orderId);
      if (prev) {
        mboLevelRemove(s, prev.side, prev.price, prev.qty);
        s.mboOrders.delete(orderId);
      }
      break;
    }
    case MSG.LEVEL_UPDATE: {
      const secId = p.readBigUInt64LE(0);
      const side = p.readUInt8(8);
      const price = p.readBigInt64LE(9);
      const qty = p.readBigInt64LE(17);
      const cnt = p.readUInt32LE(25);
      const s = getOrInitBySecId(secId);
      if (!s) break;
      s.mbpFrames++;
      s.mbpLevels.set(levelKey(side, price), { qty, count: cnt });
      break;
    }
    case MSG.LEVEL_DELETED: {
      const secId = p.readBigUInt64LE(0);
      const side = p.readUInt8(8);
      const price = p.readBigInt64LE(9);
      const s = getOrInitBySecId(secId);
      if (!s) break;
      s.mbpFrames++;
      s.mbpLevels.delete(levelKey(side, price));
      break;
    }
    case MSG.BOOK_CLEARED: {
      const secId = p.readBigUInt64LE(0);
      const clearSide = p.readUInt8(8);
      const s = getOrInitBySecId(secId);
      if (!s) break;
      // Both ladders see this — clear matching sides on both.
      const wipe = (sideKey) => {
        for (const k of [...s.mbpLevels.keys()]) if (k.startsWith(sideKey + '|')) s.mbpLevels.delete(k);
        for (const k of [...s.mboLevels.keys()]) if (k.startsWith(sideKey + '|')) s.mboLevels.delete(k);
        for (const [oid, o] of s.mboOrders) if (o.side === Number(sideKey)) s.mboOrders.delete(oid);
      };
      if (clearSide === 0) { wipe('0'); wipe('1'); }
      else if (clearSide === 1) wipe('0');
      else if (clearSide === 2) wipe('1');
      break;
    }
  }
}

function checkConsistency(s) {
  // Compare mboLevels and mbpLevels.
  if (s.mboLevels.size !== s.mbpLevels.size) {
    if (s.divergences < 3) reportDiff(s, `size mismatch: mbo=${s.mboLevels.size} mbp=${s.mbpLevels.size}`);
    s.divergences++;
    return;
  }
  for (const [k, mbo] of s.mboLevels) {
    const mbp = s.mbpLevels.get(k);
    if (!mbp || mbp.qty !== mbo.qty || mbp.count !== mbo.count) {
      if (s.divergences < 3) {
        reportDiff(s, `level ${k}: mbo=${JSON.stringify({qty: String(mbo.qty), count: mbo.count})} mbp=${mbp ? JSON.stringify({qty: String(mbp.qty), count: mbp.count}) : 'absent'}`);
      }
      s.divergences++;
      return;
    }
  }
}

function reportDiff(s, msg) {
  console.error(`[${s.symbol}] DIVERGENCE: ${msg}  (mboFrames=${s.mboFrames} mbpFrames=${s.mbpFrames})`);
}

ws.on('open', () => {
  console.log(`Connected ${WS_URL}, subscribing ${SYMBOLS.length} symbol(s) with flags=Book|Mbp`);
  for (const sym of SYMBOLS) ws.send(buildSubscribe(sym));
});

ws.on('message', (data) => {
  processFrame(Buffer.isBuffer(data) ? data : Buffer.from(data));
});

ws.on('close', () => printSummary());
ws.on('error', (e) => { console.error('ws error', e.message); process.exit(1); });

const RUN_MS = parseInt(process.env.RUN_MS || '20000', 10);
setTimeout(() => {
  ws.close();
  setTimeout(printSummary, 200);
}, RUN_MS);

function printSummary() {
  console.log('\n────────── Summary ──────────');
  let totalDiv = 0;
  for (const s of state.values()) {
    console.log(`${s.symbol}: secId=${s.secId} mboFrames=${s.mboFrames} mbpFrames=${s.mbpFrames} levels=${s.mbpLevels.size} divergences=${s.divergences}`);
    totalDiv += s.divergences;
  }
  if (totalDiv === 0) {
    console.log('\n✅ MBO and MBP ladders agree on all observed batches.');
    process.exit(0);
  } else {
    console.error(`\n❌ Total divergences observed: ${totalDiv}`);
    process.exit(2);
  }
}
