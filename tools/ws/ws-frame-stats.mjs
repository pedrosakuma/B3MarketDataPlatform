#!/usr/bin/env node
// WebSocket frame stats — bytes/frame breakdown per MessageType to identify
// economy opportunities in the INCREMENTAL stream (the snapshot phase is
// reported separately and not the focus).
//
// Usage:
//   node tools/ws/ws-frame-stats.mjs [url] [symbol] [options]
//
// Options:
//   --flags=0xNN          Subscription flags (default 0x1F = Everything)
//   --snapshot-secs=N     Snapshot phase duration after SUBSCRIBE_OK (default 3)
//   --duration=N          Incremental sampling duration in seconds (default 60)
//   --json                Emit machine-readable JSON instead of tables
//   --verbose             Log every WS frame (debug only)
//
// Examples:
//   node tools/ws/ws-frame-stats.mjs ws://localhost:8080/ws WINV25 --duration=120
//   node tools/ws/ws-frame-stats.mjs ws://host:8080/ws PETR4 --flags=0x18 --json

import WebSocket from 'ws';

// ── Args ──
const args = process.argv.slice(2);
const positional = args.filter(a => !a.startsWith('--'));
const flagArgs = Object.fromEntries(
  args.filter(a => a.startsWith('--')).map(a => {
    const eq = a.indexOf('=');
    return eq < 0 ? [a.slice(2), true] : [a.slice(2, eq), a.slice(eq + 1)];
  })
);

const WS_URL = positional[0] || 'ws://localhost:8080/ws';
const SYMBOL = (positional[1] || 'WINV25').toUpperCase();
const SUB_FLAGS = parseInt(flagArgs.flags ?? '0x1F', 16);
const SNAPSHOT_SECS = Number(flagArgs['snapshot-secs'] ?? 3);
const DURATION = Number(flagArgs.duration ?? 60);
const JSON_MODE = !!flagArgs.json;
const VERBOSE = !!flagArgs.verbose;

// ── Protocol constants (mirrors src/B3.Umdf.Server/WireProtocol.cs MessageType) ──
const MSG = {
  SUBSCRIBE: 0x0001, UNSUBSCRIBE: 0x0002, GET: 0x0003,
  SUBSCRIBE_OK: 0x0010, SUBSCRIBE_ERROR: 0x0011, UNSUBSCRIBED: 0x0012,
  BOOK_SNAPSHOT: 0x0020, INFO_SNAPSHOT: 0x0021, LEVEL_SNAPSHOT: 0x0022,
  ORDER_ADDED: 0x0030, ORDER_UPDATED: 0x0031, ORDER_DELETED: 0x0032,
  TRADE: 0x0033, BOOK_CLEARED: 0x0034, TRADE_BUST: 0x0035,
  MARKET_TIER_UPDATE: 0x0036, LEVEL_UPDATE: 0x0037, LEVEL_DELETED: 0x0038,
  RANKINGS_UPDATE: 0x0040,
  SERVER_STATUS: 0x0050,
  CANDLE_SNAPSHOT: 0x0060, CANDLE_UPDATE: 0x0061,
  SYMBOL_STALE_STATUS: 0x0070,
  RECOVERY_PROGRESS: 0x0080,
  NEWS_BEGIN: 0x0090, NEWS_CHUNK: 0x0091, NEWS_END: 0x0092,
};
const MSG_NAME = Object.fromEntries(Object.entries(MSG).map(([k, v]) => [v, k]));

// Per-MessageType "header" cost = framing(4) + securityId(8) when secId is the
// first payload field. Used to estimate aggregation savings (i.e., if we batched
// N consecutive same-secId messages in one container, we could hoist secId once
// and skip N-1 framing headers). Set to null for messages that don't carry secId
// or don't naturally aggregate.
const PER_MSG_REDUNDANT_BYTES = {
  [MSG.ORDER_ADDED]:    12, // 4 framing + 8 secId
  [MSG.ORDER_UPDATED]:  12,
  [MSG.ORDER_DELETED]:  12,
  [MSG.TRADE]:          12,
  [MSG.TRADE_BUST]:     12,
  [MSG.LEVEL_UPDATE]:   12,
  [MSG.LEVEL_DELETED]:  12,
  [MSG.MARKET_TIER_UPDATE]: 12,
  [MSG.BOOK_CLEARED]:   12,
  [MSG.SYMBOL_STALE_STATUS]: 12,
  [MSG.CANDLE_UPDATE]:  12,
  [MSG.INFO_SNAPSHOT]:  12,
};

// Snapshot-phase MessageTypes — used only to classify late-arriving
// snapshot bytes that leak into the incremental window.
const SNAPSHOT_TYPES = new Set([
  MSG.BOOK_SNAPSHOT, MSG.INFO_SNAPSHOT, MSG.LEVEL_SNAPSHOT, MSG.CANDLE_SNAPSHOT,
]);

// WS-frame size buckets for histogram (bytes).
const FRAME_BUCKETS = [
  { max: 1024,        label: '≤1k'   },
  { max: 4 * 1024,    label: '≤4k'   },
  { max: 16 * 1024,   label: '≤16k'  },
  { max: 64 * 1024,   label: '≤64k'  },
  { max: 256 * 1024,  label: '≤256k' },
  { max: Infinity,    label: '>256k' },
];

// ── State ──
let ws = null;
let phase = 'pre-subscribe'; // 'pre-subscribe' | 'snapshot' | 'incremental' | 'done'
let phaseStartMs = 0;
let incrementalStartMs = 0;
let incrementalEndMs = 0;
let securityId = null;

const stats = {
  snapshot:    makeBucket(),
  incremental: makeBucket(),
};

function makeBucket() {
  return {
    wsFrames: 0,
    wsFrameBytes: 0,
    wsFrameSizes: [],     // for percentiles
    msgsPerFrame: [],     // logical msgs per WS frame
    histogram: FRAME_BUCKETS.map(b => ({ ...b, count: 0, bytes: 0 })),
    perType: new Map(),   // type -> { count, bytes, sizes: [], runLen: { sum, max, runs } }
    perFrameRuns: {       // aggregation-savings analysis (incremental only)
      // For each WS frame we walk inner messages and group consecutive
      // (secId, type) runs. Per (type) we track: total runs, total messages
      // in runs >= 2, and bytes saved if each run >=2 hoisted its 12B.
      runsByType: new Map(),
    },
  };
}

function recordType(bucket, type, len) {
  let s = bucket.perType.get(type);
  if (!s) { s = { count: 0, bytes: 0, sizes: [] }; bucket.perType.set(type, s); }
  s.count++;
  s.bytes += len;
  if (s.sizes.length < 8192) s.sizes.push(len); // bounded reservoir-ish
}

// ── Wire helpers ──
function buildSubscribe(symbol, msgType = MSG.SUBSCRIBE, flags = SUB_FLAGS) {
  const sym = Buffer.from(symbol, 'utf8');
  const buf = Buffer.alloc(4 + 1 + 1 + sym.length);
  buf.writeUInt16LE(buf.length, 0);
  buf.writeUInt16LE(msgType, 2);
  buf.writeUInt8(flags, 4);
  buf.writeUInt8(sym.length, 5);
  sym.copy(buf, 6);
  return buf;
}

// ── Frame processor ──
function readSecId(buf, offset, type) {
  // All msgs that have PER_MSG_REDUNDANT_BYTES start payload with secId u64.
  if (PER_MSG_REDUNDANT_BYTES[type] === undefined) return null;
  if (offset + 4 + 8 > buf.length) return null;
  return buf.readBigUInt64LE(offset + 4);
}

function processFrame(data) {
  const buf = Buffer.from(data);
  const totalLen = buf.length;

  let bucket;
  if (phase === 'snapshot') bucket = stats.snapshot;
  else if (phase === 'incremental') bucket = stats.incremental;
  else return;

  bucket.wsFrames++;
  bucket.wsFrameBytes += totalLen;
  if (bucket.wsFrameSizes.length < 8192) bucket.wsFrameSizes.push(totalLen);
  for (const b of bucket.histogram) {
    if (totalLen <= b.max) { b.count++; b.bytes += totalLen; break; }
  }

  // Walk inner messages, also detect consecutive same-secId runs per type.
  let offset = 0;
  let logical = 0;
  let runType = -1;
  let runSecId = null;
  let runLen = 0;
  const finishRun = () => {
    if (runLen >= 1 && PER_MSG_REDUNDANT_BYTES[runType] !== undefined) {
      const m = bucket.perFrameRuns.runsByType;
      let r = m.get(runType);
      if (!r) { r = { runs: 0, msgsInRunsGte2: 0, bytesHoistable: 0 }; m.set(runType, r); }
      r.runs++;
      if (runLen >= 2) {
        r.msgsInRunsGte2 += runLen;
        // Hoist 12B secId per run-tail message: (runLen - 1) * 12.
        // (Framing 4B may also be saved but conservative: only count secId 8B + framing 4B header reuse.)
        r.bytesHoistable += (runLen - 1) * PER_MSG_REDUNDANT_BYTES[runType];
      }
    }
  };

  while (offset + 4 <= buf.length) {
    const len = buf.readUInt16LE(offset);
    if (len < 4 || offset + len > buf.length) {
      console.error(`[WARN] bad framing at offset ${offset}, len=${len}, remaining=${buf.length - offset}`);
      break;
    }
    const type = buf.readUInt16LE(offset + 2);
    logical++;
    recordType(bucket, type, len);

    const secId = readSecId(buf, offset, type);
    if (type === runType && secId !== null && secId === runSecId) {
      runLen++;
    } else {
      finishRun();
      runType = type;
      runSecId = secId;
      runLen = 1;
    }

    offset += len;
  }
  finishRun();

  bucket.msgsPerFrame.push(logical);

  if (VERBOSE) {
    console.error(`[ws] ${phase} frame=${totalLen}B logical=${logical}`);
  }
}

// ── Phase control ──
function onSubscribeOk(secId) {
  securityId = secId;
  phase = 'snapshot';
  phaseStartMs = Date.now();
  console.error(`[phase] snapshot (${SNAPSHOT_SECS}s) — secId=${secId}`);
  setTimeout(() => {
    phase = 'incremental';
    incrementalStartMs = Date.now();
    console.error(`[phase] incremental (${DURATION}s)`);
    setTimeout(finish, DURATION * 1000);
  }, SNAPSHOT_SECS * 1000);
}

function finish() {
  if (phase === 'done') return;
  incrementalEndMs = Date.now();
  phase = 'done';
  try { ws?.close(); } catch {}
  emitReport();
  process.exit(0);
}

// ── Reporting ──
function pct(arr, p) {
  if (arr.length === 0) return 0;
  const s = arr.slice().sort((a, b) => a - b);
  const i = Math.min(s.length - 1, Math.floor(p * s.length));
  return s[i];
}
function avg(arr) {
  if (arr.length === 0) return 0;
  let s = 0; for (const v of arr) s += v; return s / arr.length;
}
function fmt(n, w = 0) {
  if (typeof n !== 'number' || !isFinite(n)) return String(n).padStart(w);
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'G';
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(2) + 'k';
  return (Number.isInteger(n) ? String(n) : n.toFixed(2)).padStart(w);
}
function pad(s, w, right = false) {
  s = String(s);
  if (s.length >= w) return s;
  return right ? s + ' '.repeat(w - s.length) : ' '.repeat(w - s.length) + s;
}

function summarizeBucket(bucket, durationMs) {
  const sec = Math.max(0.001, durationMs / 1000);
  const totalBytes = bucket.wsFrameBytes;
  const perType = [];
  for (const [type, s] of bucket.perType) {
    const headerOverhead = (PER_MSG_REDUNDANT_BYTES[type] !== undefined ? 4 : 4) * s.count; // 4B framing per msg always
    perType.push({
      type, name: MSG_NAME[type] || `0x${type.toString(16)}`,
      count: s.count, bytes: s.bytes,
      pctBytes: totalBytes > 0 ? (s.bytes / totalBytes) * 100 : 0,
      avgBytes: s.bytes / s.count,
      p50: pct(s.sizes, 0.50),
      p95: pct(s.sizes, 0.95),
      max: s.sizes.length ? Math.max(...s.sizes) : 0,
      bytesPerSec: s.bytes / sec,
      msgsPerSec: s.count / sec,
      framingOverheadBytes: headerOverhead,
      framingOverheadPct: s.bytes > 0 ? (headerOverhead / s.bytes) * 100 : 0,
    });
  }
  perType.sort((a, b) => b.bytes - a.bytes);

  const runs = [];
  for (const [type, r] of bucket.perFrameRuns.runsByType) {
    runs.push({
      type, name: MSG_NAME[type] || `0x${type.toString(16)}`,
      runs: r.runs, msgsInRunsGte2: r.msgsInRunsGte2, bytesHoistable: r.bytesHoistable,
      hoistablePct: totalBytes > 0 ? (r.bytesHoistable / totalBytes) * 100 : 0,
    });
  }
  runs.sort((a, b) => b.bytesHoistable - a.bytesHoistable);

  return {
    durationSec: sec,
    wsFrames: bucket.wsFrames,
    wsFrameBytes: totalBytes,
    bytesPerSec: totalBytes / sec,
    framesPerSec: bucket.wsFrames / sec,
    avgMsgsPerFrame: avg(bucket.msgsPerFrame),
    p50FrameSize: pct(bucket.wsFrameSizes, 0.50),
    p95FrameSize: pct(bucket.wsFrameSizes, 0.95),
    maxFrameSize: bucket.wsFrameSizes.length ? Math.max(...bucket.wsFrameSizes) : 0,
    histogram: bucket.histogram,
    perType,
    aggregationOpportunities: runs,
  };
}

function emitReport() {
  const snapMs = Math.max(0, (incrementalStartMs || Date.now()) - phaseStartMs);
  const incMs  = Math.max(0, (incrementalEndMs   || Date.now()) - (incrementalStartMs || Date.now()));
  const snap = summarizeBucket(stats.snapshot, snapMs);
  const inc  = summarizeBucket(stats.incremental, incMs);

  if (JSON_MODE) {
    console.log(JSON.stringify({
      url: WS_URL, symbol: SYMBOL, securityId: securityId?.toString() ?? null,
      flags: SUB_FLAGS,
      snapshot: snap,
      incremental: inc,
    }, null, 2));
    return;
  }

  console.log('');
  console.log(`══ ws-frame-stats  ${WS_URL}  symbol=${SYMBOL}  flags=0x${SUB_FLAGS.toString(16)}`);
  console.log('');

  printPhase('Snapshot phase', snap);
  console.log('');
  printPhase('Incremental phase  ★ focus', inc);
  console.log('');
  printAggregationOpportunities(inc);
}

function printPhase(title, s) {
  console.log(`── ${title}  (${s.durationSec.toFixed(1)}s) ──`);
  console.log(`  WS frames: ${s.wsFrames}  bytes: ${fmt(s.wsFrameBytes)}B  rate: ${fmt(s.bytesPerSec)}B/s  ${s.framesPerSec.toFixed(1)} frames/s`);
  console.log(`  WS frame size: p50=${fmt(s.p50FrameSize)}B p95=${fmt(s.p95FrameSize)}B max=${fmt(s.maxFrameSize)}B  avg ${s.avgMsgsPerFrame.toFixed(1)} msgs/frame`);
  console.log(`  frame-size histogram:`);
  for (const b of s.histogram) {
    if (b.count === 0) continue;
    console.log(`    ${pad(b.label, 6, true)} ${pad(b.count, 7)} frames  ${pad(fmt(b.bytes) + 'B', 9)}`);
  }

  if (s.perType.length === 0) { console.log('  (no messages)'); return; }
  console.log('');
  console.log(`  ${pad('MessageType', 22, true)} ${pad('count', 8)} ${pad('bytes', 10)} ${pad('%bytes', 7)} ${pad('avg', 6)} ${pad('p50', 6)} ${pad('p95', 6)} ${pad('B/s', 9)} ${pad('msg/s', 8)} ${pad('framing%', 9)}`);
  for (const t of s.perType) {
    console.log(`  ${pad(t.name, 22, true)} ${pad(fmt(t.count), 8)} ${pad(fmt(t.bytes), 10)} ${pad(t.pctBytes.toFixed(1), 7)} ${pad(fmt(t.avgBytes), 6)} ${pad(fmt(t.p50), 6)} ${pad(fmt(t.p95), 6)} ${pad(fmt(t.bytesPerSec), 9)} ${pad(fmt(t.msgsPerSec), 8)} ${pad(t.framingOverheadPct.toFixed(1), 9)}`);
  }
}

function printAggregationOpportunities(s) {
  console.log('── Aggregation opportunities (incremental) ──');
  console.log('  Estimated bytes saveable if N consecutive same-secId messages of the same type');
  console.log('  inside a single WS frame were packed into one container (hoist 12B per tail msg).');
  console.log('');
  if (s.aggregationOpportunities.length === 0) { console.log('  (none — every type appears alone or with mixed secIds)'); return; }
  console.log(`  ${pad('MessageType', 22, true)} ${pad('runs', 8)} ${pad('msgs ≥2', 10)} ${pad('hoistable', 12)} ${pad('% of incr', 10)}`);
  let totalHoistable = 0;
  for (const o of s.aggregationOpportunities) {
    totalHoistable += o.bytesHoistable;
    if (o.bytesHoistable === 0) continue;
    console.log(`  ${pad(o.name, 22, true)} ${pad(fmt(o.runs), 8)} ${pad(fmt(o.msgsInRunsGte2), 10)} ${pad(fmt(o.bytesHoistable) + 'B', 12)} ${pad(o.hoistablePct.toFixed(1) + '%', 10)}`);
  }
  const totalPct = s.wsFrameBytes > 0 ? (totalHoistable / s.wsFrameBytes) * 100 : 0;
  console.log('');
  console.log(`  TOTAL hoistable: ${fmt(totalHoistable)}B  (${totalPct.toFixed(1)}% of incremental bytes)`);
}

// ── Connect ──
function connect() {
  console.error(`[connect] ${WS_URL} symbol=${SYMBOL} flags=0x${SUB_FLAGS.toString(16)} snapshot=${SNAPSHOT_SECS}s duration=${DURATION}s`);
  ws = new WebSocket(WS_URL);
  ws.binaryType = 'arraybuffer';

  ws.on('open', () => {
    console.error('[open] sending subscribe');
    setTimeout(() => ws.send(buildSubscribe(SYMBOL)), 200);
  });

  ws.on('message', (data) => {
    // Inspect for SUBSCRIBE_OK before we start counting (control plane).
    if (phase === 'pre-subscribe') {
      const buf = Buffer.from(data);
      let off = 0;
      while (off + 4 <= buf.length) {
        const len = buf.readUInt16LE(off);
        const type = buf.readUInt16LE(off + 2);
        if (type === MSG.SUBSCRIBE_OK && off + 12 <= buf.length) {
          onSubscribeOk(buf.readBigUInt64LE(off + 4));
        } else if (type === MSG.SUBSCRIBE_ERROR) {
          console.error('[error] SUBSCRIBE_ERROR — exiting');
          process.exit(2);
        }
        off += len;
      }
      return;
    }
    processFrame(data);
  });

  ws.on('close', (code, reason) => {
    console.error(`[close] code=${code} reason=${reason || 'none'}`);
    if (phase !== 'done') { finish(); }
  });

  ws.on('error', (e) => console.error('[error]', e.message));
}

process.on('SIGINT', () => { console.error('[sigint]'); finish(); });

connect();
