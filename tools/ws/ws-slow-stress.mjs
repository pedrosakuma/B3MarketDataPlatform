// Slow-client backpressure stress test.
//
// Spawns N concurrent ws clients. M of them deliberately throttle their
// read loop. Each client subscribes to up to MAX_SYMBOLS symbols (chosen
// from /symbols?q=PREFIX) to ensure enough fanout to expose backpressure.
//
// Usage:
//   node tools/ws/ws-slow-stress.mjs [URL] [TOTAL] [SLOW] [SYM_PREFIX] [DURATION_S] [MAX_SYMBOLS]
// Defaults: ws://localhost:8080/ws 50 10 "" 60 50
//
// The slow clients use SLOW_THROTTLE_MS env (default 5ms) — small enough
// to create steady backpressure under typical 30k+ events/s fanout.

import WebSocket from 'ws';

const URL = process.argv[2] || 'ws://localhost:8080/ws';
const TOTAL = parseInt(process.argv[3] || '50', 10);
const SLOW = parseInt(process.argv[4] || '10', 10);
const SYM_PREFIX = process.argv[5] || '';
const DURATION_S = parseInt(process.argv[6] || '60', 10);
const MAX_SYMBOLS = parseInt(process.argv[7] || '50', 10);
const SLOW_THROTTLE_MS = parseInt(process.env.SLOW_THROTTLE_MS || '5', 10);

const HTTP_BASE = URL.replace(/^ws/, 'http').replace(/\/ws$/, '');

async function fetchSymbols() {
  const url = `${HTTP_BASE}/symbols?${SYM_PREFIX ? `q=${encodeURIComponent(SYM_PREFIX)}&` : ''}limit=${MAX_SYMBOLS}`;
  const resp = await fetch(url);
  const j = await resp.json();
  return j.symbols || [];
}

function buildSubscribe(symbol) {
  const symBytes = Buffer.from(symbol, 'utf8');
  const buf = Buffer.alloc(4 + 1 + 1 + symBytes.length);
  buf.writeUInt16LE(buf.length, 0);
  buf.writeUInt16LE(0x0001, 2);
  buf.writeUInt8(0x03, 4);
  buf.writeUInt8(symBytes.length, 5);
  symBytes.copy(buf, 6);
  return buf;
}

class Client {
  constructor(id, slow, symbols) {
    this.id = id;
    this.slow = slow;
    this.symbols = symbols;
    this.frames = 0;
    this.bytes = 0;
    this.connectedAt = 0;
    this.disconnectedAt = 0;
    this.closeCode = null;
    this.closeReason = '';
    this.error = null;
    this.queue = [];
    this.processing = false;
  }

  start() {
    this.ws = new WebSocket(URL);
    this.ws.binaryType = 'arraybuffer';
    this.ws.on('open', () => {
      this.connectedAt = Date.now();
      for (const s of this.symbols) this.ws.send(buildSubscribe(s));
      if (this.slow) this.startThrottleLoop();
    });
    this.ws.on('message', (data) => {
      this.frames++;
      this.bytes += data.length || data.byteLength || 0;
    });
    this.ws.on('close', (code, reason) => {
      this.disconnectedAt = Date.now();
      this.closeCode = code;
      this.closeReason = reason?.toString() || '';
    });
    this.ws.on('error', (err) => { this.error = err.message; });
  }

  startThrottleLoop() {
    // Pause the underlying TCP socket for SLOW_PAUSE_MS, drain for SLOW_DRAIN_MS.
    // This creates real kernel-level TCP backpressure that propagates to the server.
    const sock = this.ws._socket;
    if (!sock) return;
    const pause = parseInt(process.env.SLOW_PAUSE_MS || '500', 10);
    const drain = parseInt(process.env.SLOW_DRAIN_MS || '50', 10);
    const loop = () => {
      if (this.disconnectedAt) return;
      try { sock.pause(); } catch {}
      setTimeout(() => {
        if (this.disconnectedAt) return;
        try { sock.resume(); } catch {}
        setTimeout(loop, drain);
      }, pause);
    };
    loop();
  }

  stop() { try { this.ws.close(); } catch {} }
}

const symbols = await fetchSymbols();
console.log(`subscribing each client to ${symbols.length} symbols (prefix="${SYM_PREFIX}")`);
console.log(`spawning ${TOTAL} clients (${SLOW} slow @ ${SLOW_THROTTLE_MS}ms throttle), duration=${DURATION_S}s`);

const clients = [];
for (let i = 0; i < TOTAL; i++) clients.push(new Client(i, i < SLOW, symbols));
for (const c of clients) c.start();

const startTs = Date.now();
const interval = setInterval(() => {
  const now = Date.now();
  const elapsed = ((now - startTs) / 1000).toFixed(0);
  let alive = 0, slowAlive = 0, healthyAlive = 0, slowDisc = 0, healthyDisc = 0;
  let slowFrames = 0, healthyFrames = 0;
  for (const c of clients) {
    const isAlive = c.disconnectedAt === 0 && c.connectedAt !== 0;
    if (isAlive) { alive++; if (c.slow) slowAlive++; else healthyAlive++; }
    else if (c.disconnectedAt !== 0) { if (c.slow) slowDisc++; else healthyDisc++; }
    if (c.slow) slowFrames += c.frames; else healthyFrames += c.frames;
  }
  console.log(`[t=${elapsed}s] alive=${alive}/${TOTAL}  slow_alive=${slowAlive}/${SLOW} disc=${slowDisc}  healthy_alive=${healthyAlive}/${TOTAL-SLOW} disc=${healthyDisc}  slow_frames=${slowFrames} healthy_frames=${healthyFrames}`);
}, 5000);

setTimeout(() => {
  clearInterval(interval);
  for (const c of clients) c.stop();
  console.log('\n=== final per-client report ===');
  console.log('id,kind,seconds,frames,bytes_MB,closeCode,closeReason,error');
  for (const c of clients) {
    const sec = c.disconnectedAt
      ? ((c.disconnectedAt - c.connectedAt) / 1000).toFixed(1)
      : ((Date.now() - c.connectedAt) / 1000).toFixed(1);
    const mb = (c.bytes / 1024 / 1024).toFixed(2);
    console.log(`${c.id},${c.slow ? 'slow' : 'healthy'},${sec},${c.frames},${mb},${c.closeCode ?? ''},${c.closeReason},${c.error ?? ''}`);
  }
  let healthyDisc = 0, slowDisc = 0, slowBytes = 0, healthyBytes = 0;
  for (const c of clients) {
    if (c.disconnectedAt) { (c.slow ? slowDisc++ : healthyDisc++); }
    if (c.slow) slowBytes += c.bytes; else healthyBytes += c.bytes;
  }
  console.log(`\n=== summary ===`);
  console.log(`slow disconnected:    ${slowDisc}/${SLOW}`);
  console.log(`healthy disconnected: ${healthyDisc}/${TOTAL - SLOW}   ${healthyDisc === 0 ? '✓ PASS' : '✗ FAIL (healthy fleet should be untouched)'}`);
  console.log(`bytes received - slow: ${(slowBytes/1024/1024).toFixed(1)} MB   healthy: ${(healthyBytes/1024/1024).toFixed(1)} MB`);
  process.exit(healthyDisc === 0 ? 0 : 1);
}, DURATION_S * 1000);
