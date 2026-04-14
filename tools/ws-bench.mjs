// Dummy WS consumer — reads as fast as possible, reports throughput
import WebSocket from 'ws';

const WS_URL = process.argv[2] || 'ws://localhost:8081/ws';
const SYMBOL = process.argv[3] || 'WINV25';

function buildSubscribe(symbol) {
  const symBytes = Buffer.from(symbol, 'utf8');
  const buf = Buffer.alloc(4 + 1 + 1 + symBytes.length);
  buf.writeUInt16LE(buf.length, 0);
  buf.writeUInt16LE(0x0001, 2); // Subscribe
  buf.writeUInt8(0x03, 4);      // flags: Book+Info
  buf.writeUInt8(symBytes.length, 5);
  symBytes.copy(buf, 6);
  return buf;
}

let totalFrames = 0, totalBytes = 0, totalMessages = 0;
let lastFrames = 0, lastBytes = 0, lastMessages = 0;
let lastTime = performance.now();

function countMessages(data) {
  const buf = Buffer.from(data);
  let offset = 0;
  let count = 0;
  while (offset + 4 <= buf.length) {
    const len = buf.readUInt16LE(offset);
    if (len < 4 || offset + len > buf.length) break;
    count++;
    offset += len;
  }
  return count;
}

const ws = new WebSocket(WS_URL);
ws.binaryType = 'arraybuffer';

ws.on('open', () => {
  console.log(`Connected to ${WS_URL}, subscribing to ${SYMBOL}`);
  ws.send(buildSubscribe(SYMBOL));

  setInterval(() => {
    const now = performance.now();
    const dt = (now - lastTime) / 1000;
    const dFrames = totalFrames - lastFrames;
    const dBytes = totalBytes - lastBytes;
    const dMsgs = totalMessages - lastMessages;

    console.log(
      `frames=${dFrames}/s  msgs=${(dMsgs/dt).toFixed(0)}/s  ` +
      `bytes=${(dBytes/dt/1024).toFixed(0)} KB/s  ` +
      `avgFrame=${dFrames > 0 ? (dBytes/dFrames/1024).toFixed(1) : 0} KB  ` +
      `total: ${totalFrames} frames, ${(totalBytes/1024/1024).toFixed(1)} MB`
    );

    lastFrames = totalFrames;
    lastBytes = totalBytes;
    lastMessages = totalMessages;
    lastTime = now;
  }, 2000);
});

ws.on('message', (data) => {
  totalFrames++;
  totalBytes += data.byteLength;
  totalMessages += countMessages(data);
});

ws.on('close', () => { console.log('Disconnected'); process.exit(0); });
ws.on('error', (e) => console.log('WS error:', e.message));
