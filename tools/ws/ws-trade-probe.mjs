import WebSocket from 'ws';
const URL = 'ws://localhost:8080/ws';
const SYMBOL = process.argv[2] || 'WINV25';
const FLAGS = parseInt(process.argv[3] || '0x1B', 16); // BOOK|INFO|MBP|TRADES = 0x01|0x02|0x08|0x10

const MSG = { SUBSCRIBE: 0x0001, SUBSCRIBE_OK: 0x0010, TRADE: 0x0033, SERVER_STATUS: 0x0050 };
const COUNT = {};
let secId = null;

function buildSub(sym, flags) {
  const b = Buffer.from(sym, 'utf8');
  const buf = Buffer.alloc(6 + b.length);
  buf.writeUInt16LE(buf.length, 0);
  buf.writeUInt16LE(MSG.SUBSCRIBE, 2);
  buf.writeUInt8(flags, 4);
  buf.writeUInt8(b.length, 5);
  b.copy(buf, 6);
  return buf;
}

const ws = new WebSocket(URL);
ws.on('open', () => {
  console.log(`OPEN; subscribing ${SYMBOL} flags=0x${FLAGS.toString(16)}`);
  ws.send(buildSub(SYMBOL, FLAGS));
});
ws.on('message', (data) => {
  const buf = Buffer.from(data);
  let off = 0;
  while (off + 4 <= buf.length) {
    const len = buf.readUInt16LE(off);
    if (len < 4 || off + len > buf.length) break;
    const type = buf.readUInt16LE(off + 2);
    COUNT[type] = (COUNT[type] || 0) + 1;
    if (type === MSG.SUBSCRIBE_OK) {
      secId = buf.readBigUInt64LE(off + 4);
      console.log(`SUB_OK secId=${secId}`);
    } else if (type === MSG.TRADE) {
      const sId = buf.readBigUInt64LE(off + 4);
      const px = buf.readBigInt64LE(off + 12);
      const qty = buf.readBigInt64LE(off + 20);
      const tId = buf.readBigInt64LE(off + 28);
      if (COUNT[type] <= 5)
        console.log(`TRADE secId=${sId} px=${px} qty=${qty} tId=${tId}`);
    }
    off += len;
  }
});
ws.on('error', e => console.error('ERR', e.message));
setInterval(() => {
  const lines = Object.entries(COUNT)
    .map(([t,c]) => `0x${(+t).toString(16).padStart(4,'0')}=${c}`)
    .join(' ');
  console.log(`[counters] ${lines}`);
}, 5000);
setTimeout(() => { console.log('DONE'); process.exit(0); }, 35000);
