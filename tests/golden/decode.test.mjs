// Golden-vector decode test for the v2 wire protocol (frontend decoder).
//
// Decodes the committed, implementation-independent fixtures in
// tests/golden/wire-v2-vectors.json with the production parseMessage() and
// asserts the decoded fields. This is the JS half of the cross-language
// forward-compatibility contract (the C# half lives in
// tests/B3.MarketData.WebSocketClient.Tests/GoldenVectorTests.cs).
//
// Run:  node --test tests/golden/decode.test.mjs

import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { parseMessage } from '../../frontend/js/protocol.js';

const here = dirname(fileURLToPath(import.meta.url));
const golden = JSON.parse(readFileSync(join(here, 'wire-v2-vectors.json'), 'utf8'));

function hexToArrayBuffer(hex) {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.substr(i * 2, 2), 16);
  }
  return bytes.buffer;
}

test('golden header constants match', () => {
  assert.equal(golden.protocolVersion, 2);
  assert.equal(golden.headerSize, 8);
});

for (const vec of golden.vectors) {
  test(`decode ${vec.name}`, () => {
    const buf = hexToArrayBuffer(vec.hex);
    const view = new DataView(buf);
    const totalLen = view.getUint32(0, true);
    assert.equal(totalLen, buf.byteLength, 'framing length must equal buffer size');

    const msg = parseMessage(buf, 0, totalLen);
    assert.ok(msg, `parseMessage returned null for ${vec.name}`);
    assert.equal(msg.type, vec.type);

    for (const [key, want] of Object.entries(vec.expected)) {
      if (key === 'fields') {
        for (const [fk, fv] of Object.entries(want)) {
          assert.equal(msg.fields[fk], fv, `${vec.name}.fields.${fk}`);
        }
      } else if (typeof msg[key] === 'bigint') {
        assert.equal(String(msg[key]), String(want), `${vec.name}.${key}`);
      } else {
        assert.equal(msg[key], want, `${vec.name}.${key}`);
      }
    }
  });
}
