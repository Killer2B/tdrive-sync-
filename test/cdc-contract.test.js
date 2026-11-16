'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { createHash } = require('node:crypto');
const { cdc, cdcPlan, chooseChunker, fingerprintOf } = require('../src/Chunker');

const CHUNKER = { algo: 'buzhash', min: 1024, avg: 2048, max: 4096, window: 48 };
const sha = (body) => createHash('sha256').update(body).digest('hex');

test('sanitized 14502-byte runtime plan has no chunk outside the reported 1024..4096 bounds', () => {
  // Only sanitized sizes are known; this is not a reconstruction of the private payload or server validator.
  const sizes = [4096, 2867, 2397, 3841, 1301];
  assert.equal(sizes.reduce((sum, size) => sum + size, 0), 14502);
  assert.ok(sizes.every((size) => size >= 1024 && size <= 4096));
});

test('CDC source plans tile the input and permit a below-min size only at EOF', () => {
  const descriptors = [CHUNKER, chooseChunker(1000000, 5242880), { ...CHUNKER, avg: 3000, max: 7000, window: 16 }];
  const sizes = [0, 1, 1023, 1024, 1025, 4095, 4096, 4097, 5000, 8193, 9000, 65537, 300001];
  let checked = 0;
  for (const ch of descriptors) for (const size of sizes) for (const mode of [0, 1, 2]) {
    let seed = 12345;
    const bytes = Uint8Array.from({ length: size }, (_, i) => mode === 0 ? 0 : mode === 1 ? i % 256
      : ((seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0) >>> 24));
    const plan = cdc(bytes, ch); let offset = 0;
    for (let i = 0; i < plan.length; i++) {
      assert.equal(plan[i].offset, offset);
      assert.ok(plan[i].size > 0 && plan[i].size <= ch.max);
      assert.ok(i === plan.length - 1 || plan[i].size >= ch.min);
      offset += plan[i].size;
    }
    assert.equal(offset, size);
    checked++;
  }
  assert.equal(checked, 117);
});

test('CDC source retains the short EOF tail rather than dropping bytes or exceeding max', () => {
  assert.deepEqual(cdc(new Uint8Array(5000), CHUNKER), [{ offset: 0, size: 4096 }, { offset: 4096, size: 904 }]);
});

test('CDC plan hashes and fingerprint cover only the supplied offset byte window', async () => {
  const backing = new Uint8Array(6032); backing.fill(0xde);
  const bytes = backing.subarray(16, 6016); bytes.fill(0);
  const result = await cdcPlan(bytes, CHUNKER);
  assert.deepEqual(result.chunker, CHUNKER);
  assert.deepEqual(result.chunks.map(({ offset, size }) => ({ offset, size })), [{ offset: 0, size: 4096 }, { offset: 4096, size: 1904 }]);
  const hashes = result.chunks.map((part) => sha(bytes.subarray(part.offset, part.offset + part.size)));
  assert.deepEqual(result.chunks.map((part) => part.hash), hashes);
  assert.equal(await fingerprintOf(hashes), sha(hashes.join('\n')));
  assert.ok(backing.subarray(0, 16).every((byte) => byte === 0xde));
  assert.ok(backing.subarray(6016).every((byte) => byte === 0xde));
});
