'use strict';
// In-place content updates ("revisions") with content-defined chunking: only the changed chunks cross the wire,
// the file keeps its id, the commit is atomic, and the old bytes are archived server-side.
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv: makeBaseEnv, nodeTransport } = require('./helpers');
const makeEnv = (opts = {}) => makeBaseEnv({ legacyFeatures: true, ...opts });
const { uploadFile, uploadRevision } = require('../src/UploadManager');

const sha = (b) => require('crypto').createHash('sha256').update(b).digest('hex');
// A pseudo-random but deterministic body so CDC boundaries are content-defined (not a repeating pattern).
const body = (bytes, seed = 1) => {
  const b = Buffer.alloc(bytes); let s = seed >>> 0;
  for (let i = 0; i < bytes; i++) { s = (s * 1664525 + 1013904223) >>> 0; b[i] = (s >>> 24) & 255; }
  return b;
};

function countingClient(env) {
  const stats = { bytes: 0, partBodies: 0, reuseClaims: 0 };
  const client = env.mkClient(null, {
    transport: async (r) => {
      const url = String(r.url);
      if (r.method === 'PUT' && /\/parts\//.test(url)) {
        if (r.headers && r.headers['X-Chunk-Reuse'] === '1') stats.reuseClaims++;
        else if (r.body && r.body.byteLength) { stats.partBodies++; stats.bytes += r.body.byteLength; }
      }
      return nodeTransport(r);
    },
  });
  return { client, stats };
}

test('editing one region reuses the untouched chunks (few bytes) and keeps the same file id', async () => {
  const env = await makeEnv();
  try {
    const { client, stats } = countingClient(env);
    const before = body(2 * 1024 * 1024, 7);
    const first = await uploadFile(client, { name: 'big.bin', folderPath: '/', data: before, mtime: 1, concurrency: 1 });
    const id = first.id;
    assert.equal(first.size, before.length);
    assert.ok(first.chunks && first.chunks.length > 4, 'CDC produced several chunks: ' + (first.chunks || []).length);

    // change a small region in the middle
    const after = Buffer.from(before);
    for (let i = 1000000; i < 1000200; i++) after[i] ^= 0xff;

    stats.bytes = 0; stats.partBodies = 0; stats.reuseClaims = 0;
    const rev = await uploadRevision(client, { id, name: 'big.bin', data: after, mtime: 2, concurrency: 1, chunker: first.chunker });
    assert.ok(rev && rev.id === id, 'revision used, same id');
    assert.ok(rev.reusedParts >= 1, 'some chunks reused: ' + rev.reusedParts);
    assert.ok(stats.bytes < before.length / 2, `only a fraction sent: ${stats.bytes} of ${before.length}`);
    assert.ok(stats.partBodies < rev.chunks.length, 'not all chunks were re-sent');

    const stored = env.server.files.get(id);
    assert.equal(Buffer.compare(stored.bytes, after), 0, 'atomic replace: new content stored');
    assert.ok(stored.versions && stored.versions.length === 1, 'previous version archived');
  } finally { await env.stop(); }
});

test('append-only growth reuses the leading chunks', async () => {
  const env = await makeEnv();
  try {
    const { client, stats } = countingClient(env);
    const before = body(1.5 * 1024 * 1024, 3);
    const base = await uploadFile(client, { name: 'log.bin', folderPath: '/', data: before, mtime: 1, concurrency: 1 });
    stats.bytes = 0; stats.partBodies = 0; stats.reuseClaims = 0;
    const after = Buffer.concat([before, body(200 * 1024, 99)]);
    const rev = await uploadRevision(client, { id: base.id, name: 'log.bin', data: after, mtime: 2, concurrency: 1, chunker: base.chunker });
    assert.ok(rev && rev.id === base.id);
    assert.ok(rev.reusedParts >= 1, 'leading chunks reused: ' + rev.reusedParts);
    assert.ok(stats.bytes < after.length / 2, 'only the tail was sent: ' + stats.bytes);
    assert.equal(Buffer.compare(env.server.files.get(base.id).bytes, after), 0);
  } finally { await env.stop(); }
});

test('a CDC file is revised atomically on the second run, preserving its id and old bytes', async () => {
  const env = await makeEnv();
  try {
    const before = body(2 * 1024 * 1024, 11);
    env.local.put('Notes/big.bin', before);
    let r = await env.engine.syncNow();
    assert.equal(r.uploaded, 1, JSON.stringify(r));
    const id = env.server.findLive('/Vault/Notes/', 'big.bin').id;
    const after = Buffer.from(before); after[1500000] ^= 0xff;
    env.local.put('Notes/big.bin', after);
    r = await env.engine.syncNow();
    assert.equal(r.uploaded, 1, JSON.stringify(r));
    const live = env.server.findLive('/Vault/Notes/', 'big.bin');
    assert.equal(live.id, id, 'the base id is unchanged');
    assert.equal(Buffer.compare(live.bytes, after), 0, 'content is correct');
    assert.deepEqual(live.versions[0].bytes, before, 'the previous bytes were archived');
    env.server.log.length = 0;
    r = await env.engine.syncNow();
    assert.equal(r.uploaded + r.downloaded, 0, JSON.stringify(r));
    assert.equal(env.server.log.filter((l) => l.method === 'DELETE' || /\/content$|\/parts\//.test(l.route)).length, 0);
  } finally { await env.stop(); }
});

test('with CDC off, a fixed-chunk file IS revised in place (same id, nothing trashed)', async () => {
  const env = await makeEnv({ settings: { cdcFiles: false } });
  try {
    const before = body(2 * 1024 * 1024, 13);
    env.local.put('Notes/big.bin', before);
    await env.engine.syncNow();
    const id = env.server.findLive('/Vault/Notes/', 'big.bin').id;
    const prev = env.manifest.getFile('Notes/big.bin');
    assert.ok(prev.chunkMap, 'a chunk map exists');
    assert.equal(prev.chunkMap.chunker, null, 'no CDC chunker when CDC is off');
    env.server.log.length = 0;
    const after = Buffer.from(before); after[1500000] ^= 0xff;
    env.local.put('Notes/big.bin', after);
    const r = await env.engine.syncNow();
    assert.equal(r.uploaded, 1, JSON.stringify(r));
    const live = env.server.findLive('/Vault/Notes/', 'big.bin');
    assert.equal(live.id, id, 'revised in place');
    assert.equal(env.server.log.filter((l) => l.method === 'DELETE').length, 0, 'nothing trashed');
    assert.equal(Buffer.compare(live.bytes, after), 0);
  } finally { await env.stop(); }
});

test('server without multipart revisions preserves inline-update ids and refuses oversized replacement without data loss', async () => {
  const env = await makeEnv({ server: { noRevisions: true }, settings: { cdcFiles: false } });
  try {
    const before = body(100000, 5);
    env.local.put('Notes/big.bin', before);
    await env.engine.syncNow();
    const id1 = env.server.findLive('/Vault/Notes/', 'big.bin').id;
    const after = Buffer.from(before); after[70000] ^= 0xff;
    env.local.put('Notes/big.bin', after);
    env.server.log.length = 0;
    const inline = await env.engine.syncNow();
    assert.equal(inline.status, 'ok', JSON.stringify(inline));
    assert.equal(inline.uploaded, 1);
    const updated = env.server.findLive('/Vault/Notes/', 'big.bin');
    assert.equal(updated.id, id1, 'inline replacement keeps the existing id');
    assert.deepEqual(updated.bytes, after);
    assert.deepEqual(updated.versions[0].bytes, before, 'inline replacement archives old bytes');
    assert.ok(env.server.log.some((l) => l.method === 'PATCH' && l.route === `/files/${id1}`), 'the supported inline endpoint was exercised');
    assert.equal(env.server.log.filter((l) => l.method === 'DELETE' || l.route === '/uploads').length, 0);
    const baseline = JSON.parse(JSON.stringify(env.manifest.getFile('Notes/big.bin')));
    const oversized = body(1.5 * 1024 * 1024, 6);
    assert.ok(oversized.length > (await env.client.limits()).oneShotMaxBytes, 'multipart input exceeds the advertised inline maximum');
    env.local.put('Notes/big.bin', oversized);
    env.server.log.length = 0;
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'errors', JSON.stringify(r));
    assert.equal(r.uploaded, 0);
    assert.match(r.failures[0].error, /revisions not supported/);
    const live = env.server.findLive('/Vault/Notes/', 'big.bin');
    assert.equal(live.id, id1, 'no replacement id was created');
    assert.deepEqual(live.bytes, after, 'the committed remote base remains intact after the failed oversized replacement');
    assert.deepEqual(env.local.files.get('Notes/big.bin').data, oversized, 'the oversized local edit remains available');
    assert.deepEqual(env.manifest.getFile('Notes/big.bin'), baseline, 'the failed replacement did not publish a baseline');
    assert.equal(env.server.liveFiles().length, 1);
    assert.equal(env.server.log.filter((l) => l.method === 'DELETE' || l.method === 'POST' && l.route === '/files').length, 0, 'no destructive fallback or new upload');
  } finally { await env.stop(); }
});

test('single-chunk files do not use a revision (nothing to reuse)', async () => {
  const env = await makeEnv();
  try {
    const data = body(100000, 1);
    // Force a single chunk by using a chunker whose minimum covers the whole file.
    const rev = await uploadRevision(env.client, { id: 'no-such', name: 'small.bin', data, mtime: 2, chunker: { algo: 'buzhash', min: 200000, avg: 200000, max: 300000, window: 48 } });
    assert.equal(rev, null, 'a one-chunk file returns null so the caller uploads normally');
  } finally { await env.stop(); }
});
