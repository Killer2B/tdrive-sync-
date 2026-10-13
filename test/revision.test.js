'use strict';
// In-place content updates ("revisions"): only the changed parts cross the wire, the file keeps its id,
// the commit is atomic, and the old bytes are archived server-side. Falls back safely when unavailable.
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv, nodeTransport } = require('./helpers');
const { uploadFile, uploadRevision } = require('../src/UploadManager');

const MB = 1024 * 1024;
const chunkOf = (b) => { const p = Buffer.alloc(MB); p.fill(b); return p; };
const threeChunks = (a, b, c) => Buffer.concat([chunkOf(a), chunkOf(b), chunkOf(c)]);

/** A client that records how many part bodies (bytes) and reuse claims go out. */
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

test('editing one chunk of a 3-chunk file reuses the other two (no bytes) and keeps the same file id', async () => {
  const env = await makeEnv();
  try {
    const { client, stats } = countingClient(env);
    const before = threeChunks(1, 2, 3);
    const first = await uploadFile(client, { name: 'big.bin', folderPath: '/', data: before, mtime: 1, concurrency: 1 });
    const id = first.id;
    assert.equal(first.size, before.length);

    stats.bytes = 0; stats.partBodies = 0; stats.reuseClaims = 0;
    const after = threeChunks(1, 9, 3);                       // only the middle chunk changed
    const rev = await uploadRevision(client, { id, name: 'big.bin', data: after, mtime: 2, concurrency: 1 });
    assert.ok(rev, 'a revision was used');
    assert.equal(rev.id, id, 'same file id (in place)');
    assert.equal(rev.size, after.length);
    assert.equal(stats.partBodies, 1, 'only ONE part sent bytes, got ' + stats.partBodies);
    assert.ok(stats.reuseClaims >= 2, 'the two unchanged parts were reused, got ' + stats.reuseClaims);
    assert.ok(stats.bytes <= MB, 'at most one chunk on the wire, got ' + stats.bytes);

    const stored = env.server.files.get(id);
    assert.equal(Buffer.compare(stored.bytes, after), 0, 'atomic replace: new content stored');
    assert.ok(stored.versions && stored.versions.length === 1 && Buffer.compare(stored.versions[0].bytes, before) === 0, 'previous version archived');
  } finally { await env.stop(); }
});

test('append-only growth reuses all leading chunks', async () => {
  const env = await makeEnv();
  try {
    const { client, stats } = countingClient(env);
    const before = threeChunks(4, 5, 6);
    const base = await uploadFile(client, { name: 'log.bin', folderPath: '/', data: before, mtime: 1, concurrency: 1 });
    stats.bytes = 0; stats.partBodies = 0; stats.reuseClaims = 0;
    const after = Buffer.concat([before, chunkOf(7)]);        // grew by one chunk at the end
    const rev = await uploadRevision(client, { id: base.id, name: 'log.bin', data: after, mtime: 2, concurrency: 1 });
    assert.ok(rev && rev.id === base.id);
    assert.equal(stats.partBodies, 1, 'only the appended chunk was sent');
    assert.ok(stats.reuseClaims >= 3, 'the three original chunks were reused');
    assert.equal(Buffer.compare(env.server.files.get(base.id).bytes, after), 0);
  } finally { await env.stop(); }
});

test('a whole-file sync replaces in place on the second run (no trash, same id)', async () => {
  const env = await makeEnv();
  try {
    env.local.put('Notes/big.bin', threeChunks(1, 2, 3));
    let r = await env.engine.syncNow();
    assert.equal(r.uploaded, 1, JSON.stringify(r));
    const id = env.server.findLive('/Vault/Notes/', 'big.bin').id;
    env.server.log.length = 0;
    env.local.put('Notes/big.bin', threeChunks(1, 8, 3));     // edit middle chunk only
    r = await env.engine.syncNow();
    assert.equal(r.uploaded, 1, JSON.stringify(r));
    const live = env.server.findLive('/Vault/Notes/', 'big.bin');
    assert.equal(live.id, id, 'the remote file kept its id (in-place revision, not create+trash)');
    assert.equal(env.server.log.filter((l) => l.method === 'DELETE').length, 0, 'nothing was trashed');
    assert.equal(Buffer.compare(live.bytes, threeChunks(1, 8, 3)), 0);
  } finally { await env.stop(); }
});

test('server without revision support falls back to a normal upload and still syncs', async () => {
  const env = await makeEnv({ server: { noRevisions: true } });
  try {
    env.local.put('Notes/big.bin', threeChunks(1, 2, 3));
    await env.engine.syncNow();
    const id1 = env.server.findLive('/Vault/Notes/', 'big.bin').id;
    env.local.put('Notes/big.bin', threeChunks(1, 7, 3));
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok', JSON.stringify(r));
    const live = env.server.findLive('/Vault/Notes/', 'big.bin');
    assert.equal(Buffer.compare(live.bytes, threeChunks(1, 7, 3)), 0, 'content correct after fallback');
    assert.notEqual(live.id, id1, 'fallback creates a new file, and the old one was trashed');
  } finally { await env.stop(); }
});

test('single-chunk files do not use a revision (nothing to reuse)', async () => {
  const env = await makeEnv();
  try {
    const data = Buffer.alloc(1000); data.fill(5);
    const base = await uploadFile(env.client, { name: 'small.bin', folderPath: '/', data, mtime: 1 });
    const rev = await uploadRevision(env.client, { id: base.id, name: 'small.bin', data, mtime: 2 });
    assert.equal(rev, null, 'a one-chunk file returns null so the caller uploads normally');
  } finally { await env.stop(); }
});
