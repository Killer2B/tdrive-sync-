'use strict';
// Incremental download: when the remote content changed and we kept a chunk map of the old version, only the
// changed byte ranges are fetched. Falls back to a full download whenever a delta is not worthwhile.
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv, nodeTransport } = require('./helpers');

const body = (bytes, seed = 1) => {
  const b = Buffer.alloc(bytes); let s = seed >>> 0;
  for (let i = 0; i < bytes; i++) { s = (s * 1664525 + 1013904223) >>> 0; b[i] = (s >>> 24) & 255; }
  return b;
};

test('a remote change is pulled with only the changed ranges (delta), not the whole file', async () => {
  const env = await makeEnv();
  try {
    const before = body(3 * 1024 * 1024, 21);
    env.server.addFile('/Vault/', 'big.bin', before);

    // Device 1 downloads it fully, then gets a chunk map stored for the next time.
    const d1 = env.mkDevice();
    let r = await d1.engine.syncNow();
    assert.equal(r.downloaded, 1, JSON.stringify(r));
    const entry = d1.manifest.getFile('big.bin');
    assert.ok(entry.chunkMap && entry.chunkMap.hashes.length >= 3, 'chunk map recorded for the next change');

    // The remote content changes in the middle only.
    const after = Buffer.from(before);
    for (let i = 1000000; i < 1000300; i++) after[i] ^= 0xff;
    env.server.modifyFile(env.server.findLive('/Vault/', 'big.bin').id, after);

    // Count the bytes downloaded on the delta run.
    let downloaded = 0;
    d1.engine.deltaFn = (client, id, changed, onProgress) => {
      for (const c of changed) downloaded += c.size;
      return require('../src/DownloadManager').downloadChangedRanges(client, id, changed, onProgress);
    };
    r = await d1.engine.syncNow();
    assert.equal(r.downloaded, 1, JSON.stringify(r));
    assert.equal(d1.local.files.get('big.bin').data.length, after.length);
    assert.equal(Buffer.compare(d1.local.files.get('big.bin').data, after), 0, 'spliced content is exact');
    assert.ok(downloaded > 0 && downloaded < before.length / 2, `only changed ranges fetched: ${downloaded} of ${before.length}`);
  } finally { await env.stop(); }
});

test('when the remote is unchanged the delta pull transfers nothing', async () => {
  const env = await makeEnv();
  try {
    const data = body(1024 * 1024, 33);
    env.server.addFile('/Vault/', 'x.bin', data);
    const d = env.mkDevice();
    await d.engine.syncNow();
    // Touch only the remote mtime (no content change => same manifest/etag).
    const rec = env.server.findLive('/Vault/', 'x.bin');
    rec.lastModified = rec.lastModified + 5000;
    let bytes = 0;
    d.engine.deltaFn = (client, id, changed) => { bytes += changed.reduce((n, c) => n + c.size, 0); return []; };
    const r = await d.engine.syncNow();
    assert.equal(r.downloaded, 0, JSON.stringify(r));
    assert.equal(bytes, 0, 'nothing downloaded');
  } finally { await env.stop(); }
});

test('delta is skipped (full download) when almost everything changed', async () => {
  const env = await makeEnv();
  try {
    const before = body(1024 * 1024, 41);
    env.server.addFile('/Vault/', 'big.bin', before);
    const d = env.mkDevice();
    await d.engine.syncNow();
    const after = body(1024 * 1024, 999);      // completely different bytes
    env.server.modifyFile(env.server.findLive('/Vault/', 'big.bin').id, after);
    let deltaCalls = 0;
    d.engine.deltaFn = (client, id, changed, onProgress) => { deltaCalls++; return require('../src/DownloadManager').downloadChangedRanges(client, id, changed, onProgress); };
    const r = await d.engine.syncNow();
    assert.equal(r.downloaded, 1, JSON.stringify(r));
    assert.equal(Buffer.compare(d.local.files.get('big.bin').data, after), 0);
    assert.equal(deltaCalls, 0, 'a mostly-changed file is downloaded whole');
  } finally { await env.stop(); }
});
