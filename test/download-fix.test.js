'use strict';
// Regression tests for the "Incomplete download: 941/<n> bytes" failure:
//   A single content response was treated as authoritative and any shortfall was fatal, so a truncated
//   stream or a stale Content-Range total produced a hard error even though the object was intact and a
//   retry/tail-fetch would have succeeded. These tests reproduce each mechanism and assert the fix:
//   the download completes byte-for-byte, integrity is verified, and a failed download never touches the
//   local file.
const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('crypto');
const { makeEnv, nodeTransport } = require('./helpers');
const { sha } = require('./mockServer');
const { downloadFile } = require('../src/DownloadManager');

// 5348 bytes, like the reported file (the server's own example total).
const SIZE = 5348;
const make5348 = () => { const b = Buffer.alloc(SIZE); for (let i = 0; i < SIZE; i++) b[i] = (i * 37 + 11) & 255; return b; };
const shaHex = (b) => crypto.createHash('sha256').update(Buffer.from(b)).digest('hex');

test('truncated stream (941 of 5348 advertised) is continued to the full file, byte-for-byte', async () => {
  const env = await makeEnv();
  try {
    const data = make5348();
    env.server.addFile('/Vault/', 'كيف تقلل استخدام الرامات.md', data);
    // The FIRST content response advertises the true total but delivers only 941 bytes.
    env.server.contentTruncate = { times: 1, at: 941 };
    const file = await env.client.getFile(env.server.findLive('/Vault/', 'كيف تقلل استخدام الرامات.md').id);

    const out = await downloadFile(env.client, file, { chunkBytes: 4 * 1024 * 1024 });
    assert.equal(out.byteLength, SIZE, 'downloaded the whole file');
    assert.equal(shaHex(out), sha(data), 'byte-for-byte identical to the server copy');
    assert.equal(env.server.contentTruncate.times, 0, 'the truncated response was actually exercised');
  } finally { await env.stop(); }
});

test('reproduces the exact "941/5348" case: a stale Content-Range total is reconciled and the tail fetched', async () => {
  const env = await makeEnv();
  try {
    const data = make5348();
    env.server.addFile('/Vault/', 'note.md', data);
    // The first read claims total = 941 with a 941-byte body, while the object really is 5348 bytes.
    env.server.staleContent = { partial: 941 };
    const file = await env.client.getFile(env.server.findLive('/Vault/', 'note.md').id);

    const out = await downloadFile(env.client, file, { chunkBytes: 4 * 1024 * 1024 });
    assert.equal(out.byteLength, SIZE, 'the missing tail was fetched and reassembled');
    assert.equal(shaHex(out), sha(data));
  } finally { await env.stop(); }
});

test('a 503 on the first content request is retried and the file still downloads correctly', async () => {
  const env = await makeEnv();
  try {
    const data = make5348();
    env.server.addFile('/Vault/', 'busy.md', data);
    env.server.busyFirst = 2;
    const client = env.mkClient(null, { maxRetries: 3, maxServerRetries: 4 });
    const file = await client.getFile(env.server.findLive('/Vault/', 'busy.md').id);
    const out = await downloadFile(client, file, { chunkBytes: 4 * 1024 * 1024 });
    assert.equal(out.byteLength, SIZE);
    assert.equal(shaHex(out), sha(data));
  } finally { await env.stop(); }
});

test('integrity: a same-length but corrupted response is rejected (checksum), not written', async () => {
  const env = await makeEnv();
  try {
    const data = make5348();
    const rec = env.server.addFile('/Vault/', 'bad.md', data);
    const file = await env.client.getFile(rec.id);
    const expect = sha(data);           // manifest checksum of the intact object
    env.server.corruptContent = true;   // server now returns flipped bytes (same length)
    await assert.rejects(
      downloadFile(env.client, file, { chunkBytes: 4 * 1024 * 1024, expectSha: expect, verifyLabel: 'bad.md' }),
      /checksum mismatch/i,
    );
  } finally { await env.stop(); }
});

test('engine: retries a ranged checksum mismatch with a verified full response', async () => {
  const env = await makeEnv();
  try {
    const data = make5348();
    const rec = env.server.addFile('/Vault/', 'range-cache.md', data);
    const expected = sha(data);
    const request = env.client.request.bind(env.client);
    let ranged = 0;
    let whole = 0;
    env.client.request = async (opts) => {
      const res = await request(opts);
      if (opts.path.endsWith('/content') && opts.headers && opts.headers.Range) {
        ranged++;
        const body = new Uint8Array(res.body.slice(0));
        body[0] ^= 0xff;
        return { ...res, body: body.buffer };
      }
      if (opts.path.endsWith('/content')) whole++;
      return res;
    };

    const file = await env.client.getFile(rec.id);
    const out = await env.engine._downloadVerified('range-cache.md', file, null, expected);
    assert.equal(sha(Buffer.from(out)), expected, 'the full response is checksum verified');
    assert.ok(ranged >= 1, 'the ranged path was tried first');
    assert.ok(whole >= 1, 'checksum mismatch triggered an un-ranged retry');
  } finally { await env.stop(); }
});

test('engine: a short/corrupt remote never leaves a truncated local file, and reports an error', async () => {
  const env = await makeEnv();
  try {
    const data = make5348();
    env.server.addFile('/Vault/', 'corrupt.md', data);
    env.server.corruptContent = true;      // every read returns wrong bytes
    const r = await env.engine.syncNow();
    assert.equal(r.downloaded, 0, JSON.stringify(r));
    assert.ok(r.errors >= 1 || r.failures.length >= 1, 'an error is reported, not silent acceptance');
    assert.equal(env.local.files.has('corrupt.md'), false, 'no corrupt/partial file was written locally');
  } finally { await env.stop(); }
});

test('engine: refreshes the manifest and retries if the remote changes during download', async () => {
  const env = await makeEnv();
  try {
    const before = make5348();
    const after = Buffer.from(before); after[0] ^= 0xff;
    const rec = env.server.addFile('/Vault/', 'race.md', before);
    const originalDownload = downloadFile;
    let changed = false;
    env.engine.downloadFn = async (client, file, opts) => {
      if (!changed) {
        changed = true;
        env.server.modifyFile(rec.id, after);
      }
      return originalDownload(client, file, opts);
    };

    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok', JSON.stringify(r));
    assert.equal(r.downloaded, 1, JSON.stringify(r));
    assert.equal(sha(env.local.files.get('race.md').data), sha(after), 'the retry fetched the current remote content');
  } finally { await env.stop(); }
});

test('engine: the 941/5348 case now syncs successfully and the local copy matches byte-for-byte', async () => {
  const env = await makeEnv();
  try {
    const data = make5348();
    env.server.addFile('/Vault/', 'كيف تقلل استخدام الرامات.md', data);
    env.server.staleContent = { partial: 941 };   // reproduce the reported failure mode end-to-end
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok', JSON.stringify(r));
    assert.equal(r.downloaded, 1, JSON.stringify(r));
    assert.equal(sha(env.local.files.get('كيف تقلل استخدام الرامات.md').data), sha(data), 'local file is byte-for-byte correct');
  } finally { await env.stop(); }
});

test('a stale LARGER metadata size (past-EOF range -> HTTP 416) is accepted, not treated as an error', async () => {
  // Reproduces "compare <file>: HTTP 416": the listing/metadata claimed more bytes than the object stores,
  // so the tail fetch hit a range past EOF. The 416 body-less reply with "bytes */<total>" is authoritative.
  const env = await makeEnv();
  try {
    const data = make5348();
    const rec = env.server.addFile('/Vault/', 'stale.md', data);
    const file = await env.client.getFile(rec.id);
    file.size = 9999;                       // simulate a stale/larger size from the listing
    const out = await downloadFile(env.client, file, { chunkBytes: 4 * 1024 * 1024 });
    assert.equal(out.byteLength, SIZE, 'accepted the real (shorter) object size');
    assert.equal(shaHex(out), sha(data), 'byte-for-byte identical');
    assert.equal(file.size, SIZE, 'the trusted size was corrected on the entry');
  } finally { await env.stop(); }
});

test('engine: a stale larger remote size no longer fails the "compare" step with 416', async () => {
  const env = await makeEnv();
  try {
    const data = make5348();
    env.local.put('stale.md', data);                       // same content locally
    const rec = env.server.addFile('/Vault/', 'stale.md', data);
    rec.size = 9999;                                       // listing lies (larger) about the size
    rec.lastModified = rec.lastModified + 1;               // make it look changed so a compare happens
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok', JSON.stringify(r));
    assert.ok(r.errors === 0, 'no 416 error: ' + JSON.stringify(r.failures));
    assert.equal(sha(env.local.files.get('stale.md').data), sha(data), 'local file still correct');
  } finally { await env.stop(); }
});

test('off-by-one: first response short by one byte, tail read finishes it (33-byte file)', async () => {
  const env = await makeEnv();
  try {
    const data = Buffer.alloc(33); for (let i = 0; i < 33; i++) data[i] = (i * 13 + 7) & 255;
    env.server.addFile('/Vault/', 'tiny.md', data);
    env.server.contentTruncate = { times: 1, at: 32 };      // returns 32 of 33 bytes on the first read
    const file = await env.client.getFile(env.server.findLive('/Vault/', 'tiny.md').id);
    const out = await downloadFile(env.client, file, { chunkBytes: 4 * 1024 * 1024 });
    assert.equal(out.byteLength, 33);
    assert.equal(shaHex(out), sha(data));
  } finally { await env.stop(); }
});

test('a stale LISTING size larger than the object is resolved by the manifest checksum and converges', async () => {
  const env = await makeEnv();
  try {
    const real = Buffer.alloc(941); for (let i = 0; i < 941; i++) real[i] = (i * 5 + 3) & 255;
    const rec = env.server.addFile('/Vault/', 'note.md', real);
    env.server.listingSizeOverride = { [rec.id]: 5348 };     // the listing lies: claims 5348, real is 941

    // First run: downloads, verifies against the manifest (941), accepts and records the real size.
    const r1 = await env.engine.syncNow();
    assert.equal(r1.status, 'ok', JSON.stringify(r1));
    assert.equal(r1.downloaded, 1, JSON.stringify(r1));
    assert.equal(sha(env.local.files.get('note.md').data), sha(real), 'byte-for-byte correct');
    assert.equal(env.engine.manifest.getFile('note.md').remoteSize, 941, 'the ledger converged to the real size');

    // Second run: nothing to do (the size mismatch no longer repeats).
    const r2 = await env.engine.syncNow();
    assert.equal(r2.status, 'ok', JSON.stringify(r2));
    assert.equal(r2.downloaded + r2.uploaded + r2.conflicts, 0, JSON.stringify(r2));
  } finally { await env.stop(); }
});

test('a wrong object served for a file id is REJECTED by the manifest checksum, never accepted as content', async () => {
  const env = await makeEnv();
  try {
    const real = make5348();
    const rec = env.server.addFile('/Vault/', 'x.md', real);
    const wrong = Buffer.alloc(941); wrong.fill(9);          // some other file's bytes
    env.server.contentOverride = wrong;                       // content endpoint now serves the wrong object
    const file = await env.client.getFile(rec.id);
    const expect = (await env.client.request({ path: `/files/${rec.id}/chunks` })).chunks[0].checksum;
    await assert.rejects(
      downloadFile(env.client, file, { chunkBytes: 4 * 1024 * 1024, expectSha: expect, verifyLabel: 'x.md' }),
      /checksum mismatch/i,
    );
    // And the engine reports an error rather than storing the wrong bytes.
    env.local.files.delete('x.md');
    const r = await env.engine.syncNow();
    assert.ok(r.failures.length >= 1 || r.errors >= 1, 'reported, not silent: ' + JSON.stringify(r));
    assert.equal(env.local.files.has('x.md'), false, 'the wrong object was never written locally');
  } finally { await env.stop(); }
});

test('a stale LISTING size is adopted from the manifest BEFORE the transfer (no false failure)', async () => {
  // Reproduces "compare ...: Incomplete download: 941/5348": the listing claimed a size the object does not have.
  const env = await makeEnv();
  try {
    const real = Buffer.alloc(941); for (let i = 0; i < 941; i++) real[i] = (i * 7 + 1) & 255;
    const rec = env.server.addFile('/Vault/', 'dup.md', real);
    env.server.listingSizeOverride = { [rec.id]: 5348 };   // the listing lies (larger)
    // Seed a local copy so the run takes the COMPARE branch (the one that used to error).
    env.local.put('dup.md', real);
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok', JSON.stringify(r));
    assert.equal(r.errors, 0, 'no error: ' + JSON.stringify(r.failures));
    assert.equal(sha(env.local.files.get('dup.md').data), sha(real), 'local content unchanged and correct');
    const entry = env.engine.manifest.getFile('dup.md');
    assert.equal(entry.remoteSize, 941, 'the ledger adopted the server size');
  } finally { await env.stop(); }
});

test('atomic replace: a failed download leaves the existing local file intact', async () => {
  const env = await makeEnv();
  try {
    const original = make5348();
    env.local.put('keep.md', original);
    await env.engine.syncNow();                       // establish a ledger entry + baseline local content
    const localShaBefore = sha(env.local.files.get('keep.md').data);

    // The remote changes (so the engine plans a download) but every read comes back corrupted.
    const dataB = Buffer.from(original); for (let i = 0; i < dataB.length; i++) dataB[i] ^= 0x5a;
    env.server.modifyFile(env.server.findLive('/Vault/', 'keep.md').id, dataB);
    env.server.corruptContent = true;

    const r = await env.engine.syncNow();
    assert.ok(r.failures.length >= 1 || r.errors >= 1, 'failure reported, not silently accepted');
    assert.equal(env.local.files.get('keep.md').data.length, SIZE);
    assert.equal(sha(env.local.files.get('keep.md').data), localShaBefore, 'local file untouched after a failed download');
  } finally { await env.stop(); }
});
