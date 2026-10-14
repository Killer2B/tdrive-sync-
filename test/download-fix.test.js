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
