'use strict';
// Bandwidth reduction: gzip of large text files, applied only when it is safe and worthwhile, transparent on
// download, and never applied to small or non-text files. Also documents the API's hard limit (no deltas).
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv } = require('./helpers');
const { sha } = require('./mockServer');
const { uploadFile } = require('../src/UploadManager');
const { downloadFile } = require('../src/DownloadManager');
const { Compressor, isCompressed, gzipSupported } = require('../src/Compressor');

const buf = (s) => new TextEncoder().encode(s).buffer;
const txt = (b) => new TextDecoder().decode(b);
// Highly compressible text (typical of a Markdown note).
const note = (kb) => ('# ' + 'ملاحظة نصية '.repeat(20) + '\n').repeat(Math.ceil((kb * 1024) / 220));

test('compressor is available in this runtime', () => {
  assert.equal(gzipSupported(), true, 'CompressionStream/DecompressionStream must exist (Obsidian + Node 18+)');
});

test('maybeCompress: large text shrinks a lot; the container is self-describing', async () => {
  const c = new Compressor({ minBytes: 1024, minSaving: 0.9 });
  const data = buf(note(64));
  const packed = await c.maybeCompress('note.md', data);
  assert.ok(isCompressed(packed), 'large text markdown gets compressed');
  assert.ok(packed.byteLength < data.byteLength * 0.9, `actually smaller: ${packed.byteLength} < ${data.byteLength}`);
  assert.equal(txt(await c.decompress(packed)), txt(data), 'round-trips exactly');
});

test('maybeCompress: small files, non-text extensions and incompressible data are stored unchanged', async () => {
  const c = new Compressor({ minBytes: 64 * 1024, minSaving: 0.9 });
  const small = await c.maybeCompress('tiny.md', buf('short note'));
  assert.equal(isCompressed(small), false, 'small file not compressed');
  const img = buf(note(64));
  const asImage = await c.maybeCompress('photo.png', img);
  assert.equal(isCompressed(asImage), false, 'non-text extension not compressed');
  const binary = new Uint8Array(64 * 1024); for (let i = 0; i < binary.length; i++) binary[i] = i & 255;
  const asBin = await c.maybeCompress('data.md', binary.buffer);
  assert.equal(isCompressed(asBin), false, 'binary content (NUL byte) is not compressed even with a text extension');
});

test('decompress passes plaintext through untouched (pre-compression files keep working)', async () => {
  const c = new Compressor();
  assert.equal(txt(await c.decompress(buf('plain old file'))), 'plain old file');
  const empty = await c.decompress(new ArrayBuffer(0));
  assert.equal(empty.byteLength, 0);
});

test('upload+download round-trip: the server stores fewer bytes, the client gets the original text', async () => {
  const env = await makeEnv();
  try {
    const c = new Compressor({ minBytes: 1024, minSaving: 0.9 });
    const data = buf(note(96));
    const f = await uploadFile(env.client, { name: 'big-note.md', folderPath: '/', data, mtime: 1, compressor: c });
    const stored = env.server.files.get(f.id).bytes;
    assert.ok(isCompressed(new Uint8Array(stored).buffer), 'stored payload is compressed');
    assert.ok(stored.length < data.byteLength / 2, `server stored far fewer bytes: ${stored.length} vs ${data.byteLength}`);
    assert.equal(f.size, stored.length, 'reported remote size is the compressed size (consistency check passed)');
    const file = await env.client.getFile(f.id);
    const out = await downloadFile(env.client, file, { compressor: c });
    assert.equal(txt(out), txt(data), 'downloaded and inflated back to the exact original');
    assert.equal(sha(Buffer.from(out)), sha(Buffer.from(data)));
  } finally { await env.stop(); }
});

test('compression + E2EE together: compress then encrypt, and unwrap in reverse on download', async () => {
  const { CryptoManager } = require('../src/CryptoManager');
  const env = await makeEnv();
  try {
    const c = new Compressor({ minBytes: 1024, minSaving: 0.9 });
    const cm = new CryptoManager('long enough passphrase 123', 'salt');
    const data = buf(note(48));
    const f = await uploadFile(env.client, { name: 'secret.md', folderPath: '/', data, mtime: 1, compressor: c, crypto: cm });
    const stored = env.server.files.get(f.id).bytes;
    assert.ok(!stored.toString('latin1').includes('ملاحظة'), 'plaintext never reaches the server');
    const file = await env.client.getFile(f.id);
    assert.equal(txt(await downloadFile(env.client, file, { compressor: c, crypto: cm })), txt(data));
  } finally { await env.stop(); }
});

test('a text-heavy sync run stores less on the server than it transfers locally', async () => {
  const env = await makeEnv();
  try {
    env.engine.compressor = new Compressor({ minBytes: 1024, minSaving: 0.9 });
    env.local.put('Notes/long.md', note(128));
    const r = await env.engine.syncNow();
    assert.equal(r.uploaded, 1, JSON.stringify(r));
    const rec = env.server.findLive('/Vault/Notes/', 'long.md');
    assert.ok(rec.bytes.length < 32 * 1024, 'a ~128KB note is stored in well under 32KB: ' + rec.bytes.length);
    // and it comes back intact on another device
    const d2 = env.mkDevice({ engine: { compressor: env.engine.compressor } });
    const r2 = await d2.engine.syncNow();
    assert.equal(r2.downloaded >= 1, true, JSON.stringify(r2));
    assert.equal(d2.local.text('Notes/long.md'), env.local.text('Notes/long.md'));
  } finally { await env.stop(); }
});
