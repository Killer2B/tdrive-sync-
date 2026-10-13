'use strict';
// Production-hardening: parallel transfers, configurable chunk size, size limits and E2EE.
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv, nodeTransport } = require('./helpers');
const { sha } = require('./mockServer');
const { uploadFile } = require('../src/UploadManager');
const { downloadFile } = require('../src/DownloadManager');
const { CryptoManager, isEncrypted } = require('../src/CryptoManager');
const { pool, bytesToHuman } = require('../src/util');

const big = (n) => { const b = Buffer.alloc(n); for (let i = 0; i < n; i++) b[i] = (i * 31 + 7) & 255; return b; };
const buf = (s) => new TextEncoder().encode(s).buffer;
const txt = (b) => new TextDecoder().decode(b);

// ---------- pool ----------

test('pool: respects the concurrency limit and preserves input order', async () => {
  let inflight = 0; let peak = 0;
  const items = Array.from({ length: 10 }, (_, i) => i);
  const out = await pool(items, 3, async (x) => {
    inflight++; peak = Math.max(peak, inflight);
    await new Promise((r) => setTimeout(r, 4));
    inflight--;
    return x * 2;
  });
  assert.deepEqual(out, items.map((x) => x * 2));
  assert.ok(peak <= 3, 'peak ' + peak);
  assert.ok(peak >= 2, 'should actually run in parallel, peak ' + peak);
});

test('pool: stops and rethrows the first error', async () => {
  await assert.rejects(pool([1, 2, 3, 4], 2, async (x) => { if (x === 2) throw new Error('boom'); return x; }), /boom/);
});

test('bytesToHuman formats sizes', () => {
  assert.equal(bytesToHuman(0), '0 B');
  assert.equal(bytesToHuman(1536), '1.5 KB');
  assert.equal(bytesToHuman(5 * 1024 * 1024), '5.0 MB');
});

// ---------- parallel upload ----------

test('parallel resumable upload: parts run concurrently, bytes stay identical', async () => {
  const env = await makeEnv();   // server chunk 1 MB
  try {
    let inflight = 0; let peak = 0;
    const client = env.mkClient(null, {
      transport: async (r) => {
        const isPut = r.method === 'PUT';
        if (isPut) { inflight++; peak = Math.max(peak, inflight); }
        try { return await nodeTransport(r); } finally { if (isPut) inflight--; }
      },
    });
    const data = big(4 * 1024 * 1024 + 7);   // 5 parts of 1 MB
    const f = await uploadFile(client, { name: 'p.bin', folderPath: '/', data, mtime: 1, concurrency: 3 });
    assert.equal(sha(env.server.files.get(f.id).bytes), sha(data));
    assert.ok(peak <= 3, 'peak ' + peak);
    assert.ok(peak >= 2, 'parts should overlap, peak ' + peak);
  } finally { await env.stop(); }
});

test('resumable uploads use content-defined chunking within the server part cap', async () => {
  const env = await makeEnv();   // server: 1 MB one-shot limit, 1 MB chunk
  try {
    const data = big(4 * 1024 * 1024);
    const f = await uploadFile(env.client, { name: 'c.bin', folderPath: '/', data, mtime: 1 });
    const parts = env.server.log.filter((l) => l.partIdx !== undefined);
    assert.ok(parts.length >= 2, 'split into multiple chunks: ' + parts.length);
    assert.ok(f.chunker && f.chunker.algo === 'buzhash', 'a CDC chunker was used');
    assert.ok(Array.isArray(f.chunks) && f.chunks.length === parts.length, 'chunk plan matches the parts sent');
    assert.equal(sha(env.server.files.get(f.id).bytes), sha(data));
    assert.ok(env.server.maxSeen <= 5 * 1024 * 1024);
  } finally { await env.stop(); }
});

// ---------- parallel download ----------

test('parallel ranged download: chunks overlap and reassemble correctly', async () => {
  const env = await makeEnv();
  try {
    const data = big(5 * 1024 * 1024 + 123);
    const rec = env.server.addFile('/', 'big.bin', data);
    const file = await env.client.getFile(rec.id);
    let inflight = 0; let peak = 0;
    const client = env.mkClient(null, {
      transport: async (r) => {
        const ranged = !!(r.headers && r.headers.Range);
        if (ranged) { inflight++; peak = Math.max(peak, inflight); }
        try { return await nodeTransport(r); } finally { if (ranged) inflight--; }
      },
    });
    const out = await downloadFile(client, file, { chunkBytes: 1024 * 1024, concurrency: 3 });
    assert.equal(sha(Buffer.from(out)), sha(data));
    assert.ok(peak >= 2, 'chunks should overlap, peak ' + peak);
  } finally { await env.stop(); }
});

// ---------- size limit ----------

test('files over the configured size limit are skipped (never uploaded), others are not', async () => {
  const env = await makeEnv({ settings: { maxFileBytes: 1024 } });
  try {
    env.local.put('small.md', 'ok');
    env.local.put('big.md', 'x'.repeat(5000));
    const r = await env.engine.syncNow();
    assert.ok(env.server.findLive('/Vault/', 'small.md'), 'small file uploaded');
    assert.equal(env.server.findLive('/Vault/', 'big.md'), undefined, 'large file not uploaded');
    assert.ok(r.warnings.some((w) => /size limit/.test(w)), JSON.stringify(r.warnings));
    assert.ok(env.local.files.has('big.md'), 'large local file untouched');
  } finally { await env.stop(); }
});

// ---------- E2EE ----------

test('E2EE: server stores ciphertext; download decrypts back to plaintext', async () => {
  const cm = new CryptoManager('a very long passphrase 1234', 'test-salt');
  const env = await makeEnv();
  try {
    const data = buf('secret note body');
    const f = await uploadFile(env.client, { name: 's.md', folderPath: '/', data, mtime: 1, crypto: cm });
    const stored = env.server.files.get(f.id).bytes;
    assert.ok(isEncrypted(new Uint8Array(stored).buffer), 'stored bytes carry the magic header');
    assert.notEqual(stored.toString(), 'secret note body');
    const file = await env.client.getFile(f.id);
    assert.equal(txt(await downloadFile(env.client, file, { crypto: cm })), 'secret note body');
  } finally { await env.stop(); }
});

test('E2EE: a wrong passphrase fails loudly instead of writing garbage', async () => {
  const good = new CryptoManager('correct passphrase long', 'salt');
  const bad = new CryptoManager('wrong passphrase long', 'salt');
  const env = await makeEnv();
  try {
    const f = await uploadFile(env.client, { name: 'x.md', folderPath: '/', data: buf('data'), mtime: 1, crypto: good });
    const file = await env.client.getFile(f.id);
    await assert.rejects(downloadFile(env.client, file, { crypto: bad }), /Decryption failed/);
  } finally { await env.stop(); }
});

test('E2EE: files without the magic header pass through unchanged (pre-E2EE files still work)', async () => {
  const cm = new CryptoManager('pw', 'salt');
  assert.equal(txt(await cm.decrypt(buf('plain old file'))), 'plain old file');
  const enc = await cm.encrypt(buf(''));                       // empty file still round-trips
  assert.equal((await cm.decrypt(enc)).byteLength, 0);
});

test('E2EE end-to-end: two devices share plaintext while the server only holds ciphertext', async () => {
  const cm = new CryptoManager('shared passphrase here', 'salt-x');
  const env = await makeEnv();
  try {
    const d1 = env.mkDevice({ engine: { crypto: cm } });
    d1.local.put('note.md', 'hello encrypted world');
    const r1 = await d1.engine.syncNow();
    assert.equal(r1.uploaded, 1, JSON.stringify(r1));
    const rec = env.server.findLive('/Vault/', 'note.md');
    assert.ok(rec && isEncrypted(new Uint8Array(rec.bytes).buffer), 'remote copy is ciphertext');
    assert.ok(!rec.bytes.toString().includes('hello encrypted'), 'plaintext never reaches the server');

    const d2 = env.mkDevice({ engine: { crypto: cm } });
    const r2 = await d2.engine.syncNow();
    assert.equal(r2.downloaded, 1, JSON.stringify(r2));
    assert.equal(d2.local.text('note.md'), 'hello encrypted world');
  } finally { await env.stop(); }
});
