'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv, nodeTransport } = require('./helpers');
const { sha } = require('./mockServer');
const { TdriveClient } = require('../src/TdriveClient');
const { uploadFile } = require('../src/UploadManager');
const { downloadFile } = require('../src/DownloadManager');
const { redact } = require('../src/util');

const big = (n) => { const b = Buffer.alloc(n); for (let i = 0; i < n; i++) b[i] = (i * 31 + 7) & 255; return b; };

test('1a auth success', async () => {
  const env = await makeEnv();
  try { assert.deepEqual(await env.client.me(), { name: 'tester' }); } finally { await env.stop(); }
});

test('1b auth failure: 401, not retried, key never in message', async () => {
  const env = await makeEnv();
  try {
    const c = env.mkClient('tdk_wrongwrongwrong');
    await assert.rejects(c.me(), (e) => e.status === 401 && !e.message.includes('tdk_wrong'));
    assert.equal(env.server.log.length, 1);
  } finally { await env.stop(); }
});

test('1c client refuses http:// unless explicitly allowed, and empty keys', async () => {
  assert.throws(() => new TdriveClient({ baseUrl: 'http://example.com', apiKey: 'tdk_abcdef', transport: nodeTransport }), /HTTPS/);
  assert.throws(() => new TdriveClient({ baseUrl: 'https://example.com', apiKey: '', transport: nodeTransport }), /empty/);
});

test('1d key only goes to the configured origin and is redacted from logs', async () => {
  const env = await makeEnv();
  try {
    const seen = [];
    const c = env.mkClient(null, { transport: async (r) => { seen.push(r); return nodeTransport(r); } });
    await c.me();
    assert.ok(seen.every((r) => new URL(r.url).origin === new URL(env.server.url).origin));
    assert.equal(redact('boom tdk_secret_value123 end', 'tdk_secret_value123'), 'boom *** end');
    assert.equal(redact('token tdk_abc-DEF_9'), 'token tdk_***');
  } finally { await env.stop(); }
});

test('4 retries after HTTP 429 honoring Retry-After', async () => {
  const env = await makeEnv();
  try {
    env.server.failRule((i) => i.route === '/me', { status: 429, retryAfter: 2 }, 2);
    assert.ok(await env.client.me());
    assert.deepEqual(env.sleeps, [2000, 2000]);
  } finally { await env.stop(); }
});

test('4b 5xx retried with exponential backoff, then gives up with a retryable error', async () => {
  const env = await makeEnv();
  try {
    env.server.failRule((i) => i.route === '/me', { status: 503 }, 99);
    await assert.rejects(env.client.me(), (e) => e.status === 503 && e.retryable);
    assert.deepEqual(env.sleeps, [500, 1000, 2000, 4000]);
  } finally { await env.stop(); }
});

test('2 network drop during resumable upload recovers via retry; bytes identical', async () => {
  const env = await makeEnv();
  try {
    const data = big(3.5 * 1024 * 1024);
    env.server.failRule((i) => i.method === 'PUT' && i.partIdx === 2, 'drop', 1);
    const f = await uploadFile(env.client, { name: 'big.bin', folderPath: '/', data, mtime: 1 });
    assert.equal(sha(env.server.files.get(f.id).bytes), sha(data));
  } finally { await env.stop(); }
});

test('13 interrupted upload resumes: only missing parts are re-sent', async () => {
  const env = await makeEnv();
  try {
    const data = big(3.5 * 1024 * 1024);
    const c0 = env.mkClient(null, { maxRetries: 0 });
    env.server.failRule((i) => i.method === 'PUT' && i.partIdx === 2, 'drop', 1);
    await assert.rejects(uploadFile(c0, { name: 'big.bin', folderPath: '/', data, mtime: 1 }));
    env.server.log.length = 0;
    const f = await uploadFile(env.client, { name: 'big.bin', folderPath: '/', data, mtime: 1 });
    const sent = env.server.log.filter((l) => l.method === 'PUT').map((l) => l.partIdx);
    assert.deepEqual(sent, [2, 3]);
    assert.equal(sha(env.server.files.get(f.id).bytes), sha(data));
    assert.equal(env.server.liveFiles().length, 1);
  } finally { await env.stop(); }
});

test('2b a single accepted part is not treated as a complete upload', async () => {
  const env = await makeEnv();
  try {
    const data = big(2.2 * 1024 * 1024);
    env.server.failRule((i) => i.route.endsWith('/complete'), { status: 500 }, 99);
    const c = env.mkClient(null, { maxRetries: 1 });
    await assert.rejects(uploadFile(c, { name: 'x.bin', folderPath: '/', data, mtime: 1 }));
    assert.equal(env.server.liveFiles().length, 0);
  } finally { await env.stop(); }
});

test('3 network drop during ranged download recovers from the offset', async () => {
  const env = await makeEnv();
  try {
    const data = big(3 * 1024 * 1024);
    const rec = env.server.addFile('/', 'd.bin', data);
    env.server.failRule((i) => i.range && i.range.startsWith('bytes=1048576-'), 'drop', 1);
    const file = await env.client.getFile(rec.id);
    const out = await downloadFile(env.client, file, { chunkBytes: 1024 * 1024 });
    assert.equal(sha(Buffer.from(out)), sha(data));
    const ranges = env.server.log.filter((l) => l.range).map((l) => l.range);
    assert.equal(ranges.filter((r) => r === 'bytes=0-1048575').length, 1); // first chunk not re-fetched
  } finally { await env.stop(); }
});

test('12 empty file round-trips through client', async () => {
  const env = await makeEnv();
  try {
    const f = await uploadFile(env.client, { name: 'empty.md', folderPath: '/', data: new ArrayBuffer(0), mtime: 1 });
    assert.equal(f.size, 0);
    assert.equal((await downloadFile(env.client, f)).byteLength, 0);
  } finally { await env.stop(); }
});

test('13 parts never exceed 5 MB even if server advertises bigger limits; 12 MB file is split', async () => {
  const env = await makeEnv({ server: { maxBody: 5 * 1024 * 1024, oneShotMax: 64 * 1024 * 1024, chunk: 64 * 1024 * 1024 } });
  try {
    const data = big(12 * 1024 * 1024 + 123);
    const f = await uploadFile(env.client, { name: 'huge.bin', folderPath: '/', data, mtime: 1 });
    assert.equal(f.size, data.length);
    assert.ok(env.server.maxSeen <= 5 * 1024 * 1024, 'largest request body ' + env.server.maxSeen);
    const parts = env.server.log.filter((l) => l.partIdx !== undefined);
    assert.equal(parts.length, 3);
    assert.ok(Buffer.compare(env.server.findLive('/', 'huge.bin').bytes, data) === 0);
    // a file just under the cap still goes in one request
    const small = big(4_900_000);
    await uploadFile(env.client, { name: 'one.bin', folderPath: '/', data: small, mtime: 1 });
    assert.equal(env.server.findLive('/', 'one.bin').bytes.length, small.length);
  } finally { await env.stop(); }
});
