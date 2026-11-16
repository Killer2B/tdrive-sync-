'use strict';
// QA regressions included in npm test; can also run with node --test test/audit-transfer.qa.js.
// DEFECT cases assert correct behavior and reproduced failures in the original 1.8.1.
// All HTTP, journals, payloads and lease clocks are synthetic and in memory.
const test = require('node:test');
const assert = require('node:assert/strict');
const { createHash } = require('node:crypto');
const { TdriveClient } = require('../src/TdriveClient');
const { uploadFile, uploadRevision, recoverCommittedUpload, validateUploadReceipt } = require('../src/UploadManager');
const { downloadFile } = require('../src/DownloadManager');
const { normalizeReceipt } = require('../src/normalize');
const { fixed, cdcPlan, fingerprintOf } = require('../src/Chunker');

const bytes = (s) => new TextEncoder().encode(s).buffer;
const text = (b) => new TextDecoder().decode(b);
const decode = (b) => JSON.parse(text(b));
const hash = (b) => createHash('sha256').update(typeof b === 'string' ? b : Buffer.from(b)).digest('hex');
const fingerprint = (hashes) => hash(hashes.join('\n'));
const clone = (x) => x == null ? x : JSON.parse(JSON.stringify(x));
const ok = (data) => ({ status: 200, headers: {}, body: bytes(JSON.stringify({ ok: true, data })) });
const fail = (status, code, headers = {}) => ({ status, headers, body: bytes(JSON.stringify({ ok: false, error: { code, message: code } })) });

function clientFor(handler, options = {}) {
  const calls = [];
  const client = new TdriveClient({ baseUrl: 'https://audit.invalid', apiKey: 'synthetic-audit-key', maxRetries: 0, maxServerRetries: 0,
    sleep: async () => assert.fail('unexpected delay'), ...options, transport: async (req) => {
      const url = new URL(req.url);
      assert.equal(url.origin, 'https://audit.invalid');
      assert.equal(req.headers.Authorization, 'Bearer synthetic-audit-key');
      calls.push(req);
      return handler(req, url);
    } });
  return { client, calls };
}

function manifestFor(data, chunkSize = 4096, extra = {}) {
  const chunks = fixed(data.byteLength, chunkSize).map((c, idx) => ({ ...c, idx, checksum: hash(data.slice(c.offset, c.offset + c.size)) }));
  return { size: data.byteLength, chunkSize, totalChunks: chunks.length, chunks, checksum: fingerprint(chunks.map((c) => c.checksum)), ...extra };
}

function contentResponse(req, data, etag = '"content-1"') {
  const headers = etag ? { etag } : {};
  if (!req.headers.Range) return { status: 200, headers: { ...headers, 'content-length': String(data.byteLength) }, body: data };
  const m = /^bytes=(\d+)-(\d+)$/.exec(req.headers.Range);
  assert.ok(m, 'fixture requires a valid inclusive Range');
  const from = Number(m[1]); const to = Math.min(Number(m[2]), data.byteLength - 1);
  if (from >= data.byteLength) return { status: 416, headers: { ...headers, 'content-range': `bytes */${data.byteLength}` }, body: new ArrayBuffer(0) };
  return { status: 206, headers: { ...headers, 'content-range': `bytes ${from}-${to}/${data.byteLength}` }, body: data.slice(from, to + 1) };
}

function downloadFixture(data, { manifest = false, etag = '"content-1"' } = {}) {
  return clientFor((req, url) => {
    assert.equal(req.method, 'GET');
    if (url.pathname === '/api/v1/files/f/chunks') return manifest ? ok(manifest) : fail(404, 'NOT_FOUND');
    assert.equal(url.pathname, '/api/v1/files/f/content');
    return contentResponse(req, data, etag);
  });
}

function journalFor() {
  const records = new Map(); const events = [];
  return { owner: 'audit-device', records, events, get: async (key) => clone(records.get(key)),
    set: async (key, record) => { records.set(key, clone(record)); events.push(clone(record)); } };
}

// Independent server-side hashing verifies the fixture's exact wire bytes and ordered fingerprint.
function sessionFixture(options = {}) {
  const cap = 4096; const source = bytes('A'.repeat(6000)); const parts = new Map();
  const journal = journalFor(); let opened; let status = Object.hasOwn(options, 'status') ? options.status : 'pending'; let completed = false;
  const env = clientFor((req, url) => {
    const path = url.pathname;
    if (path === '/api/v1') return ok({ upload: { chunkBytes: cap, oneShotMaxBytes: cap } });
    if (path === '/api/v1/files/f/chunks') return completed && options.noManifest ? fail(404, 'NOT_FOUND') : ok(manifestFor(completed && options.newerHead ? bytes('B'.repeat(6000)) : source));
    if (path === '/api/v1/files/f/content') return contentResponse(req, options.newerHead ? bytes('B'.repeat(6000)) : source);
    if (path === '/api/v1/uploads' && req.method === 'POST') {
      opened = decode(req.body);
      assert.equal(opened.size, source.byteLength);
      assert.equal(opened.totalChunks, 2);
      assert.equal(opened.mode, 'replace');
      assert.equal(opened.baseId, 'f');
      assert.equal(opened.expectedVersion, 1);
      assert.equal(opened.chunker, undefined);
      if (options.receivedFirst) parts.set(0, source.slice(0, cap));
      return ok({ id: 'u', uploadId: 'u', status, revision: true, clientKey: opened.clientKey,
        chunkSize: cap, totalChunks: 2, fingerprint: opened.fingerprint, received: [...parts.keys()] });
    }
    const part = /^\/api\/v1\/uploads\/u\/parts\/(\d+)$/.exec(path);
    if (part && req.method === 'PUT') {
      if (status === 'deleting') return fail(409, 'FILE_STATE_CONFLICT');
      const idx = Number(part[1]); const offset = idx * cap; const size = Math.min(cap, source.byteLength - offset);
      assert.ok(idx === 0 || idx === 1);
      assert.equal(req.headers['X-Chunk-Offset'], String(offset));
      assert.equal(req.headers['X-Chunk-Size'], String(size));
      assert.equal(req.headers['X-Content-SHA256'], hash(req.body));
      assert.deepEqual(Buffer.from(req.body), Buffer.from(source.slice(offset, offset + size)));
      if (options.partFailure) return fail(options.partFailure.status, options.partFailure.code);
      parts.set(idx, req.body); status = 'uploading'; return ok({ idx });
    }
    if (path === '/api/v1/uploads/u/complete' && req.method === 'POST') {
      assert.deepEqual(decode(req.body), { expectedVersion: 1 });
      assert.equal(parts.size, 2);
      assert.equal(fingerprint([hash(parts.get(0)), hash(parts.get(1))]), opened.fingerprint);
      completed = true; status = 'committed';
      if (options.lostComplete) throw new Error('synthetic lost completion reply');
      return ok({ fileId: 'f', contentVersion: options.strong ? 2 : null,
        checksum: options.badChecksum ? '0'.repeat(64) : options.omitChecksum ? null : opened.fingerprint, etag: null, version: 1 });
    }
    if (path === '/api/v1/uploads/u' && req.method === 'GET') return ok({ id: 'u', status, clientKey: opened.clientKey, fileId: completed ? 'f' : undefined, ...(completed ? options.sessionReceipt : {}) });
    if (path === '/api/v1/uploads/u' && req.method === 'DELETE') {
      assert.ok(['pending', 'uploading'].includes(status), 'fixture never authorizes abort of terminal sessions');
      status = 'aborted'; return ok({});
    }
    if (path === '/api/v1/files/f' && req.method === 'GET') {
      assert.ok(completed);
      return ok({ id: 'f', name: 'x.bin', path: '/', size: 6000, contentVersion: options.newerHead ? 9 : 2 });
    }
    assert.fail(`Unexpected synthetic route: ${req.method} ${path}`);
  });
  return { ...env, source, parts, journal, opts: { id: 'f', name: 'x.bin', data: source, mtime: 1700000000000,
    chunker: false, expectedVersion: 1, uploadJournal: journal }, completed: () => completed };
}

function assertNoReplayOrDelete(env, method) {
  assert.equal(env.calls.filter((r) => r.method === method).length, 1, 'committed mutation must not be replayed');
  assert.ok(env.calls.every((r) => r.method !== 'DELETE'), 'proof failure never deletes a file or a committed upload');
}

test('QA-XFER-001 CONTROL fixed resumable upload validates exact parts and skips received parts', async () => {
  const env = sessionFixture({ receivedFirst: true });
  const result = await uploadRevision(env.client, env.opts);
  assert.equal(result.id, 'f'); assert.equal(result.size, 6000); assert.equal(result.contentVersion, null);
  assert.equal(env.calls.filter((r) => r.method === 'PUT').length, 1);
  assert.equal(env.journal.events.at(-1).phase, 'completed');
});

test('QA-XFER-002 CONTROL optional null checksum accepts matching weak receipt without upgrading generation', async () => {
  const env = sessionFixture({ omitChecksum: true });
  const result = await uploadRevision(env.client, env.opts);
  assert.equal(result.contentVersion, null); assert.equal(result.checksum, null); assert.equal(result.etag, null);
  assert.equal(result.version, 1); assert.equal(result.size, 6000);
});

test('QA-XFER-003 DEFECT weak resumable receipt must not acknowledge a different same-size newer head', async () => {
  const env = sessionFixture({ omitChecksum: true, newerHead: true });
  let error;
  try { await uploadRevision(env.client, env.opts); } catch (e) { error = e; }
  assert.equal(env.completed(), true);
  assert.equal(env.journal.events.at(-1).phase, 'completed');
  assert.equal(env.calls.filter((r) => r.method === 'POST' && new URL(r.url).pathname === '/api/v1/uploads').length, 1);
  assert.equal(env.calls.filter((r) => new URL(r.url).pathname.endsWith('/complete')).length, 1);
  assert.ok(env.calls.every((r) => r.method !== 'DELETE'));
  assert.ok(error && /content_changed|checksum|content.*match/i.test(error.code + ' ' + error.message),
    'expected rejection of B*6000 as proof of the committed A*6000; actual: upload resolved successfully');
});

test('QA-XFER-004 CONTROL strong generation with null tokens preserves exact receipt despite newer head', async () => {
  const env = sessionFixture({ strong: true, omitChecksum: true, newerHead: true });
  const result = await uploadRevision(env.client, env.opts);
  assert.equal(result.contentVersion, 2); assert.equal(result.checksum, null); assert.equal(result.etag, null);
  assert.equal(result.size, 6000);
  assert.equal(env.calls.filter((r) => new URL(r.url).pathname.endsWith('/chunks')).length, 1, 'only the fixed base map, never proof against a newer generation');
});

test('QA-XFER-005 CONTROL resumable mismatched reported fingerprint rejects without committed abort', async () => {
  const env = sessionFixture({ strong: true, badChecksum: true });
  await assert.rejects(uploadRevision(env.client, env.opts), /checksum mismatch/);
  assert.equal(env.journal.events.at(-1).phase, 'completed');
  assert.ok(env.calls.every((r) => r.method !== 'DELETE'));
});

test('QA-XFER-006 DEFECT deleting session response must fail before any part write', async () => {
  const env = sessionFixture({ status: 'deleting' });
  let error;
  try { await uploadRevision(env.client, env.opts); } catch (e) { error = e; }
  assert.ok(error, 'a deleting session cannot be uploaded successfully');
  assert.equal(env.calls.filter((r) => r.method === 'PUT').length, 0,
    'expected local bad_response before PUT; actual: PUT sent into deleting session and rejected by server');
  assert.equal(error.code, 'bad_response');
  assert.ok(env.calls.every((r) => r.method !== 'DELETE'));
});

function oneShotFixture({ wrongChecksum = false, wrongSize = false, transformed = false } = {}) {
  const backing = Buffer.from([0xee, 0, 255, 128, 65, 0xff]); const input = backing.subarray(1, -1);
  const wire = transformed ? Buffer.concat([Buffer.from('EC'), input]) : input;
  const expectedFingerprint = fingerprint([hash(wire)]);
  const env = clientFor((req, url) => {
    if (url.pathname === '/api/v1') return ok({ upload: { chunkBytes: 5242880, oneShotMaxBytes: 5242880 } });
    assert.equal(url.pathname, '/api/v1/files'); assert.equal(req.method, 'POST');
    assert.ok(req.body instanceof ArrayBuffer);
    assert.deepEqual(Buffer.from(req.body), wire);
    assert.equal(req.headers['Content-Type'], 'application/octet-stream');
    assert.ok(req.headers['Idempotency-Key']);
    return ok({ id: 'f', name: 'x.bin', path: '/', size: wrongSize ? wire.length + 1 : wire.length,
      contentVersion: 1, checksum: wrongChecksum ? fingerprint([hash(Buffer.alloc(wire.length, 0x42))]) : expectedFingerprint });
  });
  return { ...env, input, wire, expectedFingerprint };
}

test('QA-XFER-007 CONTROL one-shot preserves an offset binary window and correct fingerprint', async () => {
  const env = oneShotFixture();
  const result = await uploadFile(env.client, { name: 'x.bin', folderPath: '/', data: env.input });
  assert.equal(result.size, 4); assert.equal(result.contentVersion, 1); assert.equal(result.checksum, env.expectedFingerprint);
  assertNoReplayOrDelete(env, 'POST');
});

test('QA-XFER-008 DEFECT one-shot mismatched reported fingerprint must reject', async () => {
  const env = oneShotFixture({ wrongChecksum: true });
  let error;
  try { await uploadFile(env.client, { name: 'x.bin', folderPath: '/', data: env.input }); } catch (e) { error = e; }
  assertNoReplayOrDelete(env, 'POST');
  assert.ok(error && /checksum|fingerprint/i.test(error.message),
    'expected reported wrong fingerprint to reject; actual: one-shot upload resolved with checksum of different bytes');
});

test('QA-XFER-009 CONTROL one-shot response size mismatch remains an error', async () => {
  const env = oneShotFixture({ wrongSize: true });
  await assert.rejects(uploadFile(env.client, { name: 'x.bin', folderPath: '/', data: env.input }), /size mismatch/);
  assertNoReplayOrDelete(env, 'POST');
});

test('QA-XFER-010 CONTROL compression then encryption precede one-shot hashing and wire bytes', async () => {
  const env = oneShotFixture({ transformed: true }); const phases = [];
  const result = await uploadFile(env.client, { name: 'x.bin', folderPath: '/', data: env.input,
    compressor: { maybeCompress: async (name, data) => { phases.push('compress'); return Uint8Array.from([67, ...new Uint8Array(data)]).buffer; } },
    crypto: { encrypt: async (data) => { phases.push('encrypt'); return Uint8Array.from([69, ...new Uint8Array(data)]).buffer; } } });
  assert.deepEqual(phases, ['compress', 'encrypt']); assert.equal(result.size, 6); assert.equal(result.checksum, env.expectedFingerprint);
});

test('QA-XFER-011 DEFECT legacy stale zero-size metadata must still discover nonempty stored bytes', async () => {
  const env = downloadFixture(bytes('NEW'));
  const file = { id: 'f', size: 0 };
  const result = await downloadFile(env.client, file, { chunkBytes: 4 });
  assert.equal(text(result), 'NEW', 'expected nonempty stored bytes; actual: empty buffer returned without a content request');
  assert.equal(file.size, 3);
});

test('QA-XFER-012 CONTROL coherent empty manifest is authoritative and does not request an invalid range', async () => {
  const empty = new ArrayBuffer(0); const env = downloadFixture(empty, { manifest: manifestFor(empty) });
  const result = await downloadFile(env.client, { id: 'f', size: 99 }, { chunkBytes: 4 });
  assert.equal(result.byteLength, 0); assert.equal(env.calls.length, 1);
});

test('QA-XFER-013 CONTROL actual legacy empty content remains supported', async () => {
  const env = downloadFixture(new ArrayBuffer(0)); const file = { id: 'f', size: 3 };
  assert.equal((await downloadFile(env.client, file, { chunkBytes: 4 })).byteLength, 0);
  assert.equal(file.size, 0);
});

test('QA-XFER-014 CONTROL legacy stale zero-size forceFull discovers bytes and updates size', async () => {
  const env = downloadFixture(bytes('NEW')); const file = { id: 'f', size: 0 };
  assert.equal(text(await downloadFile(env.client, file, { forceFull: true })), 'NEW');
  assert.equal(file.size, 3);
});

test('QA-XFER-015 DEFECT forceFull must enforce the available file ETag when manifest is absent', async () => {
  const env = downloadFixture(bytes('NEW'), { etag: '"content-2"' });
  let error;
  try { await downloadFile(env.client, { id: 'f', size: 3, etag: '"content-1"' }, { forceFull: true }); } catch (e) { error = e; }
  assert.equal(env.calls.length, 2);
  assert.ok(error && error.code === 'content_changed',
    'expected known content-1 vs received content-2 to reject; actual: forceFull accepted the changed object');
});

test('QA-XFER-016 CONTROL ranged legacy download enforces the same available file ETag', async () => {
  const env = downloadFixture(bytes('NEW'), { etag: '"content-2"' });
  await assert.rejects(downloadFile(env.client, { id: 'f', size: 3, etag: '"content-1"' }), (e) => e.code === 'content_changed');
  assert.equal(env.calls.at(-1).headers['If-Range'], '"content-1"');
});

test('QA-XFER-017 CONTROL forceFull accepts matching file ETag and truly unversioned legacy content', async () => {
  for (const etag of ['"content-1"', undefined]) {
    const env = downloadFixture(bytes('NEW'), { etag: etag || null });
    assert.equal(text(await downloadFile(env.client, { id: 'f', size: 3, etag }, { forceFull: true })), 'NEW');
  }
});

test('QA-XFER-018 CONTROL multi-chunk fingerprint is not whole-file SHA; corruption rejects before decryption', async () => {
  const data = bytes('ABCDEFGH'); const map = manifestFor(data, 4, { etag: '"content-1"' });
  assert.notEqual(map.checksum, hash(data));
  assert.equal(await fingerprintOf(map.chunks.map((c) => c.checksum)), map.checksum);
  for (const forceFull of [false, true]) {
    const good = downloadFixture(data, { manifest: map });
    assert.equal(text(await downloadFile(good.client, { id: 'f', size: 8 }, { chunkBytes: 4, concurrency: 2, forceFull })), 'ABCDEFGH');
    let decrypts = 0; const bad = downloadFixture(bytes('ABXDEFGH'), { manifest: map });
    await assert.rejects(downloadFile(bad.client, { id: 'f', size: 8 }, { chunkBytes: 4, forceFull,
      crypto: { decrypt: async (b) => { decrypts++; return b; } } }), /checksum mismatch/);
    assert.equal(decrypts, 0);
  }
});

test('QA-XFER-019 CONTROL received ETag continuity rejects a mixed version without a manifest', async () => {
  const env = clientFor((req, url) => url.pathname.endsWith('/chunks') ? fail(404, 'NOT_FOUND') :
    contentResponse(req, bytes('ABCDEFGH'), req.headers.Range.startsWith('bytes=0-') ? '"content-1"' : '"content-2"'));
  await assert.rejects(downloadFile(env.client, { id: 'f', size: 8 }, { chunkBytes: 4 }), (e) => e.code === 'content_changed');
});

test('QA-XFER-020 CONTROL lease expiry during retry blocks the next write and never sends an unleased request', async () => {
  let guarded = 0; const waits = [];
  const env = clientFor(() => fail(503, 'UNAVAILABLE'), { maxServerRetries: 1, sleep: async (ms) => waits.push(ms) });
  env.client.setLease({ path: '/', token: 'synthetic-lease' }, async () => {
    if (++guarded === 2) throw Object.assign(new Error('expired'), { status: 423, code: 'LEASE_CONFLICT' });
  });
  await assert.rejects(env.client.request({ method: 'PATCH', path: '/files/f' }), (e) => e.code === 'LEASE_CONFLICT');
  assert.equal(guarded, 2); assert.equal(env.calls.length, 1); assert.equal(env.calls[0].headers['X-Sync-Lease'], 'synthetic-lease');
  assert.deepEqual(waits, [500]);
});

test('QA-XFER-021 CONTROL 429 retry floor exceeds local cap but retries remain bounded', async () => {
  const waits = []; const env = clientFor(() => fail(429, 'RATE_LIMIT', { 'retry-after': '120' }),
    { maxRetries: 2, maxDelayMs: 30000, sleep: async (ms) => waits.push(ms) });
  await assert.rejects(env.client.request({ path: '/files' }), (e) => e.status === 429);
  assert.deepEqual(waits, [120000, 120000]); assert.equal(env.calls.length, 3); assert.equal(env.client._concurrencyHint, 1);
});

test('QA-XFER-022 CONTROL receipt nulls beat nested newer generation and archived version', () => {
  assert.deepEqual(normalizeReceipt({ contentVersion: null, checksum: null, etag: null, version: 1,
    file: { contentVersion: 9, checksum: 'new-head', etag: '"head-9"' } }), { contentVersion: null, checksum: null, etag: null, version: 1 });
});

test('QA-XFER-023 CONTROL CDC permits an average smaller than maximum and retains ordered wire hashes', async () => {
  const data = new Uint8Array(6000);
  const ch = { algo: 'buzhash', min: 1024, avg: 2048, max: 4096, window: 48 };
  const plan = await cdcPlan(data, ch);
  assert.deepEqual(plan.chunks.map((c) => c.size), [4096, 1904]);
  for (const c of plan.chunks) assert.equal(c.hash, hash(data.subarray(c.offset, c.offset + c.size)));
  assert.equal(await fingerprintOf(plan.chunks.map((c) => c.hash)), fingerprint(plan.chunks.map((c) => c.hash)));
});

test('QA-XFER-024 CONTROL transformed size above inline MAX selects resumable and hashes transformed parts', async () => {
  const parts = new Map(); let opened; const phases = [];
  const wire = bytes('ECABCD');
  const env = clientFor((req, url) => {
    if (url.pathname === '/api/v1') return ok({ upload: { chunkBytes: 4, oneShotMaxBytes: 4 } });
    if (url.pathname === '/api/v1/uploads') {
      opened = decode(req.body);
      assert.equal(opened.size, 6); assert.equal(opened.totalChunks, 2);
      assert.equal(opened.fingerprint, fingerprint([hash(wire.slice(0, 4)), hash(wire.slice(4))]));
      return ok({ id: 'u', status: 'pending', chunkSize: 4, totalChunks: 2, received: [], fingerprint: opened.fingerprint });
    }
    const part = /\/parts\/(\d+)$/.exec(url.pathname);
    if (part) {
      const idx = Number(part[1]);
      assert.deepEqual(Buffer.from(req.body), Buffer.from(wire.slice(idx * 4, idx * 4 + 4)));
      assert.equal(req.headers['X-Content-SHA256'], hash(req.body));
      parts.set(idx, req.body); return ok({});
    }
    assert.equal(url.pathname, '/api/v1/uploads/u/complete');
    assert.equal(parts.size, 2);
    return ok({ id: 'f', name: 'x.bin', path: '/', size: 6, checksum: opened.fingerprint });
  });
  const result = await uploadFile(env.client, { name: 'x.bin', folderPath: '/', data: bytes('ABCD'), chunker: false, dedupe: false,
    compressor: { maybeCompress: async (name, data) => { phases.push('compress'); return bytes('C' + text(data)); } },
    crypto: { encrypt: async (data) => { phases.push('encrypt'); return bytes('E' + text(data)); } } });
  assert.deepEqual(phases, ['compress', 'encrypt']); assert.equal(result.size, 6);
  assert.ok(env.calls.every((r) => new URL(r.url).pathname !== '/api/v1/files'), 'never send the smaller plaintext through one-shot');
});

test('QA-XFER-025 CONTROL strong partial inline receipt beats a later GET without checking newer bytes', async () => {
  const receipt = { contentVersion: 2, checksum: null, etag: null, version: 1 };
  const env = clientFor((req, url) => {
    if (url.pathname === '/api/v1') return ok({ upload: { chunkBytes: 5242880, oneShotMaxBytes: 5242880 } });
    assert.equal(url.pathname, '/api/v1/files/f');
    if (req.method === 'PATCH') {
      assert.deepEqual(decode(req.body), { expectedVersion: 1, content: { inline: 'UkFX' } });
      return ok({ file: { size: 3, ...receipt } });
    }
    return ok({ id: 'f', name: 'x.bin', path: '/', size: 99, contentVersion: 9 });
  });
  const result = await env.client.patchContent('f', bytes('RAW'), { expectedVersion: 1 });
  assert.equal(result.size, 3); assert.equal(result.contentVersion, 2); assert.equal(result.checksum, null); assert.equal(result.etag, null);
  assert.equal(env.calls.length, 3); assertNoReplayOrDelete(env, 'PATCH');
});

test('QA-XFER-026 CONTROL weak partial inline receipt rejects newer same-size bytes without replay', async () => {
  const env = clientFor((req, url) => {
    if (url.pathname === '/api/v1') return ok({ upload: { chunkBytes: 5242880, oneShotMaxBytes: 5242880 } });
    if (req.method === 'PATCH') return ok({ updated: true });
    if (url.pathname.endsWith('/chunks')) return ok(manifestFor(bytes('NEW')));
    assert.equal(url.pathname, '/api/v1/files/f');
    return ok({ id: 'f', name: 'x.bin', path: '/', size: 3, contentVersion: 9 });
  });
  await assert.rejects(env.client.patchContent('f', bytes('RAW')), (e) => e.code === 'content_changed');
  assertNoReplayOrDelete(env, 'PATCH');
});

test('QA-XFER-027 CONTROL permanent part failure cancels only its owned pending session', async () => {
  const env = sessionFixture({ partFailure: { status: 400, code: 'INVALID_PART' } });
  await assert.rejects(uploadRevision(env.client, env.opts), (e) => e.code === 'INVALID_PART');
  assert.equal(env.journal.events.at(-1).phase, 'aborted');
  assert.ok(env.journal.events.some((r) => r.phase === 'aborting'));
  assert.deepEqual(env.calls.filter((r) => r.method === 'DELETE').map((r) => new URL(r.url).pathname), ['/api/v1/uploads/u']);
  assert.equal(env.completed(), false);
});

test('QA-XFER-028 CONTROL lease conflict preserves its owned pending session without abort', async () => {
  const env = sessionFixture({ partFailure: { status: 423, code: 'LEASE_CONFLICT' } });
  await assert.rejects(uploadRevision(env.client, env.opts), (e) => e.status === 423 && e.code === 'LEASE_CONFLICT');
  assert.equal(env.journal.events.at(-1).phase, 'uploading');
  assert.ok(env.calls.every((r) => r.method !== 'DELETE'));
  assert.equal(env.completed(), false);
});

test('transfer regression partial one-shot fingerprints validate transformed bytes and preserve unknown receipt tokens', async () => {
  for (const wrongChecksum of [false, true]) {
    const input = Buffer.from([99, 65, 66, 67, 88]).subarray(1, 4);
    const wire = bytes('ECABC'); const expected = fingerprint([hash(wire)]);
    const env = clientFor((req, url) => {
      if (url.pathname === '/api/v1') return ok({ upload: { chunkBytes: 4096, oneShotMaxBytes: 4096 } });
      if (req.method === 'POST') {
        assert.deepEqual(Buffer.from(req.body), Buffer.from(wire));
        return ok({ fileId: 'f', checksum: wrongChecksum ? hash(wire) : expected });
      }
      assert.equal(url.pathname, '/api/v1/files/f');
      return ok({ id: 'f', name: 'x.bin', path: '/', size: 5, contentVersion: 9, checksum: 'new-head', etag: 'new-tag', version: 8, lastModified: 1700000009000 });
    });
    const opts = { name: 'x.bin', folderPath: '/', data: input,
      compressor: { maybeCompress: async (_, data) => bytes('C' + text(data)) }, crypto: { encrypt: async (data) => bytes('E' + text(data)) } };
    if (wrongChecksum) await assert.rejects(uploadFile(env.client, opts), (e) => e.code === 'bad_response' && e.fileId === 'f' && /checksum mismatch/.test(e.message));
    else {
      const result = await uploadFile(env.client, opts);
      assert.equal(result.checksum, expected); assert.equal(result.contentVersion, null); assert.equal(result.etag, null);
      assert.equal(result.version, null); assert.equal(result.mtime, null);
    }
    assertNoReplayOrDelete(env, 'POST');
  }
});

test('transfer regression weak partial one-shot never adopts a different same-size hydrated head', async () => {
  const env = clientFor((req, url) => {
    if (url.pathname === '/api/v1') return ok({ upload: { chunkBytes: 4096, oneShotMaxBytes: 4096 } });
    if (req.method === 'POST') return ok({ fileId: 'f' });
    if (url.pathname.endsWith('/chunks')) return ok(manifestFor(bytes('NEW')));
    return ok({ id: 'f', name: 'x.bin', path: '/', size: 3, contentVersion: 9, checksum: fingerprint([hash('NEW')]) });
  });
  await assert.rejects(uploadFile(env.client, { name: 'x.bin', folderPath: '/', data: bytes('OLD') }), (e) => e.code === 'content_changed' && e.fileId === 'f');
  assertNoReplayOrDelete(env, 'POST');
});

test('transfer regression first weak completion uses exact raw proof when a legacy manifest is absent', async () => {
  for (const newerHead of [false, true]) {
    const env = sessionFixture({ omitChecksum: true, noManifest: true, newerHead });
    if (newerHead) await assert.rejects(uploadRevision(env.client, env.opts), (e) => e.code === 'content_changed');
    else {
      const result = await uploadRevision(env.client, env.opts);
      assert.equal(result.contentVersion, null); assert.equal(result.checksum, null); assert.equal(result.etag, null);
    }
    assert.equal(env.journal.events.at(-1).phase, 'completed');
    assert.equal(env.calls.filter((r) => new URL(r.url).pathname.endsWith('/complete')).length, 1);
    assert.equal(env.calls.filter((r) => new URL(r.url).pathname.endsWith('/content')).length, 1);
    assert.ok(env.calls.every((r) => r.method !== 'DELETE'));
  }
});

test('transfer regression only explicit pending/uploading sessions can receive parts', async () => {
  for (const status of ['deleting', 'aborted', 'failed', 'finished', '', null, undefined]) {
    const env = sessionFixture({ status });
    await assert.rejects(uploadRevision(env.client, env.opts), (e) => e.code === 'bad_response');
    assert.equal(env.calls.filter((r) => r.method === 'PUT' || r.method === 'DELETE' || new URL(r.url).pathname.endsWith('/complete')).length, 0);
  }
  const env = sessionFixture({ status: 'uploading' });
  assert.equal((await uploadRevision(env.client, env.opts)).id, 'f');
});

test('transfer regression preplanning recovery proves the original upload without any mutation', async () => {
  const env = sessionFixture({ lostComplete: true });
  await assert.rejects(uploadRevision(env.client, env.opts), (e) => e.isNetwork);
  const [key, uncertain] = [...env.journal.records][0];
  assert.equal(uncertain.phase, 'completing');
  const before = env.calls.length;
  const result = await recoverCommittedUpload(env.client, { key, uploadJournal: env.journal });
  assert.equal(result.file.id, 'f'); assert.equal(result.file.contentVersion, null); assert.equal(result.file.checksum, null);
  assert.equal(result.record.sourceHash, hash(env.source)); assert.equal(result.record.phase, 'completed');
  assert.deepEqual(result.record.receipt, {});
  assert.ok(env.calls.slice(before).every((r) => r.method === 'GET'));
  assert.equal(env.journal.events.at(-1).phase, 'completed');
  const events = env.journal.events.length;
  await recoverCommittedUpload(env.client, { key, uploadJournal: env.journal });
  assert.equal(env.journal.events.length, events, 'already committed journals are not rewritten');
});

test('transfer regression recovery preserves the committed journal when weak head proof fails', async () => {
  const env = sessionFixture({ lostComplete: true, newerHead: true });
  await assert.rejects(uploadRevision(env.client, env.opts), (e) => e.isNetwork);
  const key = [...env.journal.records.keys()][0]; const before = env.calls.length;
  await assert.rejects(recoverCommittedUpload(env.client, { key, uploadJournal: env.journal }), (e) => e.code === 'content_changed');
  assert.equal(env.journal.events.at(-1).phase, 'completed');
  assert.ok(env.calls.slice(before).every((r) => r.method === 'GET'));
  assert.equal(env.calls.filter((r) => new URL(r.url).pathname.endsWith('/complete')).length, 1);
});

test('transfer regression strong recovered receipt stays exact despite a newer different head', async () => {
  const receipt = { contentVersion: 2, checksum: null, etag: null, version: 1 };
  const env = sessionFixture({ lostComplete: true, newerHead: true, sessionReceipt: receipt });
  await assert.rejects(uploadRevision(env.client, env.opts), (e) => e.isNetwork);
  const key = [...env.journal.records.keys()][0]; const before = env.calls.length;
  const result = await recoverCommittedUpload(env.client, { key, uploadJournal: env.journal });
  for (const field of Object.keys(receipt)) assert.equal(result.file[field], receipt[field]);
  assert.equal(result.file.mtime, null);
  assert.ok(env.calls.slice(before).every((r) => r.method === 'GET' && !new URL(r.url).pathname.endsWith('/chunks')));
});

test('transfer regression recovery ignores foreign, terminal and invalid journals without network or persistence', async () => {
  const env = sessionFixture({ lostComplete: true });
  await assert.rejects(uploadRevision(env.client, env.opts), (e) => e.isNetwork);
  const [key, original] = [...env.journal.records][0];
  for (const change of [{ owner: 'foreign' }, { scope: '0'.repeat(64) }, { phase: 'aborting' }, { phase: 'aborted' }, { sourceHash: 'bad' }, { clientKey: 'bad' }]) {
    const stored = { ...clone(original), ...change }; env.journal.records.set(key, stored);
    const before = env.calls.length; const events = env.journal.events.length;
    assert.equal(await recoverCommittedUpload(env.client, { key, uploadJournal: env.journal }), null);
    assert.equal(env.calls.length, before); assert.equal(env.journal.events.length, events);
    assert.deepEqual(env.journal.records.get(key), stored);
  }
});

test('transfer regression full download rejects missing pinned ETag before any decoding', async () => {
  const env = downloadFixture(bytes('RAW'), { etag: null }); let decrypts = 0;
  await assert.rejects(downloadFile(env.client, { id: 'f', size: 3, etag: 'pinned' }, { forceFull: true,
    crypto: { decrypt: async (data) => { decrypts++; return data; } } }), (e) => e.code === 'content_changed');
  assert.equal(decrypts, 0);
});

test('transfer regression receipt validation returns a clone and does not mutate its inputs', async () => {
  const receipt = Object.freeze({ contentVersion: 2, checksum: null, etag: null });
  const file = Object.freeze({ id: 'f', size: 99, contentVersion: 9, checksum: 'head', etag: 'head' });
  const env = clientFor(() => assert.fail('strong receipt needs no head proof'));
  const result = await validateUploadReceipt(env.client, { file, receipt, size: 3, fingerprint: fingerprint([hash('RAW')]) });
  assert.equal(result.contentVersion, 2); assert.equal(result.checksum, null); assert.equal(result.etag, null); assert.equal(result.size, 3);
  assert.equal(file.contentVersion, 9); assert.equal(file.size, 99);
});

test('transfer regression partial PATCH receipts never inherit omitted content metadata from hydration', async () => {
  const data = bytes('RAW'); const expected = fingerprint([hash(data)]);
  for (const strong of [false, true]) {
    const ack = strong ? { contentVersion: 2 } : { updated: true };
    const env = clientFor((req, url) => {
      if (url.pathname === '/api/v1') return ok({ upload: { chunkBytes: 4096, oneShotMaxBytes: 4096 } });
      if (req.method === 'PATCH') return ok(ack);
      if (url.pathname.endsWith('/chunks')) return ok(manifestFor(data));
      return ok({ id: 'f', name: 'x.bin', path: '/', size: 3, contentVersion: 9, checksum: expected,
        etag: 'head-tag', version: 8, lastModified: 1700000009000, sha256: hash(data) });
    });
    const result = await env.client.patchContent('f', data);
    assert.equal(result.contentVersion, strong ? 2 : null); assert.equal(result.checksum, null); assert.equal(result.etag, null);
    assert.equal(result.version, null); assert.equal(result.mtime, null); assert.equal(result.sha256, null);
    assert.deepEqual(normalizeReceipt(result.raw), { contentVersion: strong ? 2 : null, checksum: null, etag: null, version: null });
    const validated = await validateUploadReceipt(env.client, { file: result, receipt: normalizeReceipt(result.raw), size: 3, fingerprint: expected, data });
    assert.equal(validated.contentVersion, strong ? 2 : null); assert.equal(validated.checksum, null);
    assertNoReplayOrDelete(env, 'PATCH');
  }
});

test('review regression conflicting manifest evidence rejects before content, progress or sink writes', async () => {
  const old = manifestFor(bytes('OLD')); const data = bytes('NEW');
  for (const forceFull of [false, true]) for (const evidence of [
    { etag: 'gen-1' }, { contentVersion: 1 }, { checksum: old.checksum },
    { contentVersion: 1, etag: 'gen-1', checksum: old.checksum },
  ]) {
    const map = manifestFor(data, 4096, { contentVersion: 2, etag: 'gen-2' });
    const env = downloadFixture(data, { manifest: map, etag: 'gen-2' });
    const file = { id: 'f', size: 0, ...evidence }; const original = clone(file);
    let progress = 0; let writes = 0;
    await assert.rejects(downloadFile(env.client, file, { forceFull, onProgress: () => progress++, sink: { write: async () => writes++ } }),
      (e) => e.code === 'content_changed');
    assert.equal(env.calls.length, 1); assert.equal(progress, 0); assert.equal(writes, 0);
    assert.deepEqual(file, original, 'conflicting manifests cannot hydrate stale file metadata');
  }
});

test('review regression manifest matching evidence and unknown legacy hints remain downloadable without archived-generation guesses', async () => {
  const data = bytes('NEW');
  for (const forceFull of [false, true]) for (const evidence of [
    { contentVersion: 2, etag: 'gen-2', checksum: fingerprint([hash(data)]) },
    { contentVersion: null, checksum: null, etag: null, version: 1 },
  ]) {
    const map = manifestFor(data, 4096, { content_version: 2, etag: 'gen-2', version: 9 });
    const env = downloadFixture(data, { manifest: map, etag: 'gen-2' }); const file = { id: 'f', size: 0, ...evidence };
    assert.equal(text(await downloadFile(env.client, file, { forceFull })), 'NEW');
    assert.equal(file.size, 3); assert.equal(file.contentVersion, evidence.contentVersion);
  }
  const map = manifestFor(data, 4096, { version: 9 }); delete map.checksum;
  const env = downloadFixture(data, { manifest: map });
  assert.equal(text(await downloadFile(env.client, { id: 'f', size: 3, contentVersion: 1, checksum: fingerprint([hash(data)]) })), 'NEW');
});

test('review regression recovery rejects explicit session and nested file identity conflicts before saving receipts', async () => {
  for (const conflicting of [
    { uploadId: 'other-session' }, { id: 'other-session' },
    { fileId: 'f', file: { id: 'g', contentVersion: 2, checksum: null } },
    { fileId: 'f', item: { id: 'g', contentVersion: 2, checksum: null } },
    { fileId: 'f', file: { id: 'f', fileId: 'g', contentVersion: 2, checksum: null } },
  ]) {
    const env = sessionFixture({ lostComplete: true, newerHead: true, sessionReceipt: { contentVersion: 2, checksum: null, ...conflicting } });
    await assert.rejects(uploadRevision(env.client, env.opts), (e) => e.isNetwork);
    const [key, original] = [...env.journal.records][0]; const before = env.calls.length; const events = env.journal.events.length;
    await assert.rejects(recoverCommittedUpload(env.client, { key, uploadJournal: env.journal }), (e) => e.code === 'bad_response');
    assert.equal(env.calls.length, before + 1); assert.equal(env.calls.at(-1).method, 'GET');
    assert.equal(new URL(env.calls.at(-1).url).pathname, '/api/v1/uploads/u');
    assert.equal(env.journal.events.length, events); assert.deepEqual(env.journal.records.get(key), original);
    assert.equal(original.phase, 'completing'); assert.ok(env.calls.every((r) => r.method !== 'DELETE'));
  }
});

test('review regression matching explicit recovery identities retain the nested strong receipt despite newer metadata', async () => {
  for (const uploadId of [undefined, 'u']) {
    const env = sessionFixture({ lostComplete: true, newerHead: true,
      sessionReceipt: { uploadId, fileId: 'f', file: { id: 'f', contentVersion: 2, checksum: null, etag: null } } });
    await assert.rejects(uploadRevision(env.client, env.opts), (e) => e.isNetwork);
    const key = [...env.journal.records.keys()][0]; const before = env.calls.length;
    const result = await recoverCommittedUpload(env.client, { key, uploadJournal: env.journal });
    assert.equal(result.file.id, 'f'); assert.equal(result.file.contentVersion, 2); assert.equal(result.file.checksum, null); assert.equal(result.file.etag, null);
    assert.equal(result.record.phase, 'completed'); assert.equal(result.record.receipt.contentVersion, 2);
    assert.ok(env.calls.slice(before).every((r) => r.method === 'GET' && !new URL(r.url).pathname.endsWith('/chunks')));
  }
});

test('review regression standalone strong PATCH validates the exact wire fingerprint without replay or rebasing', async () => {
  const backing = Buffer.from([99, 82, 65, 87, 88]); const data = backing.subarray(1, 4);
  const expected = fingerprint([hash(data)]);
  for (const wrongChecksum of [false, true]) {
    const receipt = { contentVersion: 2, checksum: wrongChecksum ? fingerprint([hash('NEW')]) : expected, etag: null, version: 1 };
    const env = clientFor((req, url) => {
      if (url.pathname === '/api/v1') return ok({ upload: { chunkBytes: 4096, oneShotMaxBytes: 4096 } });
      assert.equal(url.pathname, '/api/v1/files/f');
      if (req.method === 'PATCH') { assert.deepEqual(decode(req.body), { expectedVersion: 1, content: { inline: 'UkFX' } }); return ok({ file: { size: 3, ...receipt } }); }
      return ok({ id: 'f', name: 'x.bin', path: '/', size: 99, contentVersion: 9, checksum: 'later-head' });
    });
    if (wrongChecksum) await assert.rejects(env.client.patchContent('f', data, { expectedVersion: 1 }), (e) => e.code === 'bad_response' && /checksum|fingerprint/i.test(e.message));
    else {
      const result = await env.client.patchContent('f', data, { expectedVersion: 1 });
      assert.equal(result.size, 3); for (const field of Object.keys(receipt)) assert.equal(result[field], receipt[field]);
    }
    assertNoReplayOrDelete(env, 'PATCH'); assert.equal(env.calls.length, 3);
  }
});
