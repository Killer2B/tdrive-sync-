'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { TdriveClient } = require('../src/TdriveClient');
const { normalizeList, normalizeFileResponse, remoteSig } = require('../src/normalize');
const { uploadFile, uploadRevision } = require('../src/UploadManager');
const { downloadFile, readRange } = require('../src/DownloadManager');
const { chooseChunker, normalizeChunker, fingerprintOf, sha256hex, fixed } = require('../src/Chunker');

const bytes = (s) => new TextEncoder().encode(s).buffer;
const text = (b) => new TextDecoder().decode(b);
const decode = (b) => JSON.parse(text(b));
const success = (data) => ({ status: 200, headers: {}, body: bytes(JSON.stringify({ ok: true, data })) });
const failure = (status, code) => ({ status, headers: {}, body: bytes(JSON.stringify({ ok: false, error: { code, message: code } })) });
function clientFor(handler) {
  const calls = [];
  const client = new TdriveClient({ baseUrl: 'https://synthetic.invalid', apiKey: 'synthetic-key', maxRetries: 0, maxServerRetries: 0,
    transport: async (req) => { const u = new URL(req.url); calls.push(req); return handler(req, u); } });
  return { client, calls };
}

async function manifestFor(data, size = 4) {
  const chunks = [];
  for (let offset = 0; offset < data.byteLength; offset += size) chunks.push({ idx: chunks.length, offset, size: Math.min(size, data.byteLength - offset), checksum: await sha256hex(data.slice(offset, offset + size)) });
  return { id: 'f', size: data.byteLength, chunkSize: size, totalChunks: chunks.length, chunks,
    checksum: await fingerprintOf(chunks.map((c) => c.checksum)), checksumAlgorithm: 'sha256-of-part-sha256-list', etag: '"v1"' };
}

function contentResponse(req, data, etag = '"v1"') {
  const range = /^bytes=(\d+)-(\d+)$/.exec(req.headers.Range || '');
  if (!range) return { status: 200, headers: { etag, 'content-length': String(data.byteLength) }, body: data };
  const from = Number(range[1]); const end = Math.min(Number(range[2]), data.byteLength - 1);
  if (from >= data.byteLength) return { status: 416, headers: { etag, 'content-range': 'bytes */' + data.byteLength }, body: new ArrayBuffer(0) };
  return { status: 206, headers: { etag, 'content-range': `bytes ${from}-${end}/${data.byteLength}` }, body: data.slice(from, end + 1) };
}

test('folder DELETE uses only documented NFC parent path and name, even when passed an id', async () => {
  const { client, calls } = clientFor(() => success({}));
  assert.equal(await client.deleteFolder('/Vault/Cafe\u0301/', 'ignored-id'), true);
  assert.equal(calls.length, 1);
  const u = new URL(calls[0].url);
  assert.equal(calls[0].method, 'DELETE');
  assert.equal(u.pathname, '/api/v1/folders');
  assert.deepEqual(Object.fromEntries(u.searchParams), { path: '/Vault/', name: 'Caf\u00e9' });
  assert.equal(calls[0].body, undefined);
});

test('root/traversal folder targets are refused before transport', async () => {
  const { client, calls } = clientFor(() => success({}));
  for (const p of ['/', '/Vault/../', '/Vault//A/', '/Vault/a\\b/', 'Vault/A']) await assert.rejects(client.deleteFolder(p));
  assert.equal(calls.length, 0);
});

test('folder deletion errors do not trigger speculative routes', async () => {
  for (const status of [400, 401, 404, 409]) {
    const { client, calls } = clientFor(() => failure(status, 'NOT_FOUND'));
    await assert.rejects(client.deleteFolder('/Vault/A/'), (e) => e.status === status);
    assert.equal(calls.length, 1);
  }
});

test('legacy DELETE 204 is accepted once; malformed mutation success is not replayed', async () => {
  const legacy = clientFor(() => ({ status: 204, headers: {}, body: new ArrayBuffer(0) }));
  assert.equal(await legacy.client.deleteFolder('/Vault/A/'), true);
  assert.equal(legacy.calls.length, 1);
  const malformed = clientFor(() => ({ status: 200, headers: {}, body: bytes('<html>not an API envelope</html>') }));
  malformed.client.maxRetries = 4;
  await assert.rejects(malformed.client.createFolder('A', '/'), (e) => e.code === 'bad_response' && !e.retryable);
  assert.equal(malformed.calls.length, 1);
});

test('HTTP errors retain actionable status, code and request id without exposing the key', async () => {
  const { client } = clientFor(() => ({ status: 405, headers: { 'x-request-id': 'req-123' },
    body: bytes(JSON.stringify({ ok: false, error: { code: 'method_not_allowed', message: 'synthetic-key denied' } })) }));
  await assert.rejects(client.deleteFolder('/A/'), (e) => e.status === 405 && e.code === 'method_not_allowed'
    && e.requestId === 'req-123' && /HTTP 405 method_not_allowed.*req-123/.test(e.message) && !e.message.includes('synthetic-key'));
});

test('create uses one documented request and preserves trash conflicts', async () => {
  const { client, calls } = clientFor(() => failure(409, 'ALREADY_EXISTS'));
  await assert.rejects(client.createFolder('Cafe\u0301', '/Vault/'), (e) => e.code === 'ALREADY_EXISTS');
  assert.equal(calls.length, 1);
  assert.deepEqual(decode(calls[0].body), { name: 'Caf\u00e9', path: '/Vault/' });
});

test('unknown, malformed, or unsafe listings fail closed', () => {
  for (const raw of [null, {}, { data: [] }, { files: null }, { folders: 'A' }, { folders: [] }, { folders: ['ValidFolder'] }, { files: [null] },
    { files: [{ name: 'x', size: 1 }] }, { files: [{ id: 'f', name: 'x', size: -1 }] },
    { files: [{ id: 'f', name: '../x', size: 1 }] }, { files: [{ id: 'f', name: 'x', size: '' }] },
    { folders: ['..'] }, { folders: ['a/b'] }, { folders: [{ name: 'a/b' }] }]) {
    assert.throws(() => normalizeList(raw), (e) => e.code === 'bad_response');
  }
  assert.deepEqual(normalizeList({ files: [], folders: [] }), { files: [], folders: [] });
});

test('folder names are NFC, have no required ids, and fingerprint is not whole-file sha256', () => {
  const list = normalizeList({ folders: ['Cafe\u0301'], files: [{ id: 'f', name: 'x', size: 1, updated_at: '2026-11-08T00:00:00Z', checksum: 'fingerprint' }] });
  assert.equal(list.folders[0].name, 'Caf\u00e9');
  assert.equal(list.folders[0].id, null);
  assert.equal(list.files[0].sha256, null);
  assert.equal(list.files[0].checksum, 'fingerprint');
  assert.ok(list.files[0].mtime > 0);
});

test('metadata retains canonical parent path and content version for move/change detection', () => {
  const f = normalizeFileResponse({ file: { id: 'f', name: 'x', path: '/Cafe\u0301/', size: 1, version: 2, checksum: 'a', etag: '"v2"' } });
  assert.equal(f.path, '/Caf\u00e9/');
  assert.equal(f.version, 2);
  assert.equal(f.etag, '"v2"');
  assert.notEqual(remoteSig(f), remoteSig({ ...f, checksum: 'b' }));
});

test('pagination follows nextOffset, not folder count or requested page length', async () => {
  const { client, calls } = clientFor((req, u) => success(u.searchParams.get('offset') === '0'
    ? { files: [{ id: 'a', name: 'a', size: 1 }], folders: ['Cafe\u0301'], nextOffset: 2 }
    : { files: [{ id: 'b', name: 'b', size: 1 }], folders: ['Caf\u00e9'], nextOffset: null }));
  const result = await client.listFolder('/');
  assert.deepEqual(result.files.map((f) => f.id), ['a', 'b']);
  assert.equal(result.folders.length, 1);
  assert.equal(new URL(calls[1].url).searchParams.get('offset'), '2');
});

test('folders do not force another file page and invalid continuation fails closed', async () => {
  const { client, calls } = clientFor(() => success({ files: [], folders: Array.from({ length: 100 }, (_, i) => 'd' + i) }));
  assert.equal((await client.listFolder('/')).folders.length, 100);
  assert.equal(calls.length, 1);
  const bad = clientFor(() => success({ files: [], folders: [], nextOffset: 0 }));
  await assert.rejects(bad.client.listFolder('/'), (e) => e.code === 'bad_response');
});

test('exact 5 MiB limits are preserved and discovery never opens an upload', async () => {
  const { client, calls } = clientFor(() => success({ upload: { chunkBytes: 5242880, oneShotMaxBytes: 5242880 } }));
  assert.deepEqual(await client.limits(), { chunkBytes: 5242880, oneShotMaxBytes: 5242880 });
  assert.equal(await client.uploadChunkSize(), 5242880);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, 'GET');
});

test('invalid protocol limits are rejected, not guessed', async () => {
  for (const upload of [{ chunkBytes: 0 }, { chunkBytes: 4096 }, { chunkBytes: 1.5 }, { chunkBytes: 4096, oneShotMaxBytes: -1 }, { chunkBytes: 4096, oneShotMaxBytes: 8192 }]) {
    const { client } = clientFor(() => success({ upload }));
    await assert.rejects(client.limits(), (e) => e.code === 'bad_response');
  }
});

test('Retry-After is an authoritative floor beyond the local backoff cap', async () => {
  const { client } = clientFor(() => success({}));
  const waits = []; client.sleep = async (ms) => waits.push(ms);
  await client._wait(0, 120);
  assert.deepEqual(waits, [120000]);
});

test('CDC respects server bounds without silently rewriting invalid descriptors', () => {
  for (const n of [100, 1024 * 1024, 32 * 1024 * 1024, 512 * 1024 * 1024]) {
    const c = chooseChunker(n, 2048);
    assert.ok(c.min >= 1024 && c.min <= c.avg && c.avg <= c.max && c.max <= 2048);
  }
  assert.equal(normalizeChunker({ algo: 'buzhash', min: 1024, avg: 2048, max: 9999999, window: 48 }), null);
  assert.equal(normalizeChunker({ algo: 'buzhash', min: 1024, avg: 2048, max: 4096, window: 0 }), null);
  assert.throws(() => fixed(10, 0));
});

function uploadTransport(options = {}) {
  let opened; const parts = new Map(); const cap = options.cap || 5242880;
  const env = clientFor(async (req, u) => {
    if (u.pathname === '/api/v1') return success({ upload: { chunkBytes: cap, oneShotMaxBytes: cap } });
    if (u.pathname === '/api/v1/files/base/chunks') return success({ chunkSize: options.baseChunk || cap });
    if (u.pathname === '/api/v1/uploads') {
      opened = decode(req.body);
      return success({ id: 'u', status: 'pending', revision: !!opened.baseId, base: opened.baseId ? { parts: [] } : undefined,
        chunkSize: options.sessionChunk || options.baseChunk || cap, totalChunks: opened.totalChunks,
        received: options.received || [], fingerprint: opened.fingerprint, chunker: opened.chunker });
    }
    const m = /\/parts\/(\d+)$/.exec(u.pathname);
    if (m) {
      const idx = Number(m[1]); const body = req.body;
      assert.equal(await sha256hex(body), req.headers['X-Content-SHA256']);
      if (!opened.chunker && idx < opened.totalChunks - 1) assert.equal(body.byteLength, options.baseChunk || cap);
      parts.set(idx, body); return success({ idx });
    }
    if (u.pathname.endsWith('/complete')) {
      assert.equal(parts.size, opened.totalChunks);
      const hashes = [];
      for (let i = 0; i < parts.size; i++) hashes.push(await sha256hex(parts.get(i)));
      assert.equal(await fingerprintOf(hashes), opened.fingerprint);
      return success({ id: options.wrongId || opened.baseId || 'f', checksum: options.badChecksum || opened.fingerprint,
        file: { id: options.wrongId || opened.baseId || 'f', name: opened.name || 'x', size: opened.size } });
    }
    return failure(404, 'not_found');
  });
  return { ...env, parts, opened: () => opened };
}

test('first and subsequent fixed uploads use the exact advertised chunk size', async () => {
  const env = uploadTransport();
  env.client._uploadChunk = 5000000;
  const data = new ArrayBuffer(5242880 + 17);
  for (let i = 0; i < 2; i++) {
    await uploadFile(env.client, { name: 'x', folderPath: '/', data, mtime: i + 1, chunker: false, dedupe: false });
    assert.equal(env.parts.get(0).byteLength, 5242880);
    assert.equal(env.parts.get(1).byteLength, 17);
  }
});

test('session chunk mismatches and invalid received indices fail before any PUT', async () => {
  for (const o of [{ sessionChunk: 5000000 }, { received: [99] }]) {
    const env = uploadTransport(o);
    await assert.rejects(uploadFile(env.client, { name: 'x', folderPath: '/', data: new ArrayBuffer(5242881), chunker: false, dedupe: false }), (e) => e.code === 'bad_response');
    assert.equal(env.parts.size, 0);
  }
});

test('CDC uploads honor smaller authoritative server limits and verify the ordered fingerprint', async () => {
  const env = uploadTransport({ cap: 2048 });
  const f = await uploadFile(env.client, { name: 'x', folderPath: '/', data: new ArrayBuffer(5000), dedupe: false });
  assert.equal(f.size, 5000);
  assert.ok(env.opened().chunker.max <= 2048);
  assert.ok([...env.parts.values()].every((b) => b.byteLength <= 2048));
});

test('dedup lookup authentication failures cannot fall through to byte uploads', async () => {
  const env = uploadTransport();
  env.client.chunksLookup = async () => { throw Object.assign(new Error('Revoked'), { status: 401 }); };
  await assert.rejects(uploadFile(env.client, { name: 'x', folderPath: '/', data: new ArrayBuffer(5242881), chunker: false }), (e) => e.status === 401);
  assert.equal(env.parts.size, 0);
});

test('fixed revisions use the base chunk size and preserve the base id', async () => {
  const env = uploadTransport({ baseChunk: 2048 });
  const f = await uploadRevision(env.client, { id: 'base', name: 'x', data: new ArrayBuffer(5000), chunker: false });
  assert.equal(f.id, 'base');
  assert.equal(env.opened().baseId, 'base');
  assert.equal(env.opened().chunker, undefined);
  assert.equal(env.parts.get(0).byteLength, 2048);
});

test('wrong revision id is rejected without deleting the unexpected resource', async () => {
  const env = uploadTransport({ wrongId: 'unexpected', baseChunk: 2048 });
  await assert.rejects(uploadRevision(env.client, { id: 'base', data: new ArrayBuffer(5000), chunker: false }), /different file id/);
  assert.equal(env.calls.some((c) => c.method === 'DELETE'), false);
});

test('server fingerprint mismatch cannot be reported as upload success', async () => {
  const env = uploadTransport({ badChecksum: '0'.repeat(64) });
  await assert.rejects(uploadFile(env.client, { name: 'x', folderPath: '/', data: new ArrayBuffer(5242881), chunker: false, dedupe: false }), /checksum mismatch/);
});

test('multi-chunk download verifies individual hashes, not the top-level fingerprint as whole-file SHA', async () => {
  const data = bytes('ABCDEFGH'); const manifest = await manifestFor(data);
  assert.notEqual(manifest.checksum, await sha256hex(data));
  const { client } = clientFor((req, u) => u.pathname.endsWith('/chunks') ? success(manifest) : contentResponse(req, data));
  assert.equal(text(await downloadFile(client, { id: 'f', size: 8 }, { chunkBytes: 4, concurrency: 2 })), 'ABCDEFGH');
});

test('same-size corrupted content fails manifest verification', async () => {
  const manifest = await manifestFor(bytes('ABCDEFGH'));
  const { client } = clientFor((req, u) => u.pathname.endsWith('/chunks') ? success(manifest) : contentResponse(req, bytes('ABCXEFGH')));
  await assert.rejects(downloadFile(client, { id: 'f', size: 8 }, { chunkBytes: 4 }), /checksum mismatch/);
});

test('sequential and parallel downloads reject mixed ETags', async () => {
  for (const concurrency of [1, 3]) {
    const data = bytes('ABCDEFGH'); const manifest = await manifestFor(data);
    const { client, calls } = clientFor((req, u) => u.pathname.endsWith('/chunks') ? success(manifest)
      : contentResponse(req, data, req.headers.Range.startsWith('bytes=0-') ? '"v1"' : '"v2"'));
    await assert.rejects(downloadFile(client, { id: 'f', size: 8 }, { chunkBytes: 4, concurrency }), (e) => e.code === 'content_changed');
    assert.ok(calls.filter((c) => c.headers.Range).every((c) => c.headers['If-Range'] === '"v1"'));
  }
});

test('missing, mismatched, overlong, or impossible Content-Range is rejected', async () => {
  for (const range of ['', 'bytes 1-3/4', 'bytes 0-5/8', 'bytes 0-3/3', 'bytes 0-1/4']) {
    const { client } = clientFor(() => ({ status: 206, headers: { 'content-range': range }, body: bytes('ABCD') }));
    await assert.rejects(readRange(client, 'f', 0, 3), /Content-Range/);
  }
});

test('short range bodies resume at actual offset and ignored Range is sliced exactly', async () => {
  const { client, calls } = clientFor((req) => ({ status: 206, headers: { etag: '"v1"', 'content-range': req.headers.Range === 'bytes=0-3' ? 'bytes 0-3/4' : 'bytes 2-3/4' }, body: bytes(req.headers.Range === 'bytes=0-3' ? 'AB' : 'CD') }));
  assert.equal(text(await readRange(client, 'f', 0, 3)), 'ABCD');
  assert.equal(calls[1].headers.Range, 'bytes=2-3');
  const full = clientFor(() => ({ status: 200, headers: { etag: '"v1"' }, body: bytes('ABCDEFGH') }));
  assert.equal(text(await readRange(full.client, 'f', 2, 3)), 'CD');
});

test('malformed or inconsistent manifests fail before downloading content', async () => {
  for (const mutate of [(m) => { m.chunks[1].offset = 3; }, (m) => { m.chunks[0].checksum = 'bad'; }, (m) => { m.checksum = '0'.repeat(64); }]) {
    const m = await manifestFor(bytes('ABCDEFGH')); mutate(m);
    const { client, calls } = clientFor(() => success(m));
    await assert.rejects(downloadFile(client, { id: 'f', size: 8 }), (e) => e.code === 'bad_response');
    assert.equal(calls.length, 1);
  }
});

test('streaming rejects corruption before writing the affected chunk', async () => {
  const manifest = await manifestFor(bytes('ABCDEFGH'));
  const { client } = clientFor((req, u) => u.pathname.endsWith('/chunks') ? success(manifest) : contentResponse(req, bytes('XBCDEFGH')));
  const written = [];
  await assert.rejects(downloadFile(client, { id: 'f', size: 8 }, { sink: { write: async (b) => written.push(b) } }), /checksum mismatch/);
  assert.equal(written.length, 0);
});

test('manifest verification happens before decryption, including forceFull', async () => {
  const data = bytes('ENCRYPTED'); const manifest = await manifestFor(data);
  for (const forceFull of [false, true]) {
    let decrypts = 0;
    const { client } = clientFor((req, u) => u.pathname.endsWith('/chunks') ? success(manifest) : contentResponse(req, data));
    const out = await downloadFile(client, { id: 'f', size: 0 }, { forceFull, crypto: { decrypt: async (b) => { decrypts++; assert.equal(text(b), 'ENCRYPTED'); return bytes('PLAIN'); } } });
    assert.equal(text(out), 'PLAIN'); assert.equal(decrypts, 1);
  }
});

test('only absent legacy manifests fall back; manifest auth failures propagate', async () => {
  const data = bytes('ABCD');
  const env = clientFor((req, u) => u.pathname.endsWith('/chunks') ? failure(404, 'not_found') : contentResponse(req, data));
  assert.equal(text(await downloadFile(env.client, { id: 'f', size: 4 })), 'ABCD');
  const bad = clientFor(() => failure(401, 'unauthorized'));
  await assert.rejects(downloadFile(bad.client, { id: 'f', size: 4 }), (e) => e.status === 401);
  assert.equal(bad.calls.length, 1);
});

test('legacy stale size reconciles at a valid 416 without requiring an ETag on the error', async () => {
  const data = bytes('ABCD');
  const { client } = clientFor((req, u) => {
    if (u.pathname.endsWith('/chunks')) return failure(404, 'not_found');
    const res = contentResponse(req, data);
    if (res.status === 416) delete res.headers.etag;
    return res;
  });
  const f = { id: 'f', size: 8 };
  assert.equal(text(await downloadFile(client, f, { chunkBytes: 4 })), 'ABCD');
  assert.equal(f.size, 4);
});

function patchTransport(options = {}) {
  let stored = bytes('OLD'); let version = 1;
  const sent = [];
  const metadata = async () => {
    const manifest = await manifestFor(stored, 2);
    return { id: 'f', name: 'x', path: '/', size: stored.byteLength, lastModified: 1700000000000,
      version, etag: `"v${version}"`, checksum: options.omitChecksum ? undefined : options.metadataChecksum || manifest.checksum };
  };
  const env = clientFor(async (req, u) => {
    if (u.pathname === '/api/v1') return success({ upload: { chunkBytes: 5242880, oneShotMaxBytes: 5242880 } });
    if (u.pathname === '/api/v1/files/f' && req.method === 'PATCH') {
      const body = decode(req.body);
      assert.deepEqual(Object.keys(body), ['content']);
      const raw = Buffer.from(body.content.inline, 'base64'); sent.push(raw);
      if (!options.ignorePatch) { stored = raw; version++; }
      const committed = await metadata();
      if (options.otherHead) { stored = options.otherHead; version++; }
      return success(options.fullAck ? { file: committed } : options.ackChecksum ? { checksum: committed.checksum } : options.ack || { updated: true });
    }
    if (u.pathname === '/api/v1/files/f' && req.method === 'GET') return success(await metadata());
    if (u.pathname === '/api/v1/files/f/chunks') {
      if (options.manifestStatus) return failure(options.manifestStatus, 'MANIFEST_UNAVAILABLE');
      const manifest = await manifestFor(stored, 2); manifest.etag = `"v${version}"`;
      if (options.badManifestChecksum) manifest.checksum = '0'.repeat(64);
      return success(manifest);
    }
    if (u.pathname === '/api/v1/files/f/content') return contentResponse(req, stored, `"v${version}"`);
    return failure(404, 'not_found');
  });
  return { ...env, sent, stored: () => stored };
}

function onePatch(env) {
  assert.equal(env.sent.length, 1);
  assert.equal(env.calls.filter((r) => r.method === 'PATCH').length, 1, 'proof failure must not replay the committed mutation');
  assert.ok(env.calls.every((r) => r.method === 'PATCH' || r.method === 'GET'));
}

for (const fullAck of [false, true]) {
  test(`${fullAck ? 'full' : 'partial'} PATCH ACK followed by a different same-size GET head fails exact byte proof`, async () => {
    const env = patchTransport({ fullAck, otherHead: bytes('NEW') });
    await assert.rejects(env.client.patchContent('f', bytes('RAW')), (e) => e.code === 'content_changed');
    assert.equal(text(env.sent[0]), 'RAW'); assert.equal(text(env.stored()), 'NEW');
    assert.ok(env.calls.some((r) => new URL(r.url).pathname.endsWith('/chunks')));
    onePatch(env);
  });
}

test('PATCH success metadata alone cannot acknowledge ignored content of the same size', async () => {
  const env = patchTransport({ ignorePatch: true, fullAck: true });
  await assert.rejects(env.client.patchContent('f', bytes('RAW')), (e) => e.code === 'content_changed');
  assert.equal(text(env.stored()), 'OLD'); onePatch(env);
});

test('matching hydrated metadata and coherent multi-chunk manifest prove the exact sent raw bytes', async () => {
  const env = patchTransport({ ackChecksum: true });
  const backing = Buffer.from([99, 0, 255, 128, 65, 10, 66, 88]);
  const raw = backing.subarray(1, -1);
  const result = await env.client.patchContent('f', raw);
  assert.deepEqual(env.sent[0], raw); assert.equal(result.size, raw.byteLength);
  assert.equal(result.checksum, (await manifestFor(raw, 2)).checksum);
  assert.notEqual(result.checksum, await sha256hex(raw), 'fingerprint is the ordered chunk-hash list, not a whole-file SHA');
  assert.deepEqual(env.calls.map((r) => [r.method, new URL(r.url).pathname]), [
    ['GET', '/api/v1'], ['PATCH', '/api/v1/files/f'], ['GET', '/api/v1/files/f'], ['GET', '/api/v1/files/f/chunks'],
  ]);
  onePatch(env);
});

test('a matching manifest proves PATCH bytes when hydrated metadata omits its optional checksum', async () => {
  const env = patchTransport({ omitChecksum: true, ack: { file: { size: 3 } } });
  const result = await env.client.patchContent('f', bytes('RAW'));
  assert.equal(result.id, 'f'); assert.equal(result.checksum, null);
  assert.equal(text(env.stored()), 'RAW'); onePatch(env);
});

test('hydrated metadata checksum must agree with the manifest even when all sent chunk hashes match', async () => {
  const env = patchTransport({ metadataChecksum: 'f'.repeat(64) });
  await assert.rejects(env.client.patchContent('f', bytes('RAW')), (e) => e.code === 'content_changed');
  assert.equal(text(env.stored()), 'RAW'); onePatch(env);
});

test('PATCH proof rejects an incoherent top-level manifest checksum instead of trusting metadata', async () => {
  const env = patchTransport({ badManifestChecksum: true });
  await assert.rejects(env.client.patchContent('f', bytes('RAW')), (e) => e.code === 'bad_response');
  onePatch(env);
  assert.ok(!env.calls.some((r) => new URL(r.url).pathname.endsWith('/content')));
});

test('legacy 404 manifest falls back to an exact raw content GET after PATCH hydration', async () => {
  const env = patchTransport({ manifestStatus: 404 });
  const raw = Buffer.from([0, 255, 128, 65]);
  const result = await env.client.patchContent('f', raw);
  assert.equal(result.size, raw.length); assert.deepEqual(env.sent[0], raw);
  const read = env.calls.find((r) => new URL(r.url).pathname.endsWith('/content'));
  assert.ok(read); assert.equal(read.method, 'GET'); assert.equal(read.headers.Range, undefined);
  onePatch(env);
});

test('legacy raw GET cannot poison a PATCH ACK with a different same-size current head', async () => {
  const env = patchTransport({ manifestStatus: 404, otherHead: bytes('NEW') });
  await assert.rejects(env.client.patchContent('f', bytes('RAW')), (e) => e.code === 'content_changed');
  assert.equal(text(env.stored()), 'NEW'); onePatch(env);
  assert.ok(env.calls.some((r) => new URL(r.url).pathname.endsWith('/content')));
});

test('non-404 manifest failures cannot bypass PATCH proof with a raw content fallback', async () => {
  for (const status of [401, 403, 503]) {
    const env = patchTransport({ manifestStatus: status });
    await assert.rejects(env.client.patchContent('f', bytes('RAW')), (e) => e.status === status);
    onePatch(env);
    assert.ok(!env.calls.some((r) => new URL(r.url).pathname.endsWith('/content')));
  }
});

test('empty PATCH content is proven by the coherent empty manifest, not a metadata-only ACK', async () => {
  const env = patchTransport({ ackChecksum: true });
  const result = await env.client.patchContent('f', new ArrayBuffer(0));
  assert.equal(result.size, 0); assert.equal(env.sent[0].byteLength, 0);
  assert.equal(result.checksum, await fingerprintOf([])); onePatch(env);
});
