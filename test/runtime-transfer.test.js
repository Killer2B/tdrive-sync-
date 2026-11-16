'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { TdriveClient } = require('../src/TdriveClient');
const { uploadFile, uploadRevision } = require('../src/UploadManager');
const { downloadFile } = require('../src/DownloadManager');
const { normalizeFileResponse } = require('../src/normalize');
const { sha256hex, fingerprintOf } = require('../src/Chunker');

const bytes = (s) => new TextEncoder().encode(s).buffer;
const text = (b) => new TextDecoder().decode(b);
const ok = (data) => ({ status: 200, headers: {}, body: bytes(JSON.stringify({ ok: true, data })) });
const failure = (status, code) => ({ status, headers: {}, body: bytes(JSON.stringify({ ok: false, error: { code, message: code } })) });
const clone = (value) => value == null ? value : JSON.parse(JSON.stringify(value));

function journalFor(owner = 'device-a') {
  const records = new Map(); const events = [];
  return { owner, records, events,
    get: async (key) => clone(records.get(key)),
    set: async (key, record) => { records.set(key, clone(record)); events.push(clone(record)); },
  };
}

function serverFor(options = {}) {
  const sessions = new Map(); const files = new Map(); const calls = []; const opens = []; const puts = []; const aborts = [];
  let next = 0; let completeFailures = options.completeFailures || 0;
  const handler = async (req) => {
    const path = new URL(req.url).pathname;
    calls.push({ path, method: req.method });
    if (path === '/api/v1') return ok({ upload: { chunkBytes: 5242880, oneShotMaxBytes: 5242880 } });
    if (path === '/api/v1/uploads' && req.method === 'POST') {
      const body = JSON.parse(text(req.body)); opens.push(body);
      let s = [...sessions.values()].find((s) => s.body.clientKey === body.clientKey);
      if (!s) { s = { id: 'u' + ++next, status: 'pending', body, parts: new Map() }; sessions.set(s.id, s); }
      if (options.onOpen) options.onOpen(s, body);
      return ok({ id: s.id, uploadId: s.id, status: s.status, fileId: s.fileId, revision: !!body.baseId,
        chunkSize: options.sessionChunk || (body.chunker ? body.chunker.avg : 5242880), totalChunks: body.totalChunks,
        chunker: body.chunker, received: [...s.parts.keys()], fingerprint: body.fingerprint,
        base: body.baseId ? { chunkSize: 5242880, parts: [] } : undefined });
    }
    const match = /^\/api\/v1\/uploads\/([^/]+)(?:\/parts\/(\d+)|\/complete)?$/.exec(path);
    if (match) {
      const s = sessions.get(match[1]);
      if (!s) return failure(404, 'NOT_FOUND');
      if (req.method === 'GET') return ok({ id: s.id, status: s.status, fileId: s.fileId, revision: !!s.body.baseId, clientKey: s.body.clientKey });
      if (req.method === 'DELETE') {
        aborts.push(s.id);
        assert.ok(['pending', 'uploading'].includes(s.status), 'never abort committed/ready sessions');
        if (options.queuedAbort) return { status: 202, headers: {}, body: bytes(JSON.stringify({ ok: true, queued: true })) };
        s.status = 'aborted'; return ok({ aborted: true });
      }
      if (match[2] != null) {
        if (options.failPart) return failure(options.failPart.status || 400, options.failPart.code || 'INVALID_PART');
        const idx = Number(match[2]);
        assert.equal(await sha256hex(req.body), req.headers['X-Content-SHA256']);
        assert.equal(req.body.byteLength, Number(req.headers['X-Chunk-Size']));
        s.status = 'uploading'; s.parts.set(idx, req.body); puts.push({ id: s.id, idx, data: req.body });
        return ok({ idx });
      }
      if (path.endsWith('/complete')) {
        if (completeFailures-- > 0) return failure(503, 'RevisionDeferred');
        const hashes = []; const chunks = []; let offset = 0;
        for (let i = 0; i < s.body.totalChunks; i++) {
          const part = s.parts.get(i); assert.ok(part);
          const checksum = await sha256hex(part); hashes.push(checksum);
          chunks.push({ idx: i, offset, size: part.byteLength, checksum }); offset += part.byteLength;
        }
        assert.equal(await fingerprintOf(hashes), s.body.fingerprint);
        s.status = s.body.baseId ? 'committed' : 'ready'; s.fileId = options.wrongId || s.body.baseId || 'f' + s.id;
        const f = { id: s.fileId, name: s.body.name || 'x', path: s.body.path || '/', size: s.body.size, checksum: s.body.fingerprint };
        files.set(s.fileId, { ...f, chunks });
        if (options.completeAck) return ok(options.completeAck(s, f));
        return ok({ fileId: s.fileId, file: f, checksum: f.checksum });
      }
    }
    const file = /^\/api\/v1\/files\/([^/]+)(\/chunks)?$/.exec(path);
    if (file && req.method === 'GET') {
      const f = files.get(file[1]); if (!f) return failure(404, 'NOT_FOUND');
      if (options.failMetadata) return failure(503, 'METADATA_UNAVAILABLE');
      if (file[2]) return ok({ size: f.size, totalChunks: f.chunks.length, chunks: f.chunks, checksum: f.checksum });
      return ok({ ...f, id: options.metadataWrongId || f.id, checksum: options.metadataNoChecksum ? undefined : f.checksum });
    }
    return failure(404, 'NOT_FOUND');
  };
  return { sessions, files, calls, opens, puts, aborts, options,
    client: () => new TdriveClient({ baseUrl: 'https://synthetic.invalid', apiKey: 'synthetic-key', maxRetries: 0, maxServerRetries: 0, transport: handler }),
  };
}

const revisionOpts = (journal, extra = {}) => ({ id: 'base', name: 'x', data: new ArrayBuffer(50000), mtime: 1700000000000, uploadJournal: journal, ...extra });

test('two revision invocations resume the same owned session, parts and deterministic clientKey', async () => {
  const journal = journalFor(); const server = serverFor({ completeFailures: 1 });
  await assert.rejects(uploadRevision(server.client(), revisionOpts(journal)), (e) => e.code === 'RevisionDeferred' && e.uploadId === 'u1');
  assert.equal(journal.events.at(-1).phase, 'completing');
  const sent = server.puts.length;
  const result = await uploadRevision(server.client(), revisionOpts(journal));
  assert.equal(result.id, 'base'); assert.equal(server.sessions.size, 1); assert.equal(server.puts.length, sent);
  assert.equal(server.opens[0].clientKey, server.opens[1].clientKey);
  assert.equal(server.opens[0].fingerprint, server.opens[1].fingerprint);
  assert.equal(journal.events.at(-1).phase, 'completed'); assert.deepEqual(server.aborts, []);
  assert.equal(server.opens[0].chunker.avg, 2048); assert.equal(server.opens[0].chunker.max, 4096);
  assert.ok(server.puts.some((p) => p.data.byteLength > 2048));
});

test('new upload resume survives a fresh client and preserves exact fixed part bytes', async () => {
  const journal = journalFor(); const server = serverFor({ completeFailures: 1 });
  const opts = { name: 'x', folderPath: '/', data: new ArrayBuffer(5242881), mtime: 1700000000000, chunker: false, dedupe: false, uploadJournal: journal };
  await assert.rejects(uploadFile(server.client(), opts), (e) => e.code === 'RevisionDeferred');
  const result = await uploadFile(server.client(), opts);
  assert.equal(result.size, 5242881); assert.equal(server.puts.length, 2);
  assert.equal(server.puts[0].data.byteLength, 5242880); assert.equal(server.puts[1].data.byteLength, 1);
  assert.equal(server.sessions.size, 1);
});

test('actual large CDC session average permits larger maximum-sized parts', async () => {
  const server = serverFor();
  await uploadFile(server.client(), { name: 'x', folderPath: '/', data: new ArrayBuffer(6291456), dedupe: false, uploadJournal: journalFor() });
  const body = server.opens[0];
  assert.equal(body.totalChunks, 24); assert.equal(body.chunker.avg, 65536); assert.equal(body.chunker.max, 262144);
  assert.ok(server.puts.some((p) => p.data.byteLength > body.chunker.avg));
});

test('unjournaled retries preserve cross-client identity but cannot authorize automatic cancellation', async () => {
  const server = serverFor({ completeFailures: 1 });
  await assert.rejects(uploadRevision(server.client(), revisionOpts(undefined)));
  const count = server.puts.length;
  await uploadRevision(server.client(), revisionOpts(undefined));
  assert.equal(server.sessions.size, 1); assert.equal(server.puts.length, count);
  const failed = serverFor({ failPart: true });
  await assert.rejects(uploadRevision(failed.client(), revisionOpts(undefined)), (e) => e.code === 'INVALID_PART');
  assert.deepEqual(failed.aborts, []);
});

test('random encryption is identified by plaintext hash and cancels only its changed owned payload', async () => {
  const journal = journalFor(); const server = serverFor({ completeFailures: 1 }); let nonce = 0;
  const plain = bytes('P'.repeat(50000));
  const crypto = { encrypt: async (b) => bytes(String(++nonce) + text(b)) };
  await assert.rejects(uploadRevision(server.client(), revisionOpts(journal, { data: plain, crypto })), (e) => e.code === 'RevisionDeferred');
  const first = journal.events.at(-1);
  await uploadRevision(server.client(), revisionOpts(journal, { data: plain, crypto }));
  const last = journal.events.at(-1);
  assert.equal(first.sourceHash, await sha256hex(plain)); assert.equal(last.sourceHash, first.sourceHash);
  assert.notEqual(last.fingerprint, first.fingerprint); assert.notEqual(last.clientKey, first.clientKey);
  assert.deepEqual(server.aborts, ['u1']); assert.equal(server.sessions.get('u1').status, 'aborted');
  assert.ok(!JSON.stringify([...journal.records]).includes('PPP'));
});

test('same-sized source versions have distinct identities and committed revisions are never cancelled', async () => {
  const journal = journalFor(); const server = serverFor();
  await uploadRevision(server.client(), revisionOpts(journal, { data: bytes('A'.repeat(50000)) }));
  await uploadRevision(server.client(), revisionOpts(journal, { data: bytes('B'.repeat(50000)) }));
  assert.notEqual(server.opens[0].clientKey, server.opens[1].clientKey);
  assert.notEqual(server.opens[0].fingerprint, server.opens[1].fingerprint);
  assert.deepEqual(server.aborts, []);
});

test('different devices never share or cancel another device session', async () => {
  const server = serverFor({ completeFailures: 1 }); const a = journalFor('device-a'); const b = journalFor('device-b');
  await assert.rejects(uploadRevision(server.client(), revisionOpts(a)));
  await uploadRevision(server.client(), revisionOpts(b));
  assert.notEqual(server.opens[0].clientKey, server.opens[1].clientKey);
  assert.deepEqual(server.aborts, []); assert.equal(server.sessions.get('u1').status, 'uploading');
});

test('permanent part failure journals cancellation of only the owned pending session', async () => {
  const journal = journalFor(); const server = serverFor({ failPart: true });
  await assert.rejects(uploadRevision(server.client(), revisionOpts(journal)), (e) => e.code === 'INVALID_PART' && e.uploadId === 'u1');
  assert.deepEqual(server.aborts, ['u1']); assert.equal(journal.events.at(-1).phase, 'aborted');
  assert.ok(journal.events.some((r) => r.phase === 'aborting'));
  assert.ok(server.calls.every((c) => c.method !== 'DELETE' || c.path === '/api/v1/uploads/u1'));
});

test('authentication failures and outages keep the owned session available for retry', async () => {
  for (const status of [401, 403, 503]) {
    const journal = journalFor(); const server = serverFor({ failPart: { status, code: 'UNAVAILABLE' } });
    await assert.rejects(uploadRevision(server.client(), revisionOpts(journal)), (e) => e.status === status);
    assert.deepEqual(server.aborts, []); assert.equal(journal.events.at(-1).phase, 'uploading');
    server.options.failPart = false;
    await uploadRevision(server.client(), revisionOpts(journal));
    assert.equal(server.sessions.size, 1);
  }
});

test('202 queued abort acknowledgment remains journaled until server confirms cancellation', async () => {
  const journal = journalFor(); const server = serverFor({ failPart: true, queuedAbort: true });
  await assert.rejects(uploadRevision(server.client(), revisionOpts(journal)), (e) => e.code === 'INVALID_PART');
  assert.equal(journal.events.at(-1).phase, 'aborting');
  await assert.rejects(uploadRevision(server.client(), revisionOpts(journal)), (e) => e.code === 'upload_abort_pending' && e.retryable);
  assert.equal(server.opens.length, 1);
  server.sessions.get('u1').status = 'aborted'; server.options.failPart = false;
  await uploadRevision(server.client(), revisionOpts(journal));
  assert.equal(server.sessions.size, 2); assert.equal(journal.events.at(-1).phase, 'completed');
});

test('an edit shrinking to inline cancels the old owned pending revision before fallback', async () => {
  const journal = journalFor(); const server = serverFor({ completeFailures: 1 });
  await assert.rejects(uploadRevision(server.client(), revisionOpts(journal)));
  assert.equal(await uploadRevision(server.client(), revisionOpts(journal, { data: bytes('small') })), null);
  assert.deepEqual(server.aborts, ['u1']); assert.equal(journal.events.at(-1).phase, 'aborted');
});

test('lost metadata acknowledgment recovers a completed revision without new upload or abort', async () => {
  const journal = journalFor(); const server = serverFor({ completeAck: (s, f) => ({ file: { size: f.size, checksum: f.checksum } }), failMetadata: true });
  await assert.rejects(uploadRevision(server.client(), revisionOpts(journal)), (e) => e.code === 'METADATA_UNAVAILABLE');
  assert.equal(journal.events.at(-1).phase, 'completed');
  server.options.failMetadata = false; server.options.metadataNoChecksum = true;
  const result = await uploadRevision(server.client(), revisionOpts(journal));
  assert.equal(result.id, 'base'); assert.equal(server.opens.length, 1); assert.deepEqual(server.aborts, []);
  assert.equal(result.checksum, journal.events.at(-1).receipt.checksum, 'saved exact receipt proves the committed fingerprint');
  assert.ok(!server.calls.some((c) => c.path === '/api/v1/files/base/chunks'), 'recovery need not read a possibly newer manifest');
});

test('new completion acknowledgment finds its file via the owned session rather than guessing a name', async () => {
  const journal = journalFor(); const server = serverFor({ completeAck: () => ({ completed: true }) });
  const result = await uploadFile(server.client(), { name: 'x', folderPath: '/', data: new ArrayBuffer(5242881), chunker: false, dedupe: false, uploadJournal: journal });
  assert.equal(result.id, 'fu1'); assert.ok(server.calls.some((c) => c.path === '/api/v1/uploads/u1' && c.method === 'GET'));
});

test('wrong completion and hydrated metadata ids fail closed without deleting any file', async () => {
  for (const options of [{ wrongId: 'other' }, { metadataWrongId: 'other', completeAck: () => ({ updated: true }) }]) {
    const server = serverFor(options);
    await assert.rejects(uploadRevision(server.client(), revisionOpts(journalFor())), (e) => e.code === 'bad_response');
    assert.deepEqual(server.aborts, []);
    assert.ok(server.calls.every((c) => c.method !== 'DELETE'));
  }
});

test('journal persistence failure prevents opening a session', async () => {
  const server = serverFor(); const journal = { owner: 'device-a', get: async () => null, set: async () => { throw new Error('disk unavailable'); } };
  await assert.rejects(uploadRevision(server.client(), revisionOpts(journal)), /disk unavailable/);
  assert.equal(server.opens.length, 0);
});

test('a journal from another owner is rejected before any session mutation', async () => {
  const journal = journalFor(); const server = serverFor({ completeFailures: 1 });
  await assert.rejects(uploadRevision(server.client(), revisionOpts(journal)));
  const foreign = clone(journal.events.at(-1)); const count = server.opens.length;
  const other = { owner: 'device-b', get: async () => foreign, set: async () => assert.fail('must not overwrite foreign ownership') };
  await assert.rejects(uploadRevision(server.client(), revisionOpts(other)), (e) => e.code === 'bad_response');
  assert.equal(server.opens.length, count); assert.deepEqual(server.aborts, []);
});

test('a lost completion response recovers a committed session using the known base id', async () => {
  const journal = journalFor(); const server = serverFor({ completeFailures: 1 });
  await assert.rejects(uploadRevision(server.client(), revisionOpts(journal)));
  const s = server.sessions.get('u1');
  s.status = 'committed';
  const chunks = []; let offset = 0;
  for (const [idx, part] of s.parts) {
    chunks.push({ idx, offset, size: part.byteLength, checksum: await sha256hex(part) }); offset += part.byteLength;
  }
  server.files.set('base', { id: 'base', name: 'x', path: '/', size: s.body.size, checksum: s.body.fingerprint, chunks });
  const count = server.puts.length;
  const result = await uploadRevision(server.client(), revisionOpts(journal));
  assert.equal(result.id, 'base'); assert.equal(server.puts.length, count); assert.deepEqual(server.aborts, []);
});

test('partial and acknowledgment-only PATCH success hydrate known id with no mutation replay', async () => {
  const checksum = await fingerprintOf([await sha256hex(bytes('NEW'))]);
  for (const ack of [{ updated: true }, { file: { size: 3, checksum } }, { fileId: 'f' }]) {
    const calls = [];
    let stored = bytes('OLD');
    const client = new TdriveClient({ baseUrl: 'https://synthetic.invalid', apiKey: 'synthetic-key', transport: async (req) => {
      calls.push(req.method);
      const path = new URL(req.url).pathname;
      if (path === '/api/v1') return ok({ upload: { chunkBytes: 5242880, oneShotMaxBytes: 5242880 } });
      if (path === '/api/v1/files/f' && req.method === 'PATCH') {
        const payload = JSON.parse(text(req.body)); stored = Buffer.from(payload.content.inline, 'base64');
        return ok(ack);
      }
      if (path === '/api/v1/files/f/chunks') return ok({ size: stored.byteLength, totalChunks: 1,
        chunks: [{ idx: 0, offset: 0, size: stored.byteLength, checksum: await sha256hex(stored) }], checksum });
      if (path === '/api/v1/files/f' && req.method === 'GET') return ok({ id: 'f', name: 'x', path: '/', size: stored.byteLength, checksum });
      return failure(404, 'NOT_FOUND');
    } });
    const f = await client.patchContent('f', bytes('NEW'));
    assert.equal(f.id, 'f'); assert.equal(f.size, 3); assert.equal(calls.filter((m) => m === 'PATCH').length, 1);
    assert.equal(text(stored), 'NEW');
    assert.deepEqual(calls, ['GET', 'PATCH', 'GET', 'GET']);
    assert.equal(normalizeFileResponse({ updated: true }), null);
  }
});

test('metadata move hydrates after each documented separate PATCH and rejects wrong metadata id', async () => {
  const calls = []; let f = { id: 'f', name: 'old', path: '/', size: 3 };
  const client = new TdriveClient({ baseUrl: 'https://synthetic.invalid', apiKey: 'synthetic-key', transport: async (req) => {
    calls.push(req.method);
    if (req.method === 'PATCH') { f = { ...f, ...JSON.parse(text(req.body)) }; return ok({ updated: true }); }
    return ok(f);
  } });
  assert.deepEqual((await client.move('f', { path: '/New/', name: 'new' })).path, '/New/');
  assert.deepEqual(calls, ['PATCH', 'GET', 'PATCH', 'GET']);
  f = { ...f, id: 'other' };
  await assert.rejects(client.rename('f', 'bad'), (e) => e.code === 'bad_response');
});

test('malformed successful metadata is an error, not remote absence; optional time/checksum remain optional', async () => {
  for (const data of [{ updated: true }, { id: 'f' }, { id: 'other', name: 'x', size: 3 }]) {
    const client = new TdriveClient({ baseUrl: 'https://synthetic.invalid', apiKey: 'synthetic-key', transport: async () => ok(data) });
    await assert.rejects(client.getFile('f'), (e) => e.code === 'bad_response');
  }
  const client = new TdriveClient({ baseUrl: 'https://synthetic.invalid', apiKey: 'synthetic-key', transport: async () => ok({ id: 'f', name: 'x', size: 3 }) });
  const f = await client.getFile('f'); assert.equal(f.mtime, null); assert.equal(f.checksum, null);
});

test('strict matching ETags and raw checksums remain enforced before decrypt/decompress', async () => {
  const raw = bytes('ECSTORED'); const checksum = await sha256hex(raw); const phases = [];
  const manifest = { size: 8, totalChunks: 1, chunks: [{ idx: 0, offset: 0, size: 8, checksum }], checksum: await fingerprintOf([checksum]), etag: '"v1"' };
  for (const forceFull of [false, true]) {
    const client = new TdriveClient({ baseUrl: 'https://synthetic.invalid', apiKey: 'synthetic-key', transport: async (req) => req.url.endsWith('/chunks') ? ok(manifest) :
      { status: forceFull ? 200 : 206, headers: { etag: '"v1"', 'content-range': 'bytes 0-7/8', 'content-length': '8' }, body: raw } });
    assert.equal(text(await downloadFile(client, { id: 'f', size: 8 }, { forceFull, expectSha: checksum,
      crypto: { decrypt: async (b) => { phases.push(text(b)); return bytes('CPLAIN'); } }, compressor: { decompress: async (b) => { assert.equal(text(b), 'CPLAIN'); return bytes('PLAIN'); } },
    })), 'PLAIN');
  }
  assert.deepEqual(phases, ['ECSTORED', 'ECSTORED']);
  const wrong = new TdriveClient({ baseUrl: 'https://synthetic.invalid', apiKey: 'synthetic-key', transport: async (req) => req.url.endsWith('/chunks') ? ok(manifest) :
    { status: 200, headers: { etag: '"v2"' }, body: raw } });
  await assert.rejects(downloadFile(wrong, { id: 'f', size: 8 }, { forceFull: true }), (e) => e.code === 'content_changed');
});
