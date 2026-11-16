'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { TdriveClient, MAX_PART_BYTES } = require('../src/TdriveClient');
const { uploadRevision, uploadOneShot } = require('../src/UploadManager');
const { normalizeFileResponse, normalizeList, normalizeReceipt, remoteSig } = require('../src/normalize');
const { fingerprintOf, sha256hex } = require('../src/Chunker');

const bytes = (s) => new TextEncoder().encode(s).buffer;
const decode = (b) => JSON.parse(new TextDecoder().decode(b));
const clone = (x) => x == null ? x : JSON.parse(JSON.stringify(x));
const ok = (data) => ({ status: 200, headers: {}, body: bytes(JSON.stringify({ ok: true, data })) });
const fail = (status, code, details) => ({ status, headers: {}, body: bytes(JSON.stringify({ ok: false, error: { code, message: code, details } })) });
const CAPS = { paginatedDrive: true, batchOperations: true, folderCopy: true, folderTrashRestore: true, contentVersions: true,
  contentDefinedChunking: true, atomicIfEmpty: true, syncLeases: true, expectedContentVersion: true, uploadSessionListing: true };
const metadata = (extra = {}) => ({ id: 'f', name: 'x', path: '/', size: 3, contentVersion: 3, checksum: 'head-checksum', etag: 'head-etag', ...extra });

function clientFor(handler, opts = {}) {
  const calls = [];
  const client = new TdriveClient({ baseUrl: 'https://synthetic.invalid', apiKey: 'synthetic-key', maxRetries: 0, maxServerRetries: 0,
    ...opts, transport: async (req) => { calls.push(req); return handler(req, new URL(req.url)); } });
  return { client, calls };
}

test('lease defaults and optional renewal TTL match the documented bounds', async () => {
  const { client, calls } = clientFor(() => ok({ token: 'test-token' }));
  await client.acquireLease({ path: '/', owner: 'device' });
  await client.renewLease({ path: '/', token: 'test-token' });
  assert.equal(decode(calls[0].body).ttlSeconds, 60);
  assert.equal(Object.hasOwn(decode(calls[1].body), 'ttlSeconds'), false);
  for (const ttlSeconds of [1, 14, 301, 1.5]) await assert.rejects(client.acquireLease({ path: '/', owner: 'device', ttlSeconds }), /ttlSeconds/);
  assert.equal(calls.length, 2);
});

test('upload pagination and filters reject values outside the repaired API contract', async () => {
  const { client, calls } = clientFor(() => { throw new Error('invalid request must not reach transport'); });
  for (const params of [{ limit: 501 }, { offset: 100001 }, { active: 2 }, { active: '1' }, { status: 'uploading' }]) await assert.rejects(client.listUploads(params));
  assert.equal(calls.length, 0);
});

test('contentVersion is a separate strictly positive safe generation, never an archived ordinal', () => {
  for (const field of ['contentVersion', 'content_version']) {
    const f = normalizeFileResponse({ file: metadata({ contentVersion: undefined, [field]: 7, version: 2 }) });
    assert.equal(f.contentVersion, 7); assert.equal(f.version, 2);
    assert.equal(normalizeList({ files: [metadata({ contentVersion: undefined, [field]: 7 })] }).files[0].contentVersion, 7);
    for (const value of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, '1', true, NaN, Infinity]) {
      assert.throws(() => normalizeFileResponse(metadata({ [field]: value })), (e) => e.code === 'bad_response');
      assert.throws(() => normalizeList({ files: [metadata({ [field]: value })] }), (e) => e.code === 'bad_response');
    }
  }
  assert.equal(normalizeFileResponse({ id: 'f', version: 8 }).contentVersion, null);
  assert.equal(normalizeFileResponse(metadata({ contentVersion: null, version: 8 })).contentVersion, null);
  const f = normalizeFileResponse(metadata({ contentVersion: 7, version: 2 }));
  assert.notEqual(remoteSig(f), remoteSig({ ...f, contentVersion: 8 }));
  assert.equal(remoteSig(f), remoteSig({ ...f, version: 99 }));
});

test('receipt top-level fields and explicit null beat nested fields and snake aliases', () => {
  assert.deepEqual(normalizeReceipt({ content_version: 2, content_checksum: 'exact', content_etag: 'exact-tag', version: 1,
    file: metadata({ contentVersion: 3 }) }), { contentVersion: 2, checksum: 'exact', etag: 'exact-tag', version: 1 });
  const f = normalizeFileResponse({ contentVersion: null, checksum: null, etag: null, version: 1, file: metadata() });
  assert.equal(f.contentVersion, null); assert.equal(f.checksum, null); assert.equal(f.etag, null); assert.equal(f.version, 1);
  assert.deepEqual(normalizeReceipt({ content: { content_version: 2, checksum: 'exact', etag: 'exact-tag' } }), { contentVersion: 2, checksum: 'exact', etag: 'exact-tag' });
});

test('capabilities and unchanged 5 MiB limits share cached raw discovery in either order', async () => {
  assert.equal(MAX_PART_BYTES, 5242880);
  for (const first of ['limits', 'capabilities']) {
    const { client, calls } = clientFor(() => ok({ capabilities: CAPS, upload: { chunkBytes: 5242880, oneShotMaxBytes: 5242880 } }));
    await client[first]();
    const [caps, limits, info] = await Promise.all([client.capabilities(), client.limits(), client.info()]);
    assert.deepEqual(caps, CAPS); assert.deepEqual(limits, { chunkBytes: 5242880, oneShotMaxBytes: 5242880 });
    assert.deepEqual(info.capabilities, CAPS); assert.equal(calls.length, 1);
  }
});

test('unknown and concrete legacy capabilities preserve separate metadata PATCH calls', async () => {
  for (const discover of [false, true]) {
    const { client, calls } = clientFor((req, u) => ok(u.pathname === '/api/v1' ? {} : metadata()));
    if (discover) await client.capabilities();
    await client.move('f', { name: 'renamed', path: '/next/' });
    assert.deepEqual(calls.filter((r) => r.method === 'PATCH').map((r) => decode(r.body)), [{ path: '/next/' }, { name: 'renamed' }]);
  }
});

test('known v2 metadata move is one atomic PATCH and refuses metadata mixed with content', async () => {
  const { client, calls } = clientFor((req, u) => ok(u.pathname === '/api/v1' ? { capabilities: CAPS } : metadata()));
  await client.capabilities(); await client.move('f', { name: 'renamed', path: '/next/' });
  assert.deepEqual(calls.filter((r) => r.method === 'PATCH').map((r) => decode(r.body)), [{ name: 'renamed', path: '/next/' }]);
  await client._patch('f', { name: 'renamed', path: '/next/', favorite: true });
  const before = calls.length;
  for (const field of ['name', 'path', 'favorite']) await assert.rejects(client._patch('f', { [field]: 'x', content: { inline: '' } }), (e) => e.code === 'bad_request');
  assert.equal(calls.length, before);
});

test('folder deletion adds ifEmpty only for a known true atomicIfEmpty capability', async () => {
  for (const atomic of [undefined, false, true]) {
    const { client, calls } = clientFor(() => ok({ capabilities: atomic === undefined ? {} : { atomicIfEmpty: atomic } }));
    if (atomic !== undefined) await client.capabilities();
    await client.deleteFolder('/Vault/Empty/');
    const u = new URL(calls.at(-1).url);
    assert.deepEqual(Object.fromEntries(u.searchParams), { path: '/Vault/', name: 'Empty', ...(atomic === true ? { ifEmpty: '1' } : {}) });
  }
});

for (const shape of ['top', 'file', 'snake']) {
  test(`inline PATCH preserves ${shape} committed receipt despite a newer different head`, async () => {
    const receipt = { contentVersion: 2, checksum: await fingerprintOf([await sha256hex(bytes('NEW'))]), etag: 'committed-etag', version: 1 };
    const wire = shape === 'top' ? receipt : shape === 'file' ? { file: { ...receipt, size: 3 } }
      : { content_version: 2, content_checksum: receipt.checksum, content_etag: receipt.etag, version: 1 };
    const { client, calls } = clientFor((req, u) => {
      if (u.pathname === '/api/v1') return ok({ upload: { chunkBytes: 5242880, oneShotMaxBytes: 5242880 } });
      if (req.method === 'PATCH') { assert.deepEqual(decode(req.body), { expectedVersion: 1, content: { inline: 'TkVX' } }); return ok(wire); }
      assert.equal(u.pathname, '/api/v1/files/f'); return ok(metadata({ size: 99 }));
    });
    const f = await client.patchContent('f', bytes('NEW'), { expectedVersion: 1 });
    assert.equal(f.contentVersion, 2); assert.equal(f.version, 1); assert.equal(f.checksum, receipt.checksum); assert.equal(f.etag, receipt.etag); assert.equal(f.size, 3);
    assert.equal(calls.filter((r) => r.method === 'PATCH').length, 1);
  });
}

test('explicit null inline receipts stay unknown even when latest head has identical bytes', async () => {
  const data = bytes('NEW'); const checksum = await sha256hex(data);
  const fingerprint = await fingerprintOf([checksum]);
  const { client } = clientFor((req, u) => {
    if (u.pathname === '/api/v1') return ok({ upload: { chunkBytes: 5242880, oneShotMaxBytes: 5242880 } });
    if (req.method === 'PATCH') return ok({ content_version: null, checksum: null, etag: null, version: 1 });
    if (u.pathname.endsWith('/chunks')) return ok({ size: 3, chunkSize: 5242880, totalChunks: 1, chunks: [{ idx: 0, offset: 0, size: 3, checksum }], checksum: fingerprint });
    return ok(metadata({ checksum: fingerprint }));
  });
  const f = await client.patchContent('f', data, { expectedVersion: 1 });
  assert.equal(f.contentVersion, null); assert.equal(f.checksum, null); assert.equal(f.etag, null); assert.equal(f.version, 1);
});

test('invalid expectedVersion fails before discovery or mutation for inline and revision', async () => {
  const { client, calls } = clientFor(() => assert.fail('no transport'));
  for (const expectedVersion of [null, 0, -1, 1.5, '1', true, Number.MAX_SAFE_INTEGER + 1]) {
    await assert.rejects(client.patchContent('f', bytes('NEW'), { expectedVersion }), (e) => e.code === 'bad_request');
    await assert.rejects(uploadRevision(client, { id: 'f', data: bytes('NEW'), expectedVersion }), (e) => e.code === 'bad_request');
  }
  assert.equal(calls.length, 0);
});

test('inline version and lease conflicts never retry or fall back to a new write', async () => {
  for (const [status, code] of [[409, 'VERSION_CONFLICT'], [412, 'PRECONDITION_FAILED'], [423, 'LEASE_CONFLICT']]) {
    const { client, calls } = clientFor((req) => req.method === 'PATCH' ? fail(status, code)
      : ok({ upload: { chunkBytes: 5242880, oneShotMaxBytes: 5242880 } }));
    await assert.rejects(client.patchContent('f', bytes('NEW'), { expectedVersion: 1 }), (e) => e.status === status && e.code === code);
    assert.equal(calls.filter((r) => r.method === 'PATCH').length, 1); assert.equal(calls.length, 2);
  }
});

test('lease guard and headers apply to supported writes only, never read-only POSTs or controls', async () => {
  const { client, calls } = clientFor(() => ok({})); let ensured = 0;
  client.setLease({ path: '/Vault/', token: 'lease-one' }, async () => { ensured++; });
  const writes = [['POST', '/files'], ['PATCH', '/files/f'], ['DELETE', '/files/f'], ['POST', '/folders'], ['PATCH', '/folders/f'], ['DELETE', '/folders'],
    ['POST', '/uploads'], ['PUT', '/uploads/u/parts/0'], ['POST', '/uploads/u/complete'], ['DELETE', '/uploads/u'], ['POST', '/uploads/u/abort'],
    ['POST', '/files/f/shares'], ['DELETE', '/shares/s'], ['POST', '/shares/s/revoke'], ['POST', '/trash/f/restore'], ['POST', '/files/f/versions/1/restore']];
  const reads = [['GET', '/files'], ['HEAD', '/files/f/content'], ['POST', '/chunks/lookup'], ['POST', '/files/f/delta'], ['POST', '/sync/leases'],
    ['PATCH', '/sync/leases'], ['DELETE', '/sync/leases'], ['POST', '/unrelated']];
  for (const [method, path] of [...writes, ...reads]) await client.request({ method, path });
  assert.equal(ensured, writes.length);
  for (const req of calls.slice(0, writes.length)) { assert.equal(req.headers['X-Sync-Path'], '/Vault/'); assert.equal(req.headers['X-Sync-Lease'], 'lease-one'); }
  for (const req of calls.slice(writes.length)) { assert.equal(req.headers['X-Sync-Path'], undefined); assert.equal(req.headers['X-Sync-Lease'], undefined); }
});

test('lease guard is rerun before each write retry and renewal does not drop it', async () => {
  let attempts = 0; let ensured = 0;
  const { client, calls } = clientFor(() => ++attempts === 1 ? fail(503, 'UNAVAILABLE') : ok({}), { maxServerRetries: 1, sleep: async () => {} });
  client.setLease({ path: '/Vault/', token: 'lease-one' }, async () => { ensured++; if (ensured === 2) client.setLease({ path: '/Vault/', token: 'lease-two' }); });
  await client.request({ method: 'POST', path: '/files' });
  assert.equal(ensured, 2); assert.equal(calls[0].headers['X-Sync-Lease'], 'lease-one'); assert.equal(calls[1].headers['X-Sync-Lease'], 'lease-two');
});

test('lease guard rejection blocks transport and released guard context cannot send unleased write', async () => {
  const { client, calls } = clientFor(() => assert.fail('no transport'));
  client.setLease({ path: '/', token: 'lease-one' }, async () => { throw Object.assign(new Error('lease-one synthetic-key expired'), { status: 423, code: 'LEASE_CONFLICT' }); });
  await assert.rejects(client.request({ method: 'PATCH', path: '/files/f' }), (e) => e.status === 423 && !e.message.includes('lease-one') && !e.message.includes('synthetic-key'));
  client.setLease({ path: '/', token: 'lease-two' }, async () => client.clearLease());
  await assert.rejects(client.request({ method: 'PATCH', path: '/files/f' }), (e) => e.code === 'LEASE_CONFLICT');
  assert.equal(calls.length, 0);
});

test('clearing a lease during backoff invalidates its in-flight write, even after a new lease is installed', async () => {
  for (const reinstall of [false, true]) {
    const { client, calls } = clientFor(() => fail(503, 'UNAVAILABLE'), { maxServerRetries: 2, sleep: async () => {
      client.clearLease();
      if (reinstall) client.setLease({ path: '/', token: 'new-owner-token' }, async () => {});
    } });
    client.setLease({ path: '/', token: 'old-owner-token' }, async () => {});
    await assert.rejects(client.request({ method: 'PATCH', path: '/files/f' }), (e) => e.code === 'LEASE_CONFLICT');
    assert.equal(calls.length, 1);
  }
});

test('HTTP error details, nested arrays, codes and request IDs scrub all remembered secrets', async () => {
  const { client } = clientFor(() => ({ status: 423, headers: { 'x-request-id': 'lease-old-key' }, body: bytes(JSON.stringify({ ok: false,
    error: { code: 'LEASE_CONFLICT', message: 'key lease-old lease-new', details: { sessionIds: ['u1', 'u2'], nested: [{ message: 'key lease-old lease-new', token: 'unremembered' }], apiKey: 'key' } } })) }), { apiKey: 'key' });
  client.setLease({ path: '/', token: 'lease-old' }); client.setLease({ path: '/', token: 'lease-new' }); client.clearLease();
  await assert.rejects(client.request({ path: '/files' }), (e) => {
    assert.deepEqual(e.details.sessionIds, ['u1', 'u2']);
    assert.equal(e.details.nested[0].token, '***');
    for (const secret of ['key', 'lease-old', 'lease-new', 'unremembered']) assert.ok(!JSON.stringify({ message: e.message, requestId: e.requestId, details: e.details }).includes(secret));
    return e.code === 'LEASE_CONFLICT';
  });
});

test('retry:false disables network, HTTP and malformed-read retries per call', async () => {
  for (const response of [() => { throw new Error('lost'); }, () => fail(429, 'RATE_LIMIT'), () => fail(503, 'UNAVAILABLE'),
    () => ({ status: 200, headers: {}, body: bytes('not json') })]) {
    const { client, calls } = clientFor(response, { maxRetries: 5, maxServerRetries: 5, sleep: () => assert.fail('no retry delay') });
    await assert.rejects(client.request({ path: '/files', retry: false })); assert.equal(calls.length, 1);
  }
});

test('public lease controls use exact POST/PATCH/DELETE JSON routes without write headers', async () => {
  let ensured = 0;
  const { client, calls } = clientFor((req, u) => {
    if (u.pathname !== '/api/v1/sync/leases') return ok({});
    const body = decode(req.body);
    return ok({ path: body.path, owner: 'device-a', generation: 1, fence: 1, expiresAt: 1700000060000, serverTime: 1700000000000, ttlSeconds: 60,
      ...(req.method === 'POST' ? { token: 'lease-secret' } : {}) });
  });
  const acquired = await client.acquireLease({ path: '/Vault', owner: 'device-a', ttlSeconds: 60 });
  assert.equal(acquired.token, 'lease-secret'); assert.equal(client._lease, null);
  client.setLease(acquired, async () => { ensured++; });
  const renewed = await client.renewLease({ path: '/Vault', token: acquired.token, ttlSeconds: 60 });
  assert.equal(renewed.token, undefined); assert.equal(client._lease.token, 'lease-secret');
  await client.request({ method: 'POST', path: '/files' });
  assert.equal(ensured, 1); assert.equal(calls[2].headers['X-Sync-Lease'], 'lease-secret');
  await client.releaseLease({ path: '/Vault', token: acquired.token });
  assert.equal(client._lease, null);
  const controls = calls.filter((r) => new URL(r.url).pathname === '/api/v1/sync/leases');
  assert.deepEqual(controls.map((r) => [r.method, decode(r.body)]), [
    ['POST', { path: '/Vault/', owner: 'device-a', ttlSeconds: 60 }],
    ['PATCH', { path: '/Vault/', token: 'lease-secret', ttlSeconds: 60 }],
    ['DELETE', { path: '/Vault/', token: 'lease-secret' }],
  ]);
  assert.ok(controls.every((r) => r.headers['X-Sync-Lease'] === undefined && r.headers['X-Sync-Path'] === undefined));
  assert.ok(!client.redact('lease-secret').includes('lease-secret'));
});

test('lease acquisition response loss never blindly retries or recovers by owner label', async () => {
  for (const response of [() => { throw new Error('response lost'); }, () => fail(503, 'UNAVAILABLE'), () => fail(429, 'RATE_LIMIT')]) {
    const { client, calls } = clientFor(response, { maxRetries: 5, maxServerRetries: 5, sleep: () => assert.fail('must not blindly retry lease control') });
    await assert.rejects(client.acquireLease({ path: '/', owner: 'same-owner-label', ttlSeconds: 60 }));
    assert.equal(calls.length, 1); assert.equal(client._lease, null);
  }
});

test('invalid lease arguments and missing acquired token fail closed without guessed controls', async () => {
  const { client, calls } = clientFor(() => ok({ path: '/', expiresAt: 1, serverTime: 0 }));
  await assert.rejects(client.acquireLease({ path: '/', owner: '', ttlSeconds: 60 }), (e) => e.code === 'bad_request');
  await assert.rejects(client.acquireLease({ path: '/', owner: 'device-a', ttlSeconds: 0 }), (e) => e.code === 'bad_request');
  await assert.rejects(client.renewLease({ path: '/', token: '', ttlSeconds: 60 }), (e) => e.code === 'bad_request');
  await assert.rejects(client.releaseLease({ path: '/', token: '' }), (e) => e.code === 'bad_request');
  assert.equal(calls.length, 0);
  await assert.rejects(client.acquireLease({ path: '/', owner: 'device-a', ttlSeconds: 60 }), (e) => e.code === 'bad_response');
  assert.equal(calls.length, 1);
});

test('late renewal response cannot reinstall a cancelled lease or remove a replacement guard', async () => {
  let finish; let replacementEnsured = 0;
  const { client } = clientFor((req) => req.method === 'PATCH' ? new Promise((resolve) => { finish = resolve; }) : ok({}));
  client.setLease({ path: '/', token: 'old-token' }, async () => {});
  const renewing = client.renewLease({ path: '/', token: 'old-token', ttlSeconds: 60 });
  client.clearLease(); client.setLease({ path: '/', token: 'new-token' }, async () => { replacementEnsured++; });
  finish(ok({ path: '/', expiresAt: 1700000060000, serverTime: 1700000000000 }));
  await renewing;
  assert.equal(client._lease.token, 'new-token');
  await client.request({ method: 'POST', path: '/files' }); assert.equal(replacementEnsured, 1);
});

test('renew and release controls disable retries and scrub supplied tokens even without installed context', async () => {
  for (const method of ['renewLease', 'releaseLease']) {
    const { client, calls } = clientFor(() => { throw new Error('synthetic-key control-token response lost'); }, { maxRetries: 5, sleep: () => assert.fail('no retry') });
    await assert.rejects(client[method]({ path: '/', token: 'control-token', ttlSeconds: 60 }), (e) => !e.message.includes('control-token') && !e.message.includes('synthetic-key'));
    assert.equal(calls.length, 1);
  }
});

test('upload listing follows every explicit page and preserves filters without cancelling sessions', async () => {
  const { client, calls } = clientFor((req, u) => ok(u.searchParams.get('offset') === '0'
    ? { sessions: [{ uploadId: 'u1', status: 'pending', clientKey: 'owned' }], nextOffset: 7 }
    : { sessions: [{ id: 'u2', status: 'committed' }], nextOffset: null }));
  const sessions = await client.listUploads({ path: '/Vault/', active: true, status: 'pending', limit: 1, revision: true });
  assert.deepEqual(sessions.map((s) => s.uploadId || s.id), ['u1', 'u2']);
  assert.equal(calls.length, 2); assert.equal(new URL(calls[1].url).searchParams.get('offset'), '7');
  assert.deepEqual(Object.fromEntries(new URL(calls[0].url).searchParams), { active: '1', status: 'pending', revision: 'true', path: '/Vault/', limit: '1', offset: '0' });
  assert.ok(calls.every((r) => r.method === 'GET'));
});

test('upload listing rejects malformed shape, sessions and non-progressing continuations', async () => {
  for (const data of [{}, [], { sessions: null, nextOffset: null }, { sessions: [], nextOffset: 0 }, { sessions: [], nextOffset: '1' },
    { sessions: [] }, { sessions: [null], nextOffset: null }, { sessions: [{ id: 'u' }], nextOffset: null }, { sessions: [], nextOffset: 100001 }]) {
    const { client } = clientFor(() => ok(data));
    await assert.rejects(client.listUploads(), (e) => e.code === 'bad_response');
  }
});

function revisionServer(options = {}) {
  const records = new Map(); const events = []; const sessions = new Map(); const aborts = []; const opens = []; const completes = [];
  const journal = { owner: 'device-a', get: async (key) => clone(records.get(key)), set: async (key, record) => { records.set(key, clone(record)); events.push(clone(record)); } };
  let head = metadata({ size: 9, contentVersion: 1 });
  const env = clientFor(async (req, u) => {
    const path = u.pathname;
    if (path === '/api/v1') return ok({ capabilities: CAPS, upload: { chunkBytes: 4, oneShotMaxBytes: 4 } });
    if (path === '/api/v1/files/f/chunks') {
      const committed = [...sessions.values()].find((s) => s.status === 'committed');
      if (!committed) return ok({ chunkSize: 4 });
      const data = bytes('123456789'); const chunks = [];
      for (let offset = 0; offset < data.byteLength; offset += 4) chunks.push({ idx: chunks.length, offset, size: Math.min(4, data.byteLength - offset), checksum: await sha256hex(data.slice(offset, offset + 4)) });
      return ok({ chunkSize: 4, size: data.byteLength, totalChunks: chunks.length, chunks, checksum: committed.body.fingerprint });
    }
    if (path === '/api/v1/files/f') { if (options.failMetadata) return fail(503, 'METADATA_UNAVAILABLE'); return ok(head); }
    if (path === '/api/v1/uploads' && req.method === 'POST') {
      const body = decode(req.body); opens.push(body);
      if (options.openConflict) return fail(options.openConflict.status, options.openConflict.code);
      let session = [...sessions.values()].find((s) => s.clientKey === body.clientKey);
      if (!session) { session = { id: 'u' + (sessions.size + 1), clientKey: body.clientKey, status: 'pending', body, received: [] }; sessions.set(session.id, session); }
      if (options.onOpen) options.onOpen(body);
      return ok({ ...session, revision: true, chunkSize: 4, totalChunks: body.totalChunks, fingerprint: body.fingerprint });
    }
    const match = /\/uploads\/(u\d+)(?:\/parts\/(\d+)|\/complete)?$/.exec(path);
    if (match) {
      const session = sessions.get(match[1]);
      if (req.method === 'GET') return ok(session);
      if (req.method === 'DELETE') { assert.ok(['pending', 'uploading'].includes(session.status)); aborts.push(session.id); session.status = 'aborted'; return ok({}); }
      if (req.method === 'PUT') {
        if (options.partConflict) return fail(options.partConflict.status, options.partConflict.code);
        session.status = 'uploading'; session.received.push(Number(match[2])); return ok({});
      }
      completes.push(req.body === undefined ? undefined : decode(req.body));
      if (options.completeConflict) { head = metadata({ size: 9, contentVersion: 2 }); return fail(options.completeConflict.status, options.completeConflict.code); }
      session.status = 'committed'; session.fileId = 'f';
      const receipt = options.nullReceipt || options.nullTokens ? { content_version: options.nullTokens ? 2 : null, checksum: null, etag: null, version: 1 }
        : { content_version: 2, checksum: session.body.fingerprint, etag: 'committed-etag', version: 1 };
      head = metadata({ size: options.newerHead ? 17 : 9, contentVersion: options.newerHead ? 3 : 2, checksum: options.newerHead ? 'newer-checksum' : session.body.fingerprint });
      return ok({ fileId: 'f', ...receipt });
    }
    return fail(404, 'NOT_FOUND');
  });
  return { ...env, journal, records, events, sessions, aborts, opens, completes, options,
    freshClient: () => new TdriveClient({ baseUrl: 'https://synthetic.invalid', apiKey: 'synthetic-key', maxRetries: 0, maxServerRetries: 0, transport: env.client.transport }),
    head: () => head };
}
const revisionOpts = (server, extra = {}) => ({ id: 'f', data: bytes('123456789'), chunker: false, mtime: 100, uploadJournal: server.journal, expectedVersion: 1, ...extra });

for (const stage of ['open', 'part', 'complete']) for (const [status, code] of [[409, 'VERSION_CONFLICT'], [412, 'PRECONDITION_FAILED'], [423, 'LEASE_CONFLICT']]) {
  test(`${stage} ${code} preserves owned pending state without automatic abort`, async () => {
    const server = revisionServer({ [stage + 'Conflict']: { status, code } });
    await assert.rejects(uploadRevision(server.client, revisionOpts(server)), (e) => e.status === status && e.code === code);
    assert.deepEqual(server.aborts, []); assert.ok(['opening', 'uploading', 'completing'].includes(server.events.at(-1).phase));
    assert.equal(server.events.at(-1).body.expectedVersion, 1);
    if (stage === 'complete') assert.equal(server.head().contentVersion, 2);
  });
}

test('changed reviewed expectedVersion with identical bytes creates a new owned generation only on explicit replan', async () => {
  const server = revisionServer({ completeConflict: { status: 409, code: 'VERSION_CONFLICT' } });
  await assert.rejects(uploadRevision(server.client, revisionOpts(server)));
  const first = server.events.at(-1); assert.deepEqual(server.aborts, []);
  server.options.completeConflict = null;
  await uploadRevision(server.client, revisionOpts(server, { expectedVersion: 2 }));
  const last = server.events.at(-1);
  assert.equal(first.fingerprint, last.fingerprint); assert.equal(first.sourceHash, last.sourceHash);
  assert.notEqual(first.source, last.source); assert.notEqual(first.clientKey, last.clientKey); assert.equal(last.generation, first.generation + 1);
  assert.deepEqual(server.aborts, ['u1']); assert.deepEqual(server.completes, [{ expectedVersion: 1 }, { expectedVersion: 2 }]);
});

test('revision opening and completion use the captured expectedVersion even if caller options mutate', async () => {
  const server = revisionServer(); const opts = revisionOpts(server);
  server.options.onOpen = () => { opts.expectedVersion = 8; };
  await uploadRevision(server.client, opts);
  assert.equal(server.opens[0].expectedVersion, 1); assert.deepEqual(server.completes, [{ expectedVersion: 1 }]);
});

test('committed revision receipt survives newer-head hydration and completed fresh-client retry', async () => {
  const server = revisionServer({ newerHead: true, failMetadata: true });
  await assert.rejects(uploadRevision(server.client, revisionOpts(server)), (e) => e.code === 'METADATA_UNAVAILABLE');
  const committed = server.events.at(-1);
  assert.equal(committed.phase, 'completed'); assert.equal(committed.receipt.contentVersion, 2); assert.equal(committed.receipt.version, 1);
  server.options.failMetadata = false;
  const f = await uploadRevision(server.freshClient(), revisionOpts(server));
  assert.equal(f.contentVersion, 2); assert.equal(f.version, 1); assert.equal(f.checksum, committed.fingerprint); assert.equal(f.etag, 'committed-etag'); assert.equal(f.size, 9);
  assert.equal(server.opens.length, 1); assert.equal(server.completes.length, 1); assert.deepEqual(server.aborts, []);
  const serialized = JSON.stringify([...server.records]); assert.ok(!serialized.includes('synthetic-key')); assert.ok(!serialized.includes('123456789'));
});

test('null completion receipt fields remain unknown rather than adopting latest metadata', async () => {
  const server = revisionServer({ nullReceipt: true });
  const f = await uploadRevision(server.client, revisionOpts(server));
  assert.equal(f.contentVersion, null); assert.equal(f.checksum, null); assert.equal(f.etag, null);
  assert.deepEqual(server.events.at(-1).receipt, { contentVersion: null, checksum: null, etag: null, version: 1 });
});

test('known committed generation with null checksum and etag survives newer-head completed recovery', async () => {
  const server = revisionServer({ nullTokens: true, newerHead: true });
  for (const client of [server.client, server.freshClient()]) {
    const f = await uploadRevision(client, revisionOpts(server));
    assert.equal(f.contentVersion, 2); assert.equal(f.checksum, null); assert.equal(f.etag, null); assert.equal(f.size, 9);
  }
  assert.equal(server.opens.length, 1); assert.equal(server.completes.length, 1);
});

test('omitted expectedVersion keeps legacy body and source identity, and completed sessions are never aborted', async () => {
  const server = revisionServer();
  await uploadRevision(server.client, revisionOpts(server, { expectedVersion: undefined }));
  const first = server.events.at(-1);
  assert.ok(!Object.prototype.hasOwnProperty.call(first.body, 'expectedVersion')); assert.equal(server.completes[0], undefined);
  await uploadRevision(server.freshClient(), revisionOpts(server, { expectedVersion: undefined }));
  assert.equal(server.events.at(-1).source, first.source); assert.equal(server.opens.length, 1);
  await uploadRevision(server.freshClient(), revisionOpts(server, { expectedVersion: 2 }));
  assert.equal(server.opens.length, 2); assert.deepEqual(server.aborts, []);
});

test('one-shot upload hydration also preserves top-level committed receipt', async () => {
  const checksum = await fingerprintOf([await sha256hex(bytes('NEW'))]);
  const { client } = clientFor((req) => ok(req.method === 'POST' ? { fileId: 'f', content_version: 2, checksum, etag: 'committed-etag' } : metadata()));
  const f = await uploadOneShot(client, { name: 'x', folderPath: '/', data: bytes('NEW') });
  assert.equal(f.contentVersion, 2); assert.equal(f.checksum, checksum); assert.equal(f.etag, 'committed-etag');
});
