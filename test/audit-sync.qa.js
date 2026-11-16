'use strict';
// Dedicated expected-correct QA assertions. Intentionally outside npm test's *.test.js glob.
const test = require('node:test');
const assert = require('node:assert/strict');
const { SyncEngine } = require('../src/SyncEngine');
const { ManifestStore } = require('../src/ManifestStore');
const { TdriveClient } = require('../src/TdriveClient');
const { sha256Hex } = require('../src/util');
const { fingerprintOf } = require('../src/Chunker');
const { MemFS } = require('./helpers');

const clone = (value) => JSON.parse(JSON.stringify(value));
const fault = (status, code) => Object.assign(new Error(code), { status, code });

// Real client and transfer code, synthetic transport only. Trash continues reserving names;
// metadata generations and durable upload receipts are independent of the sync ledger.
function fixture(settingsOverride = {}) {
  let clock = 1700000000000; let serial = 0;
  const files = new Map(); const folders = new Set(['/', '/Vault/']);
  const trashFolders = new Set(); const sessions = new Map(); const idempotency = new Map(); const ops = []; const hooks = {};
  const settings = { remoteFolder: '/Vault/', remoteLock: false, incrementalScan: false,
    mode: 'two-way', deletionPolicy: 'trash', maxDeletesPerRun: 20, excludePatterns: [],
    syncFolders: [], cdcFiles: false, dedupeUploads: false, mergeTextFiles: false,
    streamLargeDownloads: false, ...settingsOverride };
  const meta = (f) => ({ id: f.id, name: f.name, path: f.path, size: f.bytes.length,
    lastModified: f.mtime, contentVersion: f.contentVersion, trashed: f.trashed });
  const ensure = (path) => { let p = '/'; for (const s of path.split('/').filter(Boolean)) { p += s + '/'; folders.add(p); } };
  const reserved = (path, name, except) => [...files.values()].some((f) => f.id !== except && f.path === path && f.name === name);
  const add = (path, name, bytes) => {
    if (!folders.has(path)) throw fault(404, 'FOLDER_NOT_FOUND');
    if (reserved(path, name)) throw fault(409, 'ALREADY_EXISTS');
    const f = { id: 'f' + ++serial, path, name, bytes: Buffer.from(bytes), contentVersion: 1, mtime: ++clock, trashed: false };
    files.set(f.id, f); return f;
  };
  const edit = (f, bytes) => { f.bytes = Buffer.from(bytes); f.contentVersion++; f.mtime = ++clock; };
  const move = (f, path, name) => {
    if (!folders.has(path)) throw fault(404, 'FOLDER_NOT_FOUND');
    if (reserved(path, name, f.id)) throw fault(409, 'ALREADY_EXISTS');
    f.path = path; f.name = name; f.mtime = ++clock;
  };
  const chunkMap = async (f) => {
    const chunks = [];
    for (let offset = 0, idx = 0; offset < f.bytes.length; offset += 4096, idx++) {
      const data = f.bytes.subarray(offset, offset + 4096);
      chunks.push({ idx, offset, size: data.length, checksum: await sha256Hex(data) });
    }
    return { size: f.bytes.length, chunkSize: 4096, totalChunks: chunks.length, chunks,
      checksum: await fingerprintOf(chunks.map((c) => c.checksum)), contentVersion: f.contentVersion };
  };
  const reply = (data, status = 200) => ({ status, headers: {}, body: Buffer.from(JSON.stringify(
    status < 400 ? { ok: true, data } : { ok: false, error: { code: data, message: data } })) });
  const transport = async (request) => {
    const u = new URL(request.url); const route = u.pathname.slice('/api/v1'.length);
    const method = request.method; const body = request.body ? Buffer.from(request.body) : Buffer.alloc(0);
    const j = () => JSON.parse(body.toString());
    ops.push({ method, route, query: Object.fromEntries(u.searchParams) });
    try {
      if (!route) return reply({ capabilities: { expectedContentVersion: true, atomicIfEmpty: true },
        upload: { oneShotMaxBytes: 128, chunkBytes: 4096 } });
      if (route === '/activity') return reply({ activity: [] });
      if (route === '/files' && method === 'GET') {
        const path = u.searchParams.get('path');
        if (hooks.list) await hooks.list(path);
        if (!folders.has(path)) return reply('NOT_FOUND', 404);
        return reply({ files: [...files.values()].filter((f) => !f.trashed && f.path === path).map(meta),
          folders: [...folders].filter((p) => p !== path && p.startsWith(path) && p.slice(path.length).split('/').filter(Boolean).length === 1)
            .map((p) => p.slice(path.length, -1)), nextOffset: null });
      }
      if (route === '/files' && method === 'POST') {
        if (body.length > 128) return reply('PAYLOAD_TOO_LARGE', 413);
        const key = request.headers['Idempotency-Key'];
        if (key && idempotency.has(key)) return reply(idempotency.get(key), 201);
        const f = add(u.searchParams.get('path'), u.searchParams.get('name'), body);
        const ack = hooks.createAck ? await hooks.createAck(f) : { file: meta(f) };
        if (key) idempotency.set(key, clone(ack));
        return reply(ack, 201);
      }
      const fileMatch = /^\/files\/([^/]+)(?:\/(chunks|content))?$/.exec(route);
      if (fileMatch) {
        const f = files.get(fileMatch[1]);
        if (!f) return reply('NOT_FOUND', 404);
        if (method === 'GET' && !fileMatch[2]) {
          if (hooks.get) await hooks.get(f);
          return reply(meta(f));
        }
        if (fileMatch[2] === 'chunks') return reply(await chunkMap(f));
        if (fileMatch[2] === 'content') return { status: 200, headers: {}, body: Buffer.from(f.bytes) };
        if (method === 'DELETE') { f.trashed = true; return reply({ id: f.id }); }
        if (method === 'PATCH') {
          const value = j();
          if (value.content) {
            if (value.expectedVersion != null && value.expectedVersion !== f.contentVersion) return reply('VERSION_CONFLICT', 412);
            edit(f, Buffer.from(value.content.inline, 'base64'));
          } else move(f, value.path || f.path, value.name || f.name);
          const ack = { file: meta(f) };
          if (hooks.patch) await hooks.patch(f);
          return reply(ack);
        }
      }
      if (route === '/folders' && method === 'POST') {
        const value = j(); const path = value.path + value.name + '/';
        if (folders.has(path) || trashFolders.has(path)) return reply('ALREADY_EXISTS', 409);
        if (!folders.has(value.path)) return reply('PARENT_MISSING', 404);
        folders.add(path); return reply({ name: value.name, path: value.path }, 201);
      }
      if (route === '/folders' && method === 'DELETE') {
        const path = u.searchParams.get('path') + u.searchParams.get('name') + '/';
        if (!folders.has(path)) return reply('NOT_FOUND', 404);
        if ([...files.values()].some((f) => !f.trashed && f.path.startsWith(path)) || [...folders].some((p) => p !== path && p.startsWith(path))) return reply('FOLDER_NOT_EMPTY', 409);
        if ([...sessions.values()].some((s) => ['pending', 'uploading'].includes(s.status) && (s.body.path || files.get(s.body.baseId).path).startsWith(path))) return reply('UPLOAD_IN_PROGRESS', 409);
        folders.delete(path); trashFolders.add(path); return reply({});
      }
      if (route === '/uploads' && method === 'POST') {
        const value = j();
        let s = [...sessions.values()].find((entry) => entry.body.clientKey === value.clientKey);
        if (!s) {
          if (value.baseId && !files.has(value.baseId)) return reply('NOT_FOUND', 404);
          s = { id: 'u' + ++serial, body: value, status: 'pending', parts: new Map(), offsets: new Map() }; sessions.set(s.id, s);
        } else assert.deepEqual(value, s.body, 'idempotent open must preserve its immutable upload body');
        return reply({ id: s.id, status: s.status, fileId: s.fileId, ...s.receipt,
          chunkSize: 4096, totalChunks: s.body.totalChunks, received: [...s.parts.keys()],
          revision: !!s.body.baseId, ...(s.body.baseId ? { base: { parts: [] } } : {}) }, 201);
      }
      const uploadMatch = /^\/uploads\/([^/]+)(?:\/parts\/(\d+)|\/complete)?$/.exec(route);
      if (uploadMatch) {
        const s = sessions.get(uploadMatch[1]); if (!s) return reply('NOT_FOUND', 404);
        if (method === 'GET') return reply({ id: s.id, status: s.status, fileId: s.fileId, clientKey: s.body.clientKey, ...s.receipt });
        if (method === 'DELETE') {
          if (!['pending', 'uploading'].includes(s.status)) return reply('FILE_STATE_CONFLICT', 409);
          s.status = 'aborted'; return reply({ aborted: true });
        }
        if (uploadMatch[2] != null) {
          const idx = Number(uploadMatch[2]); const offset = idx * 4096;
          assert.equal(body.length, Math.min(4096, s.body.size - offset), 'documented fixed part length');
          assert.equal(await sha256Hex(body), request.headers['X-Content-SHA256']);
          assert.equal(Number(request.headers['X-Chunk-Offset']), offset);
          s.parts.set(idx, body); s.offsets.set(idx, offset); s.status = 'uploading'; return reply({ idx });
        }
        if (s.receipt) return reply(s.receipt);
        if (hooks.beforeComplete) await hooks.beforeComplete(s);
        const parts = [...Array(s.body.totalChunks).keys()].map((i) => s.parts.get(i));
        assert.ok(parts.every(Boolean), 'all fixed chunks must be present');
        const bytes = Buffer.concat(parts); assert.equal(bytes.length, s.body.size);
        assert.equal(await fingerprintOf(await Promise.all(parts.map((p) => sha256Hex(p)))), s.body.fingerprint);
        let f;
        if (s.body.baseId) {
          f = files.get(s.body.baseId);
          if (s.body.expectedVersion != null && s.body.expectedVersion !== f.contentVersion) return reply('VERSION_CONFLICT', 412);
          edit(f, bytes); s.status = 'committed';
        } else { f = add(s.body.path, s.body.name, bytes); s.status = 'ready'; }
        s.fileId = f.id; s.receipt = { file: meta(f), fileId: f.id, contentVersion: f.contentVersion, checksum: s.body.fingerprint };
        if (hooks.complete) await hooks.complete(s, f);
        return reply(s.receipt);
      }
      return reply('NOT_FOUND', 404);
    } catch (e) {
      if (e.isNetwork || e instanceof assert.AssertionError) throw e;
      return reply(e.code || e.message, e.status || 500);
    }
  };
  const device = ({ blob = null, local = new MemFS(), owner } = {}) => {
    let saved = blob;
    const storage = { read: async () => saved, write: async (value) => { saved = value; } };
    const manifest = new ManifestStore(storage, () => clock);
    if (!blob) manifest.data.meta.deviceId = '11111111-1111-4111-a111-111111111111';
    const client = new TdriveClient({ baseUrl: 'https://qa-sync.invalid', apiKey: 'synthetic-only-key', transport,
      maxRetries: 0, maxServerRetries: 0 });
    const engine = new SyncEngine({ client, local, manifest, getSettings: () => settings, now: () => clock,
      downloadFn: async (_, remote, options) => {
        const bytes = Buffer.from(files.get(remote.id).bytes);
        if (options.expectSha) assert.equal(await sha256Hex(bytes), options.expectSha);
        return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.length);
      }, manifestFn: async (_, id) => chunkMap(files.get(id)) });
    const load = async () => { await manifest.load(); if (owner) manifest.data.meta.deviceId = owner; };
    return { engine, local, manifest, client, load, saved: () => saved };
  };
  return { settings, files, folders, sessions, trashFolders, ops, hooks, device, add, edit, move, ensure,
    live: (path, name) => [...files.values()].find((f) => !f.trashed && f.path === path && f.name === name) };
}

async function baseline(f, paths = { 'a.md': 'BASE', 'keep.md': 'KEEP' }) {
  const d = f.device();
  for (const [path, text] of Object.entries(paths)) d.local.put(path, text);
  const report = await d.engine.syncNow(); assert.equal(report.status, 'ok', JSON.stringify(report));
  return d;
}

test('QA-SYNC-001 control: explicit local rename plus edits keeps one remote identity', async () => {
  const f = fixture(); const d = await baseline(f); const id = f.live('/Vault/', 'a.md').id;
  d.local.files.delete('a.md'); d.local.put('new.md', 'LOCL');
  d.manifest.data.meta.moveHints = { 'a.md': { to: 'new.md' } };
  const r = await d.engine.syncNow();
  assert.equal(r.moved, 1); assert.equal(r.conflicts, 0);
  assert.equal(f.live('/Vault/', 'new.md').id, id); assert.equal(f.live('/Vault/', 'new.md').bytes.toString(), 'LOCL');
});

test('QA-SYNC-002 remote rename plus local and remote edits must remain an identity-linked conflict', async (t) => {
  const f = fixture(); const d = await baseline(f); const remote = f.live('/Vault/', 'a.md');
  d.local.put('a.md', 'LOCL'); f.move(remote, '/Vault/', 'renamed.md'); f.edit(remote, 'REMO');
  const r = await d.engine.syncNow(); const next = await d.engine.syncNow();
  t.diagnostic(JSON.stringify({ first: r, second: next, remote: [...f.files.values()].map(metaForDiagnostic), local: [...d.local.files.keys()], conflicts: d.manifest.conflictKeys() }));
  assert.equal(r.uploaded, 0, 'a known remotely renamed identity must not silently fork into a fresh upload');
  assert.ok(r.conflicts > 0, 'both edits require a linked conflict, not two silently converged independent notes');
});

test('QA-SYNC-003 ambiguous basename candidates must not arbitrarily reuse a remote identity', async (t) => {
  const f = fixture(); const d = await baseline(f, { 'A/a.md': 'AAAA', 'B/a.md': 'BBBB', 'keep.md': 'KEEP' });
  const originals = ['A', 'B'].map((dir) => f.live('/Vault/' + dir + '/', 'a.md').id);
  d.local.files.delete('A/a.md'); d.local.files.delete('B/a.md'); d.local.put('C/a.md', 'EDIT');
  const r = await d.engine.syncNow(); const result = f.live('/Vault/C/', 'a.md');
  t.diagnostic(JSON.stringify({ report: r, originalIds: originals, destination: metaForDiagnostic(result), remote: [...f.files.values()].map(metaForDiagnostic) }));
  assert.equal(r.moved, 0, 'two distinct same-basename sources are not evidence of one specific move');
  assert.ok(!result || !originals.includes(result.id), 'never attach the new note to an arbitrarily chosen old share/history identity');
});

test('QA-SYNC-004 approval of old path/action must not authorize a later rebaselined content generation', async (t) => {
  const f = fixture({ maxDeletesPerRun: 0 }); const d = await baseline(f, { 'a.md': 'BASE', 'b.md': 'BBBB', 'keep.md': 'KEEP' });
  const a = f.live('/Vault/', 'a.md');
  d.local.files.delete('a.md'); d.local.files.delete('b.md');
  const blocked = await d.engine.syncNow();
  const reviewed = blocked.blockedDeletes.items.map((item) => ({ ...item,
    remoteId: d.manifest.getFile(item.path).remoteId, remoteContentVersion: d.manifest.getFile(item.path).remoteContentVersion }));
  const approvedScope = d.manifest.data.meta.syncScope;
  d.local.put('a.md', 'BASE'); f.edit(a, 'NEWW');
  const intervening = await d.engine.syncNow(); assert.equal(intervening.downloaded, 1); assert.equal(d.local.text('a.md'), 'NEWW');
  d.local.files.delete('a.md');
  const r = await d.engine.syncNow({ approveDeletes: true, approvedDeletes: reviewed, approvedScope });
  t.diagnostic(JSON.stringify({ reviewed, currentGeneration: a.contentVersion, report: r, trashed: a.trashed, deletionAudit: d.manifest.data.meta.deletions }));
  assert.equal(r.deletedRemote, 0, 'an old modal cannot approve deleting an unreviewed generation merely because its path matches');
  assert.equal(a.trashed, false);
});

test('QA-SYNC-005 control: post-transfer scan race preserves an exact committed response baseline', async () => {
  const f = fixture(); const d = await baseline(f);
  d.local.put('a.md', 'AAAA'); f.hooks.patch = async (remote) => { if (remote.name === 'a.md') f.edit(remote, 'BBBB'); };
  const first = await d.engine.syncNow(); assert.equal(first.uploaded, 1);
  delete f.hooks.patch;
  const second = await d.engine.syncNow(); assert.equal(second.downloaded, 1); assert.equal(d.local.text('a.md'), 'BBBB');
});

test('QA-SYNC-006 ID-only create receipt followed by raced metadata must not acknowledge unseen remote bytes', async (t) => {
  const f = fixture(); const d = f.device(); d.local.put('a.md', 'AAAA');
  f.hooks.createAck = async (remote) => ({ fileId: remote.id });
  let injected = false;
  f.hooks.get = async (remote) => { if (!injected) { injected = true; f.edit(remote, 'BBBB'); } };
  const first = await d.engine.syncNow(); delete f.hooks.get; delete f.hooks.createAck;
  const second = await d.engine.syncNow(); const third = await d.engine.syncNow();
  const remote = f.live('/Vault/', 'a.md');
  t.diagnostic(JSON.stringify({ injected, first, second, third, local: d.local.text('a.md'), remote: remote.bytes.toString(), entry: d.manifest.getFile('a.md') }));
  assert.ok(d.local.text('a.md') === remote.bytes.toString() || d.manifest.conflictKeys().length > 0,
    'metadata GET is a newer head, not a receipt proving the uploaded bytes are still current; silent divergence is unsafe');
});

test('QA-SYNC-007 copied same-scope ledger on a fresh partial installation must not propagate missing-local deletions', async (t) => {
  const f = fixture(); const original = await baseline(f); const remote = f.live('/Vault/', 'a.md');
  const local = new MemFS(); local.put('keep.md', 'KEEP');
  const fresh = f.device({ blob: original.saved(), local, owner: '22222222-2222-4222-a222-222222222222' }); await fresh.load();
  const r = await fresh.engine.syncNow();
  t.diagnostic(JSON.stringify({ report: r, originalOwner: original.manifest.data.meta.deviceId, freshOwner: fresh.manifest.data.meta.deviceId, trashed: remote.trashed, local: [...fresh.local.files.keys()] }));
  assert.equal(r.deletedRemote, 0, 'a copied baseline is not evidence that this installation intentionally deleted a note');
  assert.equal(remote.trashed, false); assert.equal(fresh.local.text('a.md'), 'BASE');
});

test('QA-SYNC-008 full sync after lost committed revision acknowledgement must recover journal before classifying a later edit', async (t) => {
  const f = fixture(); const base = 'A'.repeat(8193); const committed = 'B'.repeat(8193); const latest = 'C'.repeat(8193);
  const d = await baseline(f, { 'a.bin': base, 'keep.md': 'KEEP' }); const remote = f.live('/Vault/', 'a.bin');
  d.local.put('a.bin', committed); let lost = false;
  f.hooks.complete = async (session) => { if (session.body.baseId && !lost) { lost = true; throw Object.assign(new Error('lost completion acknowledgement'), { isNetwork: true }); } };
  const first = await d.engine.syncNow(); assert.equal(first.errors, 1); assert.equal(remote.bytes.toString(), committed);
  assert.ok(Object.values(d.manifest.data.meta.uploadSessions).some((r) => r.phase === 'completing'));
  delete f.hooks.complete; d.local.put('a.bin', latest);
  const restarted = f.device({ blob: d.saved(), local: d.local }); await restarted.load();
  const second = await restarted.engine.syncNow(); const third = await restarted.engine.syncNow();
  t.diagnostic(JSON.stringify({ first, second, third, remoteGeneration: remote.contentVersion,
    phases: Object.values(restarted.manifest.data.meta.uploadSessions).map((r) => ({ phase: r.phase, sessionId: r.sessionId })),
    serverSessions: [...f.sessions.values()].map((s) => ({ id: s.id, status: s.status, baseId: s.body.baseId })) }));
  assert.equal(second.conflicts, 0, 'the committed B generation is this device\'s durable pending operation, not an independent remote edit');
  assert.equal(remote.bytes.toString(), latest, 'full sync must recover the committed receipt then apply the later local edit');
});

test('QA-SYNC-009 control: excluded folder with private content is invisible and untouched', async () => {
  const f = fixture({ excludePatterns: ['Private/**'] }); const d = f.device();
  d.local.put('keep.md', 'KEEP'); d.local.put('Private/local.md', 'LOCAL');
  f.ensure('/Vault/Private/'); const privateFile = f.add('/Vault/Private/', 'remote.md', 'REMOTE');
  const r = await d.engine.syncNow(); assert.equal(r.status, 'ok');
  assert.equal(d.local.text('Private/remote.md'), undefined); assert.equal(privateFile.trashed, false);
  assert.equal(f.live('/Vault/Private/', 'local.md'), undefined); assert.equal(d.manifest.hasFolder('Private'), false);
});

test('QA-SYNC-010 control: malformed folder listing fails closed before deletion or local writes', async () => {
  const f = fixture(); const d = await baseline(f); d.local.files.delete('a.md');
  const transport = d.client.transport;
  d.client.transport = async (request) => {
    const u = new URL(request.url);
    if (u.pathname === '/api/v1/files' && request.method === 'GET' && u.searchParams.get('path') === '/Vault/') {
      return { status: 200, headers: {}, body: Buffer.from(JSON.stringify({ ok: true, data: { files: [], folders: [{}] } })) };
    }
    return transport(request);
  };
  const r = await d.engine.syncNow(); assert.equal(r.status, 'aborted'); assert.equal(r.deletedRemote, 0);
  assert.equal(f.live('/Vault/', 'a.md').trashed, false);
});

test('QA-SYNC-011 control: full sync resumes an uncommitted revision with the same session and no repeated parts', async () => {
  const f = fixture(); const d = await baseline(f, { 'a.bin': 'A'.repeat(8193), 'keep.md': 'KEEP' });
  d.local.put('a.bin', 'B'.repeat(8193));
  f.hooks.beforeComplete = async (s) => { if (s.body.baseId) throw fault(503, 'RevisionDeferred'); };
  const first = await d.engine.syncNow(); assert.equal(first.errors, 1);
  const revision = [...f.sessions.values()].find((s) => s.body.baseId);
  assert.equal(revision.status, 'uploading'); assert.equal(revision.parts.size, 3);
  const puts = f.ops.filter((o) => o.method === 'PUT').length;
  delete f.hooks.beforeComplete;
  const restarted = f.device({ blob: d.saved(), local: d.local }); await restarted.load();
  const second = await restarted.engine.syncNow();
  assert.equal(second.status, 'ok'); assert.equal(second.uploaded, 1); assert.equal(second.conflicts, 0);
  assert.equal(f.live('/Vault/', 'a.bin').bytes.toString(), 'B'.repeat(8193));
  assert.equal(f.sessions.size, 2); assert.equal(revision.status, 'committed');
  assert.equal(f.ops.filter((o) => o.method === 'PUT').length, puts);
});

test('QA-SYNC-012 control: a unique basename move with edits retains its remote identity', async () => {
  const f = fixture(); const d = await baseline(f, { 'A/a.md': 'BASE', 'keep.md': 'KEEP' });
  const id = f.live('/Vault/A/', 'a.md').id;
  d.local.files.delete('A/a.md'); d.local.put('C/a.md', 'EDIT');
  const r = await d.engine.syncNow();
  assert.equal(r.moved, 1); assert.equal(r.conflicts, 0);
  assert.equal(f.live('/Vault/C/', 'a.md').id, id); assert.equal(f.live('/Vault/C/', 'a.md').bytes.toString(), 'EDIT');
});

function metaForDiagnostic(f) { return f && { id: f.id, path: f.path, name: f.name, contentVersion: f.contentVersion, trashed: f.trashed, text: f.bytes.toString() }; }

test('QA-SYNC-013 copied authority is renewed per path without erasing foreign or unknown KEEP journals', async () => {
  const f = fixture(); const original = await baseline(f);
  await original.local.mkdir('Empty'); await original.engine.syncNow();
  original.manifest.data.meta.uploadSessions.foreign = { owner: 'foreign-owner', phase: 'completing', sessionId: 'foreign-session', keep: true, opaque: { value: 1 } };
  original.manifest.data.meta.uploadSessions.unknown = { phase: 'future-phase', keep: true, opaque: ['unchanged'] };
  await original.manifest.save();
  const local = new MemFS(); local.put('keep.md', 'KEEP');
  const fresh = f.device({ blob: original.saved(), local, owner: '22222222-2222-4222-a222-222222222222' }); await fresh.load();
  const files = clone(fresh.manifest.data.files); const folders = clone(fresh.manifest.data.folders);
  const sessions = clone(fresh.manifest.data.meta.uploadSessions);
  await fresh.engine._bindScope();
  assert.deepEqual(clone(fresh.manifest.data.files), files); assert.deepEqual(clone(fresh.manifest.data.folders), folders);
  const start = f.ops.length; const first = await fresh.engine.syncNow();
  assert.equal(first.deletedRemote + first.deletedLocal, 0); assert.equal(local.text('a.md'), 'BASE');
  assert.ok(local.dirs.has('Empty')); assert.ok(f.folders.has('/Vault/Empty/'));
  assert.deepEqual(clone(fresh.manifest.data.meta.uploadSessions), sessions);
  assert.equal(f.ops.slice(start).some((o) => o.route.startsWith('/uploads/')), false);
  local.files.delete('a.md');
  const next = await fresh.engine.syncNow(); assert.equal(next.deletedRemote, 1, 'only a freshly compared baseline can authorize a later deletion');
  assert.deepEqual(clone(fresh.manifest.data.meta.uploadSessions), sessions);
});

test('QA-SYNC-014 same-owner legacy baselines without authority require fresh comparison', async () => {
  const f = fixture(); const original = await baseline(f);
  delete original.manifest.data.meta.baselineOwner; await original.manifest.save();
  const local = new MemFS(); local.put('keep.md', 'KEEP');
  const fresh = f.device({ blob: original.saved(), local }); await fresh.load();
  const r = await fresh.engine.syncNow(); assert.equal(r.deletedRemote, 0); assert.equal(local.text('a.md'), 'BASE');
});

test('QA-SYNC-015 failed inherited download remains non-destructive after another restart', async () => {
  const f = fixture(); const original = await baseline(f);
  const local = new MemFS(); local.put('keep.md', 'KEEP');
  const fresh = f.device({ blob: original.saved(), local, owner: '22222222-2222-4222-a222-222222222222' }); await fresh.load();
  const download = fresh.engine.downloadFn;
  fresh.engine.downloadFn = async (...args) => { if (args[1].name === 'a.md') throw new Error('synthetic interrupted download'); return download(...args); };
  const first = await fresh.engine.syncNow(); assert.equal(first.errors, 1); assert.equal(first.deletedRemote, 0);
  assert.ok(fresh.manifest.data.meta.unverifiedBaselineFiles['a.md']);
  const restarted = f.device({ blob: fresh.saved(), local }); await restarted.load();
  const next = await restarted.engine.syncNow(); assert.equal(next.deletedRemote, 0); assert.equal(local.text('a.md'), 'BASE');
});

test('QA-SYNC-016 delete approval binds the local content snapshot, including same-stat edits', async () => {
  const f = fixture({ maxDeletesPerRun: 0 }); const d = await baseline(f, { 'a.md': 'BASE', 'b.md': 'BBBB', 'keep.md': 'KEEP' });
  f.live('/Vault/', 'a.md').trashed = true; f.live('/Vault/', 'b.md').trashed = true;
  const blocked = await d.engine.syncNow(); const reviewed = clone(blocked.blockedDeletes.items);
  assert.equal(reviewed.find((p) => p.path === 'a.md').localHash, await sha256Hex(Buffer.from('BASE')));
  // Simulate a stat-preserving edit: it must invalidate the reviewed hash, not merely match path/action.
  d.local.files.get('a.md').data = Buffer.from('EDIT');
  const r = await d.engine.syncNow({ approveDeletes: true, approvedDeletes: reviewed });
  assert.equal(r.deletedLocal, 0); assert.equal(r.blockedDeletes.reason, 'changed'); assert.equal(d.local.text('a.md'), 'EDIT');
});

test('QA-SYNC-017 action/path-only approval cannot authorize an unreviewed snapshot', async () => {
  const f = fixture({ maxDeletesPerRun: 0 }); const d = await baseline(f);
  d.local.files.delete('a.md'); const first = await d.engine.syncNow();
  const legacy = first.blockedDeletes.items.map(({ path, action }) => ({ path, action }));
  const r = await d.engine.syncNow({ approveDeletes: true, approvedDeletes: legacy });
  assert.equal(r.deletedRemote, 0); assert.equal(r.blockedDeletes.reason, 'changed');
});

test('QA-SYNC-018 one basename source and two edited destinations do not choose an arbitrary move target', async () => {
  const f = fixture(); const d = await baseline(f, { 'A/a.md': 'BASE', 'keep.md': 'KEEP' });
  const id = f.live('/Vault/A/', 'a.md').id;
  d.local.files.delete('A/a.md'); d.local.put('B/a.md', 'BBBB'); d.local.put('C/a.md', 'CCCC');
  const r = await d.engine.syncNow(); assert.equal(r.moved, 0);
  assert.notEqual(f.live('/Vault/B/', 'a.md').id, id); assert.notEqual(f.live('/Vault/C/', 'a.md').id, id);
});

test('QA-SYNC-019 identical hashes from multiple identities do not pick an arbitrary move source', async () => {
  const f = fixture(); const d = await baseline(f, { 'A/first.md': 'SAME', 'B/second.md': 'SAME', 'keep.md': 'KEEP' });
  const ids = [f.live('/Vault/A/', 'first.md').id, f.live('/Vault/B/', 'second.md').id];
  d.local.files.delete('A/first.md'); d.local.files.delete('B/second.md'); d.local.put('C/third.md', 'SAME');
  const r = await d.engine.syncNow(); assert.equal(r.moved, 0); assert.ok(!ids.includes(f.live('/Vault/C/', 'third.md').id));
});

async function unknownRevision() {
  const f = fixture(); const committed = 'B'.repeat(8193);
  const d = await baseline(f, { 'a.bin': 'A'.repeat(8193), 'keep.md': 'KEEP' }); const remote = f.live('/Vault/', 'a.bin');
  d.local.put('a.bin', committed);
  f.hooks.complete = async (s) => { if (s.body.baseId) throw Object.assign(new Error('synthetic lost complete reply'), { isNetwork: true }); };
  const first = await d.engine.syncNow(); assert.equal(first.errors, 1); assert.equal(remote.bytes.toString(), committed);
  delete f.hooks.complete;
  const session = [...f.sessions.values()].find((s) => s.body.baseId);
  return { f, d, remote, committed, session };
}

test('QA-SYNC-020 committed recovery with unchanged source performs no repeated upload mutation', async () => {
  const { f, d, committed } = await unknownRevision(); const start = f.ops.length;
  const restarted = f.device({ blob: d.saved(), local: d.local }); await restarted.load();
  const r = await restarted.engine.syncNow(); assert.equal(r.status, 'ok'); assert.equal(r.uploaded + r.conflicts, 0);
  assert.equal(restarted.manifest.getFile('a.bin').localHash, await sha256Hex(Buffer.from(committed)));
  assert.equal(restarted.manifest.getFile('a.bin').remoteContentVersion, 2);
  assert.equal(f.ops.slice(start).some((o) => ['POST', 'PUT', 'DELETE'].includes(o.method)), false);
});

test('QA-SYNC-021 recovery retains the exact receipt when a newer independent head exists', async () => {
  const { f, d, remote, committed, session } = await unknownRevision();
  d.local.put('a.bin', 'C'.repeat(8193)); f.edit(remote, 'D'.repeat(8193)); const start = f.ops.length;
  const restarted = f.device({ blob: d.saved(), local: d.local }); await restarted.load();
  const r = await restarted.engine.syncNow(); assert.equal(r.uploaded, 0); assert.equal(r.conflicts, 1);
  const E = restarted.manifest.getFile('a.bin'); assert.equal(E.remoteContentVersion, 2); assert.equal(E.localHash, await sha256Hex(Buffer.from(committed)));
  const journal = Object.values(restarted.manifest.data.meta.uploadSessions).find((v) => v.sessionId === session.id);
  assert.equal(journal.phase, 'completed'); assert.equal(journal.receipt.contentVersion, 2); assert.equal(remote.contentVersion, 3);
  assert.equal(f.ops.slice(start).some((o) => ['POST', 'PUT', 'DELETE'].includes(o.method)), false);
});

test('QA-SYNC-022 foreign committed unknown journals remain KEEP and cannot supply baseline authority', async () => {
  const { f, d } = await unknownRevision(); d.local.put('a.bin', 'C'.repeat(8193));
  const before = clone(d.manifest.data.meta.uploadSessions); const start = f.ops.length;
  const fresh = f.device({ blob: d.saved(), local: d.local, owner: '22222222-2222-4222-a222-222222222222' }); await fresh.load();
  const r = await fresh.engine.syncNow(); assert.equal(r.uploaded, 0); assert.equal(r.conflicts, 1);
  assert.deepEqual(clone(fresh.manifest.data.meta.uploadSessions), before);
  assert.equal(f.ops.slice(start).some((o) => o.route.startsWith('/uploads/')), false);
});

test('QA-SYNC-023 dry run does not recover or persist an uncertain committed upload', async () => {
  const { f, d } = await unknownRevision(); d.local.put('a.bin', 'C'.repeat(8193));
  const restarted = f.device({ blob: d.saved(), local: d.local }); await restarted.load();
  const before = JSON.stringify(restarted.manifest.data); const start = f.ops.length;
  await restarted.engine.syncNow({ dryRun: true });
  assert.equal(JSON.stringify(restarted.manifest.data), before);
  assert.equal(f.ops.slice(start).some((o) => o.route.startsWith('/uploads/')), false);
});

test('QA-SYNC-024 weak recovered receipt rejects a different head without replaying or aborting the commit', async () => {
  const { f, d, remote, session } = await unknownRevision();
  session.receipt = { fileId: remote.id, contentVersion: null, checksum: null, etag: null };
  d.local.put('a.bin', 'C'.repeat(8193)); f.edit(remote, 'D'.repeat(8193)); const start = f.ops.length;
  const restarted = f.device({ blob: d.saved(), local: d.local }); await restarted.load();
  const r = await restarted.engine.syncNow(); assert.ok(r.errors > 0); assert.equal(r.uploaded, 0);
  const E = restarted.manifest.getFile('a.bin'); assert.equal(E.remoteContentVersion, 1);
  const journal = Object.values(restarted.manifest.data.meta.uploadSessions).find((v) => v.sessionId === session.id);
  assert.equal(journal.phase, 'completed'); assert.equal(journal.receipt.contentVersion, null);
  assert.equal(f.ops.slice(start).some((o) => ['POST', 'PUT', 'DELETE'].includes(o.method)), false);
  assert.equal(session.status, 'committed'); assert.equal(remote.bytes.toString(), 'D'.repeat(8193));
});

test('QA-SYNC-025 uncertain committed create recovers before a later local edit without recreating B', async () => {
  const f = fixture(); const d = f.device(); d.local.put('a.bin', 'B'.repeat(8193));
  f.hooks.complete = async () => { throw Object.assign(new Error('synthetic lost create reply'), { isNetwork: true }); };
  assert.equal((await d.engine.syncNow()).errors, 1); delete f.hooks.complete;
  const remote = f.live('/Vault/', 'a.bin'); const id = remote.id; const start = f.ops.length;
  d.local.put('a.bin', 'C'.repeat(8193));
  const restarted = f.device({ blob: d.saved(), local: d.local }); await restarted.load();
  const r = await restarted.engine.syncNow(); assert.equal(r.conflicts, 0); assert.equal(r.uploaded, 1);
  assert.equal(f.live('/Vault/', 'a.bin').id, id); assert.equal(remote.bytes.toString(), 'C'.repeat(8193));
  assert.equal([...f.sessions.values()].filter((s) => !s.body.baseId).length, 1);
  assert.equal(f.ops.slice(start).some((o) => o.method === 'DELETE'), false);
});

test('QA-SYNC-026 legacy deletion snapshots prove bytes rather than unchanged size and timestamp', async () => {
  const f = fixture({ maxDeletesPerRun: 0 }); const d = await baseline(f, { 'a.md': 'BASE', 'b.md': 'BBBB', 'keep.md': 'KEEP' });
  for (const remote of f.files.values()) remote.contentVersion = null;
  d.local.files.delete('a.md'); d.local.files.delete('b.md');
  const first = await d.engine.syncNow(); const reviewed = clone(first.blockedDeletes.items);
  assert.equal(reviewed.find((p) => p.path === 'a.md').remoteHash, await sha256Hex(Buffer.from('BASE')));
  const a = f.live('/Vault/', 'a.md'); a.bytes = Buffer.from('EDIT');
  const r = await d.engine.syncNow({ approveDeletes: true, approvedDeletes: reviewed });
  assert.equal(r.deletedRemote, 0); assert.equal(a.trashed, false); assert.equal(d.local.text('a.md'), 'EDIT');
  assert.equal(r.blockedDeletes.reason, 'changed');
});

test('QA-SYNC-027 legacy byte change after snapshot is rejected again at deletion execution', async () => {
  const f = fixture(); const d = await baseline(f); const a = f.live('/Vault/', 'a.md'); a.contentVersion = null;
  d.local.files.delete('a.md');
  f.hooks.get = async (remote) => { if (remote.id === a.id) remote.bytes = Buffer.from('EDIT'); };
  const r = await d.engine.syncNow(); delete f.hooks.get;
  assert.equal(r.deletedRemote, 0); assert.equal(r.errors, 1); assert.equal(a.trashed, false);
  assert.equal((await d.engine.syncNow()).downloaded, 1); assert.equal(d.local.text('a.md'), 'EDIT');
});

test('QA-SYNC-028 legacy deletion proof still honors content transfer size guards', async () => {
  const f = fixture(); const d = await baseline(f); const a = f.live('/Vault/', 'a.md'); a.contentVersion = null;
  d.local.files.delete('a.md'); f.settings.maxFileBytes = 3;
  let reads = 0; const download = d.engine.downloadFn;
  d.engine.downloadFn = async (...args) => { reads++; return download(...args); };
  const r = await d.engine.syncNow(); assert.equal(reads, 0); assert.equal(r.deletedRemote, 0); assert.equal(a.trashed, false);
});

test('QA-SYNC-029 a remote edit after weak upload proof cannot enter baseline through reconciliation', async () => {
  const f = fixture(); const d = f.device(); d.local.put('a.md', 'AAAA');
  f.hooks.createAck = async (remote) => ({ fileId: remote.id });
  const upload = d.engine.uploadFn;
  d.engine.uploadFn = async (...args) => {
    const receipt = await upload(...args);
    f.edit(f.files.get(receipt.id), 'BBBB');
    return receipt;
  };
  const first = await d.engine.syncNow(); assert.equal(first.uploaded, 1);
  const E = d.manifest.getFile('a.md'); assert.equal(E.remoteContentVersion, null); assert.equal(E.remoteMtime, null);
  const second = await d.engine.syncNow(); assert.equal(second.downloaded, 1); assert.equal(d.local.text('a.md'), 'BBBB');
});

test('QA-SYNC-030 a proven weak recovered B is not a false conflict with newer local C', async () => {
  const { f, d, remote, session } = await unknownRevision();
  session.receipt = { fileId: remote.id, contentVersion: null, checksum: null, etag: null };
  d.local.put('a.bin', 'C'.repeat(8193)); const start = f.ops.length;
  const restarted = f.device({ blob: d.saved(), local: d.local }); await restarted.load();
  const first = await restarted.engine.syncNow(); const second = await restarted.engine.syncNow();
  assert.equal(first.conflicts + second.conflicts, 0); assert.equal(remote.bytes.toString(), 'C'.repeat(8193));
  assert.equal(session.receipt.contentVersion, null);
  assert.equal(f.ops.slice(start).filter((o) => o.method === 'POST' && o.route === '/uploads/' + session.id + '/complete').length, 0);
  assert.equal(f.ops.slice(start).some((o) => o.method === 'DELETE'), false);
});

async function deltaPins() {
  const f = fixture(); const d = f.device(); const n = 131072;
  const A = Buffer.alloc(n, 65); const B = Buffer.alloc(n, 66); const C = Buffer.alloc(n, 67);
  const old = Buffer.concat([A, B]); const hA = await sha256Hex(A); const hB = await sha256Hex(B); const hC = await sha256Hex(C);
  d.local.put('a.bin', old);
  const E = { localHash: await sha256Hex(old), remoteContentVersion: 1, remoteEtag: 'gen-1', etag: 'gen-1', chunkMap: { sizes: [n, n], hashes: [hA, hB] } };
  const R = { id: 'f', size: 2 * n, etag: 'gen-1', contentVersion: 1 };
  const map = { size: 2 * n, etag: 'gen-2', contentVersion: 2,
    chunks: [{ idx: 0, offset: 0, size: n, checksum: hA }, { idx: 1, offset: n, size: n, checksum: hC }] };
  let deltas = 0;
  d.engine.manifestFn = async () => map;
  d.engine.deltaFn = async () => { deltas++; return [{ offset: n, data: C.buffer.slice(C.byteOffset, C.byteOffset + C.length) }]; };
  return { f, d, E, R, map, old, A, C, hA, hB, hC, deltas: () => deltas };
}

test('QA-SYNC-031 delta rejects a newer manifest rather than returning gen2 bytes under a gen1 decision', async () => {
  const { d, E, R, deltas } = await deltaPins(); const before = clone(R);
  await assert.rejects(d.engine._deltaPull('a.bin', R, E), (e) => e.code === 'content_changed');
  assert.equal(deltas(), 0); assert.deepEqual(R, before);
});

test('QA-SYNC-032 delta rejects a known ETag mismatch even when generations are unknown', async () => {
  const { d, E, R, map, deltas } = await deltaPins(); delete R.contentVersion; delete map.contentVersion;
  await assert.rejects(d.engine._deltaPull('a.bin', R, E), (e) => e.code === 'content_changed'); assert.equal(deltas(), 0);
});

test('QA-SYNC-033 delta rejects a pinned fingerprint against manifest hashes when other tokens are unknown', async () => {
  const { d, E, R, map, hA, hB, deltas } = await deltaPins();
  delete R.contentVersion; delete R.etag; delete map.contentVersion; delete map.etag;
  R.checksum = await fingerprintOf([hA, hB]);
  await assert.rejects(d.engine._deltaPull('a.bin', R, E), (e) => e.code === 'content_changed'); assert.equal(deltas(), 0);
});

test('QA-SYNC-034 matching planned delta pins preserve current generation and do not confuse archived version', async () => {
  const { d, E, R, map, A, C, hA, hC } = await deltaPins();
  R.contentVersion = 2; R.etag = 'gen-2'; R.version = 1; R.checksum = await fingerprintOf([hA, hC]);
  map.version = 99; map.checksum = R.checksum;
  const result = await d.engine._deltaPull('a.bin', R, E);
  assert.deepEqual(Buffer.from(result.buffer), Buffer.concat([A, C]));
  assert.equal(result.map.contentVersion, 2); assert.equal(result.map.etag, 'gen-2'); assert.equal(result.map.checksum, R.checksum);
  assert.equal(result.map.version, 99);
});

test('QA-SYNC-035 expected SHA, size adoption and cached-map fetch cannot bypass planned manifest pins', async () => {
  const { d, R } = await deltaPins(); await d.engine._bindScope();
  d.client.limits = async () => ({ oneShotMaxBytes: R.size, chunkBytes: 4096 });
  d.manifest.setFile('a.bin', { remoteId: R.id, localHash: 'unchanged', remoteSize: R.size });
  const before = clone(R); const entry = clone(d.manifest.getFile('a.bin'));
  await assert.rejects(d.engine._expectedSha(R), (e) => e.code === 'content_changed');
  await assert.rejects(d.engine._adoptManifestSize(R), (e) => e.code === 'content_changed');
  await assert.rejects(d.engine._ensureMap('a.bin', R), (e) => e.code === 'content_changed');
  assert.deepEqual(R, before); assert.deepEqual(d.manifest.getFile('a.bin'), entry);
});

test('QA-SYNC-036 content_changed download errors stop without retry or metadata hydration', async () => {
  const { d, R } = await deltaPins(); const before = clone(R); let reads = 0; let gets = 0;
  d.engine.downloadFn = async () => { reads++; throw Object.assign(new Error('pinned content changed'), { code: 'content_changed', retryable: true }); };
  d.client.getFile = async () => { gets++; return { ...R, contentVersion: 2, etag: 'gen-2', mtime: 99 }; };
  await assert.rejects(d.engine._downloadVerified('a.bin', R), (e) => e.code === 'content_changed');
  assert.equal(reads, 1); assert.equal(gets, 0); assert.deepEqual(R, before);
});

test('QA-SYNC-037 checksum retry retains original pins and hash even if a failed transfer mutates its entry', async () => {
  const { d, R, old, hA, hB } = await deltaPins(); R.mtime = 10; R.checksum = await fingerprintOf([hA, hB]);
  const expected = await sha256Hex(old);
  const before = clone(R); let reads = 0; let gets = 0;
  d.client.getFile = async () => { gets++; return { ...R, contentVersion: 2, etag: 'gen-2', mtime: 99 }; };
  d.engine.downloadFn = async (_, remote, options) => {
    reads++; assert.equal(options.expectSha, expected); assert.deepEqual(remote, before);
    if (reads === 1) {
      Object.assign(remote, { size: 17, mtime: 99, contentVersion: 2, etag: 'gen-2', checksum: 'new-fingerprint' });
      throw new Error('Download checksum mismatch');
    }
    assert.equal(options.forceFull, true); return old;
  };
  assert.deepEqual(await d.engine._downloadVerified('a.bin', R, null, expected), old);
  assert.equal(reads, 2); assert.equal(gets, 0); assert.deepEqual(R, before);
});

test('QA-SYNC-038 delta 304 cannot acknowledge a cached old generation as a newer planned generation', async () => {
  const { d, E, R } = await deltaPins(); R.contentVersion = 2; delete R.etag;
  d.engine.manifestFn = async () => ({ notModified: true });
  await assert.rejects(d.engine._deltaPull('a.bin', R, E), (e) => e.code === 'content_changed');
});

test('QA-SYNC-039 manifest content_changed errors are not swallowed as optional map failures', async () => {
  const { d, R } = await deltaPins();
  d.engine.manifestFn = async () => { throw Object.assign(new Error('manifest pin changed'), { code: 'content_changed', retryable: true }); };
  await assert.rejects(d.engine._fetchChunkMap(R), (e) => e.code === 'content_changed');
});

test('QA-SYNC-040 a newer map in the download path leaves original bytes and baseline unchanged', async () => {
  const { d, E, R, old, A, C } = await deltaPins(); await d.engine._bindScope();
  Object.assign(E, { remoteId: R.id, remoteSize: R.size, remoteSig: 'planned-gen1' });
  d.manifest.setFile('a.bin', E); const before = clone(E); let reads = 0;
  d.engine.downloadFn = async () => { reads++; return Buffer.concat([A, C]); };
  await assert.rejects(d.engine._execute({ key: 'a.bin', action: 'download', L: { path: 'a.bin', ...await d.local.stat('a.bin') }, R, E }, {}), (e) => e.code === 'content_changed');
  assert.equal(reads, 0); assert.deepEqual(Buffer.from(await d.local.readBinary('a.bin')), old);
  assert.deepEqual(d.manifest.getFile('a.bin'), before);
});

test('QA-SYNC-041 delta checks actual chunk fingerprint rather than a contradictory declared checksum', async () => {
  const { d, E, R, map, hA, hB, deltas } = await deltaPins();
  delete R.contentVersion; delete R.etag; delete map.contentVersion; delete map.etag;
  R.checksum = await fingerprintOf([hA, hB]); map.checksum = R.checksum;
  await assert.rejects(d.engine._deltaPull('a.bin', R, E), (e) => e.code === 'content_changed');
  assert.equal(deltas(), 0);
});

test('QA-SYNC-042 a rejected planned generation invalidates cached listing for the next replan', async () => {
  const f = fixture(); const d = await baseline(f, { 'a.bin': 'A'.repeat(65536), 'keep.md': 'KEEP' });
  f.settings.incrementalScan = true;
  const remote = f.live('/Vault/', 'a.bin'); f.edit(remote, 'B'.repeat(65536));
  const list = d.client.listFolder.bind(d.client); let raced = false;
  d.client.listFolder = async (path) => {
    const response = await list(path);
    if (path === '/Vault/' && !raced) { raced = true; f.edit(remote, 'C'.repeat(65536)); }
    return response;
  };
  const first = await d.engine.syncNow(); assert.equal(first.errors, 1);
  assert.equal(d.local.text('a.bin'), 'A'.repeat(65536)); assert.equal(d.manifest.getFile('a.bin').remoteContentVersion, 1);
  const second = await d.engine.syncNow(); assert.equal(second.errors, 0); assert.equal(second.downloaded, 1);
  assert.equal(d.local.text('a.bin'), 'C'.repeat(65536)); assert.equal(d.manifest.getFile('a.bin').remoteContentVersion, 3);
});

test('QA-SYNC-043 a pin rejection clears a preexisting incremental snapshot before replanning', async () => {
  const f = fixture(); const d = await baseline(f, { 'a.bin': 'A'.repeat(65536), 'keep.md': 'KEEP' });
  f.settings.incrementalScan = true; f.settings.mode = 'download-only';
  await d.engine.syncNow(); assert.ok(d.manifest.data.meta.remoteSnapshot);
  // A previously observed activity cursor remains finite when the current page is empty.
  d.manifest.data.meta.remoteSnapshot.activityAt = d.engine.now();
  const entry = d.manifest.getFile('a.bin'); entry.remoteContentVersion = null; entry.remoteMtime = null;
  f.edit(f.live('/Vault/', 'a.bin'), 'B'.repeat(65536));
  const first = await d.engine.syncNow(); assert.equal(first.errors, 1);
  const second = await d.engine.syncNow(); assert.equal(second.errors, 0); assert.equal(second.downloaded, 1);
  assert.equal(d.local.text('a.bin'), 'B'.repeat(65536)); assert.equal(d.manifest.getFile('a.bin').remoteContentVersion, 2);
});
