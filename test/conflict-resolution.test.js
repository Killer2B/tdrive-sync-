'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { ConflictManager } = require('../src/ConflictManager');
const { ManifestStore } = require('../src/ManifestStore');
const { SyncEngine } = require('../src/SyncEngine');
const { TdriveClient } = require('../src/TdriveClient');
const { sha256Hex, makeExcluder } = require('../src/util');
const { fingerprintOf } = require('../src/Chunker');
const { makeEnv } = require('./helpers');
if (!globalThis.crypto) globalThis.crypto = require('node:crypto').webcrypto;

const bytes = (s) => new TextEncoder().encode(s).buffer;
const text = (b) => new TextDecoder('utf-8', { ignoreBOM: true }).decode(b);

// Full engine/client with a closed in-memory API, never a live or external HTTP transport.
async function fixture({ localText = 'LOCAL', remoteData = bytes('REMOTE'), base = 'BASE' } = {}) {
  const disk = new Map([['a.md', bytes(localText)]]); const files = new Map(); const calls = []; const hooks = {};
  let blob = null; let active = null; let nextId = 1;
  const manifest = new ManifestStore({ read: async () => blob, write: async (s) => { if (hooks.save) await hooks.save(JSON.parse(s)); blob = s; } });
  const local = {
    exists: async (p) => disk.has(p), stat: async (p) => disk.has(p) ? { size: disk.get(p).byteLength, mtime: 1000 } : null,
    readBinary: async (p) => { if (!disk.has(p)) throw new Error('ENOENT ' + p); return disk.get(p).slice(0); },
    writeBinary: async (p, data, _mtime, options = {}) => {
      if (hooks.write) await hooks.write(p, data, options);
      if (options.createOnly && disk.has(p)) throw Object.assign(new Error('collision'), { code: 'LOCAL_FILE_EXISTS' });
      if (options.expectedHash != null && (!disk.has(p) || await sha256Hex(disk.get(p)) !== options.expectedHash)) throw Object.assign(new Error('Local file changed in queued commit'), { code: 'local_changed' });
      disk.set(p, data.slice(0)); calls.push({ localWrite: p, options });
    },
    writeBinaryAtomic: async (...args) => local.writeBinary(...args),
    trash: async (p) => { disk.delete(p); }, list: async () => [...disk].map(([path, data]) => ({ path, size: data.byteLength, mtime: 1000 })), listFolders: async () => [],
  };
  const add = async (id, name, data, contentVersion = 11) => {
    const f = { id, name, path: '/Vault/', size: data.byteLength, lastModified: 1000, contentVersion, checksum: await fingerprintOf([await sha256Hex(data)]), data: data.slice(0) };
    files.set(id, f); return f;
  };
  const head = await add('remote-a', 'a.md', remoteData);
  const metadata = ({ data, ...f }) => f;
  const ok = (data, status = 200) => ({ status, headers: {}, body: bytes(JSON.stringify({ ok: true, data })) });
  const missing = () => ({ status: 404, headers: {}, body: bytes(JSON.stringify({ ok: false, error: { code: 'NOT_FOUND', message: 'missing synthetic file' } })) });
  const client = new TdriveClient({ baseUrl: 'https://conflict.invalid', apiKey: 'synthetic-only', maxRetries: 0, maxServerRetries: 0,
    transport: async (req) => {
      assert.equal(new URL(req.url).hostname, 'conflict.invalid');
      const path = new URL(req.url).pathname.slice('/api/v1'.length); calls.push({ path, method: req.method, body: req.body });
      if (path === '' && req.method === 'GET') return ok({ upload: { chunkBytes: 5242880, oneShotMaxBytes: 5242880 }, capabilities: { syncLeases: true, expectedContentVersion: true, contentVersions: true } });
      if (path === '/files' && req.method === 'GET') {
        if (hooks.list) await hooks.list();
        return ok(new URL(req.url).searchParams.get('path') === '/' ? { files: [], folders: ['Vault'] } : { files: [...files.values()].map(metadata), folders: [] });
      }
      if (path === '/sync/leases' && req.method === 'POST') {
        const body = JSON.parse(text(req.body)); active = { path: body.path, owner: body.owner, token: 'synthetic-lease' };
        return ok({ ...active, expiresAt: Date.now() + 60000, serverTime: Date.now(), ttlSeconds: 60 }, 201);
      }
      if (path === '/sync/leases' && req.method === 'DELETE') { active = null; return ok({ released: true }); }
      if (req.method !== 'GET') { assert.ok(active, 'content publication must hold a lease'); assert.equal(req.headers['X-Sync-Lease'], active.token); }
      const match = /^\/files\/([^/]+)(\/chunks|\/content)?$/.exec(path);
      if (match) {
        const f = files.get(match[1]); if (!f) return missing();
        if (req.method === 'GET' && match[2] === '/chunks') return ok({ size: f.size, chunkSize: 5242880, totalChunks: 1, contentVersion: f.contentVersion, checksum: f.checksum, chunks: [{ idx: 0, offset: 0, size: f.size, checksum: await sha256Hex(f.data) }] });
        if (req.method === 'GET' && match[2] === '/content') {
          const range = req.headers.Range || req.headers.range;
          if (range) { const [, a, requested] = /bytes=(\d+)-(\d+)/.exec(range); const b = Math.min(+requested, f.size - 1); return { status: 206, headers: { 'content-range': `bytes ${a}-${b}/${f.size}` }, body: f.data.slice(+a, b + 1) }; }
          return { status: 200, headers: {}, body: f.data.slice(0) };
        }
        if (req.method === 'GET') { if (hooks.get) await hooks.get(f); return ok({ file: metadata(f) }); }
        if (req.method === 'PATCH') {
          const body = JSON.parse(text(req.body)); assert.equal(body.expectedVersion, f.contentVersion);
          const data = Uint8Array.from(Buffer.from(body.content.inline, 'base64')).buffer;
          await add(f.id, f.name, data, f.contentVersion + 1);
          if (hooks.patched) await hooks.patched();
          return ok({ file: metadata(files.get(f.id)) });
        }
      }
      if (path === '/files' && req.method === 'POST') {
        const name = new URL(req.url).searchParams.get('name');
        assert.equal([...files.values()].some((f) => f.name === name), false, 'create must not duplicate a remote name');
        const f = await add('copy-' + nextId++, name, req.body, 1);
        if (hooks.created) await hooks.created(f);
        return ok({ file: metadata(f) }, 201);
      }
      throw new Error('Unexpected synthetic request ' + req.method + ' ' + path);
    },
  });
  const engine = new SyncEngine({ client, local, manifest, getSettings: () => ({ remoteFolder: '/Vault/', remoteLock: true, excludePatterns: [], cdcFiles: false }), revisionFn: async () => null });
  await engine._bindScope(); manifest.setFile('a.md', { remoteId: head.id, baseText: base });
  const manager = new ConflictManager({ manifest, local, now: () => 1000 });
  await manager.record('a.md', await client.getFile(head.id), remoteData); await manifest.save(); calls.length = 0;
  return { manager, engine, manifest, disk, files, local, calls, hooks, add, head: () => files.get('remote-a'), durable: () => JSON.parse(blob), active: () => active };
}

for (const result of [false, undefined, { conflict: true }, { id: 'remote-a' }]) {
  test('unproven push result cannot clear the conflict: ' + JSON.stringify(result), async () => {
    const f = await fixture(); f.engine.pushLocal = async () => result;
    await assert.rejects(f.manager.resolve('a.md', 'local', f.engine), /commit|push|publish|baseline/i);
    assert.ok(f.manifest.getConflict('a.md')); assert.ok(f.durable().conflicts['a.md']); assert.equal(f.engine.running, false); assert.equal(f.active(), null);
  });
}

test('keep local snapshots bytes before download and refuses a later edit', async () => {
  const f = await fixture(); const download = f.engine.downloadFn;
  f.engine.downloadFn = async (...args) => { const data = await download(...args); f.disk.set('a.md', bytes('USER-EDIT')); return data; };
  await assert.rejects(f.manager.resolve('a.md', 'local', f.engine), /Local file changed/i);
  assert.equal(text(f.head().data), 'REMOTE'); assert.equal(text(f.disk.get('a.md')), 'USER-EDIT'); assert.ok(f.manifest.getConflict('a.md'));
});

test('keep both publishes exact reviewed remote in a normal synced copy BEFORE replacing source', async () => {
  const original = Uint8Array.from([239, 187, 191, 0, 255, 128, 13, 10]).buffer;
  const f = await fixture({ remoteData: original });
  const reviewCopy = f.manifest.getConflict('a.md').copyPath; f.disk.set(reviewCopy, bytes('USER-COPY-EDIT'));
  assert.equal((await f.manager.resolve('a.md', 'both', f.engine)).resolved, true);
  const preserved = [...f.files.values()].find((file) => file.id !== 'remote-a'); assert.ok(preserved);
  assert.deepEqual(preserved.data, original); assert.deepEqual(f.disk.get(preserved.name), original); assert.equal(makeExcluder([])(preserved.name), false);
  assert.equal(text(f.disk.get(reviewCopy)), 'USER-COPY-EDIT'); assert.equal(text(f.head().data), 'LOCAL');
  assert.ok(f.calls.findIndex((c) => c.method === 'POST' && c.path === '/files') < f.calls.findIndex((c) => c.method === 'PATCH' && c.path === '/files/remote-a'));
  assert.equal(f.calls.some((c) => c.method === 'DELETE' && c.path.startsWith('/files')), false); assert.equal(f.active(), null);
});

test('lost copy-create response retries without duplicate remote IDs or replacing the old remote prematurely', async () => {
  const f = await fixture(); let failed = false;
  f.hooks.created = async () => { if (!failed) { failed = true; throw new Error('lost create response'); } };
  await assert.rejects(f.manager.resolve('a.md', 'both', f.engine), /lost create response/);
  assert.equal(text(f.head().data), 'REMOTE'); assert.equal(f.files.size, 2); assert.ok(f.manifest.getConflict('a.md'));
  f.hooks.created = null;
  assert.equal((await f.manager.resolve('a.md', 'both', f.engine)).resolved, true); assert.equal(f.files.size, 2);
  assert.equal(f.calls.filter((c) => c.method === 'POST' && c.path === '/files').length, 1);
});

test('durable clear failure keeps review and reattempt adopts the already committed target without a second PATCH', async () => {
  const f = await fixture(); f.hooks.save = async (state) => { if (!state.conflicts['a.md']) throw new Error('durable clear failed'); };
  await assert.rejects(f.manager.resolve('a.md', 'local', f.engine), /durable clear failed/);
  assert.ok(f.manifest.getConflict('a.md')); assert.ok(f.durable().conflicts['a.md']); assert.equal(text(f.head().data), 'LOCAL');
  f.hooks.save = null;
  assert.equal((await f.manager.resolve('a.md', 'local', f.engine)).resolved, true);
  assert.equal(f.calls.filter((c) => c.method === 'PATCH').length, 1);
});

test('same bytes resolve without content mutation, but identical newer generation requires fresh initial review', async () => {
  const f = await fixture({ remoteData: bytes('LOCAL') });
  assert.equal((await f.manager.resolve('a.md', 'local', f.engine)).resolved, true); assert.equal(f.calls.some((c) => c.method === 'PATCH' || c.path === '/files' && c.method === 'POST'), false);
  const stale = await fixture({ remoteData: bytes('LOCAL') }); await stale.add('remote-a', 'a.md', bytes('LOCAL'), 12);
  await assert.rejects(stale.manager.resolve('a.md', 'local', stale.engine), /Reopen|reopen/); assert.ok(stale.manifest.getConflict('a.md'));
});

test('merge overlap returns full unresolved draft and publishes nothing', async () => {
  const f = await fixture({ base: 'first\nBASE\nlast\n', localText: 'first\nLOCAL\nlast\n', remoteData: bytes('first\nREMOTE\nlast\n') });
  const result = await f.manager.resolve('a.md', 'merge', f.engine);
  assert.equal(result.resolved, false); assert.equal(result.merged, false); assert.equal(result.conflicts, 1); assert.equal(result.text, text(f.disk.get(result.draft))); assert.match(result.text, /<<<<<<< local\nLOCAL\n=======\nREMOTE\n>>>>>>> remote/);
  assert.equal(f.calls.some((c) => c.method === 'PATCH' || c.method === 'POST' && c.path === '/files'), false); assert.ok(f.manifest.getConflict('a.md'));
});

for (const remoteData of [Uint8Array.from([255, 254, 0, 97]).buffer, bytes('first\n\u0000binary\n')]) {
  test('merge refuses non-lossless UTF8 or binary instead of publishing replacement characters', async () => {
    const f = await fixture({ base: 'first\nBASE\n', localText: 'first\nLOCAL\n', remoteData });
    await assert.rejects(f.manager.resolve('a.md', 'merge', f.engine), /UTF|binary|text/i);
    assert.equal(text(f.disk.get('a.md')), 'first\nLOCAL\n'); assert.ok(f.manifest.getConflict('a.md')); assert.equal(f.calls.some((c) => c.method === 'PATCH'), false);
  });
}

test('merge carries original hash inside queued write and merged hash into upload', async () => {
  const f = await fixture({ base: 'first\nmiddle\nlast\n', localText: 'LOCAL\nmiddle\nlast\n', remoteData: bytes('first\nmiddle\nREMOTE\n') });
  let guarded = null; f.hooks.write = async (p, _data, options) => { if (p === 'a.md') { guarded = options.expectedHash; f.disk.set(p, bytes('QUEUED-USER-EDIT')); } };
  await assert.rejects(f.manager.resolve('a.md', 'merge', f.engine), /Local file changed/i);
  assert.equal(guarded, await sha256Hex(bytes('LOCAL\nmiddle\nlast\n'))); assert.equal(text(f.disk.get('a.md')), 'QUEUED-USER-EDIT'); assert.equal(text(f.head().data), 'first\nmiddle\nREMOTE\n');
});

test('merge preserves UTF8 BOM and refuses publishing an edit made after merged local commit', async () => {
  const f = await fixture({ base: '\ufefffirst\nmiddle\nlast\n', localText: '\ufeffLOCAL\nmiddle\nlast\n', remoteData: bytes('\ufefffirst\nmiddle\nREMOTE\n') });
  const push = f.engine.pushLocal.bind(f.engine);
  f.engine.pushLocal = async (...args) => { f.disk.set('a.md', bytes('AFTER-MERGE-USER')); return push(...args); };
  await assert.rejects(f.manager.resolve('a.md', 'merge', f.engine), /Local file changed/i);
  assert.ok(f.manifest.getConflict('a.md')); assert.equal(text(f.head().data), '\ufefffirst\nmiddle\nREMOTE\n'); assert.equal(text(f.disk.get('a.md')), 'AFTER-MERGE-USER');
});

test('keep both refuses a differing remote name occupant without a nested conflict or source publication', async () => {
  const f = await fixture();
  f.hooks.write = async (p) => { if (p.includes('Tdrive kept remote')) await f.add('occupied', p, bytes('FOREIGN-OCCUPANT'), 5); };
  await assert.rejects(f.manager.resolve('a.md', 'both', f.engine), (e) => e.code === 'remote_collision');
  assert.deepEqual(f.manifest.conflictKeys(), ['a.md']); assert.equal(text(f.head().data), 'REMOTE'); assert.equal(text(f.files.get('occupied').data), 'FOREIGN-OCCUPANT');
  assert.equal(f.calls.some((c) => c.method === 'POST' && c.path === '/files' || c.method === 'PATCH'), false);
});

test('keep both safely reuses an exact existing occupant rather than creating another ID', async () => {
  const f = await fixture();
  f.hooks.write = async (p) => { if (p.includes('Tdrive kept remote')) await f.add('occupied', p, bytes('REMOTE'), 5); };
  assert.equal((await f.manager.resolve('a.md', 'both', f.engine)).resolved, true);
  assert.equal(f.files.size, 2); assert.equal(f.calls.some((c) => c.path === '/files' && c.method === 'POST'), false); assert.equal(text(f.head().data), 'LOCAL');
});

test('copy verification failure leaves source untouched and never publishes partial bytes', async () => {
  const f = await fixture(); const write = f.local.writeBinary;
  f.local.writeBinary = async (...args) => { await write(...args); if (args[0].includes('Tdrive kept remote')) f.disk.set(args[0], bytes('PARTIAL')); };
  await assert.rejects(f.manager.resolve('a.md', 'both', f.engine), /copy verification failed/i);
  assert.equal(text(f.head().data), 'REMOTE'); assert.ok(f.manifest.getConflict('a.md')); assert.equal(f.calls.some((c) => c.method === 'PATCH' || c.path === '/files' && c.method === 'POST'), false);
});

test('a copy edited after upload is retained and a retry allocates a new exact preserved copy', async () => {
  const f = await fixture(); let editedPath;
  f.hooks.created = async (copy) => { editedPath = copy.name; f.disk.set(copy.name, bytes('USER-EDITED-COPY')); };
  await assert.rejects(f.manager.resolve('a.md', 'both', f.engine), /Preserved copy changed/);
  assert.equal(text(f.head().data), 'REMOTE'); assert.equal(text(f.disk.get(editedPath)), 'USER-EDITED-COPY');
  f.hooks.created = null;
  const result = await f.manager.resolve('a.md', 'both', f.engine);
  assert.equal(result.resolved, true); assert.notEqual(result.copyPath, editedPath); assert.equal(text(f.disk.get(result.copyPath)), 'REMOTE'); assert.equal(text(f.disk.get(editedPath)), 'USER-EDITED-COPY');
  assert.equal(new Set([...f.files.values()].map((file) => file.name)).size, f.files.size);
});

for (const originalSnapshotAvailable of [true, false]) {
  test('post-publication edited keep-both copy retry ' + (originalSnapshotAvailable ? 'recovers the original snapshot without another source PATCH' : 'refuses when no exact original snapshot remains'), async () => {
    const f = await fixture(); const originalCopy = f.manifest.getConflict('a.md').copyPath; let editedCopy;
    f.hooks.patched = async () => {
      const progress = f.manifest.getConflict('a.md').resolution;
      assert.equal(progress.publishing, true); editedCopy = progress.copyPath;
      f.disk.set(editedCopy, bytes('USER-EDITED-D'));
    };
    await assert.rejects(f.manager.resolve('a.md', 'both', f.engine), /Preserved copy changed/);
    assert.equal(text(f.head().data), 'LOCAL'); assert.equal(text(f.disk.get('a.md')), 'LOCAL');
    assert.equal(text(f.disk.get(editedCopy)), 'USER-EDITED-D'); assert.equal(text(f.disk.get(originalCopy)), 'REMOTE');
    assert.ok(f.manifest.getConflict('a.md')); assert.ok(f.durable().conflicts['a.md']); assert.equal(f.active(), null);
    f.hooks.patched = null;
    if (originalSnapshotAvailable) {
      const result = await f.manager.resolve('a.md', 'both', f.engine);
      assert.equal(result.resolved, true); assert.notEqual(result.copyPath, editedCopy);
      assert.equal(text(f.disk.get(result.copyPath)), 'REMOTE'); assert.equal(text(f.disk.get(originalCopy)), 'REMOTE');
      const copyId = f.manifest.getFile(result.copyPath).remoteId;
      assert.equal(text(f.files.get(copyId).data), 'REMOTE'); assert.equal(f.manifest.getFile(result.copyPath).localHash, await sha256Hex(bytes('REMOTE')));
      assert.equal(f.manifest.getFile('a.md').remoteId, 'remote-a'); assert.equal(f.manifest.getConflict('a.md'), undefined);
      assert.equal(f.files.size, 3); assert.equal(new Set([...f.files.values()].map((file) => file.name)).size, 3);
    } else {
      f.disk.set(originalCopy, bytes('USER-EDITED-SNAPSHOT'));
      await assert.rejects(f.manager.resolve('a.md', 'both', f.engine), /exact.*snapshot|Preserved remote copy changed/i);
      assert.equal(text(f.disk.get(originalCopy)), 'USER-EDITED-SNAPSHOT'); assert.ok(f.manifest.getConflict('a.md')); assert.equal(f.files.size, 2);
    }
    assert.equal(text(f.disk.get(editedCopy)), 'USER-EDITED-D'); assert.equal(text(f.head().data), 'LOCAL');
    assert.equal(f.calls.filter((c) => c.method === 'PATCH').length, 1); assert.equal(f.active(), null);
    assert.equal(f.calls.some((c) => c.method === 'DELETE' && c.path !== '/sync/leases'), false);
  });
}

test('keep both retries durable final-save failure after ledger reload without another ID or source write', async () => {
  const f = await fixture(); f.hooks.save = async (state) => { if (!state.conflicts['a.md']) throw new Error('final save lost'); };
  await assert.rejects(f.manager.resolve('a.md', 'both', f.engine), /final save lost/);
  assert.equal(f.files.size, 2); assert.equal(text(f.head().data), 'LOCAL');
  f.hooks.save = null; await f.manifest.load();
  assert.equal((await f.manager.resolve('a.md', 'both', f.engine)).resolved, true); assert.equal(f.files.size, 2);
  assert.equal(f.calls.filter((c) => c.method === 'POST' && c.path === '/files').length, 1); assert.equal(f.calls.filter((c) => c.method === 'PATCH').length, 1);
});

test('lost in-place response is recognized only by durable chosen intent and exact committed bytes', async () => {
  const f = await fixture(); f.hooks.patched = async () => { throw new Error('lost PATCH response'); };
  await assert.rejects(f.manager.resolve('a.md', 'local', f.engine), /lost PATCH response/);
  assert.ok(f.durable().conflicts['a.md'].resolution.publishing); assert.equal(f.manifest.getFile('a.md').localHash, undefined);
  f.hooks.patched = null; await f.manifest.load();
  assert.equal((await f.manager.resolve('a.md', 'local', f.engine)).resolved, true); assert.equal(f.calls.filter((c) => c.method === 'PATCH').length, 1);
});

test('failed merge upload retries exact merged bytes without remerging or dropping the BOM/backup', async () => {
  const f = await fixture({ base: '\ufefffirst\nmiddle\nlast\n', localText: '\ufeffLOCAL\nmiddle\nlast\n', remoteData: bytes('\ufefffirst\nmiddle\nREMOTE\n') });
  const push = f.engine.pushLocal.bind(f.engine); f.engine.pushLocal = async () => { throw new Error('merge upload unavailable'); };
  await assert.rejects(f.manager.resolve('a.md', 'merge', f.engine), /merge upload unavailable/);
  assert.equal(text(f.disk.get('a.md')), '\ufeffLOCAL\nmiddle\nREMOTE\n');
  const backup = f.manifest.getConflict('a.md').resolution.backupPath; assert.equal(text(f.disk.get(backup)), '\ufeffLOCAL\nmiddle\nlast\n');
  f.engine.pushLocal = push;
  assert.equal((await f.manager.resolve('a.md', 'merge', f.engine)).resolved, true); assert.equal(text(f.head().data), '\ufeffLOCAL\nmiddle\nREMOTE\n'); assert.equal(text(f.disk.get(backup)), '\ufeffLOCAL\nmiddle\nlast\n');
});

test('durable intent-save failure forbids publication and does not retain unsaved authorization', async () => {
  const f = await fixture(); f.hooks.save = async () => { throw new Error('intent save failed'); };
  await assert.rejects(f.manager.resolve('a.md', 'local', f.engine), /intent save failed/);
  assert.equal(f.manifest.getConflict('a.md').resolution, undefined); assert.equal(f.calls.some((c) => c.method === 'PATCH'), false); assert.equal(text(f.head().data), 'REMOTE');
});

test('foreign and KEEP upload journals remain byte-for-byte unchanged during keep-both', async () => {
  const f = await fixture();
  const journals = f.manifest.data.meta.uploadSessions; journals.foreign = { owner: 'other-device', phase: 'completing', KEEP: true }; journals.KEEP = { owner: 'other-device', phase: 'unknown', data: ['retain'] };
  const before = JSON.stringify(journals);
  assert.equal((await f.manager.resolve('a.md', 'both', f.engine)).resolved, true); assert.equal(JSON.stringify(f.manifest.data.meta.uploadSessions), before);
});

test('blocked lease cannot publish either version or allocate a misleading kept-both copy', async () => {
  const f = await fixture(); f.engine.acquireOperationLease = async () => ({ blocked: true });
  await assert.rejects(f.manager.resolve('a.md', 'both', f.engine), /Another device/);
  assert.equal([...f.disk.keys()].some((p) => p.includes('Tdrive kept remote')), false); assert.equal(f.calls.some((c) => c.method === 'PATCH' || c.path === '/files' && c.method === 'POST'), false); assert.ok(f.manifest.getConflict('a.md'));
});

test('initial newer generation with desired local bytes is not mistaken for a lost-response retry', async () => {
  const f = await fixture(); await f.add('remote-a', 'a.md', bytes('LOCAL'), 12);
  await assert.rejects(f.manager.resolve('a.md', 'local', f.engine), /Reopen|reopen/);
  assert.equal(f.manifest.getConflict('a.md').contentVersion, 12); assert.equal(f.calls.some((c) => c.method === 'PATCH'), false);
});

test('same-byte merge needs no base text or content write', async () => {
  const f = await fixture({ remoteData: bytes('LOCAL') }); delete f.manifest.getFile('a.md').baseText;
  assert.equal((await f.manager.resolve('a.md', 'merge', f.engine)).resolved, true);
  assert.equal(f.calls.some((c) => c.localWrite || c.method === 'PATCH' || c.path === '/files' && c.method === 'POST'), false);
});

test('copy allocation save failure creates no untracked normal-sync copy', async () => {
  const f = await fixture(); f.hooks.save = async () => { throw new Error('allocated path save failed'); };
  await assert.rejects(f.manager.resolve('a.md', 'both', f.engine), /allocated path save failed/);
  assert.equal([...f.disk.keys()].some((p) => p.includes('Tdrive kept remote')), false); assert.equal(f.files.size, 1);
  assert.equal(f.manifest.getConflict('a.md').resolution, undefined);
});

test('keep remote forwards the reviewed local hash into the queued write, not just its earlier check', async () => {
  const f = await fixture(); let expectedHash;
  f.hooks.write = async (p, _data, options) => { if (p === 'a.md') { expectedHash = options.expectedHash; f.disk.set(p, bytes('LATE-LOCAL-EDIT')); } };
  await assert.rejects(f.manager.resolve('a.md', 'remote', f.engine), /Local file changed/);
  assert.equal(expectedHash, await sha256Hex(bytes('LOCAL'))); assert.equal(text(f.disk.get('a.md')), 'LATE-LOCAL-EDIT'); assert.equal(text(f.head().data), 'REMOTE'); assert.ok(f.manifest.getConflict('a.md'));
});

test('initially missing local remote-choice creation refuses a queued arrival', async () => {
  const f = await fixture(); f.disk.delete('a.md'); let createOnly;
  f.hooks.write = async (p, _data, options) => { if (p === 'a.md') { createOnly = options.createOnly; f.disk.set(p, bytes('USER-ARRIVAL')); } };
  await assert.rejects(f.manager.resolve('a.md', 'remote', f.engine), (e) => e.code === 'LOCAL_FILE_EXISTS');
  assert.equal(createOnly, true); assert.equal(text(f.disk.get('a.md')), 'USER-ARRIVAL'); assert.ok(f.manifest.getConflict('a.md'));
});

test('keep-both converges through normal full-engine local-server syncs without new IDs or conflict loops', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', 'BASE'); assert.equal((await env.engine.syncNow()).status, 'ok');
    const original = env.server.findLive('/Vault/', 'a.md');
    env.local.put('a.md', 'LOCAL'); env.server.modifyFile(original.id, 'REMOTE');
    assert.equal((await env.engine.syncNow()).conflicts, 1);
    const result = await env.engine.conflicts.resolve('a.md', 'both', env.engine);
    assert.equal(result.resolved, true); assert.equal(env.server.findLive('/Vault/', 'a.md').id, original.id);
    assert.equal(original.bytes.toString(), 'LOCAL'); assert.equal(env.local.text(result.copyPath), 'REMOTE');
    const remoteCopy = env.server.findLive('/Vault/', result.copyPath); assert.ok(remoteCopy); assert.equal(remoteCopy.bytes.toString(), 'REMOTE');
    for (let i = 0; i < 2; i++) {
      const next = await env.engine.syncNow(); assert.equal(next.status, 'ok', JSON.stringify(next.failures));
      assert.equal(next.uploaded + next.downloaded + next.conflicts, 0); assert.equal(env.server.liveFiles().length, 2);
      assert.equal(env.server.findLive('/Vault/', result.copyPath).id, remoteCopy.id); assert.deepEqual(env.manifest.conflictKeys(), []);
    }
  } finally { await env.stop(); }
});
