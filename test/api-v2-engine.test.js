'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { SyncEngine, emptyReport } = require('../src/SyncEngine');
const { ManifestStore } = require('../src/ManifestStore');
const { MemFS } = require('./helpers');
const { sha256Hex } = require('../src/util');
const { TdriveClient } = require('../src/TdriveClient');
const { fingerprintOf } = require('../src/Chunker');

const flags = { syncLeases: true, atomicIfEmpty: true, expectedContentVersion: true, contentVersions: true, uploadSessionListing: true };
const fault = (status, code, message = code) => Object.assign(new Error(message), { status, code });
const gate = () => { let resolve; const promise = new Promise((r) => { resolve = r; }); return { promise, resolve }; };

// Entirely in memory: the lease authority and metadata/content generations are independent of the engine.
function authority() {
  let clock = 1700000000000; let serial = 0;
  const folders = new Set(['/']); const files = new Map(); const leases = new Map(); const ops = [];
  const overlap = (a, b) => a.startsWith(b) || b.startsWith(a);
  const check = (path, token) => {
    for (const lease of leases.values()) if (lease.until > clock && overlap(path, lease.path) && lease.token !== token) throw fault(423, 'SYNC_LEASE_LOCKED');
  };
  const metadata = (f) => ({ id: f.id, name: f.name, path: f.path, size: f.data.length, mtime: f.mtime, contentVersion: f.contentVersion, version: f.version, trashed: f.trashed });
  const edit = (f, data) => { f.data = Buffer.from(data); f.contentVersion++; f.mtime = ++clock; };
  const add = (path, name, data) => { const f = { id: 'f' + ++serial, path, name, data: Buffer.from(data), contentVersion: 1, version: 17, mtime: ++clock }; files.set(f.id, f); folders.add(path); return f; };
  const device = (options = {}) => {
    const settings = { remoteFolder: '/Vault/', remoteLock: true, mode: 'two-way', deletionPolicy: 'trash', maxDeletesPerRun: 20, incrementalScan: false, excludePatterns: [], syncFolders: [], cdcFiles: false, dedupeUploads: false, ...options.settings };
    const hooks = {}; let saved = null; let installed = null; let ensure = null;
    const caps = { ...flags, ...options.caps };
    const local = new MemFS();
    const manifest = new ManifestStore({ read: async () => saved, write: async (s) => { saved = s; } }, () => clock);
    const response = (l) => ({ path: l.path, token: l.token, serverTime: new Date(clock + 9000000).toISOString(), expiresAt: new Date(l.until + 9000000).toISOString() });
    const mutate = async (path, operation) => {
      if (ensure) await ensure();
      check(path, installed && installed.token);
      return operation();
    };
    const client = {
      baseUrl: 'https://engine-v2.invalid', apiKey: 'fake-key', redact: String,
      capabilities: async () => caps,
      limits: async () => ({ oneShotMaxBytes: 1048576, chunkBytes: 4096 }),
      request: async () => ({ activity: [] }),
      acquireLease: async ({ path, owner, ttlSeconds }) => {
        ops.push(['acquire', path, owner, ttlSeconds]); check(path);
        if (hooks.beforeAcquire) await hooks.beforeAcquire();
        const l = { path, owner, token: 'SECRET-LEASE-' + ++serial, until: clock + ttlSeconds * 1000 };
        leases.set(l.token, l);
        if (hooks.acquire) return hooks.acquire(l, response(l));
        return response(l);
      },
      renewLease: async ({ path, token, ttlSeconds }) => {
        ops.push(['renew', path]);
        if (hooks.renew) await hooks.renew();
        const l = leases.get(token);
        if (!l || l.until <= clock) throw fault(423, 'LEASE_EXPIRED', token);
        l.until = clock + ttlSeconds * 1000; const renewed = response(l); delete renewed.token; return renewed;
      },
      releaseLease: async ({ path, token }) => { ops.push(['release', path]); leases.delete(token); if (hooks.release) await hooks.release(token); },
      setLease: (lease, guard) => { installed = lease; ensure = guard; },
      clearLease: () => { installed = null; ensure = null; },
      listFolder: async (path) => {
        if (hooks.list) await hooks.list(path);
        if (!folders.has(path)) throw fault(404, 'NOT_FOUND');
        return { files: [...files.values()].filter((f) => f.path === path && !f.trashed).map(metadata), folders: [...folders].filter((p) => p !== path && p.startsWith(path) && p.slice(path.length).split('/').filter(Boolean).length === 1).map((p) => ({ name: p.slice(path.length, -1), id: p })) };
      },
      getFile: async (id) => { if (hooks.get) await hooks.get(id); const f = files.get(id); return f ? metadata(f) : null; },
      createFolder: async (name, parent) => mutate(parent + name + '/', async () => { ops.push(['mkdir', parent + name + '/']); folders.add(parent + name + '/'); }),
      patchContent: async (id, data, { expectedVersion } = {}) => {
        const f = files.get(id);
        if (hooks.patch) await hooks.patch(f, expectedVersion);
        return mutate(f.path, async () => {
          if (caps.expectedContentVersion && expectedVersion !== f.contentVersion) throw fault(409, 'VERSION_CONFLICT');
          ops.push(['patch', id, expectedVersion]); edit(f, data); return metadata(f);
        });
      },
      move: async (id, { name, path }) => mutate(files.get(id).path, async () => { const f = files.get(id); check(path, installed && installed.token); f.name = name; f.path = path; f.mtime = ++clock; ops.push(['move', id]); return metadata(f); }),
      trash: async (id) => mutate(files.get(id).path, async () => { ops.push(['trash', id]); files.get(id).trashed = true; }),
      deleteFolder: async (path, id, opts) => mutate(path, async () => {
        ops.push(['deleteFolder', path, opts]);
        if (hooks.deleteFolder) await hooks.deleteFolder(path);
        if ([...files.values()].some((f) => !f.trashed && f.path.startsWith(path)) || [...folders].some((p) => p !== path && p.startsWith(path))) throw fault(409, 'FOLDER_NOT_EMPTY');
        folders.delete(path);
      }),
      restore: async (id) => mutate(files.get(id).path, async () => { files.get(id).trashed = false; ops.push(['restore', id]); }),
    };
    const engine = new SyncEngine({ client, local, manifest, getSettings: () => settings, now: () => clock,
      uploadFn: async (_, o) => mutate(o.folderPath, async () => { ops.push(['upload', o.folderPath]); return metadata(add(o.folderPath, o.name, o.data)); }),
      revisionFn: async (_, o) => { if (hooks.revision) return hooks.revision(o); return null; },
      downloadFn: async (_, f) => { if (hooks.download) await hooks.download(f); return Buffer.from(files.get(f.id).data); },
      manifestFn: async () => null,
    });
    return { engine, client, local, manifest, settings, hooks, caps, saved: () => saved, installed: () => installed, ensure: () => ensure };
  };
  return { device, folders, files, leases, ops, add, edit, metadata, now: () => clock, advance: (ms) => { clock += ms; } };
}

test('whole normalized root leases reject ancestor/descendant overlaps but allow sibling devices', async () => {
  const a = authority(); a.folders.add('/Vault/'); a.folders.add('/Vault/Child/'); a.folders.add('/Sibling/');
  const root = a.device({ settings: { remoteFolder: '//Vault//', syncFolders: ['Selected'] } });
  const child = a.device({ settings: { remoteFolder: '/Vault/Child/' } });
  const parent = a.device({ settings: { remoteFolder: '/' } });
  const sibling = a.device({ settings: { remoteFolder: '/Sibling/' } });
  await root.engine.acquireOperationLease();
  try {
    await assert.rejects(child.engine.acquireOperationLease(), (e) => e.status === 423);
    await assert.rejects(parent.engine.acquireOperationLease(), (e) => e.status === 423);
    await sibling.engine.acquireOperationLease();
    assert.equal(a.ops.find(([op]) => op === 'acquire')[1], '/Vault/');
    assert.equal(a.leases.size, 2);
    assert.ok(!root.saved().includes('SECRET-LEASE'));
    assert.ok(root.saved().includes(root.manifest.data.meta.deviceId));
  } finally { await root.engine.releaseOperationLease(); await sibling.engine.releaseOperationLease(); }
});

for (const [minutes, ttl] of [[undefined, 60], [0, 15], [0.1, 15], [2, 120], [15, 300]]) {
  test(`lease TTL clamps legacy minutes ${minutes} to ${ttl} seconds`, async () => {
    const a = authority(); const d = a.device({ settings: { remoteLockMinutes: minutes } });
    await d.engine.acquireOperationLease();
    try { assert.equal(a.ops.find(([op]) => op === 'acquire')[3], ttl); }
    finally { await d.engine.releaseOperationLease(); }
  });
}

test('fresh roots bootstrap only empty ancestors before acquisition; tokenless overlap is blocked', async () => {
  const a = authority(); const d = a.device({ settings: { remoteFolder: '/A/B/Root/' } }); d.local.put('secret.md', 'secret');
  const report = await d.engine.syncNow(); assert.equal(report.status, 'ok');
  assert.deepEqual(a.ops.slice(0, 4).map((op) => op.slice(0, 2)), [['mkdir', '/A/'], ['mkdir', '/A/B/'], ['mkdir', '/A/B/Root/'], ['acquire', '/A/B/Root/']]);
  assert.ok(a.ops.findIndex(([op]) => op === 'upload') > a.ops.findIndex(([op]) => op === 'acquire'));
  const owner = a.device({ settings: { remoteFolder: '/A/' } }); await owner.engine.acquireOperationLease();
  const blocked = a.device({ settings: { remoteFolder: '/A/Missing/' } });
  try {
    assert.equal((await blocked.engine.syncNow()).status, 'locked');
    assert.equal(a.folders.has('/A/Missing/'), false);
    const unlocked = a.device({ settings: { remoteFolder: '/A/B/Root/', remoteLock: false } }); unlocked.local.put('new.md', 'new');
    assert.equal((await unlocked.engine.syncNow()).status, 'locked');
    assert.ok(![...a.files.values()].some((f) => f.name === 'new.md'));
  } finally { await owner.engine.releaseOperationLease(); }
});

test('readonly preparation, dry run and Wi-Fi deferral never acquire or bootstrap leases', async () => {
  const a = authority(); const d = a.device({ settings: { remoteFolder: '/Missing/' } });
  await d.engine.prepare({ acquireLease: false });
  d.local.put('a.md', 'local'); assert.equal((await d.engine.syncNow({ dryRun: true })).status, 'ok');
  d.settings.wifiOnly = true; d.settings.networkIsCellular = () => true;
  assert.equal((await d.engine.syncNow()).status, 'deferred');
  assert.equal(a.ops.length, 0); assert.equal(a.folders.has('/Missing/'), false); assert.equal(d.installed(), null);
});

test('lost acquisition response is not retried or recovered by owner and never leaks raw secrets', async () => {
  const a = authority(); const d = a.device(); d.local.put('a.md', 'private');
  d.hooks.acquire = (l) => { throw Object.assign(fault(0, 'NETWORK', l.token), { isNetwork: true, raw: { token: l.token } }); };
  const report = await d.engine.syncNow(); assert.equal(report.status, 'offline');
  assert.equal(a.ops.filter(([op]) => op === 'acquire').length, 1);
  assert.equal(a.ops.filter(([op]) => op === 'upload').length, 0); assert.equal(a.leases.size, 1);
  assert.ok(!JSON.stringify(report).includes('SECRET-LEASE')); assert.ok(!JSON.stringify(d.manifest.data).includes('SECRET-LEASE'));
  assert.equal(d.installed(), null); assert.equal(d.engine._leaseTimer, null);
});

test('per-attempt validation renews once for concurrent callers and uses skew-safe conservative deadlines', async () => {
  const a = authority(); const d = a.device();
  d.hooks.acquire = (l, response) => { a.advance(5000); return response; };
  await d.engine.acquireOperationLease();
  try {
    assert.equal(d.engine._lease.deadline - a.now(), 54900);
    assert.equal(d.engine._leaseTimer.hasRef(), false);
    a.advance(36000); const pending = gate(); d.hooks.renew = () => pending.promise;
    const one = d.ensure()(); const two = d.ensure()();
    await Promise.resolve(); await Promise.resolve(); pending.resolve(); await Promise.all([one, two]);
    assert.equal(a.ops.filter(([op]) => op === 'renew').length, 1);
    assert.ok(d.engine._lease.deadline > a.now() + 59000);
    a.advance(61000); await assert.rejects(d.ensure()(), (e) => e.status === 423);
    assert.equal(a.ops.filter(([op]) => op === 'renew').length, 1, 'expired leases cannot be recovered');
  } finally { await d.engine.releaseOperationLease(); }
});

test('heartbeat renewal is active without HTTP and pause prevents future mutation', async () => {
  const a = authority(); const d = a.device(); await d.engine.acquireOperationLease();
  try {
    const pending = gate(); d.hooks.renew = () => { pending.resolve(); };
    const callback = d.engine._leaseTimer._onTimeout; clearTimeout(d.engine._leaseTimer); callback(); await pending.promise; await d.engine._leaseRenewal;
    assert.equal(a.ops.filter(([op]) => op === 'renew').length, 1);
    d.engine.pause(); await assert.rejects(d.ensure()(), /cancelled/);
  } finally { await d.engine.releaseOperationLease(); }
  assert.equal(a.leases.size, 0); assert.equal(d.engine._leaseTimer, null); assert.equal(d.installed(), null);
});

for (const status of [401, 423, 503]) {
  test(`renewal failure ${status} releases tokens and clears timers after sync`, async () => {
    const a = authority(); const d = a.device(); d.local.put('a.md', 'private');
    d.engine.onProgress = (p) => { if (p.phase === 'sync' && !p.bytes) a.advance(41000); };
    d.hooks.renew = () => { throw fault(status, 'RENEW_FAILED', d.installed().token); };
    const report = await d.engine.syncNow(); assert.equal(report.status, status === 401 ? 'auth' : 'locked');
    assert.equal(a.leases.size, 0); assert.equal(d.installed(), null); assert.equal(d.engine._leaseTimer, null);
    assert.ok(!JSON.stringify(report).includes('SECRET-LEASE')); assert.ok(!d.saved().includes('SECRET-LEASE'));
  });
}

test('late acquire after cancellation and late renew during release cannot reinstall a token', async () => {
  const a = authority(); const d = a.device(); const acquired = gate(); const allow = gate();
  d.hooks.acquire = async (_, value) => { acquired.resolve(); await allow.promise; return value; };
  const opening = d.engine.acquireOperationLease(); await acquired.promise;
  d.engine.pause(); await d.engine.releaseOperationLease(); allow.resolve(); await assert.rejects(opening, /cancelled/);
  assert.equal(a.leases.size, 0); assert.equal(d.installed(), null);
  d.engine.stopRequested = false; delete d.hooks.acquire; await d.engine.acquireOperationLease();
  const renewing = gate(); const finish = gate(); d.hooks.renew = async () => { renewing.resolve(); await finish.promise; };
  a.advance(41000); const valid = d.ensure()(); await renewing.promise;
  const release = d.engine.releaseOperationLease(); finish.resolve(); await Promise.allSettled([valid, release]);
  assert.equal(a.leases.size, 0); assert.equal(d.installed(), null); assert.equal(d.engine._leaseTimer, null);
});

test('same current generation ignores metadata-only timestamps but protects contradictory hashes and sizes', async () => {
  const a = authority(); const d = a.device(); const f = a.add('/Vault/', 'a.md', 'BASE');
  const old = d.engine._remoteBaseline(a.metadata(f));
  assert.equal(old.remoteVersion, 17); assert.equal(old.remoteContentVersion, 1);
  const metadataOnly = { ...a.metadata(f), mtime: f.mtime + 9999, version: 18, etag: 'favorite-only' };
  assert.equal(d.engine._remoteChanged(metadataOnly, old), false);
  assert.equal(d.engine._remoteChanged({ ...metadataOnly, contentVersion: 2 }, old), true);
  assert.equal(d.engine._remoteChanged({ ...metadataOnly, size: 5 }, old), true);
  assert.equal(d.engine._remoteChanged({ ...metadataOnly, sha256: 'changed' }, { ...old, remoteSha256: 'old' }), true);
  delete old.remoteContentVersion;
  assert.equal(d.engine._remoteChanged(a.metadata(f), old), false, 'field appearance alone is not a content edit');
});

test('metadata-only change causes no conflict and conditional replacement binds the current generation', async () => {
  const a = authority(); const d = a.device(); d.local.put('a.md', 'BASE'); await d.engine.syncNow();
  const f = [...a.files.values()][0]; f.mtime += 10000; f.version++;
  d.local.put('a.md', 'LOCL'); const first = await d.engine.syncNow();
  assert.equal(first.uploaded, 1); assert.equal(first.conflicts, 0);
  assert.equal(a.ops.find(([op]) => op === 'patch')[2], 1);
  assert.equal(d.manifest.getFile('a.md').remoteContentVersion, 2);
  assert.equal((await d.engine.syncNow()).uploaded, 0);
});

for (const phase of ['preflight', 'patch', 'revision']) {
  test(`known content generation conflict at ${phase} preserves remote and modified local copies`, async () => {
    const a = authority(); const d = a.device(); d.local.put('a.md', 'BASE'); await d.engine.syncNow();
    const f = [...a.files.values()][0]; const entry = { ...d.manifest.getFile('a.md') }; d.local.put('a.md', 'LOCL');
    if (phase === 'preflight') d.engine.onProgress = (p) => { if (p.phase === 'sync' && p.action === 'upload' && !p.bytes) a.edit(f, 'REMO'); };
    if (phase === 'patch') d.hooks.patch = () => { a.edit(f, 'REMO'); d.local.put('a.md', 'LAST'); };
    if (phase === 'revision') d.hooks.revision = (o) => { assert.equal(o.expectedVersion, 1); a.edit(f, 'REMO'); throw fault(412, 'VERSION_CONFLICT'); };
    const report = await d.engine.syncNow(); assert.equal(report.conflicts, 1); assert.equal(report.uploaded, 0);
    assert.equal(f.data.toString(), 'REMO'); assert.equal(d.local.text('a.md'), phase === 'patch' ? 'LAST' : 'LOCL');
    const c = d.manifest.getConflict('a.md'); assert.equal(d.local.text(c.copyPath), 'REMO');
    assert.deepEqual(d.manifest.getFile('a.md'), entry);
    for (let i = 0; i < 2; i++) assert.equal((await d.engine.syncNow()).uploaded, 0);
  });
}

test('unknown baseline generation is bound from metadata, never from legacy archived version', async () => {
  const a = authority(); const d = a.device(); d.local.put('a.md', 'BASE'); await d.engine.syncNow();
  const f = [...a.files.values()][0]; delete d.manifest.getFile('a.md').remoteContentVersion;
  d.local.put('a.md', 'LOCL'); assert.equal((await d.engine.syncNow()).uploaded, 1);
  assert.equal(a.ops.find(([op]) => op === 'patch')[2], 1); assert.equal(f.version, 17);
});

test('reviewed expected generation cannot be silently refreshed by a direct conflict helper', async () => {
  const a = authority(); const d = a.device(); const f = a.add('/Vault/', 'a.md', 'REMO');
  f.contentVersion = 2;
  d.local.put('a.md', 'LOCL'); await d.engine.prepare();
  await d.engine.conflicts.record('a.md', a.metadata(f), f.data);
  try {
    await assert.rejects(d.engine.pushLocal('a.md', a.metadata(f), null, 'a.md', { expectedContentVersion: 1 }), (e) => e.code === 'remote_changed');
    assert.equal(f.data.toString(), 'REMO'); assert.ok(!a.ops.some(([op]) => op === 'patch'));
  } finally { await d.engine.releaseOperationLease(); }
});

test('move with edits binds the unchanged source generation after the metadata move', async () => {
  const a = authority(); const d = a.device(); d.local.put('Old/a.md', 'BASE'); d.local.put('keep.md', 'keep'); await d.engine.syncNow();
  const f = [...a.files.values()].find((x) => x.name === 'a.md');
  d.local.removeDir('Old'); d.local.put('New/a.md', 'LOCL'); d.manifest.data.meta.moveHints = { Old: { to: 'New' } };
  const report = await d.engine.syncNow(); assert.equal(report.moved, 1); assert.equal(report.conflicts, 0);
  assert.equal(f.path, '/Vault/New/'); assert.equal(f.data.toString(), 'LOCL');
  assert.equal(a.ops.find(([op, id]) => op === 'patch' && id === f.id)[2], 1);
  assert.equal(d.manifest.getFile('New/a.md').remoteContentVersion, 2);
});

test('a newer move-source generation cannot be rebound and overwritten even when its bytes match the base', async () => {
  const a = authority(); const d = a.device(); d.local.put('Old/a.md', 'BASE'); d.local.put('keep.md', 'keep'); await d.engine.syncNow();
  const f = [...a.files.values()].find((x) => x.name === 'a.md');
  d.local.removeDir('Old'); d.local.put('New/a.md', 'LOCL'); d.manifest.data.meta.moveHints = { Old: { to: 'New' } };
  let raced = false;
  d.engine.onProgress = (p) => { if (!raced && p.phase === 'sync' && p.current === 'New/a.md' && !p.bytes) { raced = true; a.edit(f, 'BASE'); } };
  const report = await d.engine.syncNow(); assert.equal(report.moved, 1); assert.equal(report.conflicts, 1);
  assert.equal(f.contentVersion, 2); assert.equal(f.data.toString(), 'BASE'); assert.equal(d.local.text('New/a.md'), 'LOCL');
  assert.ok(!a.ops.some(([op]) => op === 'patch')); assert.equal(d.local.text(d.manifest.getConflict('New/a.md').copyPath), 'BASE');
});

test('atomic ifEmpty protects concurrent arrival, keeps ledger, and rescans without permanent API fault', async () => {
  const a = authority(); const d = a.device(); d.local.put('keep.md', 'keep'); await d.local.mkdir('Old'); await d.engine.syncNow();
  d.local.dirs.delete('Old'); let arrived = false;
  d.hooks.deleteFolder = (path) => { if (!arrived) { arrived = true; a.add(path, 'arrival.md', 'NEW'); } };
  const first = await d.engine.syncNow(); assert.equal(first.status, 'blocked'); assert.equal(first.errors, 0); assert.equal(first.deletedRemote, 0);
  assert.equal(d.manifest.hasFolder('Old'), true); assert.equal(a.folders.has('/Vault/Old/'), true);
  assert.deepEqual(a.ops.find(([op]) => op === 'deleteFolder')[2], { ifEmpty: true });
  assert.equal(d.manifest.data.meta.remoteSnapshot, undefined);
  assert.equal(first.blockedFolders[0].code, 'FOLDER_NOT_EMPTY');
  delete d.hooks.deleteFolder; const second = await d.engine.syncNow();
  assert.equal(second.downloaded, 1); assert.equal(d.local.text('Old/arrival.md'), 'NEW'); assert.equal(second.deferredFolders, 0);
});

test('capability fingerprint invalidates snapshots and retries old backend blocks exactly once', async () => {
  const a = authority(); const d = a.device({ caps: { atomicIfEmpty: false } });
  await d.engine._bindScope(); await d.engine._discoverCapabilities();
  const meta = d.manifest.data.meta; const scope = meta.syncScope;
  meta.remoteSnapshot = { files: {} }; meta.blockedFolders = { Old: { path: 'Old', scope, code: 'UPLOAD_IN_PROGRESS', nextRetryAt: a.now() + 999999, serverRetryAt: a.now() + 999999 } };
  d.caps.atomicIfEmpty = true; await d.engine._discoverCapabilities();
  assert.equal(meta.remoteSnapshot, undefined); assert.equal(meta.blockedFolders.Old.nextRetryAt, 0); assert.equal(meta.blockedFolders.Old.serverRetryAt, 0);
  meta.blockedFolders.Old.nextRetryAt = a.now() + 999999; await d.engine._discoverCapabilities();
  assert.equal(meta.blockedFolders.Old.nextRetryAt, a.now() + 999999);
});

test('reconciliation preserves exact committed generation and never upgrades a legacy null receipt to GET head', async () => {
  const a = authority(); const d = a.device(); const f = a.add('/Vault/', 'a.md', 'BASE');
  d.local.put('a.md', 'BASE'); await d.engine.syncNow(); const entry = d.manifest.getFile('a.md');
  f.contentVersion = 2;
  const report = emptyReport(); await d.engine.refreshSigs(new Map([['a.md', f.id]])); assert.equal(entry.remoteContentVersion, 1);
  entry.remoteContentVersion = null; await d.engine._reconcile(report, new Map([['a.md', f.id]]));
  assert.equal(entry.remoteContentVersion, null); assert.equal(entry.remoteVersion, 17);
});

test('manual cleanup and deletion restore acquire and release their own API lease', async () => {
  const a = authority(); const d = a.device(); a.folders.add('/Vault/'); a.folders.add('/Vault/Old/');
  assert.equal((await d.engine.cleanEmptyRemoteFolders()).deleted, 1);
  assert.equal(a.leases.size, 0); assert.equal(d.installed(), null);
  const f = a.add('/Vault/', 'a.md', 'BASE'); f.trashed = true;
  d.manifest.data.meta.deletions = [{ runId: 1, path: 'a.md', side: 'remote', remoteId: f.id }];
  assert.equal((await d.engine.restoreLastDeletions()).restored, 1); assert.equal(f.trashed, false);
  assert.equal(a.leases.size, 0); assert.equal(a.ops.filter(([op]) => op === 'acquire').length, 2);
});

test('journal whitelist persists expectedVersion and exact receipt but never lease secrets', async () => {
  const a = authority(); const d = a.device(); await d.engine.acquireOperationLease();
  try {
    const journal = d.engine._upOpts().uploadJournal;
    await journal.set('key', { owner: journal.owner, phase: 'completing', token: d.installed().token, body: { expectedVersion: 9, token: d.installed().token }, receipt: { contentVersion: 10, version: 3, checksum: 'sum', etag: 'tag', token: d.installed().token } });
    const stored = await journal.get('key');
    assert.equal(stored.body.expectedVersion, 9); assert.deepEqual(stored.receipt, { contentVersion: 10, version: 3, checksum: 'sum', etag: 'tag' });
    assert.ok(!d.saved().includes('SECRET-LEASE')); assert.equal(stored.token, undefined);
  } finally { await d.engine.releaseOperationLease(); }
});

for (const withGeneration of [true, false]) {
  test(`durable revision resume uses a new lease and exact ${withGeneration ? 'current' : 'legacy null'} receipt generation`, async () => {
    let saved = null; let activeToken = null; let leaseNumber = 0; let failComplete = true; let session = null;
    const puts = []; const opens = []; const tokens = []; const releases = [];
    const reply = (data, status = 200) => ({ status, headers: {}, body: Buffer.from(JSON.stringify(status < 400 ? { ok: true, data } : { ok: false, error: { code: data, message: data } })) });
    const transport = async ({ url, method, headers, body }) => {
      const u = new URL(url); const path = u.pathname.slice('/api/v1'.length);
      if (!path) return reply({ capabilities: flags, upload: { chunkBytes: 4096, oneShotMaxBytes: 128 } });
      if (path === '/sync/leases') {
        assert.equal(headers['X-Sync-Lease'], undefined, 'lease controls are not scoped mutations');
        const value = JSON.parse(Buffer.from(body)); assert.equal(value.path, '/Vault/');
        if (method === 'POST') {
          assert.equal(activeToken, null); activeToken = 'RESUME-SECRET-' + ++leaseNumber;
          const now = Date.now(); return reply({ path: value.path, owner: value.owner, token: activeToken, serverTime: now, expiresAt: now + value.ttlSeconds * 1000 }, 201);
        }
        assert.equal(value.token, activeToken);
        if (method === 'DELETE') { releases.push(activeToken); activeToken = null; return reply({ released: true }); }
        assert.fail('Unexpected lease renewal during short synthetic transfer');
      }
      if (path === '/files' && method === 'GET') return reply({ files: [], folders: u.searchParams.get('path') === '/' ? [{ id: 'vault', name: 'Vault' }] : [] });
      if (path === '/files/base/chunks') return reply('NOT_FOUND', 404);
      if (path === '/files/base') return reply({ id: 'base', name: 'a.bin', path: '/Vault/', size: 8193, contentVersion: 9, version: 17, checksum: 'latest-head-checksum' });
      assert.equal(headers['X-Sync-Path'], '/Vault/'); assert.equal(headers['X-Sync-Lease'], activeToken); tokens.push(activeToken);
      if (path === '/uploads') {
        const value = JSON.parse(Buffer.from(body)); assert.equal(value.expectedVersion, 7); opens.push(value);
        const durable = Object.values(JSON.parse(saved).meta.uploadSessions)[0]; assert.equal(durable.body.expectedVersion, 7);
        if (!session) session = { id: 'u1', body: value, parts: new Map() };
        else assert.equal(value.clientKey, session.body.clientKey);
        return reply({ id: session.id, status: 'uploading', revision: true, totalChunks: value.totalChunks, chunkSize: 4096, received: [...session.parts.keys()], base: { parts: [] } });
      }
      const part = /^\/uploads\/u1\/parts\/(\d+)$/.exec(path);
      if (part) {
        const bytes = Buffer.from(body); assert.equal(await sha256Hex(bytes), headers['X-Content-SHA256']);
        session.parts.set(Number(part[1]), bytes); puts.push(Number(part[1])); return reply({ idx: Number(part[1]) });
      }
      if (path === '/uploads/u1/complete') {
        assert.equal(JSON.parse(Buffer.from(body)).expectedVersion, 7);
        if (failComplete) { failComplete = false; return reply('RevisionDeferred', 503); }
        const hashes = await Promise.all([...session.parts.values()].map((bytes) => sha256Hex(bytes)));
        assert.equal(await fingerprintOf(hashes), session.body.fingerprint);
        return reply({ fileId: 'base', checksum: session.body.fingerprint, version: 3, ...(withGeneration ? { contentVersion: 8 } : {}) });
      }
      assert.fail('Unexpected synthetic route ' + method + ' ' + path);
    };
    const settings = { remoteFolder: '/Vault/', remoteLock: true, dedupeUploads: false };
    const local = new MemFS(); const storage = { read: async () => saved, write: async (s) => { saved = s; } };
    const makeEngine = (manifest) => {
      const client = new TdriveClient({ baseUrl: 'https://resume-v2.invalid', apiKey: 'fake-key', transport, maxRetries: 0, maxServerRetries: 0 });
      return new SyncEngine({ client, local, manifest, getSettings: () => settings });
    };
    const first = makeEngine(new ManifestStore(storage));
    const options = { name: 'a.bin', folderPath: '/Vault/', data: Buffer.alloc(8193, 65), mtime: 1234, old: { id: 'base' }, chunker: false, expectedVersion: 7 };
    await first.acquireOperationLease();
    try { await assert.rejects(first._replaceUpload(options), (e) => e.code === 'RevisionDeferred'); }
    finally { await first.releaseOperationLease(); }
    assert.equal(Object.values(JSON.parse(saved).meta.uploadSessions)[0].phase, 'completing');
    assert.ok(!saved.includes('RESUME-SECRET')); const sent = puts.length;
    const reloaded = new ManifestStore(storage); await reloaded.load(); const second = makeEngine(reloaded);
    await second.acquireOperationLease();
    try {
      const result = await second._replaceUpload(options);
      assert.equal(result.contentVersion, withGeneration ? 8 : null); assert.equal(result.version, 3); assert.equal(result.checksum, session.body.fingerprint);
      assert.equal(puts.length, sent); assert.equal(opens.length, 2); assert.equal(tokens[0], 'RESUME-SECRET-1'); assert.equal(tokens.at(-1), 'RESUME-SECRET-2');
      const record = Object.values(JSON.parse(saved).meta.uploadSessions)[0]; assert.equal(record.phase, 'completed'); assert.equal(record.body.expectedVersion, 7);
      assert.equal(record.receipt.contentVersion, withGeneration ? 8 : undefined);
      const again = await second._replaceUpload(options); assert.equal(again.contentVersion, withGeneration ? 8 : null); assert.equal(opens.length, 2);
    } finally { await second.releaseOperationLease(); }
    assert.equal(releases.length, 2); assert.equal(activeToken, null); assert.ok(!saved.includes('RESUME-SECRET'));
  });
}

test('invalid acquisition acknowledgement still releases its memory-only token without exposing raw response', async () => {
  const a = authority(); const d = a.device();
  d.hooks.acquire = (_, value) => ({ ...value, expiresAt: 'invalid' });
  assert.equal((await d.engine.syncNow()).status, 'locked');
  assert.equal(a.leases.size, 0); assert.equal(d.installed(), null); assert.equal(d.engine._leaseTimer, null);
  assert.ok(!JSON.stringify(d.manifest.data).includes('SECRET-LEASE'));
});

test('ordinary mutation errors and release errors cannot persist or report the live token', async () => {
  const a = authority(); const d = a.device(); d.local.put('a.md', 'BASE'); await d.engine.syncNow(); d.local.put('a.md', 'LOCL');
  d.hooks.patch = () => { throw fault(400, 'BAD_MUTATION', d.installed().token); };
  d.hooks.release = (token) => { throw new Error(token); };
  const report = await d.engine.syncNow(); assert.equal(report.status, 'errors');
  assert.ok(!JSON.stringify(report).includes('SECRET-LEASE')); assert.ok(!d.saved().includes('SECRET-LEASE'));
  assert.equal(d.engine._lease, null); assert.equal(d.installed(), null); assert.equal(d.engine._leaseTimer, null);
});

test('dry run cannot disable lease acquisition for a subsequent operation on the same engine', async () => {
  const a = authority(); const d = a.device(); d.local.put('a.md', 'local');
  await d.engine.syncNow({ dryRun: true }); await d.engine.acquireOperationLease();
  try { assert.ok(d.installed()); assert.equal(a.leases.size, 1); }
  finally { await d.engine.releaseOperationLease(); }
});

test('lease loss after a successful file preserves its durable baseline and stops all later writes', async () => {
  const a = authority(); const d = a.device(); d.local.put('a.md', 'first'); d.local.put('b.md', 'second');
  d.engine.onProgress = (p) => { if (p.phase === 'sync' && p.current === 'b.md' && !p.bytes) a.advance(61000); };
  const report = await d.engine.syncNow(); assert.equal(report.status, 'locked'); assert.equal(report.uploaded, 1);
  assert.ok(JSON.parse(d.saved()).files['a.md']); assert.equal(JSON.parse(d.saved()).files['b.md'], undefined);
  assert.equal([...a.files.values()].filter((f) => !f.trashed).length, 1); assert.equal(a.leases.size, 0);
});

test('only a legacy server without syncLeases uses and releases the hidden marker fallback', async () => {
  const a = authority(); const d = a.device({ caps: { syncLeases: false } }); a.folders.add('/Vault/');
  let marker = null;
  d.client.request = async (request) => {
    assert.equal(request.method, 'POST'); assert.equal(request.path, '/files'); assert.equal(request.query.path, '/Vault/');
    assert.ok(request.query.name.startsWith('.tdrive-sync.lock-'));
    marker = { id: 'legacy-marker', name: request.query.name, size: request.body.byteLength }; return { file: marker };
  };
  d.client.trash = async (id) => { assert.equal(id, 'legacy-marker'); marker = null; };
  const handle = await d.engine.acquireOperationLease();
  assert.equal(handle.id, 'legacy-marker'); assert.ok(marker); assert.equal(a.ops.filter(([op]) => op === 'acquire').length, 0);
  await d.engine.releaseOperationLease(); assert.equal(marker, null);
});
