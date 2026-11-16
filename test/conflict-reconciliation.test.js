'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { SyncEngine } = require('../src/SyncEngine');
const { ManifestStore } = require('../src/ManifestStore');
const { sha256Hex } = require('../src/util');
const { fingerprintOf } = require('../src/Chunker');
const { MemFS } = require('./helpers');

const clone = (value) => JSON.parse(JSON.stringify(value));

// All metadata, bytes, races and persistence are synthetic. No vault or HTTP access.
async function fixture(options = {}) {
  const settings = { remoteFolder: '/Vault/', remoteLock: false, incrementalScan: false,
    mode: 'two-way', deletionPolicy: 'off', excludePatterns: [], syncFolders: [],
    cdcFiles: false, mergeTextFiles: false, ...options.settings };
  const hooks = {}; const ops = []; const files = new Map(); const folders = new Set(['/', '/Vault/']);
  let saved = null; let gets = 0;
  const remote = { id: 'original-id', name: 'a.md', path: '/Vault/', data: Buffer.from(options.remote || 'SAME'), contentVersion: 2, mtime: 9000 };
  files.set(remote.id, remote);
  const metadata = (file) => ({ id: file.id, path: file.path, name: file.name, size: file.data.length,
    contentVersion: file.contentVersion, etag: file.etag, checksum: file.checksum, mtime: file.mtime, trashed: file.trashed });
  const local = new MemFS(); local.put('a.md', options.local || 'SAME');
  const read = local.readBinary.bind(local);
  local.readBinary = async (path) => { ops.push(['local-read', path]); if (hooks.read) await hooks.read(path); return read(path); };
  const write = local.writeBinary.bind(local);
  local.writeBinary = async (...args) => { ops.push(['local-write', args[0]]); return write(...args); };
  const client = {
    baseUrl: 'https://conflict-fixture.invalid', apiKey: 'fake-key', redact: String,
    limits: async () => ({ oneShotMaxBytes: 1048576, chunkBytes: 4096 }),
    capabilities: async () => ({ expectedContentVersion: true }),
    request: async () => ({ activity: [] }),
    listFolder: async (path) => {
      ops.push(['list', path]);
      if (hooks.list) { const result = await hooks.list(path); if (result) return result; }
      if (!folders.has(path)) throw Object.assign(new Error('Missing folder'), { status: 404 });
      return { files: [...files.values()].filter((f) => !f.trashed && f.path === path).map(metadata), folders: [] };
    },
    getFile: async (id) => {
      ops.push(['get', id]); gets++;
      if (hooks.get) { const result = await hooks.get(id, gets); if (result !== undefined) return result; }
      const file = files.get(id); return file ? metadata(file) : null;
    },
    createFolder: async (name, path) => { ops.push(['mkdir', path + name]); folders.add(path + name + '/'); },
    patchContent: async (id, data) => {
      ops.push(['patch', id]); const file = files.get(id); file.data = Buffer.from(data); file.contentVersion++;
      return metadata(file);
    },
    trash: async (id) => { ops.push(['trash', id]); files.get(id).trashed = true; },
    move: async () => { assert.fail('Equality reconciliation must never rename remotely'); },
  };
  const manifest = new ManifestStore({ read: async () => saved, write: async (value) => {
    ops.push(['save']); if (hooks.save) await hooks.save(JSON.parse(value)); saved = value;
  } });
  const engine = new SyncEngine({ client, local, manifest, getSettings: () => settings,
    downloadFn: async (_, pinned, opts) => {
      ops.push(['download', pinned.id]);
      const bytes = Buffer.from(files.get(pinned.id).data);
      if (hooks.download) { const result = await hooks.download(pinned, opts); if (result) return result; }
      if (opts.expectSha) assert.equal(await sha256Hex(bytes), opts.expectSha);
      return bytes;
    },
    manifestFn: async (_, id) => {
      ops.push(['manifest', id]);
      if (hooks.map) return hooks.map(id);
      return null;
    },
    uploadFn: async (_, opts) => {
      ops.push(['upload', opts.name]); if (hooks.upload) await hooks.upload(opts);
      const file = { id: 'created-' + files.size, name: opts.name, path: opts.folderPath,
        data: Buffer.from(opts.data), contentVersion: 1, mtime: opts.mtime };
      files.set(file.id, file); return metadata(file);
    },
    revisionFn: async (_, opts) => {
      ops.push(['revision', opts.id]); if (hooks.upload) await hooks.upload(opts);
      const file = files.get(opts.id); file.data = Buffer.from(opts.data); file.contentVersion++;
      return metadata(file);
    },
    operationValidity: async () => { if (hooks.validity) return hooks.validity(); return true; },
  });
  await engine._bindScope();
  engine._folders.add('Vault');
  const conflict = { copyPath: 'a (Tdrive conflict 2026-01-01).md', remoteId: remote.id,
    contentVersion: 1, remoteHash: await sha256Hex('OLDER'), remoteSig: 'older-signature', at: 1,
    history: [{ copyPath: 'a (Tdrive conflict old).md', contentVersion: 0, remoteHash: 'historical' }] };
  local.put(conflict.copyPath, 'USER EDITED COPY'); local.put(conflict.history[0].copyPath, 'HISTORICAL COPY');
  manifest.setConflict('a.md', conflict);
  const baseline = { localHash: await sha256Hex('BASE'), localSize: 4, localMtime: -1,
    remoteId: remote.id, remoteContentVersion: 1, remoteSize: 4, remoteMtime: 8000, remoteSig: 'baseline', baseText: 'BASE' };
  manifest.setFile('a.md', baseline);
  await manifest.save(); ops.length = 0;
  const noWrites = () => {
    assert.ok(!ops.some(([op]) => ['local-write', 'mkdir', 'revision', 'patch', 'upload', 'trash'].includes(op)), JSON.stringify(ops));
    assert.equal(local.trashed.length, 0);
  };
  return { engine, settings, hooks, ops, local, client, remote, files, folders, manifest, conflict, baseline, metadata, noWrites,
    saved: () => JSON.parse(saved), getCount: () => gets };
}

for (const mode of ['two-way', 'upload-only', 'download-only']) {
  test(`existing conflict clears only verified current equality in ${mode}, retaining changed and historical copies`, async () => {
    const e = await fixture({ settings: { mode } });
    const report = await e.engine.syncNow({ only: ['a.md'] });
    assert.equal(report.status, 'ok'); assert.equal(report.conflicts, 0);
    assert.equal(report.uploaded + report.downloaded + report.moved, 0);
    assert.equal(e.manifest.getConflict('a.md'), undefined); assert.equal(e.saved().conflicts['a.md'], undefined);
    assert.equal(e.manifest.getFile('a.md').localHash, await sha256Hex('SAME'));
    assert.equal(e.manifest.getFile('a.md').remoteContentVersion, 2);
    assert.equal(e.local.text(e.conflict.copyPath), 'USER EDITED COPY');
    assert.equal(e.local.text(e.conflict.history[0].copyPath), 'HISTORICAL COPY');
    assert.equal(e.engine._touched.has('a.md'), false); e.noWrites();
    const downloads = e.ops.filter(([op]) => op === 'download').length;
    await e.engine.syncNow({ only: ['a.md'] });
    assert.equal(e.ops.filter(([op]) => op === 'download').length, downloads, 'saved equality is a stable baseline');
  });
}

for (const remote of ['DIFF', 'SAME\n', 'same', ' SAME']) {
  test(`differing plaintext ${JSON.stringify(remote)} never resolves or merges in upload-only mode`, async () => {
    const e = await fixture({ remote, settings: { mode: 'upload-only', mergeTextFiles: true } });
    const report = await e.engine.syncNow({ only: ['a.md'] });
    assert.equal(report.conflicts, 1); assert.equal(e.manifest.getConflict('a.md'), e.conflict);
    assert.equal(e.manifest.getFile('a.md'), e.baseline); assert.equal(e.local.text('a.md'), 'SAME');
    assert.equal(e.remote.data.toString(), remote); e.noWrites();
  });
}

for (const change of ['generation', 'missing-generation', 'etag', 'checksum', 'move', 'identity', 'trash', 'absent']) {
  test(`remote ${change} during awaited content verification preserves the conflict and baseline`, async () => {
    const e = await fixture();
    if (change === 'etag') e.remote.etag = 'head-2';
    if (change === 'checksum') e.remote.checksum = 'checksum-2';
    e.hooks.download = async () => {
      if (change === 'generation') { e.remote.contentVersion = 3; e.remote.data = Buffer.from('NEW!'); }
      if (change === 'missing-generation') e.remote.contentVersion = null;
      if (change === 'etag') e.remote.etag = 'head-3';
      if (change === 'checksum') e.remote.checksum = 'checksum-3';
      if (change === 'move') e.remote.path = '/Vault/Elsewhere/';
      if (change === 'identity') e.hooks.get = () => ({ ...e.metadata(e.remote), id: 'impostor' });
      if (change === 'trash') e.remote.trashed = true;
      if (change === 'absent') e.files.delete(e.remote.id);
    };
    const report = await e.engine.syncNow({ only: ['a.md'] });
    assert.equal(report.uploaded + report.downloaded, 0);
    assert.equal(e.manifest.getConflict('a.md'), e.conflict); assert.equal(e.manifest.getFile('a.md'), e.baseline);
    assert.equal(e.saved().conflicts['a.md'].remoteId, e.conflict.remoteId); e.noWrites();
  });
}

test('a stale listing generation cannot be silently replaced with the newer live head even if its bytes equal local', async () => {
  const e = await fixture();
  e.hooks.get = () => { e.remote.contentVersion = 3; return e.metadata(e.remote); };
  const report = await e.engine.syncNow({ only: ['a.md'] });
  assert.equal(report.errors, 1); assert.equal(e.manifest.getConflict('a.md'), e.conflict);
  assert.equal(e.manifest.getFile('a.md'), e.baseline); assert.ok(!e.ops.some(([op]) => op === 'download')); e.noWrites();
});

test('same name with a different remote ID is never associated with the conflict', async () => {
  const e = await fixture(); e.files.delete(e.remote.id);
  e.files.set('unrelated', { ...e.remote, id: 'unrelated' });
  await e.engine.syncNow({ only: ['a.md'] });
  assert.equal(e.manifest.getConflict('a.md'), e.conflict); assert.equal(e.manifest.getFile('a.md'), e.baseline);
  assert.ok(!e.ops.some(([op]) => op === 'download' || op === 'get')); e.noWrites();
});

test('an omitted listing is reconciled only by the exact conflict ID and verified location', async () => {
  const e = await fixture(); e.hooks.list = () => ({ files: [], folders: [] });
  await e.engine.syncNow({ only: ['a.md'] });
  assert.equal(e.manifest.getConflict('a.md'), undefined);
  assert.deepEqual(e.ops.filter(([op]) => op === 'get').map((op) => op[1]), [e.remote.id, e.remote.id]); e.noWrites();
});

for (const pathState of ['missing-path', 'wrong-name', 'move-pending', 'completed-path-pending-review', 'resolution-pending']) {
  test(`unsafe path or reviewed transaction ${pathState} remains open without remote rename fallback`, async () => {
    const e = await fixture();
    if (pathState === 'missing-path') e.remote.path = null;
    if (pathState === 'wrong-name') e.remote.name = 'other.md';
    if (pathState.includes('pending-review') || pathState === 'move-pending') { e.conflict.movePending = true; e.conflict.moveFrom = 'Old/a.md'; }
    if (pathState === 'resolution-pending') e.conflict.resolution = { choice: 'both', phase: 'publishing' };
    await e.engine.syncNow({ only: ['a.md'] });
    assert.equal(e.manifest.getConflict('a.md'), e.conflict); assert.equal(e.manifest.getFile('a.md'), e.baseline); e.noWrites();
  });
}

for (const timing of ['download', 'last-get', 'last-validity']) {
  test(`local same-stat edit during ${timing} is rehashed after awaited checks and never acknowledged`, async () => {
    const e = await fixture(); const before = await e.local.stat('a.md');
    const edit = () => { e.local.put('a.md', 'EDIT'); e.local.files.get('a.md').mtime = before.mtime; };
    if (timing === 'download') e.hooks.download = edit;
    if (timing === 'last-get') e.hooks.get = (_, count) => { if (count === 2) edit(); };
    if (timing === 'last-validity') e.hooks.validity = () => { if (e.getCount() >= 2) edit(); return true; };
    await e.engine.syncNow({ only: ['a.md'] });
    assert.equal(e.manifest.getConflict('a.md'), e.conflict); assert.equal(e.manifest.getFile('a.md'), e.baseline);
    assert.equal(e.local.text('a.md'), 'EDIT'); e.noWrites();
  });
}

for (const mutation of ['replacement', 'in-place']) {
  test(`a ${mutation} conflict record change during await is not cleared`, async () => {
    const e = await fixture();
    e.hooks.download = () => {
      if (mutation === 'replacement') e.manifest.setConflict('a.md', { ...e.conflict, at: 99 });
      else e.conflict.history.push({ copyPath: 'new-reviewed-copy.md' });
    };
    await e.engine.syncNow({ only: ['a.md'] });
    assert.ok(e.manifest.getConflict('a.md')); assert.equal(e.manifest.getFile('a.md'), e.baseline); e.noWrites();
  });
}

test('save failure restores the conflict, baseline and unverified marker; later refresh cannot acknowledge a newer head', async () => {
  const e = await fixture(); e.manifest.data.meta.unverifiedBaselineFiles = { 'a.md': true };
  let failed = false;
  e.hooks.save = (snapshot) => {
    if (!snapshot.conflicts['a.md'] && !failed) { failed = true; e.remote.contentVersion = 3; e.remote.data = Buffer.from('NEW!'); throw new Error('synthetic save failure'); }
  };
  const report = await e.engine.syncNow({ only: ['a.md'] });
  assert.equal(report.errors, 1); assert.equal(e.manifest.getConflict('a.md'), e.conflict);
  assert.equal(e.manifest.getFile('a.md'), e.baseline); assert.equal(e.manifest.data.meta.unverifiedBaselineFiles['a.md'], true);
  assert.equal(e.saved().files['a.md'].remoteContentVersion, 1); assert.ok(e.saved().conflicts['a.md']);
  await e.engine.syncNow({ only: ['a.md'] });
  assert.equal(e.manifest.getFile('a.md'), e.baseline); assert.equal(e.manifest.getConflict('a.md'), e.conflict); e.noWrites();
});

test('successful equality save does not later reconcile an unseen generation into its baseline', async () => {
  const e = await fixture();
  e.hooks.save = (snapshot) => { if (!snapshot.conflicts['a.md']) { e.remote.contentVersion = 3; e.remote.data = Buffer.from('NEW!'); } };
  await e.engine.syncNow({ only: ['a.md'] });
  assert.equal(e.manifest.getFile('a.md').remoteContentVersion, 2);
  assert.equal(e.manifest.getFile('a.md').localHash, await sha256Hex('SAME'));
  assert.equal(e.engine._touched.size, 0); e.noWrites();
});

for (const guard of ['dry-run', 'only', 'excluded', 'scope', 'size', 'cellular', 'wifi', 'paused', 'pause-during-download', 'memory-bound', 'unknown-size']) {
  test(`reconciliation honors ${guard} without unbounded local reads or user-data writes`, async () => {
    const e = await fixture(); let opts = { only: ['a.md'] };
    if (guard === 'dry-run') opts.dryRun = true;
    if (guard === 'only') opts.only = ['not-a.md'];
    if (guard === 'excluded') e.settings.excludePatterns = ['a.md'];
    if (guard === 'scope') e.settings.syncFolders = ['Selected'];
    if (guard === 'size') e.settings.maxFileBytes = 3;
    if (guard === 'cellular') { e.settings.networkIsCellular = () => true; e.settings.cellularMaxFileBytes = 3; }
    if (guard === 'wifi') { e.settings.networkIsCellular = () => true; e.settings.wifiOnly = true; }
    if (guard === 'paused') e.settings.paused = true;
    if (guard === 'pause-during-download') e.hooks.download = () => e.engine.pause();
    if (guard === 'memory-bound' || guard === 'unknown-size') e.local.list = async () => [{ path: 'a.md', mtime: 1, size: guard === 'memory-bound' ? 8 * 1024 * 1024 + 1 : null }];
    await e.engine.syncNow(opts);
    assert.ok(e.manifest.getConflict('a.md')); assert.deepEqual(e.manifest.getFile('a.md'), e.baseline); e.noWrites();
    if (guard !== 'pause-during-download') assert.ok(!e.ops.some(([op]) => op === 'local-read' || op === 'download'), JSON.stringify(e.ops));
  });
}

test('larger live remote or manifest size is deferred before content download', async () => {
  for (const stage of ['live', 'manifest']) {
    const e = await fixture({ settings: { maxFileBytes: 4 } });
    if (stage === 'live') e.hooks.get = () => ({ ...e.metadata(e.remote), size: 5 });
    else e.hooks.map = async () => ({ size: 5, contentVersion: 2, chunks: [{ size: 5, checksum: await sha256Hex('SAME!') }] });
    await e.engine.syncNow({ only: ['a.md'] });
    assert.ok(!e.ops.some(([op]) => op === 'download')); assert.equal(e.manifest.getConflict('a.md'), e.conflict); e.noWrites();
  }
});

test('shared manifest validator rejects a newer manifest before any reconciliation download', async () => {
  const e = await fixture();
  e.hooks.map = async () => ({ size: 4, contentVersion: 3, chunks: [{ size: 4, checksum: await sha256Hex('SAME') }] });
  assert.equal((await e.engine.syncNow({ only: ['a.md'] })).errors, 1);
  assert.ok(!e.ops.some(([op]) => op === 'download'));
  assert.equal(e.manifest.getConflict('a.md'), e.conflict); assert.equal(e.manifest.getFile('a.md'), e.baseline); e.noWrites();
});

test('manifest-only generation proof is pinned into the transfer and cannot clear against a newer live head', async () => {
  const e = await fixture(); e.remote.contentVersion = null;
  const hash = await sha256Hex('SAME'); const checksum = await fingerprintOf([hash]);
  e.hooks.map = () => ({ size: 4, contentVersion: 2, checksum, chunks: [{ size: 4, checksum: hash }] });
  e.hooks.download = (pinned) => {
    assert.equal(pinned.contentVersion, 2); assert.equal(pinned.checksum, checksum);
    e.remote.contentVersion = 3; e.remote.data = Buffer.from('NEW!');
    return Buffer.from('SAME');
  };
  assert.equal((await e.engine.syncNow({ only: ['a.md'] })).errors, 1);
  assert.equal(e.manifest.getConflict('a.md'), e.conflict); assert.equal(e.manifest.getFile('a.md'), e.baseline);
  assert.equal(e.ops.filter(([op]) => op === 'download').length, 1); e.noWrites();
});

test('a verified manifest generation can establish equality without borrowing the final GET generation', async () => {
  const e = await fixture(); e.remote.contentVersion = null;
  const hash = await sha256Hex('SAME'); const checksum = await fingerprintOf([hash]);
  e.hooks.map = () => ({ size: 4, contentVersion: 2, checksum, chunks: [{ size: 4, checksum: hash }] });
  e.hooks.get = (_, count) => { if (count === 2) return { ...e.metadata(e.remote), contentVersion: 2 }; };
  await e.engine.syncNow({ only: ['a.md'] });
  assert.equal(e.manifest.getConflict('a.md'), undefined);
  assert.equal(e.manifest.getFile('a.md').remoteContentVersion, 2); assert.equal(e.manifest.getFile('a.md').remoteChecksum, checksum); e.noWrites();
});

test('a transfer that mutates pinned generation evidence is rejected once with no retry or baseline publication', async () => {
  const e = await fixture();
  e.hooks.download = (pinned) => { pinned.contentVersion = 3; return Buffer.from('SAME'); };
  assert.equal((await e.engine.syncNow({ only: ['a.md'] })).errors, 1);
  assert.equal(e.ops.filter(([op]) => op === 'download').length, 1);
  assert.equal(e.manifest.getConflict('a.md'), e.conflict); assert.equal(e.manifest.getFile('a.md'), e.baseline); e.noWrites();
});

test('unknown legacy generation without content evidence stays review-only', async () => {
  const e = await fixture(); e.remote.contentVersion = null;
  await e.engine.syncNow({ only: ['a.md'] });
  assert.equal(e.manifest.getConflict('a.md'), e.conflict); assert.ok(!e.ops.some(([op]) => op === 'download')); e.noWrites();
});

test('plaintext equality uses configured decrypt/decompress download contract, not raw wire size or checksum', async () => {
  const e = await fixture();
  e.engine.crypto = { decrypt: async (data) => data }; e.engine.compressor = { decompress: async (data) => data };
  e.hooks.download = (_, opts) => { assert.equal(opts.crypto, e.engine.crypto); assert.equal(opts.compressor, e.engine.compressor); return Buffer.from('SAME'); };
  await e.engine.syncNow({ only: ['a.md'] });
  assert.equal(e.manifest.getConflict('a.md'), undefined); e.noWrites();
});

for (const target of ['new-file', 'revision', 'inline-patch']) {
  test(`expectedLocalHash rejects changed reviewed bytes before any remote mutation (${target})`, async () => {
    const e = await fixture(); const before = clone(e.manifest.data);
    if (target === 'inline-patch') e.engine.revisionFn = async () => null;
    const old = target === 'new-file' ? null : e.metadata(e.remote);
    await assert.rejects(e.engine.pushLocal(target === 'new-file' ? 'New/a.md' : 'a.md', old, undefined, 'a.md',
      { expectedLocalHash: await sha256Hex('REVIEWED'), expectedContentVersion: 2 }), (err) => err.code === 'local_changed');
    assert.deepEqual(clone(e.manifest.data), before);
    assert.equal(e.engine._remoteMutated, undefined); assert.ok(!e.ops.some(([op]) => op === 'list')); e.noWrites();
  });
}

test('matching expectedLocalHash uploads the immutable captured bytes despite a later source edit', async () => {
  const e = await fixture();
  const shared = Buffer.from('SAME'); e.local.readBinary = async () => shared;
  e.hooks.get = () => { shared.set(Buffer.from('EDIT')); e.local.put('a.md', 'EDIT'); };
  const result = await e.engine.pushLocal('a.md', e.metadata(e.remote), undefined, 'a.md', { expectedLocalHash: await sha256Hex('SAME') });
  assert.equal(result.id, e.remote.id); assert.equal(e.remote.data.toString(), 'SAME');
  assert.equal(e.local.text('a.md'), 'EDIT'); assert.equal(e.manifest.getFile('a.md').localHash, await sha256Hex('SAME'));
});

for (const occupant of ['different', 'folder', 'ambiguous']) {
  test(`createOnly upload refuses ${occupant} destinations without overwriting or opening nested conflicts`, async () => {
    const e = await fixture(); e.local.put('copy.md', 'COPY');
    const occupied = { ...e.remote, id: 'occupied', name: 'copy.md', data: Buffer.from('OTHER') }; e.files.set(occupied.id, occupied);
    if (occupant !== 'different') e.hooks.list = () => ({ files: occupant === 'ambiguous' ? [e.metadata(occupied), { ...e.metadata(occupied), id: 'duplicate' }] : [],
      folders: occupant === 'folder' ? [{ name: 'copy.md' }] : [] });
    await assert.rejects(e.engine.pushLocal('copy.md', null, undefined, 'copy.md', { expectedLocalHash: await sha256Hex('COPY'), createOnly: true }), (err) => err.code === 'remote_collision');
    assert.equal(e.manifest.getConflict('copy.md'), undefined); assert.equal(occupied.data.toString(), 'OTHER'); e.noWrites();
  });
}

test('createOnly can adopt an identical occupant without publishing a replacement', async () => {
  const e = await fixture(); e.local.put('copy.md', 'COPY');
  const occupant = { ...e.remote, id: 'copy-id', name: 'copy.md', data: Buffer.from('COPY') }; e.files.set(occupant.id, occupant);
  const result = await e.engine.pushLocal('copy.md', null, undefined, 'copy.md', { expectedLocalHash: await sha256Hex('COPY'), createOnly: true });
  assert.equal(result.id, occupant.id); assert.equal(result.unchanged, true);
  assert.equal(e.manifest.getFile('copy.md').localHash, await sha256Hex('COPY')); e.noWrites();
});

test('createOnly cannot be used with a replacement identity', async () => {
  const e = await fixture();
  await assert.rejects(e.engine.pushLocal('a.md', e.metadata(e.remote), undefined, 'a.md', { createOnly: true }), /cannot replace/);
  assert.equal(e.ops.length, 0);
});

for (const atomic of [true, false]) {
  test(`reviewed download forwards its local snapshot hash into the ${atomic ? 'atomic' : 'ordinary'} queued writer`, async () => {
    const e = await fixture(); const stat = await e.local.stat('a.md'); const hash = await sha256Hex('SAME');
    const guarded = async (path, data, mtime, options) => {
      assert.equal(options.expectedHash, hash);
      e.local.put(path, 'LATE');
      assert.notEqual(await sha256Hex(await e.local.readBinary(path)), options.expectedHash);
      throw Object.assign(new Error('Queued edit detected'), { code: 'LOCAL_CONTENT_CHANGED' });
    };
    if (atomic) e.local.writeBinaryAtomic = guarded; else e.local.writeBinary = guarded;
    await assert.rejects(e.engine._writeDownloaded('a.md', e.metadata(e.remote), Buffer.from('REMO'), stat,
      { ...e.baseline, localHash: hash }), (err) => err.code === 'LOCAL_CONTENT_CHANGED');
    assert.equal(e.local.text('a.md'), 'LATE'); assert.equal(e.manifest.getFile('a.md'), e.baseline);
    assert.equal(e.manifest.getConflict('a.md'), e.conflict);
  });
}

test('initially absent downloads use createOnly in the local writer and never overwrite a late-created file', async () => {
  const e = await fixture();
  e.local.writeBinaryAtomic = async (path, data, mtime, options) => {
    assert.equal(options.createOnly, true); e.local.put(path, 'NEW USER FILE');
    throw Object.assign(new Error('Create collision'), { code: 'LOCAL_FILE_EXISTS' });
  };
  await assert.rejects(e.engine._writeDownloaded('new.md', { ...e.metadata(e.remote), name: 'new.md' }, Buffer.from('REMO'), null, null),
    (err) => err.code === 'LOCAL_FILE_EXISTS');
  assert.equal(e.local.text('new.md'), 'NEW USER FILE'); assert.equal(e.manifest.getFile('new.md'), undefined);
});
