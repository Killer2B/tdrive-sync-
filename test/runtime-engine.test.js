'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { SyncEngine } = require('../src/SyncEngine');
const { ManifestStore } = require('../src/ManifestStore');
const { normalizeEntry } = require('../src/normalize');
const { sha256Hex } = require('../src/util');
const { fingerprintOf } = require('../src/Chunker');
const { TdriveClient } = require('../src/TdriveClient');
const { uploadFile, uploadRevision } = require('../src/UploadManager');
const { MemFS } = require('./helpers');

// No HTTP server or real filesystem: listing and metadata shapes vary independently.
function fixture(options = {}) {
  let clock = 1700000000000;
  let serial = 0;
  let saved = null;
  const files = new Map();
  const folders = new Set(['/', '/Vault/']);
  const hidden = new Set();
  const ops = [];
  const settings = {
    remoteFolder: '/Vault/', remoteLock: false, incrementalScan: false, mode: 'two-way',
    deletionPolicy: 'trash', maxDeletesPerRun: 20, excludePatterns: [], streamLargeDownloads: false,
    mergeTextFiles: false, cdcFiles: false, ...options.settings,
  };
  const hooks = {};
  const metadata = (f, rich = false) => normalizeEntry({
    id: f.id, name: f.name, path: f.path, size: f.data.length, lastModified: f.mtime,
    trashed: f.trashed, ...(rich ? { checksum: f.checksum, version: f.version, etag: f.etag } : {}),
  });
  const addFolder = (path) => {
    let cur = '/';
    for (const part of path.split('/').filter(Boolean)) { cur += part + '/'; folders.add(cur); }
  };
  const add = (path, name, data) => {
    addFolder(path);
    const f = { id: 'f' + ++serial, path, name, data: Buffer.from(data), mtime: ++clock, version: 1, etag: 'v1', trashed: false };
    files.set(f.id, f); return f;
  };
  const change = (f, data) => { f.data = Buffer.from(data); f.mtime = ++clock; f.version++; f.etag = 'v' + f.version; };
  const client = {
    baseUrl: 'https://synthetic.invalid', apiKey: 'synthetic-key', redact: (s) => String(s || ''),
    limits: async () => ({ oneShotMaxBytes: 1048576, chunkBytes: 1048576 }),
    request: async () => ({ activity: [] }),
    listFolder: async (path) => {
      if (hooks.list) await hooks.list(path);
      if (!folders.has(path)) throw Object.assign(new Error('Folder missing'), { status: 404 });
      return {
        files: [...files.values()].filter((f) => !f.trashed && f.path === path && !hidden.has(f.id)).map((f) => metadata(f, options.richListing)),
        folders: [...folders].filter((d) => d !== path && d.startsWith(path) && d.slice(path.length).split('/').filter(Boolean).length === 1).map((d) => ({ id: 'd' + d, name: d.slice(path.length, -1) })),
      };
    },
    getFile: async (id) => {
      ops.push(['get', id]);
      if (hooks.get) return hooks.get(id);
      const f = files.get(id); return f ? metadata(f, options.richMetadata) : null;
    },
    createFolder: async (name, path) => { addFolder(path + name + '/'); ops.push(['mkdir', path + name]); },
    patchContent: async (id, data) => {
      const f = files.get(id); change(f, data); ops.push(['patch', id]);
      const response = metadata(f, options.richMetadata);
      response.checksum = await fingerprintOf([await sha256Hex(data)]);
      response.raw.checksum = response.checksum;
      if (hooks.patch) await hooks.patch(f);
      return response;
    },
    trash: async (id) => { files.get(id).trashed = true; ops.push(['trash', id]); },
    move: async (id, { path, name }) => {
      const f = files.get(id);
      if (hooks.move) await hooks.move(f, { path, name });
      f.path = path; f.name = name;
      ops.push(['move', id]); return metadata(f, options.richMetadata);
    },
    deleteFolder: async (path) => {
      ops.push(['deleteFolder', path]);
      if (hooks.deleteFolder) await hooks.deleteFolder(path);
      folders.delete(path);
    },
  };
  const local = new MemFS();
  const manifest = new ManifestStore({ read: async () => saved, write: async (s) => { saved = s; } }, () => clock);
  const engine = new SyncEngine({
    client, local, manifest, getSettings: () => settings, now: () => clock,
    uploadFn: options.actualTransfers ? uploadFile : async (_, o) => { const f = add(o.folderPath, o.name, o.data); ops.push(['upload', f.id]); return metadata(f, options.richMetadata); },
    revisionFn: options.actualTransfers ? uploadRevision : async () => null,
    downloadFn: async (_, f, o) => {
      ops.push(['download', f.id]);
      if (hooks.download) await hooks.download(f);
      const data = files.get(f.id).data;
      if (o.expectSha) assert.equal(await sha256Hex(data), o.expectSha);
      return data.buffer.slice(data.byteOffset, data.byteOffset + data.length);
    },
    manifestFn: async (_, id, etag) => {
      if (hooks.manifest) return hooks.manifest(id, etag);
      if (!options.manifests) return null;
      const f = files.get(id);
      return { size: f.data.length, etag: f.etag, chunks: [{ idx: 0, offset: 0, size: f.data.length, checksum: await sha256Hex(f.data) }] };
    },
  });
  const remote = (name) => [...files.values()].find((f) => f.name === name && !f.trashed);
  return { engine, client, local, manifest, files, folders, hidden, hooks, settings, ops, add, remote, metadata, change,
    advance: (ms) => { clock += ms; }, now: () => clock, saved: () => JSON.parse(saved) };
}

function journalServer(snapshot, completeFailures = 0) {
  const sessions = new Map(); const files = new Map(); const opens = []; const puts = []; const completes = []; const aborts = [];
  const response = (data, status = 200) => ({ status, headers: {}, body: Buffer.from(JSON.stringify({ ok: status < 400, ...(status < 400 ? { data } : { error: { code: data, message: data } }) })) });
  const phase = (key, expected) => {
    const saved = snapshot(); assert.ok(saved, 'phase must be durable before network');
    const record = Object.values(saved.meta.uploadSessions).find((r) => r.clientKey === key);
    assert.ok(record); assert.equal(record.owner, saved.meta.deviceId);
    if (expected) assert.equal(record.phase, expected);
    return record;
  };
  const transport = async (req) => {
    const path = new URL(req.url).pathname;
    if (path === '/api/v1') return response({ upload: { chunkBytes: 4096, oneShotMaxBytes: 128 } });
    if (path === '/api/v1/uploads') {
      const body = JSON.parse(Buffer.from(req.body).toString()); phase(body.clientKey);
      opens.push(body);
      let s = [...sessions.values()].find((entry) => entry.body.clientKey === body.clientKey);
      if (!s) { s = { id: 'u' + (sessions.size + 1), body, parts: new Map(), status: 'pending' }; sessions.set(s.id, s); }
      return response({ id: s.id, status: s.status, fileId: s.fileId, revision: !!body.baseId, chunkSize: 4096, chunker: body.chunker, totalChunks: body.totalChunks, received: [...s.parts.keys()], base: { parts: [] } });
    }
    const match = /^\/api\/v1\/uploads\/([^/]+)(?:\/parts\/(\d+)|\/complete)?$/.exec(path);
    if (match) {
      const s = sessions.get(match[1]); if (!s) return response('NOT_FOUND', 404);
      if (req.method === 'GET') return response({ id: s.id, status: s.status, fileId: s.fileId, clientKey: s.body.clientKey });
      if (req.method === 'DELETE') { aborts.push(s.id); assert.fail('these engine tests must never abort a session'); }
      if (match[2] != null) {
        phase(s.body.clientKey, 'uploading');
        const idx = Number(match[2]); const data = Buffer.from(req.body);
        assert.equal(await sha256Hex(data), req.headers['X-Content-SHA256']);
        s.parts.set(idx, data); s.status = 'uploading'; puts.push([s.id, idx]); return response({ idx });
      }
      phase(s.body.clientKey, 'completing'); completes.push(s.id);
      if (completeFailures-- > 0) return response('RevisionDeferred', 503);
      const chunks = []; let offset = 0;
      for (let idx = 0; idx < s.body.totalChunks; idx++) {
        const part = s.parts.get(idx); assert.ok(part);
        chunks.push({ idx, offset, size: part.length, checksum: await sha256Hex(part) }); offset += part.length;
      }
      assert.equal(await fingerprintOf(chunks.map((c) => c.checksum)), s.body.fingerprint);
      s.status = s.body.baseId ? 'committed' : 'ready'; s.fileId = s.body.baseId || 'f' + s.id;
      const f = { id: s.fileId, name: s.body.name || 'a.bin', path: s.body.path || '/Vault/', size: s.body.size, lastModified: s.body.lastModified, checksum: s.body.fingerprint, chunks };
      files.set(f.id, f); return response({ file: f, checksum: f.checksum });
    }
    const matchFile = /^\/api\/v1\/files\/([^/]+)(\/chunks)?$/.exec(path);
    if (matchFile) {
      const f = files.get(matchFile[1]);
      if (matchFile[2]) return response({ chunkSize: 4096, size: f && f.size || 0, chunks: f && f.chunks || [] });
      return f ? response(f) : response('NOT_FOUND', 404);
    }
    return response('NOT_FOUND', 404);
  };
  return { sessions, opens, puts, completes, aborts,
    client: (key = 'synthetic-journal-key') => new TdriveClient({ baseUrl: 'https://synthetic.invalid', apiKey: key, transport, maxRetries: 0, maxServerRetries: 0 }),
  };
}

test('failed compare cannot advance a tracked baseline or overwrite the remote on retry', async () => {
  const e = fixture({ manifests: true });
  e.local.put('a.md', 'BASE'); await e.engine.syncNow();
  const before = { ...e.manifest.getFile('a.md') };
  e.local.put('a.md', 'LOCL'); e.change(e.remote('a.md'), 'REMO');
  e.hooks.download = async () => { throw new Error('body unavailable'); };
  const first = await e.engine.syncNow();
  assert.equal(first.status, 'errors');
  assert.deepEqual(e.manifest.getFile('a.md'), before);
  delete e.hooks.download;
  const second = await e.engine.syncNow();
  assert.equal(second.conflicts, 1); assert.equal(second.uploaded, 0);
  assert.equal(e.remote('a.md').data.toString(), 'REMO');
  const third = await e.engine.syncNow();
  assert.equal(third.uploaded + third.downloaded, 0);
  assert.ok(e.manifest.getConflict('a.md'));
});

test('failed first comparison creates no content baseline', async () => {
  const e = fixture({ manifests: true });
  e.local.put('a.md', 'LOCL'); e.add('/Vault/', 'a.md', 'REMO');
  e.hooks.download = async () => { throw new Error('body unavailable'); };
  assert.equal((await e.engine.syncNow()).status, 'errors');
  assert.equal(e.manifest.getFile('a.md'), undefined);
  delete e.hooks.download;
  assert.equal((await e.engine.syncNow()).conflicts, 1);
  assert.equal(e.remote('a.md').data.toString(), 'REMO');
});

for (const listingHidden of [false, true]) {
  test(`reconcile preserves committed baseline when a concurrent revision appears (${listingHidden ? 'GET rescue' : 'listing'})`, async () => {
    const e = fixture({ richMetadata: true });
    e.local.put('a.md', 'BASE'); await e.engine.syncNow();
    e.local.put('a.md', 'AAAA');
    e.hooks.patch = async (f) => { e.change(f, 'BBBB'); if (listingHidden) e.hidden.add(f.id); };
    const first = await e.engine.syncNow();
    assert.equal(first.uploaded, 1);
    assert.notEqual(e.manifest.getFile('a.md').remoteMtime, e.remote('a.md').mtime);
    delete e.hooks.patch;
    const second = await e.engine.syncNow();
    assert.equal(second.downloaded, 1); assert.equal(e.local.text('a.md'), 'BBBB');
    const third = await e.engine.syncNow();
    assert.equal(third.uploaded + third.downloaded + third.conflicts, 0);
  });
}

test('reconcile cannot forget a concurrent deletion and resurrect the committed file', async () => {
  const e = fixture();
  e.local.put('a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
  const f = e.remote('a.md'); e.local.put('a.md', 'AAAA');
  e.hooks.patch = async (file) => { file.trashed = true; };
  const first = await e.engine.syncNow();
  assert.equal(first.uploaded, 1); assert.ok(e.manifest.getFile('a.md'));
  delete e.hooks.patch;
  const second = await e.engine.syncNow();
  assert.equal(second.uploaded, 0); assert.equal(second.deletedLocal, 1);
  assert.equal(f.trashed, true); assert.equal(e.local.text('a.md'), undefined);
  const third = await e.engine.syncNow();
  assert.equal(third.uploaded + third.downloaded, 0);
});

for (const sameContent of [false, true]) {
  test(`newly discovered remote occupant is ${sameContent ? 'compared without upload' : 'preserved as conflict'}`, async () => {
    const e = fixture({ manifests: true });
    e.local.put('a.md', 'LOCL'); const f = e.add('/Vault/', 'a.md', sameContent ? 'LOCL' : 'REMO');
    let lists = 0;
    e.hooks.list = async (path) => { if (path === '/Vault/') { if (++lists === 1) e.hidden.add(f.id); else e.hidden.delete(f.id); } };
    const first = await e.engine.syncNow();
    assert.equal(first.uploaded, 0); assert.equal(first.conflicts, sameContent ? 0 : 1);
    assert.ok(!e.ops.some(([op]) => op === 'patch' || op === 'upload'));
    assert.equal(f.data.toString(), sameContent ? 'LOCL' : 'REMO');
    const second = await e.engine.syncNow();
    assert.equal(second.uploaded + second.downloaded, 0);
    assert.equal(e.local.text('a.md'), 'LOCL');
  });
}

test('deleteLocal rehashes after awaited remote absence check', async () => {
  const e = fixture();
  e.local.put('a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
  const f = e.remote('a.md'); f.trashed = true;
  let gets = 0;
  e.hooks.get = async (id) => {
    if (id === f.id) { if (++gets === 2) e.local.put('a.md', 'USER-EDIT'); return null; }
    return e.metadata(e.files.get(id));
  };
  const first = await e.engine.syncNow();
  assert.equal(first.deletedLocal, 0); assert.equal(first.status, 'errors');
  assert.equal(e.local.text('a.md'), 'USER-EDIT'); assert.ok(e.manifest.getFile('a.md'));
  delete e.hooks.get;
  assert.equal((await e.engine.syncNow()).uploaded, 1);
  assert.equal(e.remote('a.md').data.toString(), 'USER-EDIT');
  assert.equal((await e.engine.syncNow()).uploaded, 0);
});

test('remote file moved during deletion is kept and replanned', async () => {
  const e = fixture();
  e.local.put('a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
  const f = e.remote('a.md'); e.local.files.delete('a.md');
  e.engine.onProgress = (p) => { if (p.action === 'deleteRemote') { f.path = '/Vault/New/'; f.name = 'renamed.md'; e.folders.add(f.path); } };
  const first = await e.engine.syncNow();
  assert.equal(first.deletedRemote, 0); assert.equal(f.trashed, false);
  e.engine.onProgress = () => {};
  await e.engine.syncNow(); assert.equal(e.local.text('New/renamed.md'), 'BASE');
  assert.equal((await e.engine.syncNow()).downloaded, 0);
});

test('all tracked omitted files are rescued even when absent locally, without optional-field resurrection', async () => {
  const e = fixture({ richMetadata: true });
  e.local.put('a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
  const f = e.remote('a.md'); f.checksum = 'metadata-only';
  e.hidden.add(f.id); await e.engine.syncNow();
  e.local.files.delete('a.md');
  const deleted = await e.engine.syncNow();
  assert.equal(deleted.deletedRemote, 1); assert.equal(deleted.downloaded, 0); assert.equal(f.trashed, true);
  e.hidden.delete(f.id);
  assert.equal((await e.engine.syncNow()).downloaded, 0);
  assert.equal(e.local.text('a.md'), undefined);
});

test('an edited tracked remote omitted from both scans is downloaded, not forgotten', async () => {
  const e = fixture({ richMetadata: true });
  e.local.put('a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
  const f = e.remote('a.md'); e.local.files.delete('a.md'); e.hidden.add(f.id); e.change(f, 'REMOTE-EDIT');
  const first = await e.engine.syncNow();
  assert.equal(first.downloaded, 1); assert.equal(first.deletedRemote, 0);
  assert.equal(e.local.text('a.md'), 'REMOTE-EDIT'); assert.ok(e.manifest.getFile('a.md'));
  const second = await e.engine.syncNow(); const third = await e.engine.syncNow();
  assert.equal(second.uploaded + second.downloaded + third.uploaded + third.downloaded, 0);
});

test('metadata failure while rescuing an omitted tracked file cannot erase its baseline', async () => {
  const e = fixture();
  e.local.put('a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
  const f = e.remote('a.md'); const before = { ...e.manifest.getFile('a.md') };
  e.local.files.delete('a.md'); e.hidden.add(f.id);
  e.hooks.get = async () => { throw Object.assign(new Error('network unavailable'), { isNetwork: true }); };
  assert.equal((await e.engine.syncNow()).status, 'offline');
  assert.deepEqual(e.manifest.getFile('a.md'), before); assert.equal(f.trashed, false);
  delete e.hooks.get;
  assert.equal((await e.engine.syncNow()).deletedRemote, 1);
});

test('metadata/listing optional fields cannot resurrect a real local deletion', async () => {
  const e = fixture({ richMetadata: true });
  e.local.put('a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
  const f = e.remote('a.md'); e.hidden.add(f.id); await e.engine.syncNow();
  e.hidden.delete(f.id); e.local.files.delete('a.md');
  assert.equal((await e.engine.syncNow()).deletedRemote, 1);
  assert.equal(e.local.text('a.md'), undefined);
  assert.equal((await e.engine.syncNow()).downloaded, 0);
});

test('a listing-omitted remote move is rescued at its new path, never its old path', async () => {
  const e = fixture();
  e.local.put('Old/a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
  const f = e.remote('a.md'); f.path = '/Vault/New/'; e.folders.add(f.path); e.hidden.add(f.id);
  const first = await e.engine.syncNow();
  assert.equal(first.deletedLocal, 1); assert.equal(first.downloaded, 1);
  assert.equal(e.local.text('Old/a.md'), undefined); assert.equal(e.local.text('New/a.md'), 'BASE');
  const second = await e.engine.syncNow(); const third = await e.engine.syncNow();
  assert.equal(second.uploaded + second.downloaded + third.uploaded + third.downloaded, 0);
});

for (const flag of ['trashed', 'isTrashed', 'deleted', 'deletedAt', 'deleted_at', 'inTrash']) {
  test(`metadata trash variant ${flag} is not resurrected`, async () => {
    const e = fixture();
    e.local.put('a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
    const f = e.remote('a.md'); e.hidden.add(f.id); e.local.files.delete('a.md');
    e.hooks.get = async (id) => id === f.id ? normalizeEntry({ id, name: f.name, path: f.path, size: 4, [flag]: true }) : e.metadata(e.files.get(id));
    const report = await e.engine.syncNow();
    assert.equal(report.uploaded + report.downloaded + report.deletedRemote, 0);
    assert.equal(e.manifest.getFile('a.md'), undefined);
  });
}

test('rescues do not inspect excluded or out-of-scope tracked files', async () => {
  const e = fixture(); await e.engine._bindScope();
  e.settings.syncFolders = ['Notes']; e.settings.excludePatterns = ['Notes/Private/**'];
  for (const path of ['Other/a.md', 'Notes/Private/a.md']) e.manifest.setFile(path, { remoteId: path });
  await e.engine.syncNow();
  assert.ok(!e.ops.some(([op]) => op === 'get'));
});

for (const code of ['UPLOAD_IN_PROGRESS', 'UploadInProgress', 'upload-in-progress']) {
  test(`folder backend block ${code} persists, backs off, and allows unrelated transfers/deletions`, async () => {
    const e = fixture();
    e.local.put('keep.md', 'keep'); await e.local.mkdir('Blocked'); await e.local.mkdir('Other');
    await e.engine.syncNow();
    e.local.dirs.delete('Blocked'); e.local.dirs.delete('Other'); e.local.put('new.md', 'new');
    let blocked = true;
    e.hooks.deleteFolder = async (path) => { if (path === '/Vault/Blocked/' && blocked) throw Object.assign(new Error('Finish descendant uploads'), { status: 409, code }); };
    const first = await e.engine.syncNow();
    assert.equal(first.status, 'blocked'); assert.equal(first.errors, 0); assert.equal(first.uploaded, 1);
    assert.equal(first.deletedRemote, 1); assert.equal(first.deferredFolders, 1); assert.ok(e.manifest.hasFolder('Blocked'));
    assert.ok(first.blockedFolders[0].nextRetryAt >= e.now() + 15 * 60000);
    assert.equal(e.saved().meta.blockedFolders.Blocked.scope, e.manifest.data.meta.syncScope);
    const attempts = () => e.ops.filter(([op, path]) => op === 'deleteFolder' && path === '/Vault/Blocked/').length;
    const warnings = () => e.manifest.data.log.filter((l) => l.msg.includes('Remote folder deletion deferred:')).length;
    e.advance(10 * 60000); e.local.put('newer.md', 'newer');
    const second = await e.engine.syncNow();
    assert.equal(second.status, 'blocked'); assert.equal(second.uploaded, 1); assert.equal(attempts(), 1); assert.equal(warnings(), 1);
    e.advance(5 * 60000); blocked = false;
    const third = await e.engine.syncNow();
    assert.equal(third.status, 'ok'); assert.equal(third.deletedRemote, 1); assert.equal(third.deferredFolders, 0);
    assert.equal(attempts(), 2); assert.equal(e.manifest.hasFolder('Blocked'), false);
  });
}

test('folder Retry-After survives ledger reload and repeat blocks do not repeat the error log', async () => {
  const e = fixture();
  e.local.put('keep.md', 'keep'); await e.local.mkdir('Blocked'); await e.engine.syncNow(); e.local.dirs.delete('Blocked');
  e.hooks.deleteFolder = async () => { throw Object.assign(new Error('Finish uploads'), { status: 409, code: 'UPLOAD_IN_PROGRESS', retryAfter: 3600 }); };
  const first = await e.engine.syncNow(); assert.ok(first.blockedFolders[0].nextRetryAt >= e.now() + 3600000);
  await e.manifest.load(); e.advance(20 * 60000);
  assert.equal((await e.engine.syncNow()).status, 'blocked');
  assert.equal(e.ops.filter(([op]) => op === 'deleteFolder').length, 1);
  e.advance(40 * 60000);
  assert.equal((await e.engine.syncNow()).status, 'blocked');
  assert.equal(e.ops.filter(([op]) => op === 'deleteFolder').length, 2);
  assert.equal(e.manifest.data.log.filter((l) => l.msg.includes('Remote folder deletion deferred:')).length, 1);
  assert.equal(e.saved().meta.lastRun.deferredFolders, 1);
});

test('backend blocks clear on destination switch or cancellation of the folder deletion', async () => {
  const e = fixture();
  e.local.put('keep.md', 'keep'); await e.local.mkdir('Blocked'); await e.engine.syncNow(); e.local.dirs.delete('Blocked');
  e.hooks.deleteFolder = async () => { throw Object.assign(new Error('Finish uploads'), { status: 409, code: 'UPLOAD_IN_PROGRESS' }); };
  await e.engine.syncNow(); await e.local.mkdir('Blocked');
  assert.equal((await e.engine.syncNow()).status, 'ok'); assert.equal(e.manifest.data.meta.blockedFolders.Blocked, undefined);
  e.local.dirs.delete('Blocked'); await e.engine.syncNow();
  e.settings.remoteFolder = '/Other/'; e.folders.add('/Other/');
  const switched = await e.engine.syncNow(); assert.equal(switched.deferredFolders, 0); assert.equal(switched.status, 'ok');
});

test('explicit backend retry can recheck a fixed API without waiting for local cooldown', async () => {
  const e = fixture(); e.local.put('keep.md', 'keep'); await e.local.mkdir('Blocked');
  await e.engine.syncNow(); e.local.dirs.delete('Blocked');
  e.hooks.deleteFolder = async () => { throw Object.assign(new Error('Finish uploads'), { status: 409, code: 'UPLOAD_IN_PROGRESS' }); };
  assert.equal((await e.engine.syncNow()).status, 'blocked');
  delete e.hooks.deleteFolder;
  assert.equal((await e.engine.syncNow()).status, 'blocked');
  const retry = await e.engine.syncNow({ retryBlockedFolders: true });
  assert.equal(retry.status, 'ok'); assert.equal(retry.deletedRemote, 1); assert.equal(retry.deferredFolders, 0);
  assert.equal(e.ops.filter(([op]) => op === 'deleteFolder').length, 2);
});

test('explicit backend retry still honors the server Retry-After floor', async () => {
  const e = fixture(); e.local.put('keep.md', 'keep'); await e.local.mkdir('Blocked');
  await e.engine.syncNow(); e.local.dirs.delete('Blocked');
  e.hooks.deleteFolder = async () => { throw Object.assign(new Error('Finish uploads'), { status: 409, code: 'UPLOAD_IN_PROGRESS', retryAfter: 3600 }); };
  await e.engine.syncNow(); delete e.hooks.deleteFolder; e.advance(10 * 60000);
  assert.equal((await e.engine.syncNow({ retryBlockedFolders: true })).status, 'blocked');
  assert.equal(e.ops.filter(([op]) => op === 'deleteFolder').length, 1);
  e.advance(50 * 60000);
  assert.equal((await e.engine.syncNow({ retryBlockedFolders: true })).status, 'ok');
});

test('other folder errors remain errors and real transfer failures take precedence over backend blocks', async () => {
  const e = fixture();
  e.local.put('keep.md', 'keep'); await e.local.mkdir('Blocked'); await e.engine.syncNow(); e.local.dirs.delete('Blocked');
  e.hooks.deleteFolder = async () => { throw Object.assign(new Error('denied'), { status: 409, code: 'SOMETHING_ELSE' }); };
  assert.equal((await e.engine.syncNow()).status, 'errors');
  e.hooks.deleteFolder = async () => { throw Object.assign(new Error('Finish uploads'), { status: 409, code: 'UPLOAD_IN_PROGRESS' }); };
  assert.equal((await e.engine.syncNow()).status, 'blocked');
  e.add('/Vault/', 'new.md', 'remote'); e.hooks.download = async () => { throw new Error('real transfer failure'); };
  const failed = await e.engine.syncNow(); assert.equal(failed.status, 'errors'); assert.equal(failed.deferredFolders, 1);
});

test('manual empty-folder cleanup uses the same persisted backend deferral', async () => {
  const e = fixture(); e.folders.add('/Vault/Blocked/');
  e.hooks.deleteFolder = async () => { throw Object.assign(new Error('Finish uploads'), { status: 409, code: 'UPLOAD_IN_PROGRESS' }); };
  const first = await e.engine.cleanEmptyRemoteFolders();
  assert.equal(first.status, 'blocked'); assert.equal(first.failures.length, 0); assert.equal(first.deferredFolders, 1);
  assert.ok(e.saved().meta.blockedFolders.Blocked);
  const second = await e.engine.cleanEmptyRemoteFolders();
  assert.equal(second.status, 'blocked'); assert.equal(e.ops.filter(([op]) => op === 'deleteFolder').length, 1);
  e.advance(15 * 60000); delete e.hooks.deleteFolder;
  const third = await e.engine.syncNow();
  assert.equal(third.status, 'ok'); assert.equal(third.deletedRemote, 1); assert.equal(third.deferredFolders, 0);
});

test('a same-size remote edit with only ETag evidence is pulled rather than deleted', async () => {
  const e = fixture({ richListing: true, richMetadata: true });
  e.local.put('a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
  const f = e.remote('a.md');
  e.change(f, 'EDIT'); f.mtime = e.manifest.getFile('a.md').remoteMtime; f.version = 1;
  e.local.files.delete('a.md');
  const first = await e.engine.syncNow();
  assert.equal(first.deletedRemote, 0); assert.equal(first.downloaded, 1);
  assert.equal(e.local.text('a.md'), 'EDIT'); assert.equal(f.trashed, false);
  assert.equal((await e.engine.syncNow()).downloaded, 0);
});

test('a cached future chunk map cannot turn delta 304 into acknowledgement of different local bytes', async () => {
  const e = fixture(); const data = Buffer.alloc(65536, 65); e.local.put('a.bin', data);
  await e.engine.syncNow();
  const f = e.remote('a.bin'); e.change(f, Buffer.alloc(65536, 66));
  const entry = e.manifest.getFile('a.bin');
  entry.chunkMap = { sizes: [65536], hashes: [await sha256Hex(f.data)] }; entry.etag = f.etag;
  e.hooks.manifest = async (_, etag) => etag ? { notModified: true } : null;
  const result = await e.engine.syncNow();
  assert.equal(result.downloaded, 1); assert.equal(e.local.files.get('a.bin').data[0], 66);
  assert.equal((await e.engine.syncNow()).downloaded, 0);
});

for (const [revision, cdc] of [[false, false], [true, false], [true, true]]) {
  test(`engine journal resumes the same ${cdc ? 'CDC ' : ''}${revision ? 'revision' : 'new upload'} SID across a fresh engine, client and ledger`, async () => {
    const e = fixture({ actualTransfers: true, settings: { baseUrl: 'https://synthetic.invalid', dedupeUploads: false } });
    const server = journalServer(e.saved, 1);
    e.engine.client = server.client();
    const data = Buffer.from('SENSITIVE-NOTE-BYTES'.repeat(500));
    const options = { name: 'a.bin', folderPath: '/Vault/', data, mtime: e.now(), old: revision ? { id: 'base' } : null, chunker: cdc ? undefined : false };
    await assert.rejects(e.engine._replaceUpload(options), (err) => err.code === 'RevisionDeferred');
    const first = Object.values(e.saved().meta.uploadSessions)[0];
    assert.equal(first.phase, 'completing'); assert.equal(first.sourceHash, await sha256Hex(data));
    assert.equal(first.owner, e.saved().meta.deviceId); assert.equal(first.sessionId, 'u1');
    assert.equal(e.saved().meta.uploadSessionsScope, e.manifest.data.meta.syncScope);
    const sent = server.puts.length;
    const restored = new ManifestStore(e.manifest.storage, e.now); await restored.load();
    const fresh = new SyncEngine({ client: server.client(), manifest: restored, local: e.local, getSettings: () => e.settings, now: e.now });
    await fresh._bindScope();
    const result = await fresh._replaceUpload(options);
    assert.equal(result.id, revision ? 'base' : 'fu1'); assert.equal(server.sessions.size, 1);
    assert.equal(server.puts.length, sent); assert.equal(server.opens[0].clientKey, server.opens[1].clientKey);
    const completed = Object.values(e.saved().meta.uploadSessions)[0]; assert.equal(completed.phase, 'completed');
    assert.deepEqual(server.aborts, []);
    assert.ok(!JSON.stringify(e.saved().meta).includes('SENSITIVE-NOTE-BYTES'));
    assert.ok(!JSON.stringify(e.saved().meta).includes('synthetic-journal-key'));
    // Completed metadata acknowledgement retries neither open nor send a new revision.
    await fresh._replaceUpload(options);
    assert.equal(server.opens.length, 2); assert.equal(server.puts.length, sent);
  });
}

test('failed journal writes prevent POST on every retry and roll back unsaved opening records', async () => {
  const e = fixture({ actualTransfers: true, settings: { baseUrl: 'https://synthetic.invalid', dedupeUploads: false } });
  const server = journalServer(e.saved); e.engine.client = server.client();
  const write = e.manifest.storage.write; let fail = true;
  e.manifest.storage.write = async (value) => { if (fail) throw new Error('journal disk unavailable'); await write(value); };
  const options = { name: 'a.bin', folderPath: '/Vault/', data: Buffer.alloc(8193), mtime: e.now(), old: { id: 'base' }, chunker: false };
  for (let attempt = 0; attempt < 2; attempt++) {
    await assert.rejects(e.engine._replaceUpload(options), /journal disk unavailable/);
    assert.equal(server.opens.length, 0); assert.equal(Object.keys(e.manifest.data.meta.uploadSessions).length, 0);
  }
  fail = false; await e.engine._replaceUpload(options);
  assert.equal(server.opens.length, 1); assert.equal(Object.values(e.saved().meta.uploadSessions)[0].phase, 'completed');
});

for (const failedPhase of ['uploading', 'completing']) {
  test(`a failed ${failedPhase} phase save blocks its network step and recovers the same SID`, async () => {
    const e = fixture({ actualTransfers: true, settings: { baseUrl: 'https://synthetic.invalid', dedupeUploads: false } });
    const server = journalServer(e.saved); e.engine.client = server.client();
    const write = e.manifest.storage.write; let fail = true;
    e.manifest.storage.write = async (value) => {
      if (fail && Object.values(JSON.parse(value).meta.uploadSessions || {}).some((r) => r.phase === failedPhase)) throw new Error('phase save unavailable');
      await write(value);
    };
    const options = { name: 'a.bin', folderPath: '/Vault/', data: Buffer.alloc(8193), mtime: e.now(), old: { id: 'base' }, chunker: false };
    await assert.rejects(e.engine._replaceUpload(options), /phase save unavailable/);
    assert.equal(server.completes.length, 0); if (failedPhase === 'uploading') assert.equal(server.puts.length, 0);
    assert.notEqual(Object.values(e.saved().meta.uploadSessions)[0].phase, failedPhase);
    fail = false;
    const restored = new ManifestStore(e.manifest.storage, e.now); await restored.load();
    const fresh = new SyncEngine({ client: server.client(), local: e.local, manifest: restored, getSettings: () => e.settings, now: e.now });
    await fresh._replaceUpload(options);
    assert.equal(server.sessions.size, 1); assert.equal(server.completes.length, 1); assert.deepEqual(server.aborts, []);
  });
}

test('journal reads and writes are deep-cloned and retain only protocol metadata', async () => {
  const e = fixture({ actualTransfers: true, settings: { baseUrl: 'https://synthetic.invalid', dedupeUploads: false } });
  const server = journalServer(e.saved); e.engine.client = server.client();
  await e.engine._replaceUpload({ name: 'a.bin', folderPath: '/Vault/', data: Buffer.alloc(8193), mtime: e.now(), old: { id: 'base' }, chunker: false });
  const key = Object.keys(e.manifest.data.meta.uploadSessions)[0];
  const journal = e.engine._upOpts().uploadJournal;
  const copy = await journal.get(key); copy.body.name = 'changed';
  assert.notEqual(e.manifest.data.meta.uploadSessions[key].body.name, 'changed');
  copy.data = 'PRIVATE-NOTE-CONTENT'; copy.apiKey = 'PRIVATE-API-KEY'; copy.body.data = 'PRIVATE-NOTE-CONTENT';
  await journal.set(key, copy); copy.body.name = 'mutated-after-save';
  assert.equal((await journal.get(key)).body.name, 'changed');
  assert.ok(!JSON.stringify(e.saved().meta).includes('PRIVATE-NOTE-CONTENT'));
  assert.ok(!JSON.stringify(e.saved().meta).includes('PRIVATE-API-KEY'));
  await assert.rejects(journal.set(key, { ...copy, owner: 'another-device' }), /ownership mismatch/);
});

test('real destination changes archive owned sessions without aborting old-account resources', async () => {
  const e = fixture({ actualTransfers: true, settings: { baseUrl: 'https://synthetic.invalid', dedupeUploads: false } });
  const server = journalServer(e.saved, 1); e.engine.client = server.client();
  await assert.rejects(e.engine._replaceUpload({ name: 'a.bin', folderPath: '/Vault/', data: Buffer.alloc(8193), mtime: e.now(), old: { id: 'base' }, chunker: false }));
  const oldScope = e.manifest.data.meta.syncScope; const owner = e.manifest.data.meta.deviceId;
  const oldJournal = e.engine._upOpts().uploadJournal; const key = Object.keys(e.manifest.data.meta.uploadSessions)[0];
  e.settings.excludePatterns = ['Private/**']; await e.engine._bindScope();
  assert.ok(await e.engine._upOpts().uploadJournal.get(key), 'scan exclusions must not discard resumable ownership');
  e.settings.remoteFolder = '/Other/'; e.engine.client = server.client('other-account-key'); await e.engine._bindScope();
  assert.notEqual(e.manifest.data.meta.syncScope, oldScope); assert.equal(e.manifest.data.meta.deviceId, owner);
  assert.equal(Object.keys(e.manifest.data.meta.uploadSessions).length, 0);
  assert.equal(e.manifest.data.meta.previousScopeLedger.scope, oldScope);
  assert.equal(e.manifest.data.meta.previousScopeLedger.uploadSessions[key].sessionId, 'u1');
  assert.equal(await e.engine._upOpts().uploadJournal.get(key), null);
  await assert.rejects(oldJournal.get(key), /destination changed/);
  assert.deepEqual(server.aborts, []);
});

test('move edits and direct conflict-helper pushes receive the same durable journal owner', async () => {
  const e = fixture(); e.local.put('Old/a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
  const owners = [];
  e.engine.revisionFn = async (client, options) => { owners.push(options.uploadJournal.owner); return client.patchContent(options.id, options.data); };
  e.local.removeDir('Old'); e.local.put('New/a.md', 'EDIT'); e.manifest.data.meta.moveHints = { Old: { to: 'New' } };
  assert.equal((await e.engine.syncNow()).moved, 1);
  e.local.put('New/a.md', 'NEXT'); await e.engine.pushLocal('New/a.md', e.metadata(e.remote('a.md')));
  assert.deepEqual(owners, [e.manifest.data.meta.deviceId, e.manifest.data.meta.deviceId]);
});

test('optional operation validity is rechecked after a journal phase save before POST', async () => {
  const e = fixture({ actualTransfers: true, settings: { baseUrl: 'https://synthetic.invalid', dedupeUploads: false } });
  const server = journalServer(e.saved); e.engine.client = server.client(); let valid = true;
  e.engine.operationValidity = () => valid;
  const write = e.manifest.storage.write;
  e.manifest.storage.write = async (value) => { await write(value); valid = false; };
  await assert.rejects(e.engine._replaceUpload({ name: 'a.bin', folderPath: '/Vault/', data: Buffer.alloc(8193), mtime: e.now(), old: { id: 'base' }, chunker: false }), /no longer valid/);
  assert.equal(server.opens.length, 0);
  assert.equal(Object.values(e.saved().meta.uploadSessions)[0].phase, 'opening');
});

for (const localEdit of [false, true]) {
  test(`hinted folder rename preserves edited remote identity and conflicts at the new path (${localEdit ? 'both edited' : 'remote edited'})`, async () => {
    const e = fixture();
    e.local.put('Old/a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
    const original = e.remote('a.md'); const id = original.id;
    e.local.removeDir('Old'); e.local.put('New/a.md', localEdit ? 'LOCAL-EDIT' : 'BASE');
    e.manifest.data.meta.moveHints = { Old: { to: 'New' } }; e.change(original, 'REMOTE-EDIT'); e.ops.length = 0;
    const first = await e.engine.syncNow();
    assert.equal(first.moved, 1); assert.equal(first.conflicts, 1); assert.equal(first.uploaded, 0);
    assert.equal(original.path, '/Vault/New/'); assert.equal(original.id, id); assert.equal(original.data.toString(), 'REMOTE-EDIT');
    assert.equal(e.local.text('Old/a.md'), undefined); assert.equal(e.local.text('New/a.md'), localEdit ? 'LOCAL-EDIT' : 'BASE');
    const conflict = e.manifest.getConflict('New/a.md'); assert.equal(conflict.remoteId, id);
    assert.equal(e.local.text(conflict.copyPath), 'REMOTE-EDIT'); assert.equal(e.manifest.getFile('Old/a.md'), undefined);
    assert.equal(e.manifest.data.meta.moveHints.Old, undefined);
    for (let run = 0; run < 2; run++) {
      const next = await e.engine.syncNow(); assert.equal(next.moved + next.uploaded + next.downloaded, 0);
      assert.equal([...e.files.values()].filter((f) => !f.trashed && f.name === 'a.md').length, 1);
      assert.equal(original.data.toString(), 'REMOTE-EDIT'); assert.equal(e.local.text('Old/a.md'), undefined);
      assert.equal(e.local.text(conflict.copyPath), 'REMOTE-EDIT');
    }
    assert.ok(!e.ops.some(([op]) => op === 'patch' || op === 'upload' || op === 'trash'));
  });
}

test('hinted deep folder move remains in place while empty-folder cleanup exceeds the delete budget', async () => {
  const e = fixture({ settings: { maxDeletesPerRun: 2 } });
  e.local.put('Old/A/B/C/D/a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
  const original = e.remote('a.md'); const id = original.id;
  e.local.removeDir('Old'); e.local.put('New/A/B/C/D/a.md', 'BASE');
  e.manifest.data.meta.moveHints = { Old: { to: 'New' } }; e.ops.length = 0;
  const first = await e.engine.syncNow();
  assert.equal(first.moved, 1); assert.equal(first.uploaded, 0); assert.equal(first.deletedRemote, 0);
  assert.equal(first.blockedDeletes.reason, 'limit'); assert.equal(first.blockedDeletes.items.length, 5);
  assert.equal(original.path, '/Vault/New/A/B/C/D/'); assert.equal(original.id, id);
  assert.equal(e.manifest.data.meta.moveHints.Old, undefined);
  for (let run = 0; run < 2; run++) {
    const next = await e.engine.syncNow(); assert.equal(next.moved + next.uploaded + next.downloaded, 0);
    assert.equal(next.blockedDeletes.reason, 'limit');
    assert.equal([...e.files.values()].filter((f) => !f.trashed && f.name === 'a.md').length, 1);
    assert.equal(original.data.toString(), 'BASE'); assert.equal(e.local.text('Old/A/B/C/D/a.md'), undefined);
  }
  assert.ok(!e.ops.some(([op]) => op === 'upload' || op === 'trash' || op === 'deleteFolder'));
});

test('a hinted move detects a remote edit with unchanged size and metadata before replacing any bytes', async () => {
  const e = fixture(); e.local.put('Old/a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
  const original = e.remote('a.md'); original.data = Buffer.from('EDIT');
  e.local.removeDir('Old'); e.local.put('New/a.md', 'LOCAL'); e.manifest.data.meta.moveHints = { Old: { to: 'New' } }; e.ops.length = 0;
  const first = await e.engine.syncNow(); assert.equal(first.moved, 1); assert.equal(first.conflicts, 1);
  for (let run = 0; run < 2; run++) {
    await e.engine.syncNow(); assert.equal(original.data.toString(), 'EDIT'); assert.equal(e.local.text('New/a.md'), 'LOCAL');
    assert.equal([...e.files.values()].filter((f) => !f.trashed && f.name === 'a.md').length, 1);
  }
  assert.ok(!e.ops.some(([op]) => op === 'patch' || op === 'upload' || op === 'trash'));
});

test('an unsafe rejected move freezes its source and destination without fallback, even with listing gaps', async () => {
  const e = fixture(); e.local.put('Old/a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.local.mkdir('Other'); await e.engine.syncNow();
  const original = e.remote('a.md'); e.change(original, 'REMOTE-EDIT');
  e.local.removeDir('Old'); e.local.put('New/a.md', 'LOCAL-EDIT'); e.local.dirs.delete('Other');
  e.manifest.data.meta.moveHints = { Old: { to: 'New' } }; e.ops.length = 0;
  e.hooks.move = async () => { throw Object.assign(new Error('Destination not safely movable'), { status: 409, code: 'MOVE_BLOCKED' }); };
  const first = await e.engine.syncNow(); assert.equal(first.moved, 0); assert.equal(first.conflicts, 1);
  assert.equal(first.moveConflicts[0].from, 'Old/a.md'); assert.equal(first.moveConflicts[0].to, 'New/a.md');
  const conflict = e.manifest.getConflict('New/a.md'); assert.equal(conflict.movePending, true);
  assert.equal(e.local.text(conflict.copyPath), 'REMOTE-EDIT'); assert.ok(e.manifest.data.meta.moveHints.Old);
  e.hidden.add(original.id);
  for (let run = 0; run < 2; run++) {
    const next = await e.engine.syncNow(); assert.equal(next.uploaded + next.downloaded + next.moved, 0);
    assert.equal(next.conflicts, 1);
    assert.equal(original.path, '/Vault/Old/'); assert.equal(original.trashed, false); assert.equal(original.data.toString(), 'REMOTE-EDIT');
    assert.equal(e.local.text('Old/a.md'), undefined); assert.equal(e.local.text('New/a.md'), 'LOCAL-EDIT');
    assert.equal([...e.files.values()].filter((f) => !f.trashed && f.name === 'a.md').length, 1);
  }
  assert.ok(!e.ops.some(([op, path]) => op === 'patch' || op === 'upload' || op === 'trash' || op === 'deleteFolder' && path === '/Vault/Old/'));
  assert.ok(e.ops.some(([op, path]) => op === 'deleteFolder' && path === '/Vault/Other/'));
  await assert.rejects(e.engine.conflicts.resolve('New/a.md', 'local', e.engine), /complete the path move/);
  assert.equal(original.data.toString(), 'REMOTE-EDIT'); assert.ok(e.manifest.getConflict('New/a.md'));
});

test('failed verification after a committed move retries as compare, never upload or overwrite', async () => {
  const e = fixture(); e.local.put('Old/a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
  const original = e.remote('a.md'); e.change(original, 'REMOTE-EDIT');
  e.local.removeDir('Old'); e.local.put('New/a.md', 'BASE'); e.manifest.data.meta.moveHints = { Old: { to: 'New' } }; e.ops.length = 0;
  e.hooks.download = async () => { throw new Error('body unavailable after move'); };
  const first = await e.engine.syncNow(); assert.equal(first.moved, 1); assert.equal(first.status, 'errors');
  assert.equal(e.manifest.getFile('New/a.md').moveVerify, true); assert.equal(original.path, '/Vault/New/');
  delete e.hooks.download;
  const second = await e.engine.syncNow(); assert.equal(second.conflicts, 1); assert.equal(second.uploaded + second.downloaded, 0);
  await e.engine.syncNow(); assert.equal(e.local.text('New/a.md'), 'BASE'); assert.equal(original.data.toString(), 'REMOTE-EDIT');
  assert.equal([...e.files.values()].filter((f) => !f.trashed && f.name === 'a.md').length, 1);
  assert.ok(!e.ops.some(([op]) => op === 'patch' || op === 'upload' || op === 'trash'));
});

test('local edit after scan is rehashed before move and retries in place without duplicates', async () => {
  const e = fixture(); e.local.put('Old/a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
  const original = e.remote('a.md'); e.local.removeDir('Old'); e.local.put('New/a.md', 'BASE'); e.manifest.data.meta.moveHints = { Old: { to: 'New' } };
  let edited = false;
  e.hooks.get = async (id) => { if (id === original.id && !edited) { edited = true; e.local.put('New/a.md', 'LOCAL-LATE'); } return e.metadata(e.files.get(id)); };
  const first = await e.engine.syncNow(); assert.equal(first.moved + first.uploaded + first.downloaded, 0);
  assert.equal(original.path, '/Vault/Old/'); assert.equal(original.data.toString(), 'BASE');
  delete e.hooks.get;
  const second = await e.engine.syncNow(); assert.equal(second.moved, 1); assert.equal(original.data.toString(), 'LOCAL-LATE');
  const third = await e.engine.syncNow(); assert.equal(third.moved + third.uploaded + third.downloaded, 0);
  assert.equal([...e.files.values()].filter((f) => !f.trashed && f.name === 'a.md').length, 1);
});

test('trusted moves under cleanup budget still honor remote and local size guards', async () => {
  const e = fixture({ settings: { maxDeletesPerRun: 2 } });
  e.local.put('Old/A/B/C/D/a.md', 'BASE'); e.local.put('keep.md', 'keep'); await e.engine.syncNow();
  const original = e.remote('a.md'); e.change(original, 'REMOTE-EDIT');
  e.local.removeDir('Old'); e.local.put('New/A/B/C/D/a.md', 'BASE'); e.manifest.data.meta.moveHints = { Old: { to: 'New' } };
  e.settings.maxFileBytes = 10; e.ops.length = 0;
  const first = await e.engine.syncNow(); assert.equal(first.moved + first.uploaded + first.downloaded, 0);
  assert.equal(original.path, '/Vault/Old/A/B/C/D/'); assert.ok(e.manifest.data.meta.moveHints.Old);
  e.settings.maxFileBytes = 0;
  const second = await e.engine.syncNow(); assert.equal(second.moved, 1); assert.equal(second.conflicts, 1);
  const third = await e.engine.syncNow(); assert.equal(third.moved + third.uploaded + third.downloaded, 0);
  assert.equal([...e.files.values()].filter((f) => !f.trashed && f.name === 'a.md').length, 1);
});
