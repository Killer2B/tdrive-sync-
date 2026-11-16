'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const Module = require('node:module');
const { ManifestStore } = require('../src/ManifestStore');
const { ConflictManager } = require('../src/ConflictManager');
const { TdriveClient } = require('../src/TdriveClient');
const { remoteSig } = require('../src/normalize');
const { sha256Hex } = require('../src/util');

const bytes = (text) => new TextEncoder().encode(text).buffer;
const text = (data) => new TextDecoder().decode(data);
const opened = [];
class Element {
  constructor(tag = 'div', opts = {}) { this.tag = tag; this.text = opts.text || ''; this.children = []; this.disabled = false; }
  createEl(tag, opts) { const el = new Element(tag, opts); this.children.push(el); return el; }
  createDiv(opts) { return this.createEl('div', opts); }
  createSpan(opts) { return this.createEl('span', opts); }
  empty() { this.children = []; }
  addClass() {}
  setText(value) { this.text = value; }
  find(predicate) { for (const child of this.children) { if (predicate(child)) return child; const found = child.find(predicate); if (found) return found; } return null; }
}
class Modal {
  constructor(app) { this.app = app; this.contentEl = new Element(); }
  open() { opened.push(this); this.opening = this.onOpen(); return this; }
  close() { this.closed = true; }
}
class ItemView { constructor() { this.contentEl = new Element(); } }
class Plugin {}
class Setting {
  constructor(el) { this.el = el; }
  setName() { return this; }
  setDesc() { return this; }
  addButton(callback) {
    const button = this.el.createEl('button');
    callback({ setButtonText: (label) => { button.text = label; return { onClick: (fn) => { button.onclick = fn; } }; } });
    return this;
  }
}
const nativeLoad = Module._load;
let TdriveSyncPlugin;
try {
  Module._load = function (id, parent, isMain) {
    if (id === 'obsidian') return { Plugin, PluginSettingTab: class {}, Setting, Notice: class {}, Modal, ItemView, TFile: class {}, requestUrl: async () => { throw new Error('Real HTTP is forbidden in synthetic lifecycle tests'); } };
    return nativeLoad.call(this, id, parent, isMain);
  };
  TdriveSyncPlugin = require('../src/plugin');
} finally { Module._load = nativeLoad; }

function deferred() {
  let resolve; let reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
function manifest() { return new ManifestStore({ read: async () => null, write: async () => {} }); }

function pluginFixture() {
  const plugin = new TdriveSyncPlugin();
  plugin.settings = { apiKey: 'synthetic-key', baseUrl: 'https://synthetic.invalid', remoteFolder: '/Vault/', paused: false, remoteLock: true, language: 'en', excludePatterns: [], syncFolders: [], notifyStyle: 'technical' };
  plugin.listeners = new Set(); plugin.ledger = manifest(); plugin.ledger.data.meta.syncScope = 'synthetic-scope';
  plugin.ledger.data.meta.deviceId = '10000000-0000-4000-8000-000000000001';
  plugin.ledger.setFile('a.md', { remoteId: 'remote-a' });
  plugin.app = { workspace: { getActiveFile: () => ({ path: 'a.md' }) }, vault: { getName: () => 'Synthetic', adapter: {} } };
  plugin.t = (key) => key; plugin.lang = 'en'; plugin._operation = null; plugin._unloaded = false; plugin._pendingChange = false;
  plugin._timer = plugin._startupTimer = plugin._changeTimer = null;
  plugin.saveData = async () => {};
  const calls = []; const engines = [];
  plugin.makeEngine = () => {
    const settings = { ...plugin.settings };
    const engine = {
      running: false, stopRequested: false, lease: null,
      getSettings: () => settings,
      prepare: async (options) => { calls.push(['prepare', options]); },
      acquireOperationLease: async () => { calls.push(['acquire', engine]); engine.lease = { owner: plugin.ledger.data.meta.deviceId, token: 'synthetic-secret' }; },
      releaseOperationLease: async () => { calls.push(['release', engine]); if (engine.lease) { calls.push(['released-token', engine]); engine.lease = null; } },
      pause: () => { calls.push(['pause', engine]); engine.stopRequested = true; },
      _markRemoteMutated: () => calls.push(['mutated', engine]),
      client: {
        redact: String,
        getFile: async (id) => { calls.push(['getFile', id]); return { id, contentVersion: 19, version: 2 }; },
        request: async (request) => { calls.push(['request', request, engine]); return request.method === 'POST' ? { url: 'https://synthetic.invalid/share' } : { versions: [{ id: 'archived-id', version: 2, contentVersion: 7, size: 4 }] }; },
      },
    };
    plugin.engine = engine; plugin._engineSettings = settings;
    if (plugin._operation) plugin._operation.engine = engine;
    engines.push(engine);
    return engine;
  };
  return { plugin, calls, engines };
}

test('share prepares and acquires before writing with the exact engine client and live generation', async () => {
  const { plugin, calls, engines } = pluginFixture();
  const create = plugin.makeEngine;
  plugin.makeEngine = () => {
    const engine = create();
    const request = engine.client.request;
    engine.client.request = async (req) => {
      assert.equal(plugin._operation.engine, engine);
      assert.equal(plugin._busy(), true);
      assert.ok(engine.lease);
      assert.equal(await plugin.runSync({ silent: true }), null);
      return request(req);
    };
    return engine;
  };
  await plugin.shareCurrent();
  assert.deepEqual(calls.map((c) => c[0]), ['prepare', 'acquire', 'getFile', 'request', 'mutated', 'release', 'released-token']);
  assert.deepEqual(calls[0][1], { acquireLease: false });
  assert.deepEqual(JSON.parse(text(calls.find((c) => c[0] === 'request')[1].body)), { permission: 'download', expectedVersion: 19 });
  assert.equal(engines.length, 1); assert.equal(engines[0].lease, null); assert.equal(plugin._busy(), false);
});

test('read-only remote identity prepares without acquiring or mutating', async () => {
  const { plugin, calls } = pluginFixture();
  assert.equal(await plugin.remoteIdFor('a.md'), 'remote-a');
  assert.deepEqual(calls.map((c) => c[0]), ['prepare', 'release']);
  assert.deepEqual(calls[0][1], { acquireLease: false });
});

test('missing remote identity releases a bootstrap lease without acquiring another', async () => {
  const { plugin, calls, engines } = pluginFixture();
  plugin.ledger.deleteFile('a.md');
  const create = plugin.makeEngine;
  plugin.makeEngine = () => { const engine = create(); engine.prepare = async () => { engine.lease = { token: 'bootstrap' }; }; return engine; };
  await plugin.shareCurrent();
  assert.equal(calls.some((c) => c[0] === 'acquire' || c[0] === 'request'), false);
  assert.equal(calls.filter((c) => c[0] === 'released-token').length, 1);
  assert.equal(engines[0].lease, null);
});

for (const failure of ['prepare', 'getFile', 'request']) {
  test(`share ${failure} error releases only its captured lease owner`, async () => {
    const { plugin, calls, engines } = pluginFixture();
    const create = plugin.makeEngine;
    const other = { running: true, client: { redact: String }, releaseOperationLease: async () => { throw new Error('Unrelated engine must not be released'); } };
    plugin.makeEngine = () => {
      const engine = create();
      const fail = async () => { engine.lease = { token: 'owned' }; plugin.engine = other; throw new Error('synthetic failure'); };
      if (failure === 'prepare') engine.prepare = fail;
      else engine.client[failure] = fail;
      return engine;
    };
    await plugin.shareCurrent();
    assert.equal(plugin.engine, other); assert.equal(other.running, true);
    assert.equal(engines[0].running, false); assert.equal(engines[0].lease, null);
    assert.equal(calls.filter((c) => c[0] === 'release').every((c) => c[1] === engines[0]), true);
    assert.equal(plugin._operation, null);
  });
}

test('version listing remains guarded until its metadata request completes and acquires no lease', async () => {
  const { plugin, calls } = pluginFixture(); const gate = deferred(); const entered = deferred(); const create = plugin.makeEngine;
  plugin.makeEngine = () => { const engine = create(); engine.client.request = async () => { entered.resolve(); await gate.promise; return { versions: [] }; }; return engine; };
  const viewing = plugin.viewVersions(); await entered.promise;
  assert.equal(plugin._busy(), true); assert.equal(await plugin.runSync({ silent: true }), null);
  gate.resolve(); await viewing;
  assert.equal(plugin._busy(), false); assert.equal(calls.some((c) => c[0] === 'acquire'), false);
});

for (const operation of ['testConnection', 'probeApi']) {
  test(`${operation} keeps metadata reads guarded without preparing or acquiring`, async () => {
    const { plugin, calls } = pluginFixture(); const entered = deferred(); const gate = deferred(); const create = plugin.makeEngine;
    plugin.makeEngine = () => {
      const engine = create();
      const first = async () => { entered.resolve(); await gate.promise; return { upload: { chunkBytes: 5242880 } }; };
      engine.client.me = first; engine.client.info = first; engine.client.limits = async () => ({ chunkBytes: 5242880 }); engine.client.uploadChunkSize = async () => 5242880;
      return engine;
    };
    const reading = plugin[operation](); await entered.promise;
    assert.equal(plugin._busy(), true); assert.equal(await plugin.runSync({ silent: true }), null);
    gate.resolve(); await reading;
    assert.equal(calls.some((c) => c[0] === 'prepare' || c[0] === 'acquire'), false); assert.equal(plugin._busy(), false);
  });
}

test('search request is guarded through its readonly completion', async () => {
  const { plugin, calls } = pluginFixture(); const entered = deferred(); const gate = deferred(); const create = plugin.makeEngine;
  plugin.makeEngine = () => { const engine = create(); engine.client.request = async () => { entered.resolve(); await gate.promise; return { files: [] }; }; return engine; };
  plugin.searchRemote(); const modal = opened.at(-1);
  modal.contentEl.find((el) => el.tag === 'input').value = 'synthetic query';
  const searching = modal.contentEl.find((el) => el.text === 'search.go').onclick(); await entered.promise;
  assert.equal(plugin._busy(), true); assert.equal(await plugin.runSync({ silent: true }), null);
  gate.resolve(); await searching;
  assert.equal(calls.some((c) => c[0] === 'acquire' || c[0] === 'prepare'), false); assert.equal(plugin._busy(), false);
});

test('archived version selector never substitutes for the current content generation on restore', async () => {
  const { plugin, calls } = pluginFixture();
  await plugin.viewVersions(); const modal = opened.at(-1); await modal.opening;
  await modal.contentEl.find((el) => el.tag === 'button' && el.text === 'versions.restore').onclick();
  const restore = calls.find((c) => c[0] === 'request' && c[1].method === 'POST');
  assert.equal(restore[1].path, '/files/remote-a/versions/archived-id/restore');
  assert.deepEqual(JSON.parse(text(restore[1].body)), { expectedVersion: 19 });
  assert.equal(calls.filter((c) => c[0] === 'acquire').length, 1);
});

test('version restore refuses a stale scope without taking a lease or posting', async () => {
  const { plugin, calls } = pluginFixture();
  await plugin.viewVersions(); const modal = opened.at(-1); await modal.opening;
  plugin.ledger.data.meta.syncScope = 'changed-scope';
  await modal.contentEl.find((el) => el.tag === 'button').onclick();
  assert.equal(calls.some((c) => c[0] === 'acquire' || (c[0] === 'request' && c[1].method === 'POST')), false);
  assert.equal(plugin._busy(), false);
});

test('supported version restore fails closed when the live generation is missing', async () => {
  const { plugin, calls } = pluginFixture();
  await plugin.viewVersions(); const modal = opened.at(-1); const create = plugin.makeEngine;
  plugin.makeEngine = () => { const engine = create(); engine._capabilities = { expectedContentVersion: true }; engine.client.getFile = async () => ({ id: 'remote-a', contentVersion: null, version: 2 }); return engine; };
  await modal.contentEl.find((el) => el.tag === 'button').onclick();
  assert.equal(calls.some((c) => c[0] === 'request' && c[1].method === 'POST'), false);
  assert.equal(calls.filter((c) => c[0] === 'released-token').length, 1); assert.equal(plugin._busy(), false);
});

for (const method of ['cleanEmptyRemoteFolders', 'restoreLastDeletions']) {
  test(`${method} error releases its lease and the outer operation guard`, async () => {
    const { plugin, calls, engines } = pluginFixture(); const create = plugin.makeEngine;
    plugin.ledger.data.meta.deletions = [{ id: 'synthetic-deletion' }];
    plugin.makeEngine = () => { const engine = create(); engine[method] = async () => { await engine.acquireOperationLease(); throw new Error('synthetic failure'); }; return engine; };
    await plugin[method === 'restoreLastDeletions' ? 'restoreDeletes' : method]();
    assert.equal(calls.filter((c) => c[0] === 'released-token').length, 1);
    assert.equal(engines[0].lease, null); assert.equal(plugin._busy(), false);
  });
}

test('sync releases internally and plugin outer release is idempotent', async () => {
  const { plugin, calls } = pluginFixture(); const create = plugin.makeEngine;
  plugin.makeEngine = () => {
    const engine = create(); engine.syncNow = async () => { engine.running = true; await engine.acquireOperationLease(); try { return { status: 'locked' }; } finally { await engine.releaseOperationLease(); engine.running = false; } };
    return engine;
  };
  assert.equal((await plugin.runSync({ silent: true })).status, 'locked');
  assert.equal(calls.filter((c) => c[0] === 'release').length, 2);
  assert.equal(calls.filter((c) => c[0] === 'released-token').length, 1);
});

for (const cancel of ['pause', 'unload', 'settings']) {
  test(`${cancel} releases a blocked operation immediately and prevents the following remote write`, async () => {
    const { plugin, calls, engines } = pluginFixture(); const gate = deferred(); const entered = deferred(); const create = plugin.makeEngine;
    plugin.makeEngine = () => { const engine = create(); engine.client.getFile = async () => { entered.resolve(); await gate.promise; return { id: 'remote-a', contentVersion: 19 }; }; return engine; };
    const sharing = plugin.shareCurrent(); await entered.promise;
    assert.ok(engines[0].lease);
    if (cancel === 'pause') await plugin.pause();
    else if (cancel === 'unload') await plugin.onunload();
    else { plugin.settings.remoteFolder = '/Changed/'; await plugin.saveSettings(); }
    assert.equal(engines[0].lease, null); assert.equal(engines[0].stopRequested, true);
    assert.equal(plugin._busy(), true);
    gate.resolve(); await sharing;
    assert.equal(calls.some((c) => c[0] === 'request'), false);
    assert.equal(calls.filter((c) => c[0] === 'released-token').length, 1);
    assert.equal(plugin._operation, null);
  });
}

test('unload stops/releases the captured engine, not a newer mutable engine reference', async () => {
  const { plugin, engines } = pluginFixture(); const gate = deferred(); const entered = deferred(); const create = plugin.makeEngine;
  plugin.makeEngine = () => { const engine = create(); engine.client.getFile = async () => { entered.resolve(); await gate.promise; return { contentVersion: 19 }; }; return engine; };
  const sharing = plugin.shareCurrent(); await entered.promise;
  const other = { running: true, client: { redact: String }, pause: () => { throw new Error('Wrong owner'); }, releaseOperationLease: () => { throw new Error('Wrong owner'); } }; plugin.engine = other;
  await plugin.onunload(); assert.equal(engines[0].lease, null); assert.equal(other.running, true);
  gate.resolve(); await sharing;
  assert.equal(plugin.engine, other); assert.equal(other.running, true);
});

test('settings cancellation uses the captured engine snapshot even when the current engine was replaced', async () => {
  const { plugin, engines } = pluginFixture(); const gate = deferred(); const entered = deferred(); const create = plugin.makeEngine;
  plugin.makeEngine = () => { const engine = create(); engine.client.getFile = async () => { entered.resolve(); await gate.promise; return { contentVersion: 19 }; }; return engine; };
  const sharing = plugin.shareCurrent(); await entered.promise;
  plugin.settings.remoteFolder = '/Changed/';
  const other = { running: true, client: { redact: String } }; plugin.engine = other; plugin._engineSettings = { ...plugin.settings };
  await plugin.saveSettings();
  assert.equal(engines[0].stopRequested, true); assert.equal(engines[0].lease, null); assert.equal(other.running, true);
  gate.resolve(); await sharing; assert.equal(plugin.engine, other); assert.equal(other.running, true);
});

test('pause releases before a failing settings save and keeps the active operation guarded', async () => {
  const { plugin, engines } = pluginFixture(); const gate = deferred(); const entered = deferred(); const create = plugin.makeEngine;
  plugin.makeEngine = () => { const engine = create(); engine.client.getFile = async () => { entered.resolve(); await gate.promise; return { contentVersion: 19 }; }; return engine; };
  const sharing = plugin.shareCurrent(); await entered.promise; plugin.saveData = async () => { throw new Error('synthetic storage failure'); };
  await assert.rejects(plugin.pause(), /storage failure/); assert.equal(engines[0].lease, null); assert.equal(plugin._busy(), true);
  gate.resolve(); await sharing; assert.equal(plugin._busy(), false);
});

test('native remote write already in flight cannot be undone, but unload releases and forbids a following write', async () => {
  const { plugin, calls, engines } = pluginFixture(); const entered = deferred(); const gate = deferred(); const create = plugin.makeEngine;
  plugin.makeEngine = () => { const engine = create(); engine.client.request = async () => { calls.push(['native-write-started']); entered.resolve(); await gate.promise; return { url: 'https://synthetic.invalid/share' }; }; return engine; };
  const sharing = plugin.shareCurrent(); await entered.promise;
  await plugin.onunload(); assert.equal(engines[0].lease, null); assert.equal(plugin._busy(), true);
  gate.resolve(); await sharing;
  assert.equal(calls.filter((c) => c[0] === 'native-write-started').length, 1);
  assert.equal(plugin._operation, null); assert.equal(await plugin.runSync({ silent: true }), null);
});

test('remoteLock false is passed through, and tokenless server 423 is not bypassed', async () => {
  const { plugin, calls } = pluginFixture(); const create = plugin.makeEngine; plugin.settings.remoteLock = false;
  plugin.makeEngine = () => { const engine = create(); engine.acquireOperationLease = async () => { assert.equal(engine.getSettings().remoteLock, false); }; engine.client.request = async () => { calls.push(['rejected-423']); throw Object.assign(new Error('Overlap rejected'), { status: 423 }); }; return engine; };
  await plugin.shareCurrent();
  assert.equal(calls.filter((c) => c[0] === 'rejected-423').length, 1);
  assert.equal(calls.some((c) => c[0] === 'mutated'), false); assert.equal(plugin._busy(), false);
});

async function conflictFixture() {
  const ledger = manifest(); const disk = new Map([['a.md', bytes('LOCAL')]]); const calls = [];
  const local = {
    exists: async (path) => disk.has(path),
    stat: async (path) => disk.has(path) ? { size: disk.get(path).byteLength, mtime: 1000 } : null,
    readBinary: async (path) => { if (!disk.has(path)) throw new Error('No synthetic file'); return disk.get(path); },
    writeBinary: async (path, data) => { disk.set(path, data); },
    trash: async (path) => { disk.delete(path); },
  };
  let head = { id: 'remote-a', name: 'a.md', path: '/Vault/', size: 6, mtime: 1000, contentVersion: 11, version: 3, checksum: 'synthetic-checksum' };
  let remoteBytes = bytes('REMOTE');
  const manager = new ConflictManager({ manifest: ledger, local, now: () => 1000 });
  await manager.record('a.md', head, remoteBytes);
  const engine = {
    running: false, stopRequested: false, lease: null, getSettings: () => ({ paused: false }),
    prepare: async (options) => { calls.push(['prepare', options]); },
    client: { getFile: async () => ({ ...head }) },
    _downOpts: () => ({}),
    downloadFn: async () => { calls.push(['download']); return remoteBytes; },
    pushLocal: async (...args) => {
      assert.ok(engine.lease); calls.push(['push', ...args]);
      const [path, previous, , source, options] = args;
      const data = await local.readBinary(source); const hash = await sha256Hex(data);
      assert.equal(hash, options.expectedLocalHash);
      const id = previous ? previous.id : 'copy-' + path;
      ledger.setFile(path, { remoteId: id, localHash: hash, localSize: data.byteLength, remoteContentVersion: previous ? previous.contentVersion + 1 : 1 });
      return { id };
    },
    acquireOperationLease: async () => { calls.push(['acquire']); engine.lease = { token: 'conflict-owned' }; },
    releaseOperationLease: async () => { calls.push(['release']); engine.lease = null; },
    refreshSigs: async () => {},
    _writeDownloaded: async (...args) => {
      calls.push(['commit', ...args]);
      const [, , data, initial, baseline] = args;
      if (initial && await sha256Hex(disk.get('a.md')) !== baseline.localHash) return { conflict: true };
      disk.set('a.md', data);
      ledger.setFile('a.md', { remoteId: args[1].id, localHash: await sha256Hex(data), localSize: data.byteLength, remoteContentVersion: args[1].contentVersion });
      return { written: true };
    },
  };
  return { ledger, local, disk, manager, engine, calls, head: () => head, setHead: (value) => { head = value; }, setBytes: (data) => { remoteBytes = data; } };
}

test('conflict record stores live contentVersion separately from archived legacy ordinal', async () => {
  const { ledger } = await conflictFixture(); const c = ledger.getConflict('a.md');
  assert.equal(c.contentVersion, 11); assert.notEqual(c.contentVersion, 3); assert.equal(c.remoteHash, await sha256Hex(bytes('REMOTE')));
});

test('same-generation rename and favorite changes do not invalidate reviewed content', async () => {
  const f = await conflictFixture(); const reviewed = { ...f.ledger.getConflict('a.md') };
  f.setHead({ ...f.head(), name: 'renamed.md', mtime: 2000, version: 99, favorite: true });
  const result = await f.manager.resolve('a.md', 'both', f.engine, { reviewed });
  assert.equal(result.resolved, true);
  const pushes = f.calls.filter((c) => c[0] === 'push'); assert.equal(pushes.length, 2);
  const [copy, push] = pushes;
  assert.equal(copy[2], null); assert.deepEqual(copy[5], { expectedLocalHash: await sha256Hex(bytes('REMOTE')), createOnly: true });
  assert.equal(text(f.disk.get(copy[1])), 'REMOTE'); assert.equal(f.ledger.getFile(copy[1]).remoteId, 'copy-' + copy[1]);
  assert.equal(push[2].name, 'renamed.md'); assert.deepEqual(push[5], { expectedContentVersion: 11, expectedLocalHash: await sha256Hex(bytes('LOCAL')) });
  assert.equal(f.ledger.getConflict('a.md'), undefined); assert.equal(f.engine.lease, null);
});

for (const timing of ['before download', 'final recheck']) {
  test(`new content generation with identical bytes at ${timing} requires renewed review`, async () => {
    const f = await conflictFixture();
    if (timing === 'before download') f.setHead({ ...f.head(), contentVersion: 12 });
    else { let reads = 0; f.engine.client.getFile = async () => ({ ...f.head(), contentVersion: ++reads === 1 ? 11 : 12 }); }
    await assert.rejects(f.manager.resolve('a.md', 'local', f.engine), /Reopen the conflict/);
    assert.equal(f.calls.some((c) => c[0] === 'push'), false);
    assert.equal(f.ledger.getConflict('a.md').contentVersion, 12);
    assert.equal(text(f.disk.get(f.ledger.getConflict('a.md').copyPath)), 'REMOTE');
    assert.equal(f.engine.running, false); assert.equal(f.engine.lease, null);
  });
}

test('same-generation different bytes fail closed and preserve a new conflict copy', async () => {
  const f = await conflictFixture(); f.setBytes(bytes('NEWEST'));
  await assert.rejects(f.manager.resolve('a.md', 'both', f.engine), /latest version was preserved/);
  assert.equal(f.calls.some((c) => c[0] === 'push'), false);
  assert.equal(text(f.disk.get(f.ledger.getConflict('a.md').copyPath)), 'NEWEST');
});

test('legacy conflicts still validate remote signature when no content generation exists', async () => {
  const f = await conflictFixture(); const old = { ...f.head(), contentVersion: null };
  f.setHead(old); f.ledger.setConflict('a.md', { ...f.ledger.getConflict('a.md'), contentVersion: null, remoteSig: remoteSig(old) });
  f.setHead({ ...old, version: 4 });
  await assert.rejects(f.manager.resolve('a.md', 'local', f.engine), /Reopen the conflict/);
  assert.equal(f.calls.some((c) => c[0] === 'push'), false);
});

test('keep remote commits exactly reviewed bytes once with the original local hash and no lease acquisition', async () => {
  const f = await conflictFixture(); const hash = await sha256Hex(f.disk.get('a.md'));
  const result = await f.manager.resolve('a.md', 'remote', f.engine);
  assert.equal(result.resolved, true); assert.equal(f.calls.filter((c) => c[0] === 'download').length, 1);
  const commit = f.calls.find((c) => c[0] === 'commit');
  assert.equal(text(commit[3]), 'REMOTE'); assert.equal(commit[4].path, 'a.md'); assert.equal(commit[5].localHash, hash);
  assert.equal(text(f.disk.get('a.md')), 'REMOTE'); assert.deepEqual(f.calls[0], ['prepare', { acquireLease: false }]);
});

test('local edits during conflict download are retained and leave review pending', async () => {
  const f = await conflictFixture(); f.engine.downloadFn = async () => { f.disk.set('a.md', bytes('USER-EDIT')); return bytes('REMOTE'); };
  await assert.rejects(f.manager.resolve('a.md', 'remote', f.engine), /Local file changed/);
  assert.equal(text(f.disk.get('a.md')), 'USER-EDIT'); assert.ok(f.ledger.getConflict('a.md')); assert.equal(f.engine.running, false);
});

test('push failure releases the conflict lease without clearing its review', async () => {
  const f = await conflictFixture(); f.engine.pushLocal = async () => { f.engine.lease = { token: 'owned' }; throw new Error('synthetic push failure'); };
  await assert.rejects(f.manager.resolve('a.md', 'local', f.engine), /push failure/);
  assert.equal(f.engine.lease, null); assert.equal(f.engine.running, false); assert.ok(f.ledger.getConflict('a.md'));
});

test('stable installation UUID does not reset file, folder, conflict, or inherited session baselines', async () => {
  const { plugin } = pluginFixture();
  const id = '20000000-0000-4000-8000-000000000002'; const oldWindow = global.window;
  plugin.app.vault.adapter.getBasePath = () => 'Synthetic/Vault';
  plugin.ledger.setFolder('Folder', { syncedAt: 1 }); plugin.ledger.setConflict('a.md', { remoteId: 'remote-a', contentVersion: 11 });
  plugin.ledger.data.meta.uploadSessions = { inherited: { owner: 'different-owner' } };
  const before = JSON.stringify({ files: plugin.ledger.data.files, folders: plugin.ledger.data.folders, conflicts: plugin.ledger.data.conflicts, sessions: plugin.ledger.data.meta.uploadSessions });
  try {
    global.window = { localStorage: { getItem: () => id, setItem: () => { throw new Error('Stable UUID must not be rewritten'); } } };
    await plugin._bindDeviceIdentity(); await plugin._bindDeviceIdentity();
  } finally { global.window = oldWindow; }
  assert.equal(plugin.ledger.data.meta.deviceId, id);
  assert.equal(JSON.stringify({ files: plugin.ledger.data.files, folders: plugin.ledger.data.folders, conflicts: plugin.ledger.data.conflicts, sessions: plugin.ledger.data.meta.uploadSessions }), before);
});

test('folder status distinguishes preserved new arrivals and scopes/disables Recheck API while busy', async () => {
  const { plugin } = pluginFixture(); let factory;
  plugin.registerView = (_type, create) => { factory = create; };
  // Register the same view factory without reading a real vault or loading persisted plugin state.
  const sourceLoad = TdriveSyncPlugin.prototype.onload;
  const originalMethods = new Map();
  for (const name of ['applyLanguage', '_bindDeviceIdentity', 'addSettingTab', 'addRibbonIcon', 'addCommand', 'registerChangeSync', 'setupAutoSync', 'refreshStatus']) { originalMethods.set(name, plugin[name]); plugin[name] = () => {}; }
  plugin.loadData = async () => ({ apiKey: 'synthetic-key', language: 'en' });
  plugin.manifest = { dir: '.synthetic-plugin' }; plugin.app.vault.adapter.exists = async () => false;
  plugin.app.vault.adapter.write = async () => {}; plugin.addStatusBarItem = () => null;
  try { await sourceLoad.call(plugin); }
  finally { for (const [name, method] of originalMethods) plugin[name] = method; }
  plugin.t = (key) => key; plugin.lang = 'en'; plugin.connection = 'synthetic';
  plugin.ledger.data.meta.syncScope = 'scope-a';
  plugin.ledger.data.meta.blockedFolders = { one: { path: 'New/', code: 'FOLDER_NOT_EMPTY', scope: 'scope-a' }, foreign: { path: 'Foreign/', code: 'UPLOAD_IN_PROGRESS', scope: 'scope-b' } };
  const view = factory({}); view.render();
  const explanation = view.contentEl.find((el) => el.text.includes('New arrivals were not deleted'));
  assert.ok(explanation); assert.equal(view.contentEl.find((el) => el.text.includes('because of an upload session')), null);
  assert.equal(view.contentEl.find((el) => el.text.includes('Foreign/')), null);
  const button = view.contentEl.find((el) => el.text === 'Recheck API'); const gate = deferred(); const runs = [];
  plugin.runSync = async (options) => { runs.push(options); plugin._operation = {}; await gate.promise; plugin._operation = null; };
  const clicking = button.onclick(); assert.equal(button.disabled, true); await button.onclick(); assert.equal(runs.length, 1);
  gate.resolve(); await clicking; assert.deepEqual(runs, [{ retryBlockedFolders: true }]);
  plugin._operation = {}; view.render(); assert.equal(view.contentEl.find((el) => el.text === 'Recheck API').disabled, true);
  plugin._operation = null; plugin.ledger.data.meta.blockedFolders.one.scope = 'scope-b'; view.render();
  assert.equal(view.contentEl.find((el) => el.text === 'Recheck API'), null);
});

async function nativeFixture() {
  const { plugin } = pluginFixture(); const clients = []; const requests = []; const controls = [];
  const hooks = {}; let active = null;
  const current = { id: 'remote-a', name: 'a.md', path: '/Vault/', size: 6, lastModified: 1000, contentVersion: 19, version: 2 };
  const disk = new Map([['a.md', bytes('LOCAL!')]]);
  const ok = (data, status = 200) => ({ status, headers: {}, body: bytes(JSON.stringify({ ok: true, data })) });
  plugin.local = {
    list: async () => [], listFolders: async () => [], exists: async (path) => disk.has(path),
    stat: async (path) => disk.has(path) ? { size: disk.get(path).byteLength, mtime: 1000 } : null,
    readBinary: async (path) => disk.get(path), writeBinary: async (path, data) => { disk.set(path, data); }, trash: async (path) => { disk.delete(path); },
  };
  plugin.makeEngine = TdriveSyncPlugin.prototype.makeEngine;
  plugin.makeClient = (settings) => {
    let client;
    client = new TdriveClient({ baseUrl: settings.baseUrl, apiKey: settings.apiKey, maxRetries: 0, maxServerRetries: 0,
      transport: async (req) => {
        const path = new URL(req.url).pathname.slice('/api/v1'.length);
        const body = req.body ? JSON.parse(text(req.body)) : {};
        requests.push({ ...req, path, client });
        if (path === '' && req.method === 'GET') return ok({ upload: { chunkBytes: 5242880, oneShotMaxBytes: 5242880 }, capabilities: { syncLeases: true, atomicIfEmpty: true, expectedContentVersion: true, contentVersions: true, uploadSessionListing: true } });
        if (req.method === 'GET' && path === '/files') return ok(new URL(req.url).searchParams.get('path') === '/' ? { files: [], folders: ['Vault'] } : { files: [current], folders: [] });
        if (req.method === 'GET' && path === '/files/remote-a') {
          if (hooks.getFile) await hooks.getFile(client);
          return ok({ file: current });
        }
        if (req.method === 'GET' && path === '/files/remote-a/chunks') return { status: 404, headers: {}, body: bytes(JSON.stringify({ ok: false, error: { code: 'NOT_FOUND', message: 'synthetic legacy manifest fallback' } })) };
        if (req.method === 'PATCH' && path === '/files/remote-a') {
          assert.equal(req.headers['X-Sync-Path'], '/Vault/'); assert.equal(req.headers['X-Sync-Lease'], active.token);
          if (hooks.patch) await hooks.patch(client, req);
          if (body.expectedVersion !== current.contentVersion) return { status: 409, headers: {}, body: bytes(JSON.stringify({ ok: false, error: { code: 'VERSION_CONFLICT', message: 'generation changed' } })) };
          current.contentVersion++; current.size = Buffer.from(body.content.inline, 'base64').length;
          return ok({ file: { ...current }, content: { contentVersion: current.contentVersion } });
        }
        if (req.method === 'POST' && path === '/files/remote-a/shares') {
          assert.equal(req.headers['X-Sync-Path'], '/Vault/'); assert.equal(req.headers['X-Sync-Lease'], active.token);
          if (hooks.share) return hooks.share(client, req);
          return ok({ url: 'https://synthetic.invalid/shared' });
        }
        if (path === '/sync/leases' && req.method === 'POST') {
          controls.push(['acquire', client, body]); if (hooks.acquire) await hooks.acquire(client);
          active = { token: 'native-synthetic-token-' + clients.indexOf(client), path: body.path, owner: body.owner };
          return ok({ ...active, expiresAt: Date.now() + 60000, serverTime: Date.now(), ttlSeconds: 60 }, 201);
        }
        if (path === '/sync/leases' && req.method === 'PATCH') {
          controls.push(['renew', client, body]); if (hooks.renew) await hooks.renew(client);
          return ok({ path: body.path, owner: plugin.ledger.data.meta.deviceId, expiresAt: Date.now() + 60000, serverTime: Date.now(), ttlSeconds: 60 });
        }
        if (path === '/sync/leases' && req.method === 'DELETE') {
          controls.push(['release', client, body]); if (hooks.release) await hooks.release(client);
          assert.equal(body.token, active.token); active = null; return ok({ released: true });
        }
        throw new Error('Unexpected synthetic request: ' + req.method + ' ' + path);
      },
    });
    clients.push(client); return client;
  };
  await plugin._withOperation(async () => { const engine = plugin.makeEngine(); await engine._bindScope(); plugin.ledger.setFile('a.md', { remoteId: 'remote-a' }); });
  requests.length = 0; controls.length = 0;
  return { plugin, clients, requests, controls, hooks, current, disk, active: () => active, ok };
}

test('actual plugin, engine and client prepare/acquire/write/release with one captured owner client', async () => {
  const f = await nativeFixture(); await f.plugin.shareCurrent();
  const client = f.clients.at(-1);
  assert.deepEqual(f.controls.map((c) => c[0]), ['acquire', 'release']);
  assert.equal(f.controls.every((c) => c[1] === client), true);
  assert.equal(f.controls[0][2].owner, f.plugin.ledger.data.meta.deviceId);
  assert.equal(f.requests.filter((r) => r.path.endsWith('/shares')).length, 1);
  assert.equal(client._lease, null); assert.equal(f.plugin.engine._lease, null); assert.equal(f.plugin.engine._leaseTimer, null);
  assert.equal(f.active(), null); assert.equal(f.plugin._operation, null);
  assert.equal(JSON.stringify(f.plugin.ledger.data).includes('native-synthetic-token'), false);
});

test('actual readonly preparation neither bootstraps nor acquires a root lease', async () => {
  const f = await nativeFixture(); assert.equal(await f.plugin.remoteIdFor('a.md'), 'remote-a');
  assert.equal(f.controls.length, 0); assert.equal(f.requests.every((r) => r.method === 'GET'), true);
});

for (const cancel of ['pause', 'unload']) {
  test(`actual ${cancel} clears engine/client token and heartbeat while metadata is in flight`, async () => {
    const f = await nativeFixture(); const entered = deferred(); const gate = deferred();
    f.hooks.getFile = async () => { entered.resolve(); await gate.promise; };
    const sharing = f.plugin.shareCurrent(); await entered.promise;
    const engine = f.plugin.engine; const client = engine.client;
    assert.ok(engine._lease); assert.ok(engine._leaseTimer);
    await f.plugin[cancel === 'pause' ? 'pause' : 'onunload']();
    assert.equal(engine._lease, null); assert.equal(engine._leaseTimer, null); assert.equal(client._lease, null); assert.equal(f.active(), null);
    gate.resolve(); await sharing;
    assert.equal(f.requests.some((r) => r.path.endsWith('/shares')), false); assert.equal(f.plugin._operation, null);
  });
}

test('actual late acquisition after unload releases the returned token without a write', async () => {
  const f = await nativeFixture(); const entered = deferred(); const gate = deferred();
  f.hooks.acquire = async () => { entered.resolve(); await gate.promise; };
  const sharing = f.plugin.shareCurrent(); await entered.promise;
  await f.plugin.onunload(); gate.resolve(); await sharing;
  assert.deepEqual(f.controls.map((c) => c[0]), ['acquire', 'release']);
  assert.equal(f.active(), null); assert.equal(f.plugin.engine._lease, null); assert.equal(f.plugin.engine.client._lease, null);
  assert.equal(f.requests.some((r) => r.path.endsWith('/shares')), false);
});

test('actual in-flight renewal cannot republish a token after unload', async () => {
  const f = await nativeFixture(); const reading = deferred(); const readGate = deferred();
  f.hooks.getFile = async () => { reading.resolve(); await readGate.promise; };
  const sharing = f.plugin.shareCurrent(); await reading.promise;
  const engine = f.plugin.engine; const entered = deferred(); const gate = deferred();
  f.hooks.renew = async () => { entered.resolve(); await gate.promise; };
  const renewing = engine._renewOperationLease().catch(() => {}); await entered.promise;
  const unloading = f.plugin.onunload();
  assert.equal(engine._lease, null); assert.equal(engine._leaseTimer, null); assert.equal(engine.client._lease, null);
  gate.resolve(); await renewing; await unloading; readGate.resolve(); await sharing;
  assert.deepEqual(f.controls.map((c) => c[0]), ['acquire', 'renew', 'release']);
  assert.equal(engine._lease, null); assert.equal(engine._leaseTimer, null); assert.equal(engine.client._lease, null); assert.equal(f.active(), null);
});

test('actual remote error releases the scoped lease with no secret in persistent state', async () => {
  const f = await nativeFixture();
  f.hooks.share = async () => ({ status: 409, headers: {}, body: bytes(JSON.stringify({ ok: false, error: { code: 'VERSION_CONFLICT', message: 'synthetic conflict' } })) });
  await f.plugin.shareCurrent();
  assert.deepEqual(f.controls.map((c) => c[0]), ['acquire', 'release']);
  assert.equal(f.plugin.engine.client._lease, null); assert.equal(f.active(), null); assert.equal(f.plugin._operation, null);
  assert.equal(JSON.stringify(f.plugin.ledger.data).includes('native-synthetic-token'), false);
});

test('actual unload during client backoff cannot retry the write without its released lease', async () => {
  const f = await nativeFixture(); const entered = deferred(); const gate = deferred();
  const makeEngine = f.plugin.makeEngine;
  f.plugin.makeEngine = function () {
    const engine = makeEngine.call(this); engine.client.maxServerRetries = 1;
    engine.client.sleep = async () => { entered.resolve(); await gate.promise; };
    return engine;
  };
  f.hooks.share = async () => ({ status: 503, headers: {}, body: bytes(JSON.stringify({ ok: false, error: { code: 'SERVER_BUSY', message: 'synthetic retry' } })) });
  const sharing = f.plugin.shareCurrent(); await entered.promise;
  await f.plugin.onunload(); gate.resolve(); await sharing;
  assert.equal(f.requests.filter((r) => r.path.endsWith('/shares')).length, 1);
  assert.equal(f.active(), null); assert.equal(f.plugin.engine.client._lease, null); assert.equal(f.plugin._operation, null);
});

test('actual conflict push passes the reviewed generation through engine to client expectedVersion', async () => {
  const f = await nativeFixture(); const makeEngine = f.plugin.makeEngine;
  f.plugin.makeEngine = function () { const engine = makeEngine.call(this); engine.revisionFn = async () => null; engine.downloadFn = async () => bytes('REMOTE'); return engine; };
  const conflicts = new ConflictManager({ manifest: f.plugin.ledger, local: f.plugin.local });
  await conflicts.record('a.md', f.current, bytes('REMOTE'));
  const reviewed = { conflict: { ...f.plugin.ledger.getConflict('a.md') }, scope: f.plugin.ledger.data.meta.syncScope };
  assert.equal((await f.plugin.resolveConflict('a.md', 'local', reviewed)).resolved, true);
  const patch = f.requests.find((r) => r.method === 'PATCH' && r.path === '/files/remote-a');
  assert.equal(JSON.parse(text(patch.body)).expectedVersion, 19);
  assert.equal(f.plugin.ledger.getFile('a.md').remoteContentVersion, 20);
  assert.equal(f.plugin.ledger.getConflict('a.md'), undefined);
  assert.deepEqual(f.controls.map((c) => c[0]), ['acquire', 'release']); assert.equal(f.active(), null);
});

test('actual external generation change after review rejects the write and preserves renewed review', async () => {
  const f = await nativeFixture(); const makeEngine = f.plugin.makeEngine;
  f.plugin.makeEngine = function () { const engine = makeEngine.call(this); engine.revisionFn = async () => null; engine.downloadFn = async () => bytes('REMOTE'); return engine; };
  const conflicts = new ConflictManager({ manifest: f.plugin.ledger, local: f.plugin.local });
  await conflicts.record('a.md', f.current, bytes('REMOTE'));
  f.hooks.patch = async () => { f.current.contentVersion = 20; };
  await assert.rejects(f.plugin.resolveConflict('a.md', 'local'), /review the preserved versions/);
  assert.equal(f.plugin.ledger.getConflict('a.md').contentVersion, 20);
  assert.equal(text(f.disk.get(f.plugin.ledger.getConflict('a.md').copyPath)), 'REMOTE');
  assert.equal(text(f.disk.get('a.md')), 'LOCAL!'); assert.equal(f.current.contentVersion, 20);
  const patches = f.requests.filter((r) => r.method === 'PATCH' && r.path === '/files/remote-a');
  assert.equal(patches.length, 1); assert.equal(JSON.parse(text(patches[0].body)).expectedVersion, 19);
  assert.deepEqual(f.controls.map((c) => c[0]), ['acquire', 'release']); assert.equal(f.active(), null); assert.equal(f.plugin._operation, null);
});
