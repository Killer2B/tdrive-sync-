'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const Module = require('node:module');
const { ManifestStore } = require('../src/ManifestStore');
const { SyncEngine } = require('../src/SyncEngine');
const { remoteSig } = require('../src/normalize');
if (!globalThis.crypto) globalThis.crypto = require('node:crypto').webcrypto;

const bytes = (text) => new TextEncoder().encode(text).buffer;
class TFile { constructor(path, size, mtime) { this.path = path; this.stat = { size, mtime }; } }
class Element {
  constructor(opts = {}) { this.text = opts.text; this.children = []; }
  createEl(tag, opts) { const el = new Element(opts); this.children.push(el); return el; }
  createDiv(opts) { return this.createEl('div', opts); }
  createSpan(opts) { return this.createEl('span', opts); }
  setText(text) { this.text = text; }
  empty() { this.children = []; }
  addClass() {}
}
let transportCalls = 0;
const notices = [];
const stub = {
  Plugin: class {
    constructor(app, manifest) { this.app = app; this.manifest = manifest; this.data = {}; this.disposers = []; }
    async loadData() { return this.data; }
    async saveData(data) { this.data = data; }
    addCommand() {} addSettingTab() {} registerView() {} addRibbonIcon() {}
    addStatusBarItem() { return { setText() {} }; }
    registerInterval() {} registerEvent() {}
    register(fn) { this.disposers.push(fn); }
  },
  PluginSettingTab: class {}, Setting: class {}, ItemView: class {}, TFile,
  Modal: class {
    constructor(app) { this.app = app; this.contentEl = new Element(); }
    open() { this.app.opened.push(this); this.opening = this.onOpen(); }
    close() { this.onClose(); }
  },
  Notice: class { constructor(message) { notices.push(message); } },
  requestUrl: async () => { transportCalls++; throw new Error('Tests must not contact a real server'); },
};
const originalLoad = Module._load;
let SourcePlugin;
try {
  Module._load = function (request, ...args) { return request === 'obsidian' ? stub : originalLoad.call(this, request, ...args); };
  delete require.cache[require.resolve('../src/plugin')];
  SourcePlugin = require('../src/plugin');
} finally { Module._load = originalLoad; }

async function harness(t, settings = {}, state = null) {
  const disk = new Map(); const folders = new Set(); const indexed = new Map(); const removed = []; const trashed = [];
  let clock = 1000;
  const put = (path, data) => {
    const buffer = Buffer.from(data); disk.set(path, buffer);
    indexed.set(path, new TFile(path, buffer.length, ++clock));
  };
  const adapter = {
    exists: async (p) => disk.has(p) || folders.has(p),
    stat: async (p) => disk.has(p) ? { type: 'file', ...indexed.get(p).stat } : (folders.has(p) ? { type: 'folder' } : null),
    read: async (p) => disk.get(p).toString(), write: async (p, s) => put(p, s),
    readBinary: async (p) => { if (!disk.has(p)) throw new Error('ENOENT'); const b = disk.get(p); return b.buffer.slice(b.byteOffset, b.byteOffset + b.length); },
    writeBinary: async (p, b) => put(p, b),
    appendBinary: async (p, b) => put(p, Buffer.concat([disk.get(p), Buffer.from(b)])),
    mkdir: async (p) => { folders.add(p); },
    rename: async (from, to) => { put(to, disk.get(from)); disk.delete(from); indexed.delete(from); },
    remove: async (p) => { removed.push(p); disk.delete(p); indexed.delete(p); },
    list: async () => ({ files: [], folders: [] }),
  };
  const layouts = []; const events = new Map();
  const app = {
    opened: [],
    vault: {
      adapter, getName: () => 'TestVault',
      getFiles: () => [...indexed.values()].filter((f) => !f.path.split('/').some((part) => part.startsWith('.'))),
      getAbstractFileByPath: (p) => indexed.get(p) || (folders.has(p) ? { path: p, children: [] } : null),
      createFolder: async (p) => { folders.add(p); },
      createBinary: async (p, b) => { if (disk.has(p) || folders.has(p)) throw new Error('EEXIST'); put(p, b); },
      modifyBinary: async (f, b) => adapter.writeBinary(f.path, b),
      process: async (f, transform) => {
        const current = new TextDecoder('utf-8', { ignoreBOM: true }).decode(await adapter.readBinary(f.path));
        const next = transform(current);
        if (next !== current) await adapter.writeBinary(f.path, bytes(next));
        return next;
      },
      on: (name, cb) => { events.set(name, cb); return cb; },
    },
    fileManager: { trashFile: async (f) => { trashed.push(f.path); disk.delete(f.path); } },
    workspace: { onLayoutReady: (cb) => layouts.push(cb), getActiveFile: () => null },
  };
  const timers = new Map(); let timerId = 0; const focus = new Set();
  const oldWindow = global.window;
  global.window = {
    setTimeout: (fn) => { timers.set(++timerId, fn); return timerId; },
    clearTimeout: (id) => timers.delete(id),
    setInterval: (fn) => { timers.set(++timerId, fn); return timerId; },
    clearInterval: (id) => timers.delete(id),
    addEventListener: (event, cb) => focus.add(cb), removeEventListener: (event, cb) => focus.delete(cb),
  };
  const plugin = new SourcePlugin(app, { dir: '.test/tdrive-sync', id: 'tdrive-sync', version: 'source' });
  plugin.data = { language: 'en', baseUrl: 'https://example.invalid', apiKey: 'test-key', remoteFolder: '/Vault/', ...settings };
  if (state) put('.test/tdrive-sync/sync-state.json', JSON.stringify(state));
  transportCalls = 0; notices.length = 0;
  t.after(() => {
    plugin.onunload(); for (const dispose of plugin.disposers) dispose();
    assert.equal(timers.size, 0, 'onunload must clear scheduled work');
    assert.equal(focus.size, 0, 'registered focus listener is disposed');
    if (oldWindow === undefined) delete global.window; else global.window = oldWindow;
  });
  await plugin.onload();
  return { plugin, local: plugin.local, app, adapter, put, disk, folders, removed, trashed, timers, layouts, events };
}

function memoryLedger() {
  let saved;
  const manifest = new ManifestStore({ read: async () => saved, write: async (value) => { saved = value; } });
  return manifest;
}
const diskPaths = (h) => [...h.disk.keys()].filter((p) => p !== h.plugin.manifest.dir + '/sync-state.json');

function legacyMetadataClient(client) {
  const upload = { chunkBytes: 5242880, oneShotMaxBytes: 5242880 };
  client._capabilities = {};
  client._rawInfo = Promise.resolve({ capabilities: {}, upload });
  client._info = upload;
  client.listFolder = async () => ({ files: [], folders: [] });
  client.getFile = async (id) => ({ id, name: 'note.md', path: '/Vault/', size: 13, mtime: 2000, contentVersion: null });
  return client;
}

test('folder trash keeps an unindexed disk folder, a replacement file, and malformed disk listings', async (t) => {
  const h = await harness(t);
  h.folders.add('Unindexed'); h.app.vault.getAbstractFileByPath = () => null;
  assert.equal(await h.local.trashFolder('Unindexed'), false);
  h.put('Replacement', 'new-file'); h.app.vault.getAbstractFileByPath = () => new TFile('Replacement', 8, 1);
  assert.equal(await h.local.trashFolder('Replacement'), false);
  h.app.vault.getAbstractFileByPath = () => ({ path: 'Unknown', children: [] }); h.adapter.list = async () => ({});
  assert.equal(await h.local.trashFolder('Unknown'), false);
  assert.equal(h.trashed.length, 0);
});
async function conflictEngine(h, options = {}) {
  h.put('note.md', 'first\nsecond\n'); h.put('note conflict.md', 'first\nremote\n');
  const manifest = h.plugin.ledger;
  h.plugin.settings.remoteLock = false;
  const settings = { ...h.plugin.settings };
  const engine = new SyncEngine({
    client: { apiKey: settings.apiKey, capabilities: async () => ({}), listFolder: async () => ({ files: [], folders: [] }), getFile: async () => ({ id: 'remote-note', name: 'note.md', size: 13, mtime: 2000 }), redact: (s) => s },
    local: h.local, manifest, getSettings: () => settings,
    downloadFn: async () => bytes('first\nremote\n'), ...options,
  });
  // A real lease acquisition rebinds scope; reviewed state must belong to that scope already.
  await engine._bindScope();
  manifest.setFile('note.md', { baseText: 'first\nsecond\n', remoteId: 'remote-note' });
  manifest.setConflict('note.md', { copyPath: 'note conflict.md', remoteId: 'remote-note', remoteSig: remoteSig({ id: 'remote-note', size: 13, mtime: 2000 }) });
  engine.refreshSigs = async () => {};
  return engine;
}

test('public vault update failure preserves original, unrelated sibling and recovery backup', async (t) => {
  const h = await harness(t); h.put('note.md', 'original'); h.put('note.md.tdrive-part', 'unrelated');
  h.adapter.rename = async () => { throw new Error('EACCES'); };
  const write = h.adapter.writeBinary;
  h.adapter.writeBinary = async (p, b) => { if (p === 'note.md') throw new Error('ENOSPC'); return write(p, b); };
  let failure;
  await assert.rejects(h.local.writeBinaryAtomic('note.md', bytes('remote')), (e) => { failure = e; return /ENOSPC/.test(e.message); });
  assert.equal(h.disk.get('note.md').toString(), 'original');
  assert.equal(h.disk.get('note.md.tdrive-part').toString(), 'unrelated');
  assert.equal(h.disk.get(failure.backupPath).toString(), 'original');
  assert.ok(!h.removed.includes('note.md') && !h.removed.includes(failure.backupPath));
});

test('recovery backup failure stops before touching the original', async (t) => {
  const h = await harness(t); h.put('note.md', 'original');
  h.adapter.writeBinary = async (p) => { h.put(p, 'partial'); throw new Error('ENOSPC'); };
  await assert.rejects(h.local.writeBinaryAtomic('note.md', bytes('remote')), /ENOSPC/);
  assert.ok([...h.disk.keys()].some((p) => p.startsWith('.note.md.tdrive-backup-')));
  assert.equal(h.disk.get('note.md').toString(), 'original');
});

test('verified commit uses exactly one supported vault update, not raw rename replacement', async (t) => {
  const h = await harness(t); h.put('Notes/note.md', 'original');
  let commits = 0; const process = h.app.vault.process;
  h.app.vault.process = async (...args) => { commits++; return process(...args); };
  h.adapter.rename = async () => { throw new Error('raw rename must not replace an indexed file'); };
  await h.local.writeBinaryAtomic('Notes/note.md', bytes('remote'), 5000);
  assert.equal(h.disk.get('Notes/note.md').toString(), 'remote');
  assert.deepEqual(diskPaths(h), ['Notes/note.md']);
  assert.equal(commits, 1);
});

test('indexed creation does not require raw rename and streaming requires supported capabilities', async (t) => {
  const h = await harness(t); delete h.adapter.rename;
  assert.equal(h.local.canStream, true);
  await h.local.writeBinaryAtomic('note.md', bytes('remote'));
  assert.equal(h.disk.get('note.md').toString(), 'remote');
  delete h.adapter.appendBinary;
  assert.equal(h.local.canStream, false);
  await assert.rejects(h.local.beginStreamWrite('stream.md'), /appendBinary/);
});

test('streamed bytes remain staged until close, preserve sibling, and abort only their own temp', async (t) => {
  const h = await harness(t); h.put('large.bin.tdrive-part', 'unrelated');
  const siblingStat = await h.local.stat('large.bin.tdrive-part');
  const sink = await h.local.beginStreamWrite('large.bin');
  await sink.write(Buffer.from('one')); await sink.write(new Uint8Array(bytes('two')));
  assert.equal(h.disk.has('large.bin'), false);
  assert.deepEqual(await h.local.list(), [{ path: 'large.bin.tdrive-part', ...siblingStat }]);
  assert.equal(await sink.close(), 'large.bin');
  await sink.abort();
  assert.equal(h.disk.get('large.bin').toString(), 'onetwo');
  assert.equal(h.disk.get('large.bin.tdrive-part').toString(), 'unrelated');
  const failed = await h.local.beginStreamWrite('failed.bin'); await failed.write(bytes('partial')); await failed.abort();
  assert.equal(h.disk.has('failed.bin'), false);
  assert.ok(diskPaths(h).every((p) => !p.startsWith('.')));
});

test('stream close refuses a destination created during download without removing it', async (t) => {
  const h = await harness(t);
  const sink = await h.local.beginStreamWrite('large.bin'); await sink.write(bytes('remote'));
  h.put('large.bin', 'new local note');
  await assert.rejects(sink.close(), /appeared during download/);
  await sink.abort();
  assert.equal(h.disk.get('large.bin').toString(), 'new local note');
  assert.deepEqual(diskPaths(h), ['large.bin']);
  await assert.rejects(h.local.beginStreamWrite('large.bin'), /already exists/);
});

test('stream commit failure preserves unrelated files and clears the staging file', async (t) => {
  const h = await harness(t); h.put('unrelated.bin', 'local');
  const sink = await h.local.beginStreamWrite('large.bin'); await sink.write(bytes('remote'));
  h.app.vault.createBinary = async () => { throw new Error('EACCES'); };
  await assert.rejects(sink.close(), /EACCES/);
  assert.deepEqual(diskPaths(h), ['unrelated.bin']);
});

test('empty enabled E2EE passphrase blocks SOURCE run before transport or engine construction', async (t) => {
  const h = await harness(t, { e2eeEnabled: true, e2eePassphrase: '' }); h.put('secret.md', 'SECRET');
  assert.throws(() => h.plugin.makeCrypto(), /passphrase is empty/);
  assert.equal(await h.plugin.runSync({ silent: true }), null);
  assert.equal(transportCalls, 0); assert.equal(h.plugin.engine, null);
  assert.ok(notices.some((message) => /passphrase is empty/.test(message)));
});

test('crypto cache identity cannot collide on passphrase/salt delimiters', async (t) => {
  const h = await harness(t, { e2eeEnabled: true, e2eePassphrase: 'a|b', e2eeSalt: 'c' });
  const first = h.plugin.makeCrypto();
  h.plugin.settings.e2eePassphrase = 'a'; h.plugin.settings.e2eeSalt = 'b|c';
  assert.notEqual(h.plugin.makeCrypto(), first);
  h.plugin.settings.e2eeEnabled = false; assert.equal(h.plugin.makeCrypto(), null);
});

test('engine client, transfer settings, crypto and arrays use one immutable creation snapshot', async (t) => {
  const h = await harness(t, { e2eeEnabled: true, e2eePassphrase: 'original', syncFolders: ['Notes'], excludePatterns: ['skip.md'] });
  const engine = h.plugin.makeEngine();
  h.plugin.settings.baseUrl = 'https://other.invalid'; h.plugin.settings.apiKey = 'other-key'; h.plugin.settings.remoteFolder = '/Other/';
  h.plugin.settings.syncFolders.push('Other'); h.plugin.settings.excludePatterns.push('another.md'); h.plugin.settings.e2eePassphrase = '';
  const settings = engine.getSettings();
  assert.equal(engine.client.apiKey, 'test-key'); assert.equal(settings.remoteFolder, '/Vault/');
  assert.deepEqual(settings.syncFolders, ['Notes']); assert.deepEqual(settings.excludePatterns, ['skip.md']);
  assert.equal(engine.crypto.passphrase, 'original');
  assert.ok(Object.isFrozen(settings)); assert.ok(Object.isFrozen(settings.syncFolders));
  assert.equal(engine.getSettings(), settings);
});

test('settings edits request pause before a rejected save, and pause itself does not await persistence', async (t) => {
  const h = await harness(t);
  await h.plugin._withOperation(async () => {
    const engine = h.plugin.makeEngine(); engine.running = true;
    h.plugin.saveData = async () => { throw new Error('storage unavailable'); };
    h.plugin.settings.remoteFolder = '/Other/';
    await assert.rejects(h.plugin.saveSettings(), /storage unavailable/); assert.equal(engine.stopRequested, true);
    engine.stopRequested = false;
    await assert.rejects(h.plugin.pause(), /storage unavailable/); assert.equal(engine.stopRequested, true);
    assert.equal(h.plugin.settings.paused, true);
  });
  assert.equal(h.plugin._operation, null);
});

test('onunload clears interval/change/startup work, including a queued layout callback', async (t) => {
  const h = await harness(t, { autoSyncMinutes: 1, syncOnStartup: true, syncOnChange: true });
  h.layouts[0](); h.events.get('modify')({ path: 'note.md' });
  assert.equal(h.timers.size, 3);
  const staleCallbacks = [...h.timers.values()];
  h.plugin.onunload(); assert.equal(h.timers.size, 0);
  h.layouts[0](); assert.equal(h.timers.size, 0);
  h.events.get('modify')({ path: 'after-unload.md' }); h.plugin.setupAutoSync(); assert.equal(h.timers.size, 0);
  for (const callback of staleCallbacks) callback();
  assert.equal(await h.plugin.runSync(), null); assert.equal(transportCalls, 0);
});

test('plugin conflict entry point rejects active sync, paused plugin, missing key and invalid E2EE', async (t) => {
  const h = await harness(t);
  h.plugin.engine = { running: true, pause() {} };
  await assert.rejects(h.plugin.resolveConflict('note.md', 'local'), /already running/i);
  h.plugin.engine.running = false; h.plugin.settings.paused = true;
  await assert.rejects(h.plugin.resolveConflict('note.md', 'local'), /paused/i);
  h.plugin.settings.paused = false; h.plugin.settings.apiKey = '';
  await assert.rejects(h.plugin.resolveConflict('note.md', 'local'), /API key/i);
  h.plugin.settings.apiKey = 'test-key'; h.plugin.settings.e2eeEnabled = true;
  await assert.rejects(h.plugin.resolveConflict('note.md', 'local'), /passphrase is empty/);
  assert.equal(transportCalls, 0);
});

test('conflict running guard covers awaited preparation and is released after failure', async (t) => {
  const h = await harness(t); const engine = await conflictEngine(h);
  let release; engine.prepare = () => new Promise((resolve) => { release = resolve; });
  engine.pushLocal = async () => { throw new Error('upload failed'); };
  h.plugin.makeEngine = () => { h.plugin.engine = engine; h.plugin._operation.engine = engine; return engine; };
  const pending = h.plugin.resolveConflict('note.md', 'both');
  assert.equal(engine.running, true);
  assert.equal(await h.plugin.runSync({ silent: true }), null);
  await assert.rejects(engine.conflicts.resolve('note.md', 'local', engine), /already running/);
  release(); await assert.rejects(pending, /upload failed/);
  assert.equal(engine.running, false); assert.ok(h.plugin.ledger.getConflict('note.md'));
});

test('conflict is fetched after preparation so an engine scope reset cannot use a stale remote id', async (t) => {
  const h = await harness(t); const engine = await conflictEngine(h);
  engine.prepare = async () => h.plugin.ledger.deleteConflict('note.md');
  engine.client.getFile = async () => { throw new Error('must not fetch stale remote id'); };
  assert.deepEqual(await engine.conflicts.resolve('note.md', 'remote', engine), { resolved: false });
  assert.equal(engine.running, false);
});

test('remote conflict resolution leaves conflict open when reviewed-byte commit detects a local edit', async (t) => {
  const h = await harness(t); const engine = await conflictEngine(h);
  engine._writeDownloaded = async () => { h.put('note.md', 'new edit'); return { conflict: true }; };
  await assert.rejects(engine.conflicts.resolve('note.md', 'remote', engine), /changed during conflict resolution/);
  assert.equal(h.disk.get('note.md').toString(), 'new edit');
  assert.ok(h.plugin.ledger.getConflict('note.md')); assert.equal(engine.running, false);
  assert.ok([...h.disk.keys()].some((p) => /Tdrive local backup/.test(p)));
});

test('actual SOURCE conflict verification preserves edits made during the single reviewed download', async (t) => {
  const h = await harness(t);
  const engine = await conflictEngine(h, { downloadFn: async () => { h.put('note.md', 'new edit'); return bytes('first\nremote\n'); } });
  await assert.rejects(engine.conflicts.resolve('note.md', 'remote', engine), /changed during conflict resolution/);
  assert.equal(h.disk.get('note.md').toString(), 'new edit');
  assert.ok(h.plugin.ledger.getConflict('note.md')); assert.equal(engine.running, false);
});

test('actual SOURCE engine stream cleanup never trashes a destination created during transfer', async (t) => {
  const h = await harness(t);
  const engine = new SyncEngine({
    client: {}, local: h.local, manifest: h.plugin.ledger,
    getSettings: () => ({ streamLargeDownloads: true, streamThresholdBytes: 1 }),
    downloadFn: async (client, remote, opts) => { await opts.sink.write(bytes('remote')); h.put('large.bin', 'new local file'); },
  });
  await assert.rejects(engine._execute({ action: 'download', key: 'large.bin', L: null, R: { id: 'r', size: 6 } }, {}), /appeared during download/);
  assert.equal(h.disk.get('large.bin').toString(), 'new local file'); assert.deepEqual(h.trashed, []);
  assert.deepEqual(diskPaths(h), ['large.bin']);
});

test('SOURCE engine/plugin scope contract clears deletion baselines when server, key or root changes', async (t) => {
  const h = await harness(t); h.put('note.md', 'healthy local note');
  const first = h.plugin.makeEngine(); await first._bindScope();
  for (const [key, value] of [['baseUrl', 'https://other.invalid'], ['apiKey', 'different-account-key'], ['remoteFolder', '/Other/']]) {
    const ledger = h.plugin.ledger;
    ledger.setFile('note.md', { remoteId: 'old-id', localHash: 'old-hash' }); ledger.setFolder('Notes', { syncedAt: 1 });
    ledger.setConflict('note.md', { copyPath: 'old-copy.md', remoteId: 'old-id' });
    Object.assign(ledger.data.meta, { deletions: [{ side: 'remote', remoteId: 'old-id' }], blockedDeletes: { items: [{ path: 'note.md' }] }, remoteSnapshot: { files: {} }, moveHints: { old: { to: 'new' } }, uploadChunkSize: 123 });
    h.plugin.settings[key] = value;
    const engine = h.plugin.makeEngine(); await engine._bindScope();
    assert.equal(ledger.count(), 0); assert.deepEqual(ledger.folderKeys(), []); assert.deepEqual(ledger.conflictKeys(), []);
    assert.deepEqual(ledger.data.meta.deletions, []); assert.equal(ledger.data.meta.blockedDeletes, undefined);
    assert.equal(ledger.data.meta.remoteSnapshot, undefined); assert.equal(ledger.data.meta.moveHints, undefined);
    assert.equal(engine.client._uploadChunk, undefined); assert.equal(h.disk.get('note.md').toString(), 'healthy local note');
  }
  assert.equal(first.getSettings().remoteFolder, '/Vault/'); assert.equal(first.client.apiKey, 'test-key');
  assert.equal(transportCalls, 0);
});

test('merge resolution preserves edits made during download and keeps the conflict open', async (t) => {
  const h = await harness(t); const engine = await conflictEngine(h, { downloadFn: async () => { h.put('note.md', 'new edit'); return bytes('first\nremote\n'); } });
  engine.pushLocal = async () => { throw new Error('must not upload stale merged content'); };
  await assert.rejects(engine.conflicts.resolve('note.md', 'merge', engine), /changed during conflict resolution/);
  assert.equal(h.disk.get('note.md').toString(), 'new edit');
  assert.ok(h.plugin.ledger.getConflict('note.md')); assert.equal(engine.running, false);
});

test('merge resolution preserves a local backup when subsequent upload fails', async (t) => {
  const h = await harness(t); const engine = await conflictEngine(h);
  const remoteText = 'first\nmiddle\nREMOTE\n'; const remote = { id: 'remote-note', name: 'note.md', size: bytes(remoteText).byteLength, mtime: 2000 };
  h.put('note.md', 'LOCAL\nmiddle\nlast\n'); h.put('note conflict.md', remoteText);
  h.plugin.ledger.setFile('note.md', { baseText: 'first\nmiddle\nlast\n', remoteId: remote.id });
  h.plugin.ledger.setConflict('note.md', { copyPath: 'note conflict.md', remoteId: remote.id, remoteSig: remoteSig(remote) });
  engine.client.getFile = async () => ({ ...remote }); engine.downloadFn = async () => bytes(remoteText);
  engine.pushLocal = async () => { throw new Error('upload failed'); };
  await assert.rejects(engine.conflicts.resolve('note.md', 'merge', engine), /upload failed/);
  const backup = [...h.disk.keys()].find((p) => /Tdrive local backup/.test(p));
  assert.equal(h.disk.get(backup).toString(), 'LOCAL\nmiddle\nlast\n');
  assert.ok(h.plugin.ledger.getConflict('note.md')); assert.equal(engine.running, false);
});

test('deletion modal submits an isolated copy of the reviewed identities', async (t) => {
  const h = await harness(t);
  const pending = { reason: 'limit', items: [{ path: 'a.md', action: 'deleteRemote', remoteId: 'a' }] };
  h.plugin.ledger.data.meta.syncScope = 'reviewed-scope';
  h.plugin.ledger.data.meta.blockedDeletes = pending; h.plugin.approveDeletes();
  const modal = h.app.opened[0]; const row = modal.contentEl.children.at(-1); const button = row.children[0];
  let options; h.plugin.runSync = (opts) => { options = opts; };
  pending.items[0].path = 'unreviewed.md'; pending.items.push({ path: 'b.md', action: 'deleteRemote' });
  h.plugin.ledger.data.meta.syncScope = 'new-scope';
  button.onclick();
  assert.deepEqual(options.approvedDeletes, [{ path: 'a.md', action: 'deleteRemote', remoteId: 'a' }]);
  assert.equal(options.approveDeletes, true);
  assert.equal(options.approvedScope, 'reviewed-scope');
});

test('changed deletion plans remain reviewable', async (t) => {
  const h = await harness(t);
  h.plugin.ledger.data.meta.blockedDeletes = { reason: 'changed', items: [{ path: 'new.md', action: 'deleteRemote' }] };
  h.plugin.approveDeletes(); assert.equal(h.app.opened.length, 1);
});

test('metadata lookup binds scope before versions/share and never uses an old account remote id', async (t) => {
  const h = await harness(t); const makeClient = h.plugin.makeClient.bind(h.plugin);
  h.plugin.makeClient = (settings) => legacyMetadataClient(makeClient(settings));
  const engine = h.plugin.makeEngine(); await engine._bindScope();
  h.plugin.ledger.setFile('note.md', { remoteId: 'old-account-id' });
  assert.equal(await h.plugin.remoteIdFor('note.md'), 'old-account-id');
  h.plugin.settings.apiKey = 'new-account-key';
  h.app.workspace.getActiveFile = () => ({ path: 'note.md' });
  await h.plugin.viewVersions(); await h.plugin.shareCurrent();
  assert.equal(h.app.opened.length, 0); assert.equal(transportCalls, 0);
  assert.equal(h.plugin.ledger.getFile('note.md'), undefined);
});

test('metadata lookup refuses scope rebinding during an active sync', async (t) => {
  const h = await harness(t); const engine = h.plugin.makeEngine(); engine.running = true;
  h.plugin.settings.remoteFolder = '/Other/';
  await assert.rejects(h.plugin.remoteIdFor('note.md'), /already running/i);
  assert.equal(h.plugin.engine, engine);
});

test('a versions modal opened in an old scope cannot restore an old account file after settings change', async (t) => {
  const h = await harness(t); const initial = h.plugin.makeEngine(); await initial._bindScope();
  h.plugin.ledger.setFile('note.md', { remoteId: 'old-account-id' }); h.app.workspace.getActiveFile = () => ({ path: 'note.md' });
  const makeClient = h.plugin.makeClient.bind(h.plugin); const requests = [];
  h.plugin.makeClient = (settings) => {
    const client = legacyMetadataClient(makeClient(settings));
    client.request = async (req) => { assert.equal(req.path, '/files/old-account-id/versions'); requests.push(req); return { versions: [{ id: 'version-one', createdAt: 1000, size: 3 }] }; };
    return client;
  };
  await h.plugin.viewVersions(); const modal = h.app.opened[0]; await modal.opening;
  const restore = modal.contentEl.children.at(-1).children.find((el) => el.onclick);
  assert.ok(restore, 'version restore action is rendered');
  h.plugin.settings.apiKey = 'new-account-key'; await restore.onclick();
  assert.equal(requests.length, 1); assert.ok(!requests.some((req) => req.method === 'POST'));
  assert.ok(notices.some((message) => /destination changed/i.test(message)));
});

test('share mutation holds the running guard and uses the scope-bound immutable client', async (t) => {
  const h = await harness(t, { remoteLock: false }); const initial = h.plugin.makeEngine(); await initial._bindScope();
  h.plugin.ledger.setFile('note.md', { remoteId: 'current-id' }); h.app.workspace.getActiveFile = () => ({ path: 'note.md' });
  const makeClient = h.plugin.makeClient.bind(h.plugin);
  let started; const requestStarted = new Promise((resolve) => { started = resolve; });
  let release; const response = new Promise((resolve) => { release = resolve; });
  let usedClient;
  h.plugin.makeClient = (settings) => {
    const client = legacyMetadataClient(makeClient(settings));
    client.request = (req) => { assert.equal(req.method, 'POST'); assert.equal(req.path, '/files/current-id/shares'); usedClient = client; started(); return response; };
    return client;
  };
  const pending = h.plugin.shareCurrent(); await requestStarted;
  assert.equal(h.plugin.engine.running, true); assert.equal(await h.plugin.runSync({ silent: true }), null);
  h.plugin.settings.apiKey = 'new-account-key';
  assert.equal(usedClient.apiKey, 'test-key');
  release({ url: 'https://example.invalid/share' }); await pending;
  assert.equal(h.plugin.engine.running, false); assert.equal(transportCalls, 0);
});

test('local deletion refuses a folder replacing a file and folder deletion fails closed on list errors', async (t) => {
  const h = await harness(t); h.folders.add('note.md');
  await assert.rejects(h.local.trash('note.md'), /Not a file/);
  h.adapter.list = async () => { throw new Error('EACCES'); };
  assert.equal(await h.local.trashFolder('note.md'), false); assert.deepEqual(h.trashed, []);
});

test('ledger dictionary prototype names are real paths, not phantom files or conflicts', async () => {
  const manifest = memoryLedger();
  for (const name of ['constructor', 'toString', '__proto__']) {
    assert.equal(manifest.getFile(name), undefined); assert.equal(manifest.getConflict(name), undefined); assert.equal(manifest.hasFolder(name), false);
    manifest.setFile(name, { remoteId: name }); manifest.setFolder(name, { syncedAt: 1 }); manifest.setConflict(name, { copyPath: name + '.copy' });
  }
  await manifest.save(); await manifest.load();
  for (const name of ['constructor', 'toString', '__proto__']) {
    assert.equal(manifest.getFile(name).remoteId, name); assert.equal(manifest.getConflict(name).copyPath, name + '.copy'); assert.equal(manifest.hasFolder(name), true);
  }
  assert.equal(manifest.fileKeys().length, 3); assert.equal(Object.getPrototypeOf(manifest.data.files), null);
});

test('valid JSON with malformed ledger maps/metadata resets every destructive baseline', async () => {
  for (const invalid of [null, [], { files: [] }, { conflicts: null }, { files: { 'a.md': false } }, { meta: [] }, { meta: { deletions: {} } }, { log: [null] }]) {
    const manifest = new ManifestStore({ read: async () => JSON.stringify(invalid), write: async () => {} });
    await manifest.load();
    assert.equal(manifest.count(), 0); assert.deepEqual(manifest.folderKeys(), []); assert.deepEqual(manifest.conflictKeys(), []);
    assert.deepEqual(manifest.data.meta.deletions, []); assert.match(manifest.data.log[0].msg, /reset/);
  }
});
