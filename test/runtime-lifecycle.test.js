'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const Module = require('node:module');
const { ManifestStore } = require('../src/ManifestStore');
const { sha256Hex } = require('../src/util');

const bytes = (s) => new TextEncoder().encode(s).buffer;
const buffer = (b) => b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength);
const flush = () => new Promise((resolve) => setImmediate(resolve));
const deferred = () => { let resolve; const promise = new Promise((r) => { resolve = r; }); return { promise, resolve }; };
const report = () => ({ status: 'ok', uploaded: 0, downloaded: 0, skipped: 0, errors: 0, conflicts: 0, warnings: [], failures: [] });
class TFile { constructor(path, stat) { this.path = path; this.stat = { ...stat }; } }
class Element {
  constructor(opts = {}) { this.text = opts.text; this.children = []; }
  createEl(tag, opts) { const el = new Element(opts); this.children.push(el); return el; }
  createDiv(opts) { return this.createEl('div', opts); }
  createSpan(opts) { return this.createEl('span', opts); }
  empty() { this.children = []; }
  setText(text) { this.text = text; }
  addClass() {}
}
const notices = [];
let networkCalls = 0;
const stub = {
  Plugin: class {
    constructor(app, manifest) { this.app = app; this.manifest = manifest; this.disposers = []; this.commands = []; }
    async loadData() { return this.data; }
    async saveData() {}
    addCommand(command) { this.commands.push(command); } addSettingTab() {} addRibbonIcon() {} registerInterval() {} registerEvent() {}
    registerView(type, factory) { this.viewFactory = factory; }
    addStatusBarItem() { return { setText() {} }; }
    register(fn) { this.disposers.push(fn); }
  },
  PluginSettingTab: class {}, TFile,
  ItemView: class { constructor() { this.contentEl = new Element(); } },
  Modal: class {
    constructor(app) { this.app = app; this.contentEl = new Element(); }
    open() { this.app.opened.push(this); this.opening = this.onOpen(); }
    close() { this.onClose(); }
  },
  Setting: class {
    constructor(el) { this.el = el; }
    setName() { return this; } setDesc() { return this; }
    addButton(cb) {
      const el = this.el.createEl('button');
      const button = { setButtonText(text) { el.text = text; return button; }, onClick(fn) { el.onclick = fn; return button; } };
      cb(button); return this;
    }
  },
  Notice: class { constructor(message) { notices.push(message); } },
  requestUrl: async () => { networkCalls++; throw new Error('Real network is forbidden in runtime lifecycle tests'); },
};
const load = Module._load;
let SourcePlugin;
try {
  Module._load = function (request, ...args) { return request === 'obsidian' ? stub : load.call(this, request, ...args); };
  SourcePlugin = require('../src/plugin');
} finally { Module._load = load; }

async function harness(t, settings = {}, options = {}) {
  const disk = new Map(), index = new Map(), folders = new Set(), folderIndex = new Map(), events = new Map();
  const calls = [], removed = [], timers = new Map(), layouts = [];
  const clock = { now: 1000000 }; let stamp = 1000, timerId = 0, saved = options.ledger ? JSON.stringify(options.ledger) : null;
  const putDisk = (path, data, opts) => { disk.set(path, { data: Buffer.from(data), mtime: opts && Number.isFinite(opts.mtime) ? opts.mtime : ++stamp }); };
  const stat = (path) => disk.has(path) ? { type: 'file', size: disk.get(path).data.length, mtime: disk.get(path).mtime } : folders.has(path) ? { type: 'folder' } : null;
  const emit = (kind, file, oldPath) => { const handler = events.get(kind); if (handler) return handler(file, oldPath); };
  const adapter = {
    exists: async (p) => p === '.fake-plugin/sync-state.json' ? saved !== null : disk.has(p) || folders.has(p),
    stat: async (p) => stat(p),
    read: async () => saved, write: async (p, s) => { saved = s; },
    readBinary: async (p) => { if (!disk.has(p)) throw new Error('ENOENT ' + p); return buffer(disk.get(p).data); },
    writeBinary: async (p, b, opts) => { calls.push(['raw-write', p]); putDisk(p, b, opts); },
    appendBinary: async (p, b) => { calls.push(['append', p]); putDisk(p, Buffer.concat([disk.get(p).data, Buffer.from(b)])); },
    remove: async (p) => { removed.push(p); disk.delete(p); },
    rename: async () => { calls.push(['rename']); throw new Error('EEXIST: mobile adapter does not replace destinations'); },
    list: async () => ({ files: [], folders: [] }),
  };
  for (const key of ['getBasePath', 'basePath']) if (key in options) adapter[key] = options[key];
  const vault = {
    adapter, getName: () => 'SyntheticVault',
    getFiles: () => [...index.values()].filter((f) => !f.path.split('/').some((p) => p.startsWith('.'))),
    getAllLoadedFiles: () => [...index.values(), ...folderIndex.values()],
    getAbstractFileByPath: (p) => index.get(p) || folderIndex.get(p) || null,
    on: (name, fn) => { events.set(name, fn); return fn; },
    createFolder: async (p) => {
      if (folders.has(p) || disk.has(p)) throw new Error('EEXIST');
      folders.add(p); const f = { path: p, children: [] }; folderIndex.set(p, f); emit('create', f); return f;
    },
    createBinary: async (p, b, opts) => {
      calls.push(['create', p]);
      if (disk.has(p) || folders.has(p) || index.has(p)) throw new Error('EEXIST');
      const parent = p.split('/').slice(0, -1).join('/');
      if (parent && !folderIndex.has(parent)) throw new Error('Parent is not registered');
      putDisk(p, b, opts); const f = new TFile(p, stat(p)); index.set(p, f); emit('create', f); return f;
    },
    modifyBinary: async (f, b, opts) => {
      calls.push(['modify', f.path]);
      assert.equal(index.get(f.path), f);
      putDisk(f.path, b, opts); f.stat = { ...stat(f.path) }; emit('modify', f);
    },
    process: async (f, transform, opts) => {
      const current = new TextDecoder('utf-8', { ignoreBOM: true }).decode(await adapter.readBinary(f.path));
      const next = transform(current);
      if (next !== current) {
        calls.push(['modify', f.path]); assert.equal(index.get(f.path), f);
        putDisk(f.path, bytes(next), opts); f.stat = { ...stat(f.path) }; emit('modify', f);
      }
      return next;
    },
    read: async (f) => disk.get(f.path).data.toString(),
  };
  const app = {
    opened: [], vault,
    fileManager: { trashFile: async (f) => { disk.delete(f.path); index.delete(f.path); emit('delete', f); } },
    workspace: { onLayoutReady: (fn) => layouts.push(fn), getActiveFile: () => ({ path: 'note.md' }) },
  };
  const oldWindow = global.window, oldNow = Date.now;
  Date.now = () => clock.now;
  global.window = {
    setTimeout: (fn, ms) => { const id = ++timerId; timers.set(id, { fn, at: clock.now + ms }); return id; },
    clearTimeout: (id) => timers.delete(id),
    setInterval: (fn, ms) => { const id = ++timerId; timers.set(id, { fn, at: clock.now + ms, interval: ms }); return id; },
    clearInterval: (id) => timers.delete(id), addEventListener() {}, removeEventListener() {},
  };
  if ('localStorage' in options) global.window.localStorage = options.localStorage;
  const plugin = new SourcePlugin(app, { dir: '.fake-plugin', id: 'tdrive-sync', version: 'source' });
  const plugins = [plugin];
  plugin.data = { apiKey: 'account-A', language: 'en', baseUrl: 'https://example.invalid', remoteFolder: '/Vault/', remoteLock: false, ...settings };
  notices.length = 0; networkCalls = 0;
  t.after(() => {
    for (const loaded of plugins) { loaded.onunload(); for (const fn of loaded.disposers) fn(); }
    assert.equal(timers.size, 0); assert.equal(networkCalls, 0);
    Date.now = oldNow; if (oldWindow === undefined) delete global.window; else global.window = oldWindow;
  });
  await plugin.onload();
  const reload = async (next = {}) => {
    plugins.at(-1).onunload();
    if ('ledger' in next) saved = JSON.stringify(next.ledger);
    if ('localStorage' in next) global.window.localStorage = next.localStorage;
    for (const key of ['getBasePath', 'basePath']) if (key in next) adapter[key] = next[key];
    const fresh = new SourcePlugin(app, plugin.manifest); fresh.data = { ...plugin.data, ...(next.settings || {}) }; plugins.push(fresh);
    await fresh.onload(); return fresh;
  };
  const put = (p, data) => {
    const parts = p.split('/'); parts.pop(); let parent = '';
    for (const part of parts) { parent = parent ? parent + '/' + part : part; folders.add(parent); folderIndex.set(parent, { path: parent, children: [] }); }
    putDisk(p, data); index.set(p, new TFile(p, stat(p)));
  };
  const advance = async (ms) => {
    const end = clock.now + ms;
    for (let n = 0; n < 100; n++) {
      const next = [...timers.entries()].filter(([, task]) => task.at <= end).sort((a, b) => a[1].at - b[1].at)[0];
      if (!next) break;
      const [id, task] = next; clock.now = task.at; timers.delete(id);
      if (task.interval) timers.set(id, { ...task, at: clock.now + task.interval });
      task.fn(); await flush();
      assert.ok(n < 99, 'no synthetic event retry loop');
    }
    clock.now = end; await flush();
  };
  return { plugin, local: plugin.local, app, vault, adapter, disk, index, folders, folderIndex, calls, removed, events, emit, clock, timers, layouts, put, putDisk, advance, reload, plugins, saved: () => saved };
}

function syncStub(h, run = async () => report()) {
  const make = h.plugin.makeEngine.bind(h.plugin);
  h.plugin.makeEngine = () => {
    const engine = make();
    engine.syncNow = async (opts) => { engine.running = true; try { return await run(opts, engine); } finally { engine.running = false; } };
    return engine;
  };
}
const allElements = (el) => [el, ...el.children.flatMap(allElements)];

function attachEngine(plugin, engine) {
  plugin.engine = engine;
  if (plugin._operation) plugin._operation.engine = engine;
  return engine;
}

function legacyMetadataClient(client) {
  const upload = { chunkBytes: 5242880, oneShotMaxBytes: 5242880 };
  client._capabilities = {};
  client._rawInfo = Promise.resolve({ capabilities: {}, upload });
  client._info = upload;
  client.listFolder = async () => ({ files: [], folders: [] });
  client.getFile = async (id) => ({ id, name: 'note.md', path: '/Vault/', size: 6, mtime: 2000, contentVersion: null });
  return client;
}

function localStore() {
  const values = new Map();
  return { values, getItem: (key) => values.has(key) ? values.get(key) : null, setItem: (key, value) => values.set(key, String(value)) };
}
function copiedLedger() {
  const owner = '11111111-1111-4111-a111-111111111111';
  return {
    version: 1, files: { 'note.md': { remoteId: 'r', localHash: 'baseline' } }, folders: { Notes: { path: 'Notes' } },
    conflicts: { 'conflict.md': { copyPath: 'copy.md', remoteId: 'conflict-r' } }, log: [],
    meta: { deviceId: owner, syncScope: 'copied-scope', uploadSessionsScope: 'copied-scope', uploadSessions: { inherited: { owner, sessionId: 'other-device-session', phase: 'uploading' } } },
  };
}

test('identical copied ledgers on independent local stores select different installation owners without erasing baselines', async (t) => {
  const copy = copiedLedger(), firstStore = localStore(), secondStore = localStore();
  copy.meta.syncScope = copy.meta.uploadSessionsScope = await sha256Hex(JSON.stringify(['https://example.invalid', 'account-A', '/Vault/']));
  const h = await harness(t, {}, { ledger: copy, localStorage: firstStore, getBasePath: () => 'C:\\Vaults\\Shared\\' });
  const first = h.plugin.ledger.data.meta.deviceId;
  const secondPlugin = await h.reload({ ledger: copy, localStorage: secondStore });
  const second = secondPlugin.ledger.data.meta.deviceId;
  assert.notEqual(first, copy.meta.deviceId); assert.notEqual(second, copy.meta.deviceId); assert.notEqual(first, second);
  const persisted = JSON.parse(h.saved()); assert.equal(persisted.meta.deviceId, second);
  for (const key of ['files', 'folders', 'conflicts']) assert.deepEqual(persisted[key], copy[key]);
  assert.deepEqual(persisted.meta.uploadSessions, copy.meta.uploadSessions);
  await secondPlugin._withOperation(async () => {
    const engine = secondPlugin.makeEngine(); await engine._bindScope();
    const journal = engine._upOpts().uploadJournal;
    assert.equal(journal.owner, second);
    await assert.rejects(journal.set('inherited', copy.meta.uploadSessions.inherited), /ownership mismatch/);
  });
  for (const key of ['files', 'folders', 'conflicts']) assert.deepEqual(JSON.parse(JSON.stringify(secondPlugin.ledger.data[key])), copy[key]);
  assert.deepEqual(secondPlugin.ledger.data.meta.uploadSessions, copy.meta.uploadSessions);
  assert.equal(networkCalls, 0); assert.equal(firstStore.values.size, 1); assert.equal(secondStore.values.size, 1);
});

test('same installation-local store keeps its owner stable across reloads and another copied ledger', async (t) => {
  const storage = localStore(), copy = copiedLedger();
  const h = await harness(t, {}, { ledger: copy, localStorage: storage }); const owner = h.plugin.ledger.data.meta.deviceId;
  const fresh = await h.reload(); assert.equal(fresh.ledger.data.meta.deviceId, owner);
  const replacedLedger = await h.reload({ ledger: copy, settings: { apiKey: 'account-B', baseUrl: 'https://other.invalid' } });
  assert.equal(replacedLedger.ledger.data.meta.deviceId, owner); assert.equal(storage.values.size, 1);
  const key = [...storage.values.keys()][0];
  assert.match(key, /^tdrive-sync:device-id:/); assert.ok(!key.includes('account-A') && !key.includes('account-B') && !key.includes('example.invalid') && !key.includes('other.invalid'));
});

test('real engine scope binding preserves content baselines and foreign upload records after installation owner changes', async (t) => {
  const copy = copiedLedger(); const h = await harness(t, {}, { ledger: copy }); const originalEngine = h.plugin.makeEngine();
  await originalEngine._bindScope();
  const scope = h.plugin.ledger.data.meta.syncScope;
  copy.meta.syncScope = copy.meta.uploadSessionsScope = scope;
  const fresh = await h.reload({ ledger: copy, localStorage: localStore() }); const engine = fresh.makeEngine(); await engine._bindScope();
  assert.notEqual(fresh.ledger.data.meta.deviceId, copy.meta.deviceId);
  for (const key of ['files', 'folders', 'conflicts']) assert.deepEqual(JSON.parse(JSON.stringify(fresh.ledger.data[key])), copy[key]);
  assert.deepEqual(fresh.ledger.data.meta.uploadSessions, copy.meta.uploadSessions);
  assert.equal(engine._upOpts().uploadJournal.owner, fresh.ledger.data.meta.deviceId); assert.equal(networkCalls, 0);
});

test('installation key prefers base path, separates same-name vaults, and falls back to basePath when its accessor throws', async (t) => {
  const storage = localStore(), copy = copiedLedger();
  const h = await harness(t, {}, { ledger: copy, localStorage: storage, getBasePath: () => { throw new Error('unavailable'); }, basePath: '/phone/vault-one/' });
  const first = h.plugin.ledger.data.meta.deviceId;
  const second = await h.reload({ ledger: copy, basePath: '/phone/vault-two/' }); assert.notEqual(second.ledger.data.meta.deviceId, first);
  const again = await h.reload({ ledger: copy, getBasePath: () => '/phone/vault-one', basePath: '/ignored' }); assert.equal(again.ledger.data.meta.deviceId, first);
  assert.equal(storage.values.size, 2);
});

test('unavailable or throwing local storage retains the copied ledger owner as the explicit fallback', async (t) => {
  const copy = copiedLedger(); const h = await harness(t, {}, { ledger: copy });
  assert.equal(h.plugin.ledger.data.meta.deviceId, copy.meta.deviceId);
  for (const storage of [
    { getItem() { throw new Error('SecurityError'); }, setItem() {} },
    { getItem: () => null, setItem() { throw new Error('QuotaExceededError'); } },
    { getItem: () => null, setItem() {} },
  ]) {
    const fresh = await h.reload({ ledger: copy, localStorage: storage }); assert.equal(fresh.ledger.data.meta.deviceId, copy.meta.deviceId);
    assert.deepEqual(JSON.parse(h.saved()).files, copy.files);
  }
  Object.defineProperty(global.window, 'localStorage', { configurable: true, get() { throw new Error('SecurityError'); } });
  const fresh = await h.reload({ ledger: copy }); assert.equal(fresh.ledger.data.meta.deviceId, copy.meta.deviceId);
});

test('fallback identity without local storage is generated once and persisted for the next load', async (t) => {
  const h = await harness(t); const owner = h.plugin.ledger.data.meta.deviceId;
  assert.match(owner, /^[0-9a-f-]{36}$/i); assert.equal(JSON.parse(h.saved()).meta.deviceId, owner);
  const fresh = await h.reload(); assert.equal(fresh.ledger.data.meta.deviceId, owner);
});

test('identity ledger-save failure stops startup before commands, timers, engine or network activate', async (t) => {
  const copy = copiedLedger(), storage = localStore(); const h = await harness(t, {}, { ledger: copy });
  h.adapter.write = async () => { throw new Error('EIO'); };
  await assert.rejects(h.reload({ ledger: copy, localStorage: storage, settings: { syncOnStartup: true, autoSyncMinutes: 1 } }), /EIO/);
  const failed = h.plugins.at(-1); assert.equal(failed.engine, null); assert.equal(failed.commands.length, 0);
  assert.equal(h.timers.size, 0); assert.equal(networkCalls, 0);
  assert.equal(JSON.parse(h.saved()).meta.deviceId, copy.meta.deviceId); assert.equal(storage.values.size, 1);
});

test('indexed replacements use one supported modify, update stats, and ignore rename refusal', async (t) => {
  const h = await harness(t); h.put('Notes/note.md', 'old');
  const result = await h.local.writeBinaryAtomic('Notes/note.md', bytes('new much longer'), 5000);
  assert.deepEqual(result, { size: 15, mtime: 5000 });
  assert.deepEqual(await h.local.list(), [{ path: 'Notes/note.md', ...result }]);
  assert.equal(h.disk.get('Notes/note.md').data.toString(), 'new much longer');
  assert.equal(h.calls.filter(([kind]) => kind === 'modify').length, 1);
  assert.equal(h.calls.filter(([kind, path]) => kind === 'rename' || (kind === 'raw-write' && path === 'Notes/note.md')).length, 0);
  assert.ok(h.removed.every((p) => p.includes('.tdrive-backup-')));
});

test('new downloads and parents are registered by supported create APIs', async (t) => {
  const h = await harness(t); await h.local.writeBinaryAtomic('New/note.md', bytes('remote'), 6000);
  assert.ok(h.folderIndex.has('New')); assert.ok(h.index.has('New/note.md'));
  assert.deepEqual(await h.local.list(), [{ path: 'New/note.md', size: 6, mtime: 6000 }]);
  assert.equal(h.calls.filter(([kind]) => kind === 'create').length, 1);
  assert.equal(h.calls.filter(([kind]) => kind === 'raw-write' || kind === 'rename').length, 0);
});

test('failed supported replacement retains recoverable old bytes and never removes the original', async (t) => {
  const h = await harness(t); h.put('note.md', 'original');
  h.vault.process = async () => { h.putDisk('note.md', 'partial'); throw new Error('ENOSPC'); };
  let failure; try { await h.local.writeBinaryAtomic('note.md', bytes('remote')); } catch (e) { failure = e; }
  assert.match(failure.message, /ENOSPC.*recovery backup/); assert.ok(failure.backupPath);
  assert.equal(h.disk.get(failure.backupPath).data.toString(), 'original');
  assert.ok(h.disk.has('note.md')); assert.ok(!h.removed.includes('note.md'));
});

test('backup failure cannot commit, and concurrent edits during backup remain untouched', async (t) => {
  const h = await harness(t); h.put('note.md', 'original'); const raw = h.adapter.writeBinary;
  h.adapter.writeBinary = async (p, b) => { await raw(p, b); h.put('note.md', 'user edit'); };
  await assert.rejects(h.local.writeBinary('note.md', bytes('remote')), /changed while preparing/);
  assert.equal(h.disk.get('note.md').data.toString(), 'user edit');
  assert.equal(h.calls.filter(([kind]) => kind === 'modify').length, 0);
  assert.equal([...h.disk.values()].filter((v) => v.data.toString() === 'original').length, 1);
});

test('partial recovery-backup failure leaves the original untouched and never reaches modifyBinary', async (t) => {
  const h = await harness(t); h.put('note.md', 'original');
  h.adapter.writeBinary = async (p) => { h.putDisk(p, 'partial backup'); throw new Error('ENOSPC'); };
  await assert.rejects(h.local.writeBinary('note.md', bytes('remote')), (e) => {
    assert.match(e.message, /ENOSPC/); assert.ok(e.backupPath); return true;
  });
  assert.equal(h.disk.get('note.md').data.toString(), 'original');
  assert.equal(h.calls.filter(([kind]) => kind === 'modify').length, 0);
});

test('a vault API that leaves stale index stats is an explicit failure with a recovery backup', async (t) => {
  const h = await harness(t); h.put('note.md', 'old');
  h.vault.process = async (f, transform) => h.putDisk(f.path, bytes(transform(new TextDecoder().decode(await h.adapter.readBinary(f.path)))));
  await assert.rejects(h.local.writeBinary('note.md', bytes('long remote')), (e) => {
    assert.match(e.message, /matching vault index/); assert.equal(h.disk.get(e.backupPath).data.toString(), 'old'); return true;
  });
});

test('disk-only files and disk-only parents fail without raw overwrite or false success', async (t) => {
  const h = await harness(t); h.putDisk('note.md', 'unindexed');
  await assert.rejects(h.local.writeBinary('note.md', bytes('remote')), /not registered/);
  assert.equal(h.disk.get('note.md').data.toString(), 'unindexed');
  h.folders.add('DiskOnly');
  await assert.rejects(h.local.writeBinary('DiskOnly/new.md', bytes('remote')), /not registered/);
  assert.ok(!h.disk.has('DiskOnly/new.md')); assert.equal(h.calls.length, 0);
});

test('stream final commit uses indexed createBinary without requiring rename', async (t) => {
  const h = await harness(t); delete h.adapter.rename;
  assert.equal(h.local.canStream, true);
  const sink = await h.local.beginStreamWrite('large.bin'); await sink.write(bytes('one')); await sink.write(bytes('two'));
  assert.equal(h.index.has('large.bin'), false); assert.equal(h.disk.has('large.bin'), false);
  assert.equal(await sink.close(), 'large.bin'); assert.ok(h.index.has('large.bin'));
  assert.equal(h.disk.get('large.bin').data.toString(), 'onetwo');
  assert.deepEqual(await h.local.list(), [{ path: 'large.bin', size: 6, mtime: h.index.get('large.bin').stat.mtime }]);
  assert.deepEqual([...h.disk.keys()], ['large.bin']);
});

test('stream close preserves destinations appearing before or inside supported creation', async (t) => {
  const h = await harness(t); const sink = await h.local.beginStreamWrite('large.bin'); await sink.write(bytes('remote'));
  const create = h.vault.createBinary;
  h.vault.createBinary = async (p, b) => { h.put(p, 'new local'); return create(p, b); };
  await assert.rejects(sink.close(), /EEXIST/);
  assert.equal(h.disk.get('large.bin').data.toString(), 'new local'); assert.ok(!h.removed.includes('large.bin'));
  assert.deepEqual([...h.disk.keys()], ['large.bin']);
});

test('synthetic supported create/modify events do not schedule another sync', async (t) => {
  const h = await harness(t, { syncOnChange: true });
  await h.local.writeBinary('note.md', bytes('remote')); await flush(); await flush();
  await h.local.writeBinary('note.md', bytes('updated')); await flush(); await flush();
  assert.equal(h.plugin._pendingChange, false); assert.equal(h.timers.size, 0);
  h.emit('modify', h.index.get('note.md')); await flush(); await flush();
  assert.equal(h.plugin._pendingChange, false); assert.equal(h.timers.size, 0);
});

test('external edit to a recently sync-written path is retained, not mistaken for a synthetic event', async (t) => {
  const h = await harness(t, { syncOnChange: true }); await h.local.writeBinary('note.md', bytes('remote')); await flush();
  h.plugin._quietUntil = h.clock.now + 3000; h.put('note.md', 'user edit'); await h.emit('modify', h.index.get('note.md'));
  assert.equal(h.plugin._pendingChange, true); assert.equal(h.timers.size, 1);
  let runs = 0; syncStub(h, async () => { runs++; return report(); });
  await h.advance(2999); assert.equal(runs, 0); await h.advance(1); assert.equal(runs, 1);
});

test('external changes during an operation get exactly one trailing sync; no-event runs do not loop', async (t) => {
  const h = await harness(t, { syncOnChange: true }); const gate = deferred(); let runs = 0;
  syncStub(h, async () => { runs++; return report(); });
  const operation = h.plugin._withOperation(() => gate.promise);
  h.emit('modify', { path: 'user.md' }); h.emit('modify', { path: 'another.md' });
  assert.equal(h.plugin._pendingChange, true); assert.equal(h.timers.size, 0);
  gate.resolve(); await operation; await h.advance(2000); assert.equal(runs, 1);
  await h.advance(10000); assert.equal(runs, 1); assert.equal(h.timers.size, 0);
});

test('queued change callbacks revalidate enablement, schedule, pause, key, and unload', async (t) => {
  const h = await harness(t, { syncOnChange: true }); let runs = 0; syncStub(h, async () => { runs++; return report(); });
  h.emit('modify', { path: 'user.md' }); h.plugin.settings.syncOnChange = false; await h.advance(2000); assert.equal(runs, 0);
  h.plugin.settings.syncOnChange = true; let allowed = false; h.plugin.withinSchedule = () => allowed;
  h.emit('modify', { path: 'user.md' }); await h.advance(60000); assert.equal(runs, 0); assert.equal(h.plugin._pendingChange, true);
  allowed = true; h.plugin.settings.paused = true; await h.plugin.saveSettings(); assert.equal(h.timers.size, 0);
  h.plugin.settings.paused = false; h.plugin.settings.apiKey = ''; await h.plugin.saveSettings(); assert.equal(h.timers.size, 0);
  h.plugin.settings.apiKey = 'account-A'; await h.plugin.saveSettings(); h.plugin.onunload(); await h.advance(60000); assert.equal(runs, 0);
});

test('startup callbacks honor the current schedule and disabled startup setting', async (t) => {
  const h = await harness(t, { syncOnStartup: true }); let runs = 0; syncStub(h, async () => { runs++; return report(); });
  h.plugin.withinSchedule = () => false; h.layouts[0](); await h.advance(5000); assert.equal(runs, 0);
  h.plugin.withinSchedule = () => true; h.layouts[0](); h.plugin.settings.syncOnStartup = false; await h.advance(5000); assert.equal(runs, 0);
});

test('ledger saves serialize immutable snapshots instead of allowing older writes to complete last', async () => {
  const gates = [], writes = []; let persisted;
  const ledger = new ManifestStore({ write: (s) => { writes.push(s); const gate = deferred(); gates.push(gate); return gate.promise.then(() => { persisted = s; }); } });
  ledger.setFile('a.md', { remoteId: 'a' }); const first = ledger.save();
  ledger.setFile('b.md', { remoteId: 'b' }); const second = ledger.save();
  await flush(); assert.equal(writes.length, 1); assert.deepEqual(Object.keys(JSON.parse(writes[0]).files), ['a.md']);
  gates[0].resolve(); await first; await flush(); assert.equal(writes.length, 2);
  gates[1].resolve(); await second; assert.deepEqual(Object.keys(JSON.parse(persisted).files), ['a.md', 'b.md']);
});

test('failed ledger saves reject their caller without poisoning the next queued save', async () => {
  let calls = 0, persisted;
  const ledger = new ManifestStore({ write: async (s) => { if (++calls === 1) throw new Error('EIO'); persisted = s; } });
  const first = ledger.save(); ledger.setFile('b.md', { remoteId: 'b' }); const second = ledger.save();
  await assert.rejects(first, /EIO/); await second; assert.equal(JSON.parse(persisted).files['b.md'].remoteId, 'b');
});

test('read EIO retains in-memory state and blocks saves until a successful reload', async () => {
  let failed = true, writes = 0;
  const saved = JSON.stringify({ files: { 'saved.md': { remoteId: 'saved' } }, conflicts: { 'saved.md': { copyPath: 'copy.md' } } });
  const ledger = new ManifestStore({ read: async () => { if (failed) throw new Error('EIO'); return saved; }, write: async () => { writes++; } });
  ledger.setFile('in-memory.md', { remoteId: 'memory' });
  await assert.rejects(ledger.load(), /Could not read.*EIO/); assert.equal(ledger.getFile('in-memory.md').remoteId, 'memory');
  await assert.rejects(ledger.save(), /existing state must be recovered/); assert.equal(writes, 0);
  failed = false; await ledger.load(); assert.equal(ledger.getFile('saved.md').remoteId, 'saved');
  assert.ok(ledger.getConflict('saved.md')); await ledger.save(); assert.equal(writes, 1);
});

test('in-flight ledger reads block competing saves before a delayed EIO can overwrite valid state', async () => {
  const reading = deferred(); let writes = 0;
  const ledger = new ManifestStore({ read: async () => { await reading.promise; throw new Error('EIO'); }, write: async () => { writes++; } });
  const loading = ledger.load(); await assert.rejects(ledger.save(), /being read/);
  reading.resolve(); await assert.rejects(loading, /EIO/); assert.equal(writes, 0);
});

async function conflict(h) {
  h.put('note.md', 'local'); const engine = h.plugin.makeEngine();
  legacyMetadataClient(engine.client);
  await engine._bindScope();
  const state = { remote: { id: 'r', name: 'note.md', size: 6, mtime: 2000, version: 1 }, data: bytes('remote'), pushes: 0, copyPushes: 0, pulls: 0 };
  engine.prepare = async () => {}; engine.refreshSigs = async () => {};
  engine.client.getFile = async () => ({ ...state.remote });
  engine.downloadFn = async () => state.data;
  engine.pushLocal = async (path, previous, _onProgress, source, options) => {
    const data = await h.local.readBinary(source); const hash = await sha256Hex(data);
    assert.equal(hash, options.expectedLocalHash);
    const id = previous ? previous.id : 'copy-' + path;
    if (previous) state.pushes++; else { assert.equal(options.createOnly, true); state.copyPushes++; }
    h.plugin.ledger.setFile(path, { remoteId: id, localHash: hash, localSize: data.byteLength });
    return { id };
  };
  engine.pullRemote = async () => { state.pulls++; return { written: true }; };
  await engine.conflicts.record('note.md', state.remote, state.data); await flush();
  h.plugin.makeEngine = () => attachEngine(h.plugin, engine);
  return { engine, state, record: h.plugin.ledger.getConflict('note.md') };
}

test('stale conflict modal cannot approve a replacement conflict record', async (t) => {
  const h = await harness(t); const c = await conflict(h); h.plugin.t = (key, params) => params ? key + JSON.stringify(params) : key;
  const view = h.plugin.viewFactory({}); view.render(); allElements(view.contentEl).find((el) => el.text === 'status.resolve').onclick();
  const modal = h.app.opened.at(-1); const keepLocal = allElements(modal.contentEl).find((el) => el.text === 'conflict.local');
  h.plugin.ledger.setConflict('note.md', { ...c.record, remoteId: 'other' });
  await keepLocal.onclick(); assert.equal(c.state.pushes, 0); assert.equal(h.plugin.ledger.getConflict('note.md').remoteId, 'other');
  assert.ok(notices.some((s) => /Conflict changed/.test(s)));
});

test('newer remote revision is preserved and requires renewed review before keep-both overwrites it', async (t) => {
  const h = await harness(t); const { engine, state, record: original } = await conflict(h);
  state.remote.version = 2; state.data = bytes('latest');
  await assert.rejects(h.plugin.resolveConflict('note.md', 'both'), /latest version was preserved/);
  const latest = h.plugin.ledger.getConflict('note.md');
  assert.equal(state.pushes, 0); assert.notEqual(latest.copyPath, original.copyPath);
  assert.equal(h.disk.get(original.copyPath).data.toString(), 'remote'); assert.equal(h.disk.get(latest.copyPath).data.toString(), 'latest');
  assert.equal(latest.remoteHash, await sha256Hex(state.data)); assert.equal(h.disk.get('note.md').data.toString(), 'local');
  await h.plugin.resolveConflict('note.md', 'both', { conflict: { ...latest }, scope: h.plugin.ledger.data.meta.syncScope });
  assert.equal(state.pushes, 1); assert.equal(state.copyPushes, 1); assert.equal(h.plugin.ledger.getConflict('note.md'), undefined); assert.equal(engine.running, false);
});

test('content hashes detect changed remote bytes even when metadata signatures are identical', async (t) => {
  const h = await harness(t); const { state } = await conflict(h); state.data = bytes('LATEST');
  await assert.rejects(h.plugin.resolveConflict('note.md', 'local'), /latest version was preserved/);
  assert.equal(state.pushes, 0); assert.equal(h.disk.get(h.plugin.ledger.getConflict('note.md').copyPath).data.toString(), 'LATEST');
});

test('remote version is checked again after awaited backup creation before replacement', async (t) => {
  const h = await harness(t); const { state } = await conflict(h); const write = h.local.writeBinary.bind(h.local);
  h.local.writeBinary = async (p, b, m) => { const result = await write(p, b, m); if (p.includes('local backup')) { state.remote.version = 2; state.data = bytes('latest'); } return result; };
  await assert.rejects(h.plugin.resolveConflict('note.md', 'remote'), /latest version was preserved/);
  assert.equal(state.pulls, 0); assert.equal(h.disk.get('note.md').data.toString(), 'local');
});

test('keep-remote commits exactly the reviewed bytes without a second unapproved download', async (t) => {
  const h = await harness(t); const { engine, state } = await conflict(h); let downloads = 0;
  engine.downloadFn = async () => { if (++downloads > 1) { state.remote.version = 2; return bytes('latest'); } return state.data; };
  engine.pullRemote = Object.getPrototypeOf(engine).pullRemote;
  await h.plugin.resolveConflict('note.md', 'remote');
  assert.equal(downloads, 1); assert.equal(h.disk.get('note.md').data.toString(), 'remote');
});

test('reviewed-byte commit binds its initial stat snapshot to the reviewed path', async (t) => {
  const h = await harness(t); const { engine } = await conflict(h); h.put('other.md', 'unrelated');
  const stat = h.local.stat.bind(h.local), commit = engine._writeDownloaded.bind(engine); let snapshot;
  h.local.stat = async (p) => ({ ...(await stat(p)), path: 'other.md' });
  engine._writeDownloaded = async (...args) => { snapshot = args[3]; return commit(...args); };
  await h.plugin.resolveConflict('note.md', 'remote');
  assert.equal(snapshot.path, 'note.md'); assert.equal(snapshot.exists, true);
  assert.equal(h.disk.get('note.md').data.toString(), 'remote'); assert.equal(h.disk.get('other.md').data.toString(), 'unrelated');
});

test('a local file created during the initial verification cannot be overwritten, even with identical remote bytes', async (t) => {
  const h = await harness(t); const { engine, state, record: original } = await conflict(h);
  h.disk.delete('note.md'); h.index.delete('note.md'); let downloads = 0;
  engine.downloadFn = async () => { downloads++; h.put('note.md', state.data); return state.data; };
  await assert.rejects(h.plugin.resolveConflict('note.md', 'remote'), /appeared during conflict resolution/);
  assert.equal(downloads, 1); assert.equal(h.disk.get('note.md').data.toString(), 'remote');
  assert.equal(h.plugin.ledger.getConflict('note.md'), original);
  assert.equal(h.calls.filter(([kind, path]) => kind === 'modify' && path === 'note.md').length, 0);
});

test('an initially missing local file can receive exactly the verified remote bytes without a phantom backup', async (t) => {
  const h = await harness(t); const { engine, state } = await conflict(h); h.disk.delete('note.md'); h.index.delete('note.md'); let downloads = 0;
  engine.downloadFn = async () => { downloads++; return state.data; };
  await h.plugin.resolveConflict('note.md', 'remote');
  assert.equal(downloads, 1); assert.equal(h.disk.get('note.md').data.toString(), 'remote'); assert.ok(h.index.has('note.md'));
  assert.equal([...h.disk.keys()].filter((p) => p.includes('local backup')).length, 0); assert.equal(h.plugin.ledger.getConflict('note.md'), undefined);
});

test('keep-remote retains edits made during review download and leaves the conflict open', async (t) => {
  const h = await harness(t); const { engine, state, record: original } = await conflict(h);
  engine.downloadFn = async () => { h.put('note.md', 'user edit'); return state.data; };
  await assert.rejects(h.plugin.resolveConflict('note.md', 'remote'), /changed during conflict resolution/);
  assert.equal(h.disk.get('note.md').data.toString(), 'user edit'); assert.equal(h.plugin.ledger.getConflict('note.md'), original);
  assert.ok([...h.disk.values()].some((f) => f.data.toString() === 'local'));
});

test('an edit during the first metadata GET reaches the original local-hash guard and keeps review pending', async (t) => {
  const h = await harness(t); const { engine, state, record: original } = await conflict(h); const commit = engine._writeDownloaded.bind(engine);
  let gets = 0, downloads = 0, result;
  engine.client.getFile = async () => { if (++gets === 1) h.put('note.md', 'GET-time edit'); return { ...state.remote }; };
  engine.downloadFn = async () => { downloads++; return state.data; };
  engine._writeDownloaded = async (...args) => { result = await commit(...args); return result; };
  await assert.rejects(h.plugin.resolveConflict('note.md', 'remote'), /changed during conflict resolution/);
  assert.equal(downloads, 1); assert.equal(result.conflict, true); assert.equal(h.disk.get('note.md').data.toString(), 'GET-time edit');
  assert.equal(h.plugin.ledger.getConflict('note.md'), original);
});

test('reviewed-byte commit still uses the engine final local-content guard', async (t) => {
  const h = await harness(t); const { record: original } = await conflict(h); const read = h.local.readBinary.bind(h.local); let reads = 0;
  h.local.readBinary = async (p) => { if (p === 'note.md' && ++reads === 2) h.put(p, 'last edit'); return read(p); };
  await assert.rejects(h.plugin.resolveConflict('note.md', 'remote'), /changed during conflict resolution/);
  assert.equal(h.disk.get('note.md').data.toString(), 'last edit'); assert.equal(h.plugin.ledger.getConflict('note.md'), original);
});

test('failed conflict persistence keeps the conflict and its remote copy recoverable', async (t) => {
  const h = await harness(t); const { state, record: original } = await conflict(h);
  const write = h.plugin.ledger.storage.write;
  h.plugin.ledger.storage.write = async (snapshot) => { if (!JSON.parse(snapshot).conflicts['note.md']) throw new Error('EIO'); return write(snapshot); };
  await assert.rejects(h.plugin.resolveConflict('note.md', 'local'), /EIO/);
  assert.equal(state.pushes, 1); assert.equal(h.plugin.ledger.getConflict('note.md'), original);
  assert.equal(h.disk.get(original.copyPath).data.toString(), 'remote');
});

test('share keeps the plugin guard through lookup-to-mutation microtasks and refuses changed settings', async (t) => {
  const h = await harness(t); let builds = 0, posts = 0, busyResult;
  const make = h.plugin.makeEngine.bind(h.plugin);
  h.plugin.makeEngine = () => {
    builds++; const engine = make(); legacyMetadataClient(engine.client);
    engine._bindScope = async () => {
      h.plugin.ledger.setFile('note.md', { remoteId: 'account-A-file' });
      queueMicrotask(() => queueMicrotask(() => { h.plugin.settings.apiKey = 'account-B'; busyResult = h.plugin.runSync({ silent: true }); }));
    };
    engine.client.request = async (req) => { assert.equal(req.method, 'POST'); posts++; return { url: 'https://example.invalid/share' }; };
    return engine;
  };
  await h.plugin.shareCurrent(); assert.equal(await busyResult, null); assert.equal(builds, 1); assert.equal(posts, 0);
  assert.equal(h.plugin._operation, null); assert.equal(h.plugin.engine.running, false);
  assert.ok(notices.some((s) => /settings changed/.test(s)));
});

test('share holds the exact immutable client and running guard until its request finishes', async (t) => {
  const h = await harness(t); const request = deferred(), started = deferred(); let used;
  const make = h.plugin.makeEngine.bind(h.plugin);
  h.plugin.makeEngine = () => {
    const engine = make(); legacyMetadataClient(engine.client); engine._bindScope = async () => h.plugin.ledger.setFile('note.md', { remoteId: 'account-A-file' });
    engine.client.request = (req) => { assert.equal(req.method, 'POST'); assert.equal(req.path, '/files/account-A-file/shares'); used = engine.client; started.resolve(); return request.promise; }; return engine;
  };
  const sharing = h.plugin.shareCurrent(); await started.promise;
  const active = h.plugin.engine; assert.equal(active.running, true); assert.ok(h.plugin._operation);
  assert.equal(await h.plugin.runSync({ silent: true }), null);
  await assert.rejects(h.plugin.remoteIdFor('note.md'), /already running/i);
  h.plugin.settings.apiKey = 'account-B'; assert.equal(used.apiKey, 'account-A');
  request.resolve({ url: 'https://example.invalid/share' }); await sharing;
  assert.equal(active.running, false); assert.equal(h.plugin._operation, null);
});

test('version restore holds a guard through rebinding and aborts a changed-settings microtask', async (t) => {
  const h = await harness(t); let posts = 0, restoring = false, busyResult;
  const make = h.plugin.makeEngine.bind(h.plugin);
  h.plugin.makeEngine = () => {
    const engine = make(); legacyMetadataClient(engine.client); engine._bindScope = async () => {
      h.plugin.ledger.data.meta.syncScope = 'scope-A'; h.plugin.ledger.setFile('note.md', { remoteId: 'account-A-file' });
      if (restoring) queueMicrotask(() => queueMicrotask(() => { h.plugin.settings.apiKey = 'account-B'; busyResult = h.plugin.runSync({ silent: true }); }));
    };
    engine.client.request = async (req) => { if (req.method === 'POST') posts++; return { versions: [{ id: 'v1', size: 6 }] }; }; return engine;
  };
  await h.plugin.viewVersions(); const modal = h.app.opened.at(-1); await modal.opening;
  restoring = true; await allElements(modal.contentEl).find((el) => el.onclick).onclick();
  assert.equal(await busyResult, null); assert.equal(posts, 0); assert.equal(h.plugin.engine.running, false); assert.equal(h.plugin._operation, null);
});

test('conflict operations validate settings after awaited preparation before any remote mutation', async (t) => {
  const h = await harness(t); const { engine, state } = await conflict(h); const gate = deferred();
  engine.prepare = () => gate.promise;
  const resolving = h.plugin.resolveConflict('note.md', 'local');
  assert.equal(await h.plugin.runSync({ silent: true }), null); h.plugin.settings.remoteFolder = '/Different/'; gate.resolve();
  await assert.rejects(resolving, /settings changed/); assert.equal(state.pushes, 0); assert.ok(h.plugin.ledger.getConflict('note.md'));
  assert.equal(h.plugin._operation, null); assert.equal(engine.running, false);
});

test('sync operation guard remains held during post-run ledger persistence even after engine running clears', async (t) => {
  const h = await harness(t); const saving = deferred(), started = deferred();
  h.plugin.ledger.storage.write = () => { started.resolve(); return saving.promise; };
  syncStub(h, async (opts, engine) => { engine.client._uploadChunk = 123; return report(); });
  const syncing = h.plugin.runSync({ silent: true }); await started.promise;
  assert.equal(h.plugin.engine.running, false); assert.ok(h.plugin._operation);
  await assert.rejects(h.plugin.remoteIdFor('note.md'), /already running/i); assert.equal(await h.plugin.runSync({ silent: true }), null);
  saving.resolve(); await syncing; assert.equal(h.plugin._operation, null);
});

test('plugin supplies exact-engine operation validity after an awaited upload-journal save', async (t) => {
  const h = await harness(t); const saving = deferred(), started = deferred(); let posts = 0;
  syncStub(h, async (opts, engine) => {
    await engine._bindScope();
    h.plugin.ledger.storage.write = () => { started.resolve(); return saving.promise; };
    const journal = engine._upOpts().uploadJournal;
    await journal.set('target', { owner: journal.owner, phase: 'opening', body: { name: 'note.md' } });
    posts++; return report();
  });
  const syncing = h.plugin.runSync({ silent: true }); await started.promise;
  h.plugin.settings.apiKey = 'account-B'; saving.resolve();
  assert.equal(await syncing, null); assert.equal(posts, 0); assert.equal(h.plugin._operation, null);
  assert.ok(notices.some((s) => /settings changed/.test(s)));
});

for (const [label, invalidate] of [
  ['settings change', (h) => { h.plugin.settings.apiKey = 'account-B'; }],
  ['pause', (h) => h.plugin.pause()],
  ['unload', (h) => h.plugin.onunload()],
  ['settings change with a replaced mutable engine reference', (h, engine) => {
    h.plugin.settings.apiKey = 'account-B';
    h.plugin.engine = { client: engine.client, getSettings: () => ({ ...h.plugin.settings }), running: true, pause() {} };
  }],
]) {
  test(`real plugin/engine upload cannot POST after ${label} during its opening-phase save`, async (t) => {
    const h = await harness(t, { dedupeUploads: false }); const saving = deferred(), started = deferred();
    const checked = []; const check = h.plugin._checkOperation.bind(h.plugin); let captured, posts = 0;
    h.plugin._checkOperation = (engine) => { checked.push(engine); return check(engine); };
    syncStub(h, async (opts, engine) => {
      captured = engine; assert.equal(typeof engine.operationValidity, 'function');
      await engine._bindScope();
      engine.client.maxRetries = 0; engine.client.maxServerRetries = 0;
      engine.client.transport = async (req) => {
        if (req.method === 'POST') posts++;
        if (req.method === 'GET' && new URL(req.url).pathname === '/api/v1') return { status: 200, headers: {}, body: bytes(JSON.stringify({ ok: true, data: { upload: { chunkBytes: 4096, oneShotMaxBytes: 128 } } })) };
        return { status: 400, headers: {}, body: bytes(JSON.stringify({ ok: false, error: { code: 'UNEXPECTED_MUTATION', message: 'No upload mutation should reach transport' } })) };
      };
      const write = h.plugin.ledger.storage.write;
      h.plugin.ledger.storage.write = async (s) => {
        const phase = Object.values(JSON.parse(s).meta.uploadSessions || {}).find((entry) => entry.phase === 'opening');
        if (phase) { started.resolve(phase); await saving.promise; }
        await write(s);
      };
      await engine._replaceUpload({ name: 'new.bin', folderPath: '/Vault/', data: Buffer.alloc(8193), mtime: h.clock.now, old: null, chunker: false });
      return report();
    });
    const syncing = h.plugin.runSync({ silent: true });
    const phase = await Promise.race([started.promise, syncing.then(() => { throw new Error('Transfer ended before its real opening-phase save'); })]);
    assert.equal(phase.owner, h.plugin.ledger.data.meta.deviceId); assert.ok(h.plugin._operation);
    await invalidate(h, captured); saving.resolve();
    assert.equal(await syncing, null); assert.equal(posts, 0); assert.ok(checked.length >= 3);
    assert.ok(checked.every((engine) => engine === captured)); assert.equal(captured.running, false); assert.equal(h.plugin._operation, null);
    assert.equal(Object.values(JSON.parse(h.saved()).meta.uploadSessions)[0].phase, 'opening');
  });
}

test('status view prominently exposes scoped blocked-folder reason and request ID', async (t) => {
  const h = await harness(t);
  Object.assign(h.plugin.ledger.data.meta, { syncScope: 'current', blockedFolders: { Folder: { path: 'Folder', scope: 'current', code: 'UPLOAD_IN_PROGRESS', requestId: 'request-one', nextRetryAt: 2000000 } } });
  const view = h.plugin.viewFactory({}); view.render(); const text = allElements(view.contentEl).map((el) => el.text || '').join('\n');
  assert.match(text, /Folder Deletions Blocked by Server/); assert.match(text, /UPLOAD_IN_PROGRESS/); assert.match(text, /request-one/);
  assert.match(text, /committed sessions will not be aborted/);
});

test('scoped Recheck API action alone requests folder retries and stays disabled while busy', async (t) => {
  const h = await harness(t); const pending = deferred(), calls = [];
  Object.assign(h.plugin.ledger.data.meta, { syncScope: 'current', blockedFolders: { Folder: { path: 'Folder', scope: 'current', code: 'UPLOAD_IN_PROGRESS', nextRetryAt: 2000000 } } });
  h.plugin.runSync = (opts) => {
    calls.push(opts);
    if (opts && opts.retryBlockedFolders) { h.plugin._operation = {}; return pending.promise.finally(() => { h.plugin._operation = null; }); }
    return Promise.resolve(report());
  };
  const view = h.plugin.viewFactory({}); view.render();
  const button = allElements(view.contentEl).find((el) => el.text === 'Recheck API'); assert.ok(button); assert.equal(button.disabled, false);
  const rechecking = button.onclick(); assert.equal(button.disabled, true); assert.deepEqual(calls, [{ retryBlockedFolders: true }]);
  await button.onclick(); assert.equal(calls.length, 1);
  view.render(); assert.equal(allElements(view.contentEl).find((el) => el.text === 'Recheck API').disabled, true);
  pending.resolve(report()); await rechecking; assert.equal(button.disabled, false);
  h.plugin.engine = { running: true, pause() {} }; view.render();
  const busy = allElements(view.contentEl).find((el) => el.text === 'Recheck API'); assert.equal(busy.disabled, true); await busy.onclick(); assert.equal(calls.length, 1);
  h.plugin.engine.running = false; view.render();
  await allElements(view.contentEl).find((el) => el.text === h.plugin.t('status.syncNow')).onclick(); assert.equal(calls[1], undefined);
  h.plugin.settings.autoSyncMinutes = 1; h.plugin.setupAutoSync(); await h.advance(60000);
  assert.deepEqual(calls[2], { silent: true, reason: 'interval' });
  h.plugin.lang = 'ar'; view.render(); assert.ok(allElements(view.contentEl).some((el) => el.text === 'إعادة فحص API'));
  h.plugin.ledger.data.meta.syncScope = 'different'; view.render(); assert.ok(!allElements(view.contentEl).some((el) => el.text === 'إعادة فحص API' || el.text === 'Recheck API'));
});
