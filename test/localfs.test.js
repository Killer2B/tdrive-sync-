'use strict';
// The Obsidian-facing file adapter on a "strict" vault (like mobile): vault.createBinary only works when the parent
// folder was registered through vault.createFolder, adapter.mkdir/createFolder throw on "already exists",
// and files written straight to disk are not listed by vault.getFiles().
const test = require('node:test');
const assert = require('node:assert/strict');
const Module = require('module');
const { makeEnv, MemFS } = require('./helpers');
const { splitRel } = require('../src/util');

class TFile { constructor(path, data) { this.path = path; this.data = data; this.stat = { size: data.length, mtime: 1000 }; } }

function loadPlugin() {
  const proxy = () => new Proxy(function () {}, { get: (t, k) => (k === 'then' ? undefined : (k in t ? t[k] : proxy())), apply: () => proxy(), set: (t, k, v) => { t[k] = v; return true; } });
  const stub = {
    Plugin: class { constructor(app, manifest) { this.app = app; this.manifest = manifest; this.data = {}; }
      async loadData() { return this.data; } async saveData(d) { this.data = d; }
      addCommand() {} addSettingTab() {} registerView() {} addRibbonIcon() {} addStatusBarItem() { return { setText() {} }; } registerInterval() {} registerEvent() {} },
    PluginSettingTab: class {}, Setting: class {}, Notice: class {}, Modal: class {}, ItemView: class {}, TFile, requestUrl: async () => ({}),
  };
  const orig = Module._load;
  Module._load = function (req, ...a) { return req === 'obsidian' ? stub : orig.call(this, req, ...a); };
  global.window = globalThis;
  try { delete require.cache[require.resolve('../main.js')]; return require('../main.js'); } finally { Module._load = orig; }
}

function strictApp({ indexLag = false, hideFolderStat = false } = {}) {
  const disk = new Map(); const diskFolders = new Set(); const indexedFolders = new Set(); const indexed = new Map();
  const parent = (p) => p.split('/').slice(0, -1).join('/');
  const adapter = {
    exists: async (p) => disk.has(p) || diskFolders.has(p),
    stat: async (p) => (disk.has(p) ? { type: 'file', size: disk.get(p).length, mtime: 2000 } : (diskFolders.has(p) && !hideFolderStat ? { type: 'folder' } : null)),
    mkdir: async (p) => { if (diskFolders.has(p)) throw new Error('Folder already exists.'); diskFolders.add(p); },
    writeBinary: async (p, b) => { disk.set(p, Buffer.from(b)); },
    readBinary: async (p) => { const d = disk.get(p); return d.buffer.slice(d.byteOffset, d.byteOffset + d.length); },
    read: async () => null, write: async () => {},
  };
  const vault = {
    adapter, getName: () => 'V',
    createFolder: async (p) => { if (diskFolders.has(p)) throw new Error('Folder already exists.'); diskFolders.add(p); indexedFolders.add(p); },
    createBinary: async (p, b) => {
      const par = parent(p);
      if (par && !indexedFolders.has(par)) throw new Error('ENOENT: parent folder unknown to vault');
      disk.set(p, Buffer.from(b)); if (!indexLag) indexed.set(p, new TFile(p, Buffer.from(b)));
    },
    getAbstractFileByPath: (p) => indexed.get(p) || null,
    getFiles: () => [...indexed.values()],
  };
  return { app: { vault, fileManager: {}, workspace: { onLayoutReady() {} } }, disk, diskFolders, indexed };
}

async function localFor(fa) {
  const Plugin = loadPlugin();
  const plugin = new Plugin(fa.app, { dir: '.obsidian/plugins/tdrive-sync' });
  await plugin.onload();
  return plugin.local;
}
const buf = (s) => new TextEncoder().encode(s).buffer;

test('writing into nested folders on a strict vault: creates them, tolerates "already exists", file appears', async () => {
  const fa = strictApp({ hideFolderStat: true });   // stat cannot tell us a folder exists
  fa.diskFolders.add('Notes');                       // a parent that already exists on disk
  const local = await localFor(fa);
  await local.writeBinary('Notes/Deep/ملاحظة.md', buf('نص'), 5000);
  assert.equal(fa.disk.get('Notes/Deep/ملاحظة.md').toString(), 'نص');
  assert.ok(fa.diskFolders.has('Notes/Deep'));
});

test('a real folder-creation failure names the folder (shown in the sync report)', async () => {
  const fa = strictApp();
  fa.disk.set('Notes', Buffer.from('a file, not a folder'));
  fa.app.vault.createFolder = async () => { throw new Error('boom'); };
  fa.app.vault.adapter.mkdir = async () => { throw new Error('boom2'); };
  const local = await localFor(fa);
  await assert.rejects(() => local.writeBinary('Notes/a.md', buf('x')), /(تعذر إنشاء المجلد المحلي "Notes"|Could not create the local folder "Notes").*(ملف بنفس الاسم|same name)/);
});

test('file written to disk but missing from the vault index is NOT treated as deleted locally', async () => {
  const env = await makeEnv();
  try {
    env.local.put('Notes/a.md', 'a'); env.local.put('b.md', 'b');
    const r1 = await env.engine.syncNow(); assert.equal(r1.uploaded, 2);
    // Obsidian's index lags: list() no longer shows Notes/a.md, but the file is on disk.
    const realList = env.local.list.bind(env.local);
    env.local.list = async () => (await realList()).filter((f) => f.path !== 'Notes/a.md');
    env.server.log.length = 0;
    const r2 = await env.engine.syncNow();
    assert.equal(r2.deletedRemote, 0, JSON.stringify(r2));
    assert.equal(env.server.liveFiles().length, 2, 'remote copy must survive');
    assert.equal(env.server.opsLog().length, 0, 'no write requests: ' + JSON.stringify(env.server.opsLog()));
  } finally { await env.stop(); }
});

test('a genuinely deleted local file is still propagated (index rescue does not block real deletions)', async () => {
  const env = await makeEnv();
  try {
    for (const n of ['a.md', 'b.md', 'c.md']) env.local.put(n, n);
    await env.engine.syncNow();
    env.local.files.delete('a.md');
    const r = await env.engine.syncNow();
    assert.equal(r.deletedRemote, 1, JSON.stringify(r));
  } finally { await env.stop(); }
});

test('ObsidianLocalFS: listFolders / trashFolder (folders = objects with children; root excluded; null when API missing)', async () => {
  const fa = strictApp();
  const folder = (path) => ({ path, children: [] });
  const trashed = [];
  fa.app.vault.getAllLoadedFiles = () => [folder('/'), folder('Notes'), folder('Notes/Deep'), { path: 'a.md', stat: {} }];
  fa.app.vault.getAbstractFileByPath = (p) => (p === 'Notes' ? folder('Notes') : null);
  fa.app.fileManager = { trashFile: async (f) => { trashed.push(f.path); } };
  const local = await localFor(fa);
  assert.deepEqual(await local.listFolders(), ['Notes', 'Notes/Deep']);
  await local.trashFolder('Notes'); await local.trashFolder('Missing');   // missing = no-op, never throws
  assert.deepEqual(trashed, ['Notes']);
  delete fa.app.vault.getAllLoadedFiles;
  assert.equal(await local.listFolders(), null);
});
