'use strict';
// The editor's rename/move event must be captured as a persisted move hint, so the engine can move the file
// in place (same server id) instead of re-creating it — the fix for "move the note, then keep editing it".
const test = require('node:test');
const assert = require('node:assert/strict');
const Module = require('module');

function loadPlugin() {
  const handlers = {};
  const proxy = () => new Proxy(function () {}, { get: (t, k) => (k === 'then' ? undefined : (k in t ? t[k] : proxy())), apply: () => proxy(), set: (t, k, v) => { t[k] = v; return true; } });
  const stub = {
    Plugin: class { constructor(app, manifest) { this.app = app; this.manifest = manifest; } async loadData() { return null; } async saveData() {} addCommand() {} addSettingTab() {} registerView() {} addRibbonIcon() {} addStatusBarItem() { return null; } registerInterval() {} registerEvent() {} register() {} setupAutoSync() {} refreshStatus() {} },
    PluginSettingTab: class { constructor() { this.containerEl = proxy(); } },
    Setting: class { constructor() { return proxy(); } },
    Notice: class { constructor() {} }, Modal: class {}, ItemView: class {}, TFile: class {},
    setIcon() {}, requestUrl: async () => ({ status: 200, headers: {}, arrayBuffer: new ArrayBuffer(0) }),
  };
  const orig = Module._load;
  Module._load = function (req, ...a) { return req === 'obsidian' ? stub : orig.call(this, req, ...a); };
  try { delete require.cache[require.resolve('../src/plugin.js')]; return require('../src/plugin.js'); } finally { Module._load = orig; }
}

test('rememberMove persists a bounded old->new hint', () => {
  const Plugin = loadPlugin();
  const p = new Plugin({}, { dir: '.' });
  let saved = 0;
  p.ledger = { data: { meta: {} }, save: async () => { saved++; } };

  p.rememberMove('Notes/a.md', 'Pc/a.md');
  assert.deepEqual(p.ledger.data.meta.moveHints['Notes/a.md'].to, 'Pc/a.md');
  assert.ok(saved >= 1, 'the hint was persisted');

  // Noise is ignored: same path, hidden files, missing args.
  p.rememberMove('x.md', 'x.md');
  p.rememberMove('.obsidian/x.md', 'y.md');
  p.rememberMove('', 'z.md');
  assert.equal(Object.keys(p.ledger.data.meta.moveHints).length, 1);

  // The map stays bounded.
  for (let i = 0; i < 700; i++) p.rememberMove('f' + i + '.md', 'g' + i + '.md');
  assert.ok(Object.keys(p.ledger.data.meta.moveHints).length <= 500, 'bounded to 500 hints');
});

test('a rename event is wired to rememberMove', () => {
  const Plugin = loadPlugin();
  const p = new Plugin({}, { dir: '.' });
  p.ledger = { data: { meta: {} }, save: async () => {} };
  p.settings = { syncOnChange: false, instantSync: false, paused: true };   // no auto-sync needed
  const registered = {};
  p.app = { vault: { on: (name, cb) => { registered[name] = cb; return {}; } } };
  p.registerEvent = () => {};
  p.withinSchedule = () => true;
  p.registerChangeSync();
  assert.equal(typeof registered.rename, 'function', 'a rename handler is registered');
  registered.rename({ path: 'Pc/note.md' }, 'Notes/note.md');
  assert.deepEqual(p.ledger.data.meta.moveHints['Notes/note.md'].to, 'Pc/note.md', 'the rename produced a hint');
});
