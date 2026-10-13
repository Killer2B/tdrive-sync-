'use strict';
// Glue test for the *built* main.js against a stubbed Obsidian API + the mock server.
// It proves the bundle loads, wiring works, and the Obsidian-facing file adapter does what the engine needs.
// It does NOT prove behaviour inside the real Obsidian app (see docs/TEST-REPORT.md).
const test = require('node:test');
const assert = require('node:assert/strict');
const Module = require('module');
const { createMockServer } = require('./mockServer');

const proxy = () => new Proxy(function () {}, { get: (t, k) => (k === 'then' ? undefined : (k in t ? t[k] : proxy())), apply: () => proxy(), set: (t, k, v) => { t[k] = v; return true; } });
class TFile { constructor(path, data) { this.path = path; this.data = data; this.stat = { size: data.length, mtime: Date.now() }; } }

function makeApp() {
  const files = new Map(); const trashed = [];
  const tf = (p) => { const f = files.get(p); return f && f.tfile; };
  const put = (p, buf) => { const data = Buffer.from(buf); const old = files.get(p); const t = old ? old.tfile : new TFile(p, data); t.data = data; t.stat = { size: data.length, mtime: (old ? old.tfile.stat.mtime : 1000) + 1 }; files.set(p, { tfile: t }); };
  const folders = new Set();
  const adapter = {
    exists: async (p) => files.has(p) || folders.has(p), mkdir: async (p) => { folders.add(p); },
    read: async (p) => files.get(p).tfile.data.toString(), write: async (p, s) => put(p, s),
    readBinary: async (p) => { const d = files.get(p).tfile.data; return d.buffer.slice(d.byteOffset, d.byteOffset + d.length); },
    writeBinary: async (p, b) => put(p, b), stat: async (p) => (files.has(p) ? { size: files.get(p).tfile.data.length, mtime: files.get(p).tfile.stat.mtime } : null),
  };
  const app = {
    vault: { adapter, getName: () => 'TestVault', getFiles: () => [...files.values()].map((f) => f.tfile).filter((f) => !f.path.startsWith('.')),
      getAbstractFileByPath: (p) => tf(p) || null, createBinary: async (p, b) => put(p, b), modifyBinary: async (f, b) => put(f.path, b) },
    fileManager: { trashFile: async (f) => { trashed.push(f.path); files.delete(f.path); } },
    workspace: { onLayoutReady: (cb) => cb(), getActiveFile: () => null },
  };
  return { app, files, put, trashed };
}

test('built main.js loads, registers commands, tests connection, syncs both ways', async () => {
  const server = await createMockServer().start();
  const notices = [];
  const stub = {
    Plugin: class { constructor(app, manifest) { this.app = app; this.manifest = manifest; this.commands = []; this.data = null; }
      async loadData() { return this.data; } async saveData(d) { this.data = d; }
      addCommand(c) { this.commands.push(c); } addSettingTab(t) { this.tab = t; } registerView() {} addRibbonIcon() {}
      addStatusBarItem() { return { setText() {} }; } registerInterval() {} registerEvent() {} },
    PluginSettingTab: class { constructor(app, plugin) { this.app = app; this.plugin = plugin; this.containerEl = proxy(); } },
    Setting: class { constructor() { return proxy(); } },
    Notice: class { constructor(m) { notices.push(m); } },
    Modal: class {}, ItemView: class { constructor(leaf) { this.contentEl = proxy(); } }, TFile,
    requestUrl: async ({ url, method, headers, body, contentType }) => {
      const h = { ...headers }; if (contentType) h['Content-Type'] = contentType;
      const r = await fetch(url, { method, headers: h, body });
      return { status: r.status, headers: Object.fromEntries(r.headers), arrayBuffer: await r.arrayBuffer() };
    },
  };
  const orig = Module._load;
  Module._load = function (req, ...a) { return req === 'obsidian' ? stub : orig.call(this, req, ...a); };
  global.window = globalThis;
  let Plugin;
  try { Plugin = require('../main.js'); } finally { Module._load = orig; }

  const fa = makeApp();
  const plugin = new Plugin(fa.app, { dir: '.obsidian/plugins/tdrive-sync' });
  plugin.data = { baseUrl: server.url, apiKey: server.key, remoteFolder: '/Vault/', remoteLock: false };
  try {
    await plugin.onload();

    const names = plugin.commands.map((c) => c.name);
    for (const n of ['Test Connection', 'Sync Now', 'Upload Current File', 'Download Remote Changes', 'View Conflicts', 'View Sync Log', 'Pause Sync', 'Resume Sync']) assert.ok(names.includes('Tdrive Sync: ' + n), n);
    plugin.tab.display();                                   // settings UI builds without throwing

    await plugin.testConnection();
    assert.match(plugin.connection, /^(متصل|✓)/);

    fa.put('ملاحظاتي/مشروع جديد/الدرس الأول.md', 'نص عربي'); fa.put('a.md', 'A');
    const rep = await plugin.runSync();
    assert.equal(rep.status, 'ok'); assert.equal(rep.uploaded, 2);
    assert.equal(server.findLive('/Vault/ملاحظاتي/مشروع جديد/', 'الدرس الأول.md').bytes.toString(), 'نص عربي');
    assert.ok(await fa.app.vault.adapter.exists('.obsidian/plugins/tdrive-sync/sync-state.json'));  // ledger persisted

    server.addFile('/Vault/', 'remote.md', 'R');
    const rep2 = await plugin.runSync({ mode: 'download-only' });
    assert.equal(rep2.downloaded, 1); assert.equal(fa.files.get('remote.md').tfile.data.toString(), 'R');

    // ---- notification styles: default is technical, the choice persists, and it changes the notice text ----
    const NOTIFY = require('../src/notify');
    assert.equal(plugin.settings.notifyStyle, 'technical', 'default notification style');
    notices.length = 0;
    plugin.settings.notifyStyle = 'elegant';
    await plugin.saveSettings();
    assert.equal(plugin.data.notifyStyle, 'elegant', 'style saved');
    fa.put('styled.md', 'S');
    await plugin.runSync();
    const styled = notices[notices.length - 1];
    // No setIcon in this stub -> the plain-text fallback uses the typographic glyph (never an emoji).
    assert.ok(styled.startsWith(NOTIFY.GLYPH.success), 'elegant notice starts with the success glyph: ' + JSON.stringify(styled));
    assert.ok(styled.split('\n').length >= 2, 'elegant notice is two lines');

    plugin.settings.notifyStyle = 'concise';
    notices.length = 0;
    fa.put('styled2.md', 'S2');
    await plugin.runSync();
    assert.ok(notices[notices.length - 1].startsWith(NOTIFY.GLYPH.success));
    assert.equal(notices[notices.length - 1].split('\n').length, 1, 'concise notice is one line');

    // technical output must stay identical to the original format
    plugin.settings.notifyStyle = 'technical';
    notices.length = 0;
    fa.put('styled3.md', 'S3');
    await plugin.runSync();
    assert.match(notices[notices.length - 1], /^Tdrive: /);
    assert.match(notices[notices.length - 1], /↑\d+ ↓\d+ ⇄\d+ =\d+ ⚠\d+ ✗\d+/);

    await plugin.pause(); assert.equal(await plugin.runSync(), null);   // paused -> nothing runs
    await plugin.resume();
    plugin.probeApi && await plugin.probeApi();
  } finally { await server.stop(); }
});
