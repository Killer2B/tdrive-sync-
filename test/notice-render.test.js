'use strict';
// End-to-end check of the notification icon path: the built main.js renders real Lucide icons into notices and
// the status bar via Obsidian's setIcon, picks the first available name in each fallback chain, and degrades to
// plain text when the DOM/icons are unavailable (never an emoji, never a crash).
const test = require('node:test');
const assert = require('node:assert/strict');
const Module = require('module');
const { createMockServer } = require('./mockServer');

// ---- a tiny DOM stub with just what the plugin uses ----
function makeEl(tag = 'div') {
  const el = {
    tag, children: [], classes: [], text: '', icons: [],
    empty() { el.children = []; el.text = ''; return el; },
    addClass(c) { el.classes.push(c); },
    createDiv(o = {}) { const c = makeEl('div'); if (o.cls) c.addClass(o.cls); el.children.push(c); return c; },
    createSpan(o = {}) { const c = makeEl('span'); if (o.cls) c.addClass(o.cls); if (o.text != null) c.text = o.text; el.children.push(c); return c; },
    setText(t) { el.text = t; },
    get childElementCount() { return el.children.length; },
    querySelector(sel) { return sel === 'svg' && el.icons.length ? {} : null; },
  };
  return el;
}
const walk = (el, out = []) => { out.push(el); for (const c of el.children || []) walk(c, out); return out; };
const iconsIn = (el) => walk(el).flatMap((e) => e.icons || []);
const textsIn = (el) => walk(el).map((e) => e.text).filter(Boolean).join(' | ');

function loadPlugin({ knownIcons = null } = {}) {
  const noticeLog = []; const applied = [];
  const proxy = () => new Proxy(function () {}, { get: (t, k) => (k === 'then' ? undefined : (k in t ? t[k] : proxy())), apply: () => proxy(), set: (t, k, v) => { t[k] = v; return true; } });
  class TFile { constructor(path, data) { this.path = path; this.data = data; this.stat = { size: data.length, mtime: Date.now() }; } }
  const stub = {
    Plugin: class { constructor(app, manifest) { this.app = app; this.manifest = manifest; this.commands = []; this.data = null; }
      async loadData() { return this.data; } async saveData(d) { this.data = d; }
      addCommand(c) { this.commands.push(c); } addSettingTab(t) { this.tab = t; } registerView() {} addRibbonIcon() {}
      addStatusBarItem() { return makeEl('div'); } registerInterval() {} registerEvent() {} },
    PluginSettingTab: class { constructor(app, plugin) { this.app = app; this.plugin = plugin; this.containerEl = proxy(); } },
    Setting: class { constructor() { return proxy(); } },
    Notice: class { constructor(message, timeout) { this.message = message; this.timeout = timeout; this.noticeEl = makeEl('div'); noticeLog.push(this); } },
    Modal: class {}, ItemView: class { constructor() { this.contentEl = proxy(); } }, TFile,
    // Simulates Obsidian's setIcon: only names in `known` are drawn; others draw nothing.
    setIcon(el, name) { applied.push(name); if (knownIcons && !knownIcons.has(name)) return el; el.icons.push(name); el.children.push(makeEl('svg')); return el; },
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
  try { delete require.cache[require.resolve('../main.js')]; Plugin = require('../main.js'); } finally { Module._load = orig; }
  return { Plugin, noticeLog, applied };
}

function makeApp() {
  const files = new Map(); const folders = new Set();
  const put = (p, buf) => { const d = Buffer.from(buf); const old = files.get(p); const t = old ? old.tfile : new (class { constructor(path, data) { this.path = path; this.data = data; this.stat = { size: data.length, mtime: Date.now() }; } })(p, d); t.data = d; t.stat = { size: d.length, mtime: (old ? old.tfile.stat.mtime : 1000) + 1 }; files.set(p, { tfile: t }); };
  const adapter = { exists: async (p) => files.has(p) || folders.has(p), mkdir: async (p) => { folders.add(p); }, read: async (p) => files.get(p).tfile.data.toString(), write: async (p, s) => put(p, s), readBinary: async (p) => { const d = files.get(p).tfile.data; return d.buffer.slice(d.byteOffset, d.byteOffset + d.length); }, writeBinary: async (p, b) => put(p, b), stat: async (p) => (files.has(p) ? { size: files.get(p).tfile.data.length, mtime: files.get(p).tfile.stat.mtime } : null), appendBinary: async (p, b) => { const prev = files.get(p); const add = Buffer.from(b); put(p, Buffer.concat([prev.tfile.data, add])); } };
  const app = { vault: { adapter, getName: () => 'TestVault', getFiles: () => [...files.values()].map((f) => f.tfile).filter((f) => !f.path.startsWith('.')), getAbstractFileByPath: (p) => (files.get(p) ? files.get(p).tfile : null), createBinary: async (p, b) => put(p, b), modifyBinary: async (f, b) => put(f.path, b), createFolder: async (p) => { folders.add(p); } }, fileManager: { trashFile: async (f) => { files.delete(f.path); } }, workspace: { onLayoutReady: (cb) => cb(), getActiveFile: () => null, getLeavesOfType: () => [], getRightLeaf: () => null } };
  return { app, files, put };
}

test('styled notices render real Lucide icons (cloud-check / cloud-upload) into the notice DOM', async () => {
  const server = await createMockServer().start();
  try {
    const { Plugin, noticeLog } = loadPlugin();
    const fa = makeApp();
    const plugin = new Plugin(fa.app, { dir: '.obsidian/plugins/tdrive-sync' });
    plugin.data = { baseUrl: server.url, apiKey: server.key, remoteFolder: '/Vault/', remoteLock: false, notifyStyle: 'concise' };
    await plugin.onload();

    fa.put('a.md', 'A'); fa.put('t/img.png', 'PNG');
    const rep = await plugin.runSync();
    assert.equal(rep.status, 'ok');

    const done = noticeLog[noticeLog.length - 1];
    const icons = iconsIn(done.noticeEl);
    assert.ok(icons.includes('cloud-check'), 'success icon is cloud-check, got ' + JSON.stringify(icons));
    assert.ok(icons.includes('cloud-upload'), 'upload icon is cloud-upload, got ' + JSON.stringify(icons));
    assert.ok(done.noticeEl.classes.includes('tdrive-notice'), 'notice element is styled');
    const txt = textsIn(done.noticeEl);
    assert.match(txt, /2/, 'upload count still rendered as text next to the icon');
  } finally { await server.stop(); }
});

test('when the running Obsidian lacks a primary icon name, the next fallback in the chain is used', async () => {
  const server = await createMockServer().start();
  try {
    // Pretend this Obsidian only knows the second entry of each chain (e.g. an older Lucide).
    const { Plugin, noticeLog } = loadPlugin({ knownIcons: new Set(['circle-check', 'upload', 'download']) });
    const fa = makeApp();
    const plugin = new Plugin(fa.app, { dir: '.obsidian/plugins/tdrive-sync' });
    plugin.data = { baseUrl: server.url, apiKey: server.key, remoteFolder: '/Vault/', remoteLock: false, notifyStyle: 'concise' };
    await plugin.onload();

    fa.put('a.md', 'A');
    await plugin.runSync();
    const icons = iconsIn(noticeLog[noticeLog.length - 1].noticeEl);
    assert.ok(icons.includes('circle-check'), 'fell back to circle-check, got ' + JSON.stringify(icons));
    assert.ok(icons.includes('upload'), 'fell back to upload, got ' + JSON.stringify(icons));
    assert.ok(!icons.includes('cloud-check'), 'the unknown primary must not be drawn');
  } finally { await server.stop(); }
});

test('the status bar shows a real progress icon during a styled run', async () => {
  const server = await createMockServer().start();
  try {
    const { Plugin } = loadPlugin();
    const fa = makeApp();
    const plugin = new Plugin(fa.app, { dir: '.obsidian/plugins/tdrive-sync' });
    plugin.data = { baseUrl: server.url, apiKey: server.key, remoteFolder: '/Vault/', remoteLock: false, notifyStyle: 'elegant' };
    await plugin.onload();

    plugin.engine = { running: true };
    plugin.progress = { phase: 'sync', action: 'upload', done: 6, total: 10, bytes: { done: 6 * 1024 * 1024, total: 10 * 1024 * 1024 } };
    plugin.refreshStatus();
    const icons = iconsIn(plugin.statusEl);
    assert.ok(icons.includes('loader'), 'progress icon is the loader, got ' + JSON.stringify(icons));
    assert.match(textsIn(plugin.statusEl), /60%/, 'percentage is still shown as text');

    // Technical style stays text-only (no icon), exactly as before.
    plugin.settings.notifyStyle = 'technical';
    plugin.refreshStatus();
    assert.deepEqual(iconsIn(plugin.statusEl), [], 'technical status bar has no icon');
    assert.match(textsIn(plugin.statusEl), /↑60%/);
  } finally { await server.stop(); }
});
