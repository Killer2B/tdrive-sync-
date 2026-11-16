'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const Module = require('node:module');
const { ManifestStore } = require('../src/ManifestStore');
const { sha256Hex } = require('../src/util');
class TFile { constructor(path, buffer) { this.path = path; this.stat = { size: buffer.byteLength, mtime: 1 }; } }

class Element {
  constructor(tag = 'div', opts = {}) { this.tag = tag; this.text = opts.text || ''; this.children = []; this.attrs = {}; this.disabled = false; }
  createEl(tag, opts = {}) { const el = new Element(tag, opts); this.children.push(el); return el; }
  createDiv(opts) { return this.createEl('div', opts); }
  createSpan(opts) { return this.createEl('span', opts); }
  addClass() {}
  empty() { this.children = []; }
  setText(value) { this.text = value; }
  setAttribute(key, value) { this.attrs[key] = String(value); }
  find(predicate) { if (predicate(this)) return this; for (const child of this.children) { const found = child.find(predicate); if (found) return found; } return null; }
}
const notices = []; const opened = [];
class Modal {
  constructor(app) { this.app = app; this.contentEl = new Element(); }
  open() { opened.push(this); this.onOpen(); return this; }
  close() { this.closed = true; this.onClose(); }
}
class Setting {
  constructor(el) { this.el = el; }
  setName() { return this; }
  setDesc(value) { this.el.createEl('p', { text: value }); return this; }
  addButton(callback) {
    const buttonEl = this.el.createEl('button');
    const component = { buttonEl, setButtonText(text) { buttonEl.text = text; return this; }, onClick(fn) { buttonEl.onclick = fn; return this; }, setDisabled(value) { buttonEl.disabled = value; return this; } };
    callback(component); return this;
  }
}
const filename = path.resolve(__dirname, '../src/plugin.js');
const instance = new Module(filename, module); instance.filename = filename; instance.paths = Module._nodeModulePaths(path.dirname(filename));
const nativeLoad = Module._load;
try {
  Module._load = function (id, parent, main) {
    if (id === 'obsidian') return { Plugin: class {}, PluginSettingTab: class {}, ItemView: class {}, Modal, Setting, Notice: class { constructor(message) { notices.push(message); } }, TFile, requestUrl: async () => { throw new Error('No real HTTP'); } };
    return nativeLoad.call(this, id, parent, main);
  };
  instance._compile(fs.readFileSync(filename, 'utf8') + '\nmodule.exports.testModal = ConflictModal; module.exports.testLocalFS = ObsidianLocalFS;', filename);
} finally { Module._load = nativeLoad; }
const Plugin = instance.exports; const ConflictModal = Plugin.testModal;

function fixture() {
  const plugin = new Plugin(); const ledger = new ManifestStore({ read: async () => null, write: async () => {} });
  ledger.data.meta.syncScope = 'synthetic-scope'; ledger.setFile('a.md', { baseText: 'base' });
  ledger.setConflict('a.md', { copyPath: 'a conflict.md', remoteId: 'f1', contentVersion: 2, remoteHash: 'synthetic-hash' });
  const files = new Map(['a.md', 'a conflict.md', 'draft.md'].map((p) => [p, { path: p }])); const openedFiles = [];
  plugin.ledger = ledger; plugin.settings = { apiKey: 'synthetic-key', paused: false }; plugin.listeners = new Set();
  plugin.app = { vault: { getAbstractFileByPath: (p) => files.get(p), read: async () => 'Synthetic text' }, workspace: { getLeaf: () => ({ openFile: async (file) => openedFiles.push(file.path) }), getActiveFile: () => files.get('a.md') } };
  plugin.t = (key, vars = {}) => key + (Object.keys(vars).length ? ':' + JSON.stringify(vars) : ''); plugin.redact = String;
  plugin._busy = () => false; plugin.notify = () => {}; plugin.resolveConflict = async () => ({ resolved: true });
  const modal = new ConflictModal(plugin.app, plugin, 'a.md'); modal.open();
  const button = (key) => modal.contentEl.find((el) => el.tag === 'button' && el.text === 'conflict.' + key);
  return { plugin, modal, ledger, files, openedFiles, button };
}
function deferred() { let resolve; let reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { resolve, reject, promise }; }

test('conflict UI stays open and prevents duplicate choices while resolution is pending', async () => {
  const { plugin, modal, button } = fixture(); const pending = deferred(); let calls = 0;
  plugin.resolveConflict = async () => { calls++; return pending.promise; };
  const first = button('local').onclick();
  assert.notEqual(modal.closed, true, 'The dialog must remain open until the operation result');
  assert.equal(button('remote').disabled, true);
  await button('remote').onclick(); assert.equal(calls, 1);
  pending.resolve({ resolved: true }); await first; assert.equal(modal.closed, true);
});

test('overlapping merge displays its draft and keeps the conflict dialog open', async () => {
  const { plugin, modal, button } = fixture();
  plugin.resolveConflict = async () => ({ resolved: false, merged: false, conflicts: 2, draft: 'draft.md' });
  await button('merge').onclick();
  assert.notEqual(modal.closed, true);
  assert.ok(modal.contentEl.find((el) => el.text.includes('conflict.mergeNeedsReview')));
  assert.ok(modal.contentEl.find((el) => el.tag === 'button' && el.text === 'conflict.openDraft'));
});

test('failed resolution is visible inline and can be retried without losing review', async () => {
  const { plugin, modal, button, ledger } = fixture(); const original = ledger.getConflict('a.md');
  plugin.resolveConflict = async () => { throw new Error('Synthetic lease conflict'); };
  await button('local').onclick(); assert.notEqual(modal.closed, true);
  assert.ok(modal.contentEl.find((el) => el.text.includes('Synthetic lease conflict')));
  assert.equal(button('local').disabled, false); assert.equal(ledger.getConflict('a.md'), original);
});

test('late previews cannot populate a closed dialog', async () => {
  const { plugin } = fixture(); const pending = deferred(); plugin.app.vault.read = () => pending.promise;
  const modal = new ConflictModal(plugin.app, plugin, 'a.md'); modal.open(); modal.close();
  pending.resolve('Synthetic text'); await Promise.resolve(); await Promise.resolve(); await Promise.resolve();
  assert.equal(modal.contentEl.children.length, 0);
});

test('missing base disables merge while leaving explicit keep choices available', () => {
  const { modal, ledger, button } = fixture(); ledger.setFile('a.md', {});
  modal.onOpen(); assert.equal(button('merge').disabled, true); assert.equal(button('local').disabled, false);
  assert.ok(modal.contentEl.find((el) => el.text === 'conflict.mergeUnavailable'));
});

test('review snapshots do not alias nested pending resolution fields', async () => {
  const { plugin, modal, ledger, button } = fixture();
  ledger.getConflict('a.md').resolution = { phase: 'before' }; modal.onOpen();
  ledger.getConflict('a.md').resolution.phase = 'after';
  let reviewed;
  plugin.resolveConflict = async (_path, _choice, snapshot) => { reviewed = snapshot.conflict; return { resolved: true }; };
  await button('local').onclick(); assert.equal(reviewed.resolution.phase, 'before');
});

test('draft opening uses an existing vault file rather than creating a replacement', async () => {
  const { plugin, modal, button, openedFiles } = fixture();
  plugin.resolveConflict = async () => ({ resolved: false, draft: 'draft.md' });
  await button('merge').onclick(); await modal.contentEl.find((el) => el.text === 'conflict.openDraft').onclick();
  assert.deepEqual(openedFiles, ['draft.md']); assert.equal(modal.closed, true);
});

test('missing saved copy cannot open or create an empty file', async () => {
  const { modal, files, openedFiles } = fixture(); files.delete('a conflict.md');
  await modal.contentEl.find((el) => el.text === 'conflict.openRemote').onclick();
  assert.deepEqual(openedFiles, []); assert.notEqual(modal.closed, true);
});

test('view conflicts opens the active or sole conflict directly', async () => {
  const { plugin } = fixture(); const before = opened.length;
  await plugin.viewConflicts(); assert.equal(opened.length, before + 1); assert.equal(opened.at(-1).path, 'a.md');
  plugin.ledger.setConflict('other.md', { remoteId: 'f2' }); plugin.app.workspace.getActiveFile = () => null;
  let activated = 0; plugin.activateView = async () => { activated++; };
  await plugin.viewConflicts(); assert.equal(activated, 1);
});

test('busy resolution reports the original error even without an initialized client', async () => {
  const { plugin } = fixture(); plugin._busy = () => true; plugin.engine = { running: true }; plugin.redact = Plugin.prototype.redact;
  await assert.rejects(Plugin.prototype.resolveConflict.call(plugin, 'a.md', 'local'), /notice.busy/);
  assert.equal(plugin.ledger.data.log.at(-1).level, 'error'); assert.equal(plugin.redact('synthetic-key'), '***');
});

test('plugin retry accepts internal progress but still rejects changed reviewed versions', async () => {
  const { plugin, ledger } = fixture(); const conflict = ledger.getConflict('a.md');
  conflict.resolution = { stage: 'before' };
  const reviewed = { scope: ledger.data.meta.syncScope, conflict: JSON.parse(JSON.stringify(conflict)) };
  conflict.resolution.stage = 'after'; let calls = 0;
  plugin._checkOperation = () => {};
  plugin.makeEngine = () => ({ conflicts: { resolve: async () => { calls++; return { resolved: true }; } } });
  assert.equal((await Plugin.prototype.resolveConflict.call(plugin, 'a.md', 'local', reviewed)).resolved, true);
  conflict.remoteId = 'different-file';
  await assert.rejects(Plugin.prototype.resolveConflict.call(plugin, 'a.md', 'local', reviewed), /Conflict changed/);
  assert.equal(calls, 1);
});

for (const paused of [false, true]) test(`a newly ${paused ? 'paused' : 'busy'} operation explains why a displayed choice cannot run`, async () => {
  const { plugin, modal, button } = fixture(); let calls = 0;
  plugin.resolveConflict = async () => { calls++; return { resolved: true }; };
  plugin.settings.paused = paused; plugin._busy = () => !paused;
  await button('local').onclick(); assert.equal(calls, 0); assert.notEqual(modal.closed, true);
  assert.ok(modal.contentEl.find((el) => el.text === (paused ? 'notice.pausedDone' : 'notice.busy')));
  plugin.settings.paused = false; plugin._busy = () => false;
  modal.contentEl.find((el) => el.text === 'conflict.refresh').onclick();
  await button('local').onclick(); assert.equal(calls, 1); assert.equal(modal.closed, true);
});

for (const method of ['writeBinary', 'writeBinaryAtomic']) test(`${method} rejects a changed reviewed hash before any backup or target mutation`, async () => {
  const current = new TextEncoder().encode('Late edit').buffer;
  const file = new TFile('a.md', current); let writes = 0; let commits = 0;
  const app = { vault: {
    getAbstractFileByPath: () => file,
    adapter: { exists: async () => true, readBinary: async () => current, writeBinary: async () => { writes++; } },
    process: async () => { commits++; }
  } };
  const local = new Plugin.testLocalFS(app);
  const expectedHash = await sha256Hex(new TextEncoder().encode('Reviewed text'));
  await assert.rejects(local[method]('a.md', new TextEncoder().encode('Chosen merged text').buffer, undefined, { expectedHash }), { code: 'LOCAL_CHANGED' });
  assert.equal(writes, 0); assert.equal(commits, 0);
});

test('reviewed missing file is rejected before creating its missing parent folder', async () => {
  let folders = 0; let files = 0;
  const local = new Plugin.testLocalFS({ vault: { adapter: { exists: async () => false }, createFolder: async () => { folders++; }, createBinary: async () => { files++; } } });
  await assert.rejects(local.writeBinaryAtomic('missing/a.md', new Uint8Array([65]).buffer, undefined, { expectedHash: await sha256Hex(new Uint8Array([66])) }), { code: 'LOCAL_CHANGED' });
  assert.equal(folders, 0); assert.equal(files, 0);
});
