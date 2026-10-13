'use strict';
const obsidian = require('obsidian');
const { Plugin, PluginSettingTab, Setting, Notice, Modal, ItemView, requestUrl, TFile, setIcon } = obsidian;
const { TdriveClient } = require('./TdriveClient');
const { ManifestStore } = require('./ManifestStore');
const { SyncEngine } = require('./SyncEngine');
const { CryptoManager } = require('./CryptoManager');
const { Compressor } = require('./Compressor');
const { makeT, resolveLang } = require('./i18n');
const NOTIFY = require('./notify');

const VIEW_TYPE = 'tdrive-sync-status';

const DEFAULTS = {
  baseUrl: 'https://tdriver.x2o.workers.dev',
  apiKey: '',
  remoteFolder: '',            // empty -> "/Obsidian/<vault name>/"
  mode: 'two-way',             // two-way | upload-only | download-only
  deletionPolicy: 'trash',     // trash (remote -> Tdrive trash, local -> Obsidian trash) | off
  maxDeletesPerRun: 10,
  autoSyncMinutes: 0,
  syncOnStartup: false,
  paused: false,
  excludePatterns: ['**/*.tmp', '**/Thumbs.db'],
  downloadChunkBytes: 4 * 1024 * 1024,
  uploadChunkBytes: 4 * 1024 * 1024,   // 0 = auto (server value, capped at 5 MB)
  maxConcurrentParts: 3,               // parallel upload/download parts (1 = sequential)
  maxFileSizeMB: 0,                    // skip files larger than this (0 = unlimited)
  wifiOnly: false,                     // do not use the network on a cellular connection
  cellularMaxFileMB: 5,                // on cellular, defer files larger than this (0 = never defer)
  syncOnChange: false,                 // auto-sync shortly after a local edit
  syncOnChangeDelaySeconds: 2,         // debounce window for change-triggered syncs (1 = near-instant)
  instantSync: false,                  // react to edits immediately (1s debounce) and on window focus
  maxUploadKBps: 0,                    // upload speed cap in KB/s (0 = unlimited)
  maxDownloadKBps: 0,                  // download speed cap in KB/s (0 = unlimited)
  scheduleEnabled: false,              // only auto-sync inside the window below
  scheduleStart: 0,                    // hour 0-23
  scheduleEnd: 0,                      // hour 0-23 (end <= start means it wraps midnight)
  dedupeUploads: true,                 // reuse chunks the server already stores (saves upload data)
  mergeTextFiles: true,                // keep a small text base so conflicts can be auto-merged
  streamLargeDownloads: true,          // write very large plaintext downloads straight to disk (bounded memory)
  streamThresholdMB: 8,                // ...for files at least this large
  incrementalScan: true,               // skip the remote tree walk when the server reports no changes
  fullScanMinutes: 15,                 // ...but always do a full scan at least this often
  remoteLock: true,                    // warn if another device is syncing right now
  remoteLockMinutes: 15,               // a lock older than this is ignored
  syncFolders: [],                     // selective sync: only these subtrees on this device (empty = everything)
  e2eeEnabled: false,                  // encrypt file contents before upload
  e2eePassphrase: '',
  e2eeSalt: '',
  compressTextFiles: true,             // gzip large text files before upload (saves data; server stores gzip)
  compressMinKB: 64,                   // only compress text files at least this large
  cdcFiles: true,                      // content-defined chunking: cheap incremental downloads; false = revisable fixed chunks
  language: 'auto',                    // auto | ar | en
  notifyStyle: 'technical',            // technical | concise | elegant | detailed
};

/** Best-effort cellular detection. Unknown -> false (allow), so a false positive can never block sync. */
function networkLooksCellular() {
  try {
    const c = (typeof navigator !== 'undefined') && (navigator.connection || navigator.mozConnection || navigator.webkitConnection);
    if (!c) return false;
    const t = c.type ? String(c.type).toLowerCase() : '';
    if (t) return /cellular|2g|3g|4g|5g|mobile/.test(t);
    const et = c.effectiveType ? String(c.effectiveType).toLowerCase() : '';
    if (et) return /2g|3g|slow/.test(et);
  } catch (e) { /* unknown connection: treat as fine */ }
  return false;
}

/** requestUrl bypasses CORS and works on Android + Windows. Body/response are ArrayBuffers. */
async function obsidianTransport({ url, method, headers, body }) {
  const h = { ...headers };
  let contentType;
  for (const k of Object.keys(h)) if (k.toLowerCase() === 'content-type') { contentType = h[k]; delete h[k]; }
  const r = await requestUrl({ url, method, headers: h, body, contentType, throw: false });
  const out = {};
  for (const [k, v] of Object.entries(r.headers || {})) out[k.toLowerCase()] = v;
  return { status: r.status, headers: out, body: r.arrayBuffer };
}

/** A simple token-bucket rate limiter (bytes observed -> delayed start) so the user can cap bandwidth.
 *  Shared, per direction, so parallel parts still respect the overall limit. 0 KB/s = unlimited. */
function makeRateLimiter(getKBps) {
  let next = 0;
  return async (bytes) => {
    const kbps = getKBps();
    if (!kbps || kbps <= 0 || !bytes) return;
    const now = Date.now();
    const start = Math.max(now, next);
    next = start + (bytes / 1024) / kbps * 1000;
    const wait = start - now;
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
  };
}

/** Wraps the transport with the upload/download limits taken from the live settings. */
function makeThrottledTransport(getLimits) {
  const up = makeRateLimiter(() => getLimits().maxUploadKBps);
  const down = makeRateLimiter(() => getLimits().maxDownloadKBps);
  return async (req) => {
    if (req && req.body && req.body.byteLength) await up(req.body.byteLength);
    const res = await obsidianTransport(req);
    if (res && res.body && res.body.byteLength) await down(res.body.byteLength);
    return res;
  };
}

class ObsidianLocalFS {
  constructor(app, t) { this.app = app; this.t = t || ((s) => s); }
  get ad() { return this.app.vault.adapter; }
  async list() { return this.app.vault.getFiles().map((f) => ({ path: f.path, size: f.stat.size, mtime: f.stat.mtime })); }
  readBinary(p) { return this.ad.readBinary(p); }
  async stat(p) { const s = await this.ad.stat(p); return s ? { size: s.size, mtime: s.mtime } : null; }
  exists(p) { return this.ad.exists(p); }
  async _isFile(p) { try { const s = await this.ad.stat(p); return !!s && s.type === 'file'; } catch (e) { return false; } }
  async _isDir(p) { try { const s = await this.ad.stat(p); return !!s && s.type === 'folder'; } catch (e) { return false; } }
  /** Creates every missing parent folder. vault.createFolder is tried first because it also registers the folder in
   *  Obsidian's own index (on mobile, a folder made only through the raw adapter may be unknown to vault.create*).
   *  An "already exists" error is never fatal: we re-check and carry on. A real failure names the folder. */
  async _mkdirs(p) {
    let cur = '';
    for (const seg of p.split('/').slice(0, -1)) {
      cur = cur ? cur + '/' + seg : seg;
      if (await this._isDir(cur)) continue;
      let err = null;
      try { await this.app.vault.createFolder(cur); }
      catch (e1) {
        err = e1;
        if (!(await this._isDir(cur))) { try { await this.ad.mkdir(cur); err = null; } catch (e2) { err = e2; } }
      }
      // "already exists" while stat cannot confirm the folder: trust it unless something known to be a file is there.
      if (err && /exist/i.test(String(err.message || err)) && (await this.ad.exists(cur)) && !(await this._isFile(cur))) continue;
      if (err && !(await this._isDir(cur))) {
        const clash = (await this.ad.exists(cur)) ? this.t('local.nameClash') : '';
        throw new Error(this.t('local.couldNotCreate', { dir: cur, clash, msg: (err.message || err) }));
      }
    }
  }
  async writeBinary(p, buf, mtime) {
    await this._mkdirs(p);
    const opts = mtime ? { mtime } : undefined;
    const attempt = async () => {
      const f = this.app.vault.getAbstractFileByPath(p);
      if (f instanceof TFile) await this.app.vault.modifyBinary(f, buf, opts);
      else if (await this.ad.exists(p)) await this.ad.writeBinary(p, buf, opts);
      else await this.app.vault.createBinary(p, buf, opts);
    };
    try { await attempt(); }
    catch (e) {
      // Folder not yet known to the vault index, or the file already exists on disk but not in the index.
      await this._mkdirs(p);
      try { await this.ad.writeBinary(p, buf, opts); }
      catch (e2) { throw new Error(this.t('local.couldNotWrite', { path: p, msg: (e2.message || e.message) })); }
    }
    if (!(await this.ad.exists(p))) throw new Error(this.t('local.notAppeared', { path: p }));
  }
  /** Folder paths known to the vault (null when the API is unavailable, so the engine skips folder deletion). */
  async listFolders() {
    const all = this.app.vault.getAllLoadedFiles;
    if (typeof all !== 'function') return null;
    return all.call(this.app.vault).filter((f) => f && Array.isArray(f.children) && f.path && f.path !== '/').map((f) => f.path);
  }
  async mkdir(p) { await this._mkdirs(p + '/_'); }
  /** Returns false (and trashes nothing) if the folder still holds anything on disk, e.g. hidden files the vault index never lists. */
  async trashFolder(p) {
    const f = this.app.vault.getAbstractFileByPath(p);
    if (!f) return true;                              // already gone
    try { const l = await this.ad.list(p); if ((l.files && l.files.length) || (l.folders && l.folders.length)) return false; } catch (e) { /* cannot list: fall through to the engine's own emptiness check */ }
    if (this.app.fileManager.trashFile) await this.app.fileManager.trashFile(f);
    else await this.app.vault.trash(f, true);
    return true;
  }
  async trash(p) {
    const f = this.app.vault.getAbstractFileByPath(p);
    if (!f) throw new Error('File not found: ' + p);
    if (this.app.fileManager.trashFile) await this.app.fileManager.trashFile(f);
    else await this.app.vault.trash(f, true);
  }
  /** True when the adapter can append bytes (enables streaming large downloads to disk). */
  get canStream() { return typeof this.ad.appendBinary === 'function'; }
  /** Opens a streaming sink at `p`: the first chunk overwrites, later chunks append. */
  async beginStreamWrite(p) {
    await this._mkdirs(p);
    const self = this; let n = 0;
    return {
      async write(chunk) {
        const buf = chunk instanceof ArrayBuffer ? chunk : (chunk.buffer ? chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.byteLength) : chunk);
        if (n++ === 0) await self.ad.writeBinary(p, buf);
        else await self.ad.appendBinary(p, buf);
      },
      async close() { return p; },
    };
  }
}

const fmtTime = (ms) => (ms ? new Date(ms).toLocaleString() : '—');

/** Renders a real Lucide icon into `el` (Obsidian's own icon set), trying the fallback chain.
 *  Returns true when an icon was drawn; false lets the caller fall back to a text glyph. */
function applyIcon(el, names) {
  if (!el || typeof setIcon !== 'function' || !names || !names.length) return false;
  for (const name of names) {
    try {
      setIcon(el, name);
      if (el.childElementCount > 0 && el.querySelector && el.querySelector('svg')) return true;
      el.empty && el.empty();
    } catch (e) { /* unknown icon name: try the next one */ }
  }
  return false;
}

/** Builds one line of a notice: a sequence of [icon, text] segments. */
function buildNoticeLine(lineEl, segs) {
  for (const s of segs || []) {
    const box = lineEl.createSpan({ cls: 'tdrive-notice-seg' });
    if (s.icon) {
      const g = box.createSpan({ cls: 'tdrive-notice-icon' });
      if (!applyIcon(g, NOTIFY.ICONS[s.icon])) g.setText(NOTIFY.GLYPH[s.icon] || '');
    }
    if (s.text) box.createSpan({ cls: 'tdrive-notice-text', text: (s.icon ? ' ' : '') + s.text });
  }
}

class ConflictModal extends Modal {
  constructor(app, plugin, path) { super(app); this.plugin = plugin; this.path = path; }
  onOpen() {
    const { contentEl } = this; const t = this.plugin.t;
    contentEl.createEl('h3', { text: t('conflict.title', { path: this.path }) });
    const c = this.plugin.ledger.getConflict(this.path);
    contentEl.createEl('p', { text: t('conflict.body', { copy: c ? c.copyPath : '?' }) });
    // Simple, read-only side-by-side preview (no line-by-line merge editor): the local file next to the
    // conflict copy of the remote version, so the user can decide. Text files only, first 40 lines each.
    try {
      const local = this.plugin.app.vault.getAbstractFileByPath(this.path);
      const copyFile = c && c.copyPath ? this.plugin.app.vault.getAbstractFileByPath(c.copyPath) : null;
      const isText = (f) => f && /\.(md|markdown|txt|text|json|csv|yml|yaml|js|ts|css|html|xml)$/i.test(f.path);
      if (local && copyFile && isText(local) && isText(copyFile)) {
        const read = async (f) => { try { return await this.plugin.app.vault.read(f); } catch (e) { return null; } };
        Promise.all([read(local), read(copyFile)]).then(([a, b]) => {
          if (a == null || b == null) return;
          const preview = contentEl.createDiv({ cls: 'tdrive-conflict-preview' });
          const left = preview.createDiv({ cls: 'tdrive-conflict-col' });
          left.createEl('h5', { text: t('conflict.localCol') });
          left.createEl('pre', { cls: 'tdrive-sync-log' }).setText(a.split('\n').slice(0, 40).join('\n'));
          const right = preview.createDiv({ cls: 'tdrive-conflict-col' });
          right.createEl('h5', { text: t('conflict.remoteCol') });
          right.createEl('pre', { cls: 'tdrive-sync-log' }).setText(b.split('\n').slice(0, 40).join('\n'));
        }).catch(() => { /* preview is optional */ });
      }
    } catch (e) { /* preview is best effort */ }
    const opts = [
      ['merge', 'conflict.merge', 'conflict.mergeDesc'],
      ['local', 'conflict.local', 'conflict.localDesc'],
      ['remote', 'conflict.remote', 'conflict.remoteDesc'],
      ['both', 'conflict.both', 'conflict.bothDesc'],
      ['cancel', 'conflict.cancel', 'conflict.cancelDesc'],
    ];
    for (const [key, labelKey, descKey] of opts) {
      new Setting(contentEl).setName(t(labelKey)).setDesc(t(descKey)).addButton((b) => b.setButtonText(t(labelKey)).onClick(async () => {
        this.close();
        if (key === 'cancel') return;
        try { await this.plugin.resolveConflict(this.path, key); new Notice(t('conflict.resolved')); }
        catch (e) { new Notice(t('conflict.resolveFailed', { msg: this.plugin.redact(e.message) }), 8000); }
      }));
    }
  }
  onClose() { this.contentEl.empty(); }
}

class ApproveDeletesModal extends Modal {
  constructor(app, plugin, pending) { super(app); this.plugin = plugin; this.pending = pending; }
  onOpen() {
    const { contentEl } = this; const t = this.plugin.t; const items = this.pending.items || [];
    contentEl.createEl('h3', { text: t('pending.title', { n: items.length }) });
    contentEl.createEl('p', { text: t('pending.body') });
    const pre = contentEl.createEl('pre', { cls: 'tdrive-sync-log' });
    pre.setText(items.slice(0, 60).map((i) => (i.action === 'deleteRemote' ? t('pending.remoteItem') : t('pending.localItem')) + i.path).join('\n') + (items.length > 60 ? t('pending.more', { n: items.length - 60 }) : ''));
    const row = contentEl.createDiv({ cls: 'tdrive-sync-row' });
    row.createEl('button', { text: t('pending.approve') }).onclick = () => { this.close(); this.plugin.runSync({ approveDeletes: true }); };
    row.createEl('button', { text: t('common.cancel') }).onclick = () => this.close();
  }
  onClose() { this.contentEl.empty(); }
}

class PreviewModal extends Modal {
  constructor(app, plugin, rep) { super(app); this.plugin = plugin; this.rep = rep; }
  onOpen() {
    const { contentEl } = this; const p = this.rep.plan || {}; const t = this.plugin.t;
    contentEl.createEl('h3', { text: t('preview.title') });
    const row = (labelKey, arr, fmt) => {
      if (!arr || !arr.length) return;
      contentEl.createEl('h4', { text: `${t(labelKey)} (${arr.length})` });
      const pre = contentEl.createEl('pre', { cls: 'tdrive-sync-log' });
      pre.setText(arr.slice(0, 50).map(fmt).join('\n') + (arr.length > 50 ? t('preview.more', { n: arr.length - 50 }) : ''));
    };
    row('lane.upload', p.upload, (x) => '↑ ' + x);
    row('lane.download', p.download, (x) => '↓ ' + x);
    row('lane.compare', p.compare, (x) => '⇅ ' + x);
    row('lane.move', p.move, (x) => `→ ${x.from}  ⇒  ${x.to}`);
    row('lane.deleteRemote', p.deleteRemote, (x) => '☁ ' + x);
    row('lane.deleteLocal', p.deleteLocal, (x) => '💻 ' + x);
    row('lane.blocked', p.blocked, (x) => `⊘ ${x.path} (${x.why})`);
    row('lane.conflicts', p.skipConflict, (x) => '⚠ ' + x);
    const total = ['upload', 'download', 'compare', 'move', 'deleteRemote', 'deleteLocal'].reduce((n, k) => n + ((p[k] && p[k].length) || 0), 0);
    if (!total) contentEl.createEl('p', { text: t('preview.none') });
    const bar = contentEl.createDiv({ cls: 'tdrive-sync-row' });
    bar.createEl('button', { text: t('common.runNow') }).onclick = () => { this.close(); this.plugin.runSync(); };
    bar.createEl('button', { text: t('common.close') }).onclick = () => this.close();
  }
  onClose() { this.contentEl.empty(); }
}

class NotifyPreviewModal extends Modal {
  constructor(app, plugin) { super(app); this.plugin = plugin; }
  onOpen() { this.render(); }
  render() {
    const { contentEl } = this; const p = this.plugin; const t = p.t;
    contentEl.empty(); contentEl.addClass('tdrive-sync-view');
    contentEl.createEl('h3', { text: t('notify.previewTitle') });
    contentEl.createEl('p', { text: t('notify.previewHint') });
    const bar = contentEl.createDiv({ cls: 'tdrive-sync-row' });
    for (const st of NOTIFY.STYLES) {
      const active = p.settings.notifyStyle === st;
      const b = bar.createEl('button', { text: (active ? '● ' : '○ ') + t('notify.style.' + st) });
      if (active) b.addClass('mod-cta');
      b.onclick = async () => { p.settings.notifyStyle = st; await p.saveSettings(); this.render(); p.notify(); };
    }
    contentEl.createEl('p', { cls: 'tdrive-sync-preview-desc', text: t('notify.style.' + (p.settings.notifyStyle || 'technical') + 'Desc') });
    for (const smp of NOTIFY.samples(p.settings.notifyStyle, t)) {
      const box = contentEl.createDiv({ cls: 'tdrive-sync-box' });
      box.createEl('h4', { text: smp.title });
      const prev = box.createDiv({ cls: 'tdrive-notice tdrive-sync-preview-notice' });
      const rows = (smp.rows && smp.rows.length) ? smp.rows : [[{ icon: null, text: smp.text }]];
      for (const row of rows) buildNoticeLine(prev.createDiv({ cls: 'tdrive-notice-line' }), row);
    }
    const foot = contentEl.createDiv({ cls: 'tdrive-sync-row' });
    foot.createEl('button', { text: t('common.close') }).onclick = () => this.close();
  }
  onClose() { this.contentEl.empty(); }
}

class VersionsModal extends Modal {
  constructor(app, plugin, path, remoteId) { super(app); this.plugin = plugin; this.path = path; this.remoteId = remoteId; }
  async onOpen() {
    const { contentEl } = this; const p = this.plugin; const t = p.t;
    contentEl.empty(); contentEl.addClass('tdrive-sync-view');
    contentEl.createEl('h3', { text: t('versions.title', { path: this.path }) });
    const status = contentEl.createEl('p', { text: t('common.loading') });
    try {
      const client = p.makeClient();
      const r = await client.request({ path: `/files/${encodeURIComponent(this.remoteId)}/versions` });
      const list = (r && (r.versions || r.items)) || (Array.isArray(r) ? r : []);
      if (!list.length) { status.setText(t('versions.none')); return; }
      status.setText(t('versions.hint'));
      for (const v of list) {
        const row = contentEl.createDiv({ cls: 'tdrive-sync-row' });
        const vid = v.id || v.versionId || v.version;
        row.createSpan({ text: `${new Date(v.created_at || v.createdAt || 0).toLocaleString()} · ${v.size != null ? v.size + ' B' : ''}` });
        row.createEl('button', { text: t('versions.restore') }).onclick = async () => {
          try { await client.request({ method: 'POST', path: `/files/${encodeURIComponent(this.remoteId)}/versions/${encodeURIComponent(String(vid))}/restore` }); new Notice(t('versions.restored')); this.close(); p.runSync({ silent: true }); }
          catch (e) { new Notice(t('versions.failed', { msg: p.redact(e.message) }), 8000); }
        };
      }
    } catch (e) { status.setText(t('versions.failed', { msg: p.redact(e.message) })); }
  }
  onClose() { this.contentEl.empty(); }
}

class FolderPickerModal extends Modal {
  constructor(app, plugin) { super(app); this.plugin = plugin; }
  onOpen() {
    const { contentEl } = this; const p = this.plugin; const t = p.t;
    contentEl.empty(); contentEl.addClass('tdrive-sync-view');
    contentEl.createEl('h3', { text: t('selective.title') });
    contentEl.createEl('p', { text: t('selective.hint') });
    const folders = (typeof p.app.vault.getAllLoadedFiles === 'function')
      ? p.app.vault.getAllLoadedFiles().filter((f) => f && Array.isArray(f.children) && f.path && f.path !== '/')
      : [];
    const chosen = new Set(p.settings.syncFolders || []);
    const all = contentEl.createDiv({ cls: 'tdrive-sync-row' });
    all.createEl('button', { text: t('selective.all') }).onclick = async () => { p.settings.syncFolders = []; await p.saveSettings(); this.close(); new Notice(t('selective.saved')); };
    if (!folders.length) contentEl.createEl('p', { text: t('selective.empty') });
    for (const f of folders.slice(0, 400)) {
      new Setting(contentEl).setName(f.path).addToggle((tg) => tg.setValue(chosen.has(f.path)).onChange((v) => { if (v) chosen.add(f.path); else chosen.delete(f.path); }));
    }
    const bar = contentEl.createDiv({ cls: 'tdrive-sync-row' });
    bar.createEl('button', { text: t('common.save'), cls: 'mod-cta' }).onclick = async () => { p.settings.syncFolders = [...chosen]; await p.saveSettings(); this.close(); new Notice(t('selective.saved')); };
    bar.createEl('button', { text: t('common.cancel') }).onclick = () => this.close();
  }
  onClose() { this.contentEl.empty(); }
}

class SearchModal extends Modal {
  constructor(app, plugin) { super(app); this.plugin = plugin; }
  onOpen() {
    const { contentEl } = this; const p = this.plugin; const t = p.t;
    contentEl.empty(); contentEl.addClass('tdrive-sync-view');
    contentEl.createEl('h3', { text: t('search.title') });
    const box = contentEl.createDiv({ cls: 'tdrive-sync-row' });
    const input = box.createEl('input', { type: 'text', placeholder: t('search.placeholder') });
    const results = contentEl.createDiv();
    const run = async () => {
      results.empty();
      const q = input.value.trim(); if (!q) return;
      try {
        const r = await p.makeClient().request({ path: '/search', query: { q, limit: 50 } });
        const files = (r && (r.files || r.results)) || [];
        if (!files.length) { results.createEl('p', { text: t('search.none') }); return; }
        for (const f of files) results.createEl('div').setText(`${f.path || '/'}${f.name} · ${f.size || 0} B`);
      } catch (e) { results.createEl('p', { text: t('search.failed', { msg: p.redact(e.message) }) }); }
    };
    input.onkeydown = (e) => { if (e.key === 'Enter') run(); };
    box.createEl('button', { text: t('search.go') }).onclick = run;
  }
  onClose() { this.contentEl.empty(); }
}

class SyncStatusView extends ItemView {
  constructor(leaf, plugin) { super(leaf); this.plugin = plugin; this._h = () => this.render(); }
  getViewType() { return VIEW_TYPE; }
  getDisplayText() { return 'Tdrive Sync'; }
  getIcon() { return 'refresh-cw'; }
  async onOpen() { this.plugin.listeners.add(this._h); this.render(); }
  async onClose() { this.plugin.listeners.delete(this._h); }
  render() {
    const el = this.contentEl; el.empty(); el.addClass('tdrive-sync-view');
    const p = this.plugin; const t = p.t; const meta = p.ledger.data.meta; const last = meta.lastRun;
    el.createEl('h3', { text: 'Tdrive Sync' });
    const box = el.createDiv({ cls: 'tdrive-sync-box' });
    const line = (k, v) => box.createDiv().setText(`${k}: ${v}`);
    line(t('status.connection'), p.connection);
    line(t('status.state'), p.settings.paused ? t('status.paused') : (p.engine && p.engine.running ? t('status.running') : t('status.idle')));
    if (p.progress && p.engine && p.engine.running) line(t('status.progress'), `${p.progress.done}/${p.progress.total} ${p.progress.current || ''}`);
    line(t('status.lastSuccess'), fmtTime(meta.lastSuccessAt));
    if (last) {
      line(t('status.lastRun'), `${fmtTime(last.at)} — ${last.status}`);
      line(t('status.counts'), `${last.uploaded} / ${last.downloaded} / ${last.skipped}`);
      line(t('status.conflictsErrors'), `${p.ledger.conflictKeys().length} / ${last.errors}`);
    }
    const row = el.createDiv({ cls: 'tdrive-sync-row' });
    row.createEl('button', { text: t('status.syncNow') }).onclick = () => p.runSync();
    row.createEl('button', { text: p.settings.paused ? t('status.resume') : t('status.pause') }).onclick = () => (p.settings.paused ? p.resume() : p.pause());
    row.createEl('button', { text: t('status.testConn') }).onclick = () => p.testConnection();
    const pend = meta.blockedDeletes;
    if (pend && pend.items && pend.items.length) {
      el.createEl('h4', { text: t('status.blockedDeletes', { n: pend.items.length }) });
      const why = { limit: t('status.reasonLimit'), 'empty-local': t('status.reasonEmptyLocal'), 'empty-remote': t('status.reasonEmptyRemote') }[pend.reason] || pend.reason;
      el.createDiv().setText(t('status.reason', { why, at: fmtTime(pend.at) }));
      if (pend.reason === 'limit') el.createEl('button', { text: t('common.reviewApprove') }).onclick = () => new ApproveDeletesModal(p.app, p, pend).open();
    }
    const cs = p.ledger.conflictKeys();
    el.createEl('h4', { text: t('status.conflicts', { n: cs.length }) });
    for (const path of cs) {
      const r = el.createDiv({ cls: 'tdrive-sync-row' }); r.createSpan({ text: path });
      r.createEl('button', { text: t('status.resolve') }).onclick = () => new ConflictModal(p.app, p, path).open();
    }
    el.createEl('h4', { text: t('status.log') });
    const pre = el.createEl('pre', { cls: 'tdrive-sync-log' });
    pre.setText(p.ledger.data.log.slice(-100).reverse().map((l) => `${new Date(l.t).toLocaleTimeString()} [${l.level}] ${l.msg}`).join('\n'));
  }
}

class TdriveSettingTab extends PluginSettingTab {
  constructor(app, plugin) { super(app, plugin); this.plugin = plugin; }
  display() {
    const { containerEl } = this; const p = this.plugin; const s = p.settings; const t = p.t;
    containerEl.empty();
    containerEl.createEl('h2', { text: 'Tdrive Sync' });
    new Setting(containerEl).setName(t('settings.language'))
      .addDropdown((d) => d.addOptions({ auto: t('settings.languageAuto'), ar: 'العربية', en: 'English' }).setValue(s.language)
        .onChange(async (v) => { s.language = v; p.applyLanguage(); await p.saveSettings(); this.display(); }));
    containerEl.createEl('p', { cls: 'mod-warning', text: t('security.warning') });
    new Setting(containerEl).setName(t('settings.server')).addText((el) => el.setValue(s.baseUrl).onChange(async (v) => { s.baseUrl = v.trim(); await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.apiKey')).setDesc(t('settings.apiKeyDesc'))
      .addText((el) => { el.inputEl.type = 'password'; el.setValue(s.apiKey).onChange(async (v) => { s.apiKey = v.trim(); await p.saveSettings(); }); });
    new Setting(containerEl).setName(t('settings.remoteFolder')).setDesc(t('settings.remoteFolderDesc'))
      .addText((el) => el.setPlaceholder(p.defaultRemoteFolder()).setValue(s.remoteFolder).onChange(async (v) => { s.remoteFolder = v.trim(); await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.mode')).addDropdown((d) => d
      .addOptions({ 'two-way': t('settings.modeTwo'), 'upload-only': t('settings.modeUp'), 'download-only': t('settings.modeDown') })
      .setValue(s.mode).onChange(async (v) => { s.mode = v; await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.deletion')).setDesc(t('settings.deletionDesc'))
      .addDropdown((d) => d.addOptions({ trash: t('settings.deletionTrash'), off: t('settings.deletionOff') }).setValue(s.deletionPolicy).onChange(async (v) => { s.deletionPolicy = v; await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.maxDeletes')).setDesc(t('settings.maxDeletesDesc'))
      .addText((el) => el.setValue(String(s.maxDeletesPerRun)).onChange(async (v) => { const n = parseInt(v, 10); if (n >= 0) { s.maxDeletesPerRun = n; await p.saveSettings(); } }));
    new Setting(containerEl).setName(t('settings.autoSync')).setDesc(t('settings.autoSyncDesc'))
      .addText((el) => el.setValue(String(s.autoSyncMinutes)).onChange(async (v) => { const n = parseInt(v, 10); if (n >= 0) { s.autoSyncMinutes = n; await p.saveSettings(); p.setupAutoSync(); } }));
    new Setting(containerEl).setName(t('settings.syncOnStartup')).addToggle((el) => el.setValue(s.syncOnStartup).onChange(async (v) => { s.syncOnStartup = v; await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.exclude')).setDesc(t('settings.excludeDesc'))
      .addTextArea((el) => el.setValue(s.excludePatterns.join('\n')).onChange(async (v) => { s.excludePatterns = v.split('\n').map((x) => x.trim()).filter(Boolean); await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.selective')).setDesc(t('settings.selectiveDesc'))
      .addTextArea((el) => el.setValue(s.syncFolders.join('\n')).onChange(async (v) => { s.syncFolders = v.split('\n').map((x) => x.trim()).filter(Boolean); await p.saveSettings(); }))
      .addButton((b) => b.setButtonText(t('settings.chooseFolders')).onClick(() => p.chooseSyncFolders()));
    new Setting(containerEl).setName(t('settings.merge')).setDesc(t('settings.mergeDesc'))
      .addToggle((el) => el.setValue(s.mergeTextFiles).onChange(async (v) => { s.mergeTextFiles = v; await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.stream')).setDesc(t('settings.streamDesc'))
      .addToggle((el) => el.setValue(s.streamLargeDownloads).onChange(async (v) => { s.streamLargeDownloads = v; await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.remoteLock')).setDesc(t('settings.remoteLockDesc'))
      .addToggle((el) => el.setValue(s.remoteLock).onChange(async (v) => { s.remoteLock = v; await p.saveSettings(); }));

    containerEl.createEl('h3', { text: t('sec.performance') });
    new Setting(containerEl).setName(t('settings.uploadChunk')).setDesc(t('settings.uploadChunkDesc'))
      .addText((el) => el.setValue(String(s.uploadChunkBytes ? s.uploadChunkBytes / 1048576 : 0)).onChange(async (v) => {
        const n = parseFloat(v); if (!Number.isFinite(n) || n < 0) return;
        s.uploadChunkBytes = n === 0 ? 0 : Math.round(Math.min(5, Math.max(0.25, n)) * 1048576); await p.saveSettings();
      }));
    new Setting(containerEl).setName(t('settings.downloadChunk')).setDesc(t('settings.downloadChunkDesc'))
      .addText((el) => el.setValue(String(Math.round(s.downloadChunkBytes / 1048576))).onChange(async (v) => {
        const n = parseFloat(v); if (!Number.isFinite(n) || n <= 0) return;
        s.downloadChunkBytes = Math.round(Math.min(5, Math.max(1, n)) * 1048576); await p.saveSettings();
      }));
    new Setting(containerEl).setName(t('settings.parallel')).setDesc(t('settings.parallelDesc'))
      .addText((el) => el.setValue(String(s.maxConcurrentParts)).onChange(async (v) => {
        const n = parseInt(v, 10); if (!Number.isFinite(n)) return;
        s.maxConcurrentParts = Math.min(6, Math.max(1, n)); await p.saveSettings();
      }));
    new Setting(containerEl).setName(t('settings.maxFile')).setDesc(t('settings.maxFileDesc'))
      .addText((el) => el.setValue(String(s.maxFileSizeMB)).onChange(async (v) => {
        const n = parseFloat(v); if (!Number.isFinite(n) || n < 0) return;
        s.maxFileSizeMB = Math.round(n); await p.saveSettings();
      }));

    containerEl.createEl('h3', { text: t('sec.network') });
    new Setting(containerEl).setName(t('settings.wifiOnly')).setDesc(t('settings.wifiOnlyDesc'))
      .addToggle((el) => el.setValue(s.wifiOnly).onChange(async (v) => { s.wifiOnly = v; await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.cellularMax')).setDesc(t('settings.cellularMaxDesc'))
      .addText((el) => el.setValue(String(s.cellularMaxFileMB)).onChange(async (v) => {
        const n = parseFloat(v); if (!Number.isFinite(n) || n < 0) return;
        s.cellularMaxFileMB = Math.round(n); await p.saveSettings();
      }));

    containerEl.createEl('h3', { text: t('sec.change') });
    new Setting(containerEl).setName(t('settings.syncOnChange')).setDesc(t('settings.syncOnChangeDesc'))
      .addToggle((el) => el.setValue(s.syncOnChange).onChange(async (v) => { s.syncOnChange = v; await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.instantSync')).setDesc(t('settings.instantSyncDesc'))
      .addToggle((el) => el.setValue(s.instantSync).onChange(async (v) => { s.instantSync = v; await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.syncOnChangeDelay')).setDesc(t('settings.syncOnChangeDelayDesc'))
      .addText((el) => el.setValue(String(s.syncOnChangeDelaySeconds)).onChange(async (v) => {
        const n = parseInt(v, 10); if (!Number.isFinite(n)) return;
        s.syncOnChangeDelaySeconds = Math.min(300, Math.max(1, n)); await p.saveSettings();
      }));

    containerEl.createEl('h3', { text: t('sec.speed') });
    new Setting(containerEl).setName(t('settings.maxUploadKB')).setDesc(t('settings.throttleDesc'))
      .addText((el) => el.setValue(String(s.maxUploadKBps)).onChange(async (v) => { const n = parseInt(v, 10); if (!Number.isFinite(n) || n < 0) return; s.maxUploadKBps = n; await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.maxDownloadKB')).setDesc(t('settings.throttleDesc'))
      .addText((el) => el.setValue(String(s.maxDownloadKBps)).onChange(async (v) => { const n = parseInt(v, 10); if (!Number.isFinite(n) || n < 0) return; s.maxDownloadKBps = n; await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.schedule')).setDesc(t('settings.scheduleDesc'))
      .addToggle((el) => el.setValue(s.scheduleEnabled).onChange(async (v) => { s.scheduleEnabled = v; await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.scheduleStart'))
      .addText((el) => el.setValue(String(s.scheduleStart)).onChange(async (v) => { const n = parseInt(v, 10); if (!Number.isFinite(n)) return; s.scheduleStart = Math.min(23, Math.max(0, n)); await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.scheduleEnd')).setDesc(t('settings.scheduleEndDesc'))
      .addText((el) => el.setValue(String(s.scheduleEnd)).onChange(async (v) => { const n = parseInt(v, 10); if (!Number.isFinite(n)) return; s.scheduleEnd = Math.min(23, Math.max(0, n)); await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.dedupe')).setDesc(t('settings.dedupeDesc'))
      .addToggle((el) => el.setValue(s.dedupeUploads).onChange(async (v) => { s.dedupeUploads = v; await p.saveSettings(); }));

    containerEl.createEl('h3', { text: t('sec.bandwidth') });
    containerEl.createEl('p', { cls: 'tdrive-sync-hint', text: t('compress.note') });
    new Setting(containerEl).setName(t('settings.compress')).setDesc(t('settings.compressDesc'))
      .addToggle((el) => el.setValue(s.compressTextFiles).onChange(async (v) => { s.compressTextFiles = v; await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.compressMin')).setDesc(t('settings.compressMinDesc'))
      .addText((el) => el.setValue(String(s.compressMinKB)).onChange(async (v) => {
        const n = parseFloat(v); if (!Number.isFinite(n) || n < 0) return;
        s.compressMinKB = Math.round(Math.min(1024, Math.max(4, n))); await p.saveSettings();
      }));
    new Setting(containerEl).setName(t('settings.cdc')).setDesc(t('settings.cdcDesc'))
      .addToggle((el) => el.setValue(s.cdcFiles).onChange(async (v) => { s.cdcFiles = v; await p.saveSettings(); }));

    containerEl.createEl('h3', { text: t('sec.e2ee') });
    containerEl.createEl('p', { cls: 'mod-warning', text: t('e2ee.warning') });
    new Setting(containerEl).setName(t('settings.e2eeEnable')).setDesc(t('settings.e2eeEnableDesc'))
      .addToggle((el) => el.setValue(s.e2eeEnabled).onChange(async (v) => { s.e2eeEnabled = v; await p.saveSettings(); }));
    new Setting(containerEl).setName(t('settings.e2eePass')).setDesc(t('settings.e2eePassDesc'))
      .addText((el) => { el.inputEl.type = 'password'; el.setValue(s.e2eePassphrase).onChange(async (v) => { s.e2eePassphrase = v; await p.saveSettings(); }); });
    new Setting(containerEl).setName(t('settings.e2eeSalt')).setDesc(t('settings.e2eeSaltDesc'))
      .addText((el) => el.setValue(s.e2eeSalt).onChange(async (v) => { s.e2eeSalt = v.trim(); await p.saveSettings(); }));

    containerEl.createEl('h3', { text: t('sec.notifications') });
    new Setting(containerEl).setName(t('settings.notifyStyle')).setDesc(t('settings.notifyStyleDesc'))
      .addDropdown((d) => d
        .addOptions({ technical: t('notify.style.technical'), concise: t('notify.style.concise'), elegant: t('notify.style.elegant'), detailed: t('notify.style.detailed') })
        .setValue(s.notifyStyle)
        .onChange(async (v) => { s.notifyStyle = v; await p.saveSettings(); p.notify(); }));
    new Setting(containerEl).setName(t('settings.notifyPreview')).setDesc(t('settings.notifyPreviewDesc'))
      .addButton((b) => b.setButtonText(t('notify.previewButton')).onClick(() => new NotifyPreviewModal(p.app, p).open()));

    containerEl.createEl('h3', { text: t('sec.connection') });
    new Setting(containerEl).setName(t('settings.testConn')).addButton((b) => b.setButtonText(t('common.test')).onClick(() => p.testConnection()));
    new Setting(containerEl).setName(t('settings.statusLog')).addButton((b) => b.setButtonText(t('common.open')).onClick(() => p.activateView()));
    new Setting(containerEl).setName(t('settings.exportLog')).setDesc(t('settings.exportLogDesc'))
      .addButton((b) => b.setButtonText(t('common.export')).onClick(() => p.exportLog()));
    new Setting(containerEl).setName(t('settings.pauseResume')).addButton((b) => b.setButtonText(s.paused ? t('status.resume') : t('status.pause')).onClick(async () => { await (s.paused ? p.resume() : p.pause()); this.display(); }));
    new Setting(containerEl).setName(t('settings.probe')).setDesc(t('settings.probeDesc'))
      .addButton((b) => b.setButtonText(t('common.test')).onClick(() => p.probeApi()));
  }
}

class TdriveSyncPlugin extends Plugin {
  async onload() {
    this.listeners = new Set(); this.progress = null; this.engine = null; this.connection = '—';
    this._timer = null; this._changeTimer = null; this._quietUntil = 0; this._crypto = null; this._cryptoTag = '';
    this._retryNotified = false;
    this.settings = Object.assign({}, DEFAULTS, await this.loadData());
    this.applyLanguage();
    this.connection = this.t('status.idle');
    const ad = this.app.vault.adapter; const statePath = this.manifest.dir + '/sync-state.json';
    this.ledger = new ManifestStore({
      read: async () => ((await ad.exists(statePath)) ? ad.read(statePath) : null),
      write: (s) => ad.write(statePath, s),
    });
    await this.ledger.load();
    this.local = new ObsidianLocalFS(this.app, this.t);

    this.registerView(VIEW_TYPE, (leaf) => new SyncStatusView(leaf, this));
    this.addSettingTab(new TdriveSettingTab(this.app, this));
    this.addRibbonIcon('refresh-cw', 'Tdrive Sync: ' + this.t('cmd.syncNow'), () => this.runSync());
    this.statusEl = this.addStatusBarItem();
    const cmd = (id, key, cb, opts) => this.addCommand({ id, name: 'Tdrive Sync: ' + this.t(key), callback: cb, ...(opts || {}) });
    cmd('test-connection', 'cmd.testConnection', () => this.testConnection());
    cmd('sync-now', 'cmd.syncNow', () => this.runSync());
    cmd('upload-current-file', 'cmd.uploadCurrent', () => this.uploadCurrent());
    cmd('download-remote-changes', 'cmd.downloadRemote', () => this.runSync({ mode: 'download-only' }));
    cmd('clean-empty-remote-folders', 'cmd.cleanEmpty', () => this.cleanEmptyRemoteFolders());
    cmd('approve-deletes', 'cmd.approveDeletes', () => this.approveDeletes());
    cmd('restore-deletes', 'cmd.restoreDeletes', () => this.restoreDeletes());
    cmd('preview-sync', 'cmd.preview', () => this.previewSync());
    cmd('view-versions', 'cmd.versions', () => this.viewVersions());
    cmd('share-file', 'cmd.share', () => this.shareCurrent());
    cmd('search-remote', 'cmd.search', () => this.searchRemote());
    cmd('choose-folders', 'cmd.chooseFolders', () => this.chooseSyncFolders());
    cmd('view-conflicts', 'cmd.viewConflicts', () => this.activateView());
    cmd('view-sync-log', 'cmd.viewLog', () => this.activateView());
    cmd('export-log', 'cmd.exportLog', () => this.exportLog());
    cmd('pause-sync', 'cmd.pause', () => this.pause());
    cmd('resume-sync', 'cmd.resume', () => this.resume());
    this.registerChangeSync();
    this.setupAutoSync(); this.refreshStatus();
    if (this.settings.syncOnStartup) this.app.workspace.onLayoutReady(() => window.setTimeout(() => this.runSync({ silent: true }), 5000));
  }

  onunload() {
    if (this._timer) window.clearInterval(this._timer);
    if (this._changeTimer) window.clearTimeout(this._changeTimer);
    if (this.engine) this.engine.pause();
  }

  /** (Re)computes the translation function from the language setting. */
  applyLanguage() { this.lang = resolveLang(this.settings.language); this.t = makeT(this.lang); if (this.local) this.local.t = this.t; }

  /** True when auto-syncs are allowed right now (schedule window). Manual runs are never blocked by this. */
  withinSchedule() {
    const s = this.settings;
    if (!s.scheduleEnabled) return true;
    const h = new Date().getHours();
    const a = Number(s.scheduleStart) || 0; const b = Number(s.scheduleEnd) || 0;
    if (a === b) return true;
    return a < b ? (h >= a && h < b) : (h >= a || h < b);   // wraps midnight when end < start
  }

  /** Debounced auto-sync on local edits (opt-in). Sync's own writes are ignored via the running flag and a
   *  short quiet window after each run, so a sync never re-triggers itself in a loop. "Instant" mode uses a 1s
   *  debounce and also syncs when the app regains focus. */
  registerChangeSync() {
    const vault = this.app && this.app.vault;
    if (!vault || typeof vault.on !== 'function') return;   // adapter without events (tests/older builds)
    const schedule = (file) => {
      if (!(this.settings.syncOnChange || this.settings.instantSync) || this.settings.paused) return;
      if (!this.settings.apiKey) return;
      if (this.engine && this.engine.running) return;
      if (Date.now() < this._quietUntil) return;
      if (!this.withinSchedule()) return;
      const p = file && file.path ? String(file.path) : '';
      if (p.startsWith('.')) return;                        // .obsidian, hidden files, sync-state
      if (this._changeTimer) window.clearTimeout(this._changeTimer);
      const secs = this.settings.instantSync ? 1 : Math.max(2, Number(this.settings.syncOnChangeDelaySeconds) || 5);
      this._changeTimer = window.setTimeout(() => { this._changeTimer = null; this.runSync({ silent: true, reason: 'change' }); }, secs * 1000);
    };
    this.registerEvent(this.app.vault.on('modify', schedule));
    this.registerEvent(this.app.vault.on('create', schedule));
    this.registerEvent(this.app.vault.on('delete', schedule));
    this.registerEvent(this.app.vault.on('rename', (f) => schedule(f)));
    // Near-instant: catch up as soon as the app is focused again (covers edits made while backgrounded).
    if (typeof window !== 'undefined' && window.addEventListener) {
      const onFocus = () => { if (this.settings.instantSync && !this.settings.paused && this.settings.apiKey && !(this.engine && this.engine.running) && this.withinSchedule()) this.runSync({ silent: true, reason: 'focus' }); };
      window.addEventListener('focus', onFocus);
      if (this.register) this.register(() => window.removeEventListener('focus', onFocus));
    }
  }

  defaultRemoteFolder() { return '/Obsidian/' + this.app.vault.getName() + '/'; }
  redact(str) { return this.engine ? this.engine.client.redact(str) : String(str).replace(/tdk_[A-Za-z0-9_\-]+/g, 'tdk_***'); }
  async saveSettings() { await this.saveData(this.settings); }

  /** Shows a styled notification. Non-technical styles render real Lucide icons (Obsidian's icon set) next to
   *  each value; when the DOM/icons are unavailable (or for the technical style) the plain text is shown. */
  showNotice(fmt) {
    if (!fmt || !fmt.text) return;
    const timeout = fmt.timeout || 0;
    const notice = new Notice(fmt.text, timeout);
    const rows = fmt.rows;
    if (!rows || !rows.length || typeof setIcon !== 'function') return;
    const el = notice.noticeEl || notice.containerEl;
    if (!el || typeof el.empty !== 'function' || typeof el.createDiv !== 'function') return;
    // DOM available: swap the plain message for the icon-rendered version.
    el.empty(); el.addClass('tdrive-notice');
    for (const row of rows) buildNoticeLine(el.createDiv({ cls: 'tdrive-notice-line' }), row);
  }

  notify() { this.listeners.forEach((h) => { try { h(); } catch (e) { /* view gone */ } }); this.refreshStatus(); }

  refreshStatus() {
    if (!this.statusEl) return;
    const m = this.ledger.data.meta; const c = this.ledger.conflictKeys().length;
    let txt = 'Tdrive: '; let icon = null;
    if (this.settings.paused) txt += this.t('status.paused');
    else if (this.engine && this.engine.running && this.progress) {
      const pr = NOTIFY.formatProgress(this.settings.notifyStyle, this.t, this.progress);
      icon = pr.icon; txt += pr.text;
    } else if (m.lastRun && m.lastRun.status !== 'ok') txt += m.lastRun.status;
    else txt += m.lastSuccessAt ? this.t('status.synced') : '—';
    if (c) txt += ` ⚠${c}`;
    // Real icon when the status-bar element allows it, otherwise plain text (never breaks the old behaviour).
    const el = this.statusEl;
    if (el && typeof el.empty === 'function' && typeof el.createSpan === 'function' && typeof setIcon === 'function') {
      el.empty();
      const box = el.createSpan({ cls: 'tdrive-status' });
      if (icon) {
        const g = box.createSpan({ cls: 'tdrive-status-icon' });
        if (!applyIcon(g, NOTIFY.ICONS[icon])) g.setText(NOTIFY.GLYPH[icon] || '');
      }
      box.createSpan({ text: (icon ? ' ' : '') + txt });
    } else el.setText(txt);
  }

  makeClient() {
    const client = new TdriveClient({ baseUrl: this.settings.baseUrl, apiKey: this.settings.apiKey, transport: makeThrottledTransport(() => this.settings) });
    const known = this.ledger && this.ledger.data && this.ledger.data.meta && this.ledger.data.meta.uploadChunkSize;
    if (known) client._uploadChunk = known;   // reuse the server's enforced part size (avoids a re-opened session)
    // Retry notice (once per run, so notifications stay concise). Pure formatting; no engine change.
    client.onRetry = (info) => { if (this._retryNotified) return; this._retryNotified = true; this.showNotice(NOTIFY.formatRetry(this.settings.notifyStyle, this.t, info)); };
    return client;
  }

  /** Returns a cached CryptoManager when E2EE is on and a passphrase is set, else null (plaintext).
   *  The salt is a constant (or a user-set value) so all devices with the same passphrase agree on the key. */
  makeCrypto() {
    const s = this.settings;
    if (!s.e2eeEnabled || !s.e2eePassphrase) { this._crypto = null; this._cryptoTag = ''; return null; }
    const salt = s.e2eeSalt || CryptoManager.DEFAULT_SALT || 'tdrive-sync-e2ee-v1';
    const tag = s.e2eePassphrase + '|' + salt;
    if (!this._crypto || this._cryptoTag !== tag) { this._crypto = new CryptoManager(s.e2eePassphrase, salt); this._cryptoTag = tag; }
    return this._crypto;
  }

  /** Returns a cached Compressor when compression is enabled, else null (store bytes as-is). */
  makeCompressor() {
    const s = this.settings;
    if (!s.compressTextFiles) return null;
    return new Compressor({ minBytes: Math.max(4, Number(s.compressMinKB) || 64) * 1024 });
  }

  makeEngine() {
    const client = this.makeClient();
    const getSettings = () => ({
      ...this.settings,
      remoteFolder: this.settings.remoteFolder || this.defaultRemoteFolder(),
      maxFileBytes: this.settings.maxFileSizeMB > 0 ? this.settings.maxFileSizeMB * 1024 * 1024 : 0,
      cellularMaxFileBytes: this.settings.cellularMaxFileMB > 0 ? this.settings.cellularMaxFileMB * 1024 * 1024 : 0,
      streamThresholdBytes: this.settings.streamThresholdMB > 0 ? this.settings.streamThresholdMB * 1024 * 1024 : 8 * 1024 * 1024,
      dedupeUploads: this.settings.dedupeUploads,
      networkIsCellular: networkLooksCellular,
    });
    let lastTick = 0;
    this.engine = new SyncEngine({
      client, local: this.local, manifest: this.ledger, getSettings, crypto: this.makeCrypto(), compressor: this.makeCompressor(),
      onProgress: (p) => { this.progress = p; const now = Date.now(); if (now - lastTick > 400) { lastTick = now; this.notify(); } },
    });
    return this.engine;
  }

  async testConnection() {
    try {
      const client = this.makeClient();
      await client.me(); const lim = await client.limits();
      let part = null;
      try { part = await client.uploadChunkSize(); } catch (e) { /* optional */ }
      this.connection = `✓ ${new Date().toLocaleTimeString()} — ${Math.round((part || lim.chunkBytes) / 1024)} KB`;
      new Notice(this.t('notice.connectionOk'));
    } catch (e) {
      this.connection = '✗ ' + this.redact(e.message);
      new Notice(this.t('notice.connectionFail', { msg: this.redact(e.message) }), 8000);
    }
    this.notify();
  }

  async runSync(opts = {}) {
    const style = this.settings.notifyStyle;
    if (this.settings.paused) { if (!opts.silent) this.showNotice(NOTIFY.formatState(style, this.t, 'paused')); return null; }
    if (!this.settings.apiKey) { if (!opts.silent) this.showNotice(NOTIFY.formatState(style, this.t, 'noApiKey')); return null; }
    if (this.engine && this.engine.running) { if (!opts.silent) this.showNotice(NOTIFY.formatState(style, this.t, 'busy')); return null; }
    this._retryNotified = false;
    const started = Date.now();
    let rep;
    try { rep = await this.makeEngine().syncNow(opts); }
    catch (e) { new Notice('Tdrive: ' + this.redact(e.message), 8000); this.notify(); return null; }
    rep.durationMs = Date.now() - started;   // for the detailed style; the engine is untouched
    if (rep.status === 'locked') {
      if (!opts.silent) this.showNotice(NOTIFY.formatState(style, this.t, 'locked'));
      this.notify();
      return rep;
    }
    const blockedNotice = NOTIFY.formatWarning(style, this.t, rep);
    if (blockedNotice && !opts.silent) this.showNotice(blockedNotice);
    if (rep.status === 'deferred') {
      // The server is briefly busy (retried, still deferred). Treat it as "try again soon", not a failure.
      if (!opts.silent) this.showNotice({ style, text: (style === 'technical' ? `Tdrive: ${this.t('notify.state.deferred')}` : this.t('notify.state.deferred')), rows: style === 'technical' ? null : [[{ icon: 'retry', text: this.t('notify.state.deferred') }]], timeout: 9000 });
    } else if (!opts.silent) this.showNotice(NOTIFY.formatResult(style, this.t, rep));
    this._quietUntil = Date.now() + 3000;   // ignore our own writes firing vault events right after
    // Remember the server's real part size so the next run does not re-open a probe session.
    const ch = this.engine && this.engine.client && this.engine.client._uploadChunk;
    if (ch && ch !== this.ledger.data.meta.uploadChunkSize) { this.ledger.data.meta.uploadChunkSize = ch; try { await this.ledger.save(); } catch (e) { /* non-fatal */ } }
    this.notify();
    return rep;
  }

  /** Writes the sync log + a small diagnostic header to a text file inside the plugin folder (never synced:
   *  it lives under .obsidian). Handy to attach when reporting a problem. */
  async exportLog() {
    try {
      const meta = this.ledger.data.meta;
      const head = [
        'Tdrive Sync — log export',
        'generated: ' + new Date().toISOString(),
        'plugin: ' + (this.manifest && this.manifest.id) + ' ' + (this.manifest && this.manifest.version),
        'server: ' + this.settings.baseUrl,
        'mode: ' + this.settings.mode + ' | deletion: ' + this.settings.deletionPolicy + ' | e2ee: ' + (this.settings.e2eeEnabled ? 'on' : 'off'),
        'lastRun: ' + JSON.stringify(meta.lastRun),
        'lastSuccessAt: ' + (meta.lastSuccessAt ? new Date(meta.lastSuccessAt).toISOString() : '—'),
        'open conflicts: ' + this.ledger.conflictKeys().length + ' | files tracked: ' + this.ledger.count(),
        '---',
      ].join('\n');
      const body = this.ledger.data.log.map((l) => `${new Date(l.t).toISOString()} [${l.level}] ${l.msg}`).join('\n');
      const path = this.manifest.dir + '/tdrive-sync-log.txt';
      await this.app.vault.adapter.write(path, this.redact(head + '\n' + body + '\n'));
      new Notice(this.t('notice.logExported', { path }), 8000);
    } catch (e) { new Notice(this.t('notice.logExportFailed', { msg: this.redact(e.message) }), 8000); }
  }

  /** Computes what a sync would do and shows it without changing anything. */
  async previewSync() {
    const style = this.settings.notifyStyle;
    if (this.settings.paused) { this.showNotice(NOTIFY.formatState(style, this.t, 'paused')); return; }
    if (!this.settings.apiKey) { this.showNotice(NOTIFY.formatState(style, this.t, 'noApiKey')); return; }
    if (this.engine && this.engine.running) { this.showNotice(NOTIFY.formatState(style, this.t, 'busy')); return; }
    this._retryNotified = false;
    try {
      const rep = await this.makeEngine().syncNow({ dryRun: true });
      new PreviewModal(this.app, this, rep).open();
    } catch (e) { new Notice('Tdrive: ' + this.redact(e.message), 8000); }
  }

  /** Restores the remote files deleted by the most recent sync from the Tdrive trash. */
  async restoreDeletes() {
    if (!this.settings.apiKey) { new Notice(this.t('notice.noApiKey')); return; }
    if (this.engine && this.engine.running) { new Notice(this.t('notice.busy')); return; }
    const audit = (this.ledger.data.meta.deletions || []);
    if (!audit.length) { new Notice(this.t('notice.noDeletes')); return; }
    try {
      const rep = await (this.engine || this.makeEngine()).restoreLastDeletions();
      let m = this.t('notice.restored', { restored: rep.restored });
      if (rep.failed) m += this.t('notice.restoredFailed', { failed: rep.failed });
      if (rep.skipped) m += this.t('notice.restoredSkipped', { skipped: rep.skipped });
      new Notice(m, 10000);
    } catch (e) { new Notice('Tdrive: ' + this.redact(e.message), 8000); }
    this.notify();
  }

  approveDeletes() {
    const pend = this.ledger.data.meta.blockedDeletes;
    if (!pend || !pend.items || !pend.items.length) { new Notice(this.t('notice.noPendingDeletes')); return; }
    if (pend.reason !== 'limit') { new Notice(this.t('notice.deleteBlockedReason', { reason: pend.reason }), 8000); return; }
    new ApproveDeletesModal(this.app, this, pend).open();
  }

  /** One-off cleanup of empty folders that older versions left on Tdrive (they never deleted folders). */
  async cleanEmptyRemoteFolders() {
    if (!this.settings.apiKey) { new Notice(this.t('notice.noApiKey')); return; }
    if (this.engine && this.engine.running) { new Notice(this.t('notice.busy')); return; }
    try {
      const rep = await this.makeEngine().cleanEmptyRemoteFolders();
      await this.ledger.save();
      new Notice(rep.failures.length ? this.t('notice.cleanFailed', { deleted: rep.deleted, failed: rep.failures.length, msg: this.redact(rep.failures[0].error) }) : this.t('notice.cleaned', { deleted: rep.deleted }), 8000);
    } catch (e) { new Notice('Tdrive: ' + this.redact(e.message), 8000); }
    this.notify();
  }

  async uploadCurrent() {
    const f = this.app.workspace.getActiveFile();
    if (!f) { new Notice(this.t('notice.noOpenFile')); return; }
    await this.runSync({ only: [f.path.normalize('NFC')] });
  }

  async resolveConflict(path, choice) {
    const eng = this.engine || this.makeEngine();
    await eng.conflicts.resolve(path, choice, eng);
    this.notify();
  }

  /** The remote id we recorded for a local path (needed for versions / share links). */
  remoteIdFor(path) {
    const e = this.ledger.getFile(path);
    return e && e.remoteId ? e.remoteId : null;
  }

  /** Lists archived versions of the current file and lets the user roll back to one. */
  async viewVersions() {
    const f = this.app.workspace.getActiveFile();
    if (!f) { new Notice(this.t('notice.noOpenFile')); return; }
    const id = this.remoteIdFor(f.path.normalize('NFC'));
    if (!id) { new Notice(this.t('versions.notSynced')); return; }
    new VersionsModal(this.app, this, f.path, id).open();
  }

  /** Creates a share link for the current file and copies it to the clipboard. */
  async shareCurrent() {
    const f = this.app.workspace.getActiveFile();
    if (!f) { new Notice(this.t('notice.noOpenFile')); return; }
    const id = this.remoteIdFor(f.path.normalize('NFC'));
    if (!id) { new Notice(this.t('versions.notSynced')); return; }
    try {
      const r = await this.makeClient().request({ method: 'POST', path: `/files/${encodeURIComponent(id)}/shares`, headers: { 'Content-Type': 'application/json' }, body: new TextEncoder().encode(JSON.stringify({ permission: 'download' })).buffer });
      const url = (r && (r.url || (r.share && r.share.url))) || '';
      if (!url) throw new Error('no url');
      try { await navigator.clipboard.writeText(url); new Notice(this.t('share.copied')); }
      catch (e) { new Notice(this.t('share.here', { url }), 12000); }
    } catch (e) { new Notice(this.t('share.failed', { msg: this.redact(e.message) }), 8000); }
  }

  /** Quick remote search (uses the server's own index). */
  searchRemote() { new SearchModal(this.app, this).open(); }

  /** Selective-sync folder picker. */
  chooseSyncFolders() { new FolderPickerModal(this.app, this).open(); }

  async pause() { this.settings.paused = true; await this.saveSettings(); if (this.engine) this.engine.pause(); new Notice(this.t('notice.pausedDone')); this.notify(); }
  async resume() { this.settings.paused = false; await this.saveSettings(); new Notice(this.t('notice.resumedDone')); this.notify(); }

  setupAutoSync() {
    if (this._timer) { window.clearInterval(this._timer); this._timer = null; }
    const m = this.settings.autoSyncMinutes;
    if (m > 0) {
      this._timer = window.setInterval(() => { if (this.withinSchedule()) this.runSync({ silent: true }); }, m * 60000);
      if (this.registerInterval) this.registerInterval(this._timer);
    }
  }

  async activateView() {
    let leaf = this.app.workspace.getLeavesOfType(VIEW_TYPE)[0];
    if (!leaf) {
      leaf = this.app.workspace.getRightLeaf(false);
      if (!leaf) { new Notice(this.t('notice.noView')); return; }
      await leaf.setViewState({ type: VIEW_TYPE, active: true });
    }
    this.app.workspace.revealLeaf(leaf);
  }

  /** Logs field NAMES only (never values/contents) of the API's real responses, to verify assumptions. */
  async probeApi() {
    try {
      const c = this.makeClient(); const shape = (o) => (o && typeof o === 'object' ? Object.keys(o).join(', ') : typeof o);
      const info = await c.info(); this.ledger.addLog('info', 'GET /api/v1 keys: ' + shape(info) + ' | upload keys: ' + shape(info && info.upload));
      const root = this.settings.remoteFolder || this.defaultRemoteFolder();
      const raw = await c.request({ path: '/files', query: { path: root, limit: 100 } });
      this.ledger.addLog('info', 'GET /files keys: ' + shape(raw));
      for (const k of Object.keys(raw || {})) if (Array.isArray(raw[k]) && raw[k][0] != null) this.ledger.addLog('info', `  ${k}: ${raw[k].length} item(s); [0] keys: ` + shape(raw[k][0]));
      await this.ledger.save(); new Notice(this.t('notice.probed'));
    } catch (e) { new Notice(this.t('notice.probeFailed', { msg: this.redact(e.message) }), 8000); }
    this.notify();
  }
}

module.exports = TdriveSyncPlugin;
