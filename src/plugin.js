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
const { randomUuid, folderPathFor, sha256Hex } = require('./util');

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
  if (ArrayBuffer.isView(body)) body = new Uint8Array(body.buffer, body.byteOffset, body.byteLength).slice().buffer;
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
  constructor(app, t) { this.app = app; this.t = t || ((s) => s); this._ownChanges = new Map(); this._writes = new Map(); }
  get ad() { return this.app.vault.adapter; }
  async list() { return this.app.vault.getFiles().map((f) => ({ path: f.path, size: f.stat.size, mtime: f.stat.mtime })); }
  readBinary(p) { return this.ad.readBinary(p); }
  async stat(p) { const s = await this.ad.stat(p); return s ? { size: s.size, mtime: s.mtime } : null; }
  exists(p) { return this.ad.exists(p); }
  async _isFile(p) { try { const s = await this.ad.stat(p); return !!s && s.type === 'file'; } catch (e) { return false; } }
  async _isDir(p) { try { const s = await this.ad.stat(p); return !!s && s.type === 'folder'; } catch (e) { return false; } }
  /** Only the supported vault API registers folders; disk-only parents cannot safely be used by createBinary. */
  async _mkdirs(p) {
    let cur = '';
    for (const seg of p.split('/').slice(0, -1)) {
      cur = cur ? cur + '/' + seg : seg;
      const indexed = this.app.vault.getAbstractFileByPath(cur);
      if (indexed && Array.isArray(indexed.children) && await this.ad.exists(cur)) continue;
      try {
        if (await this.ad.exists(cur)) throw new Error('Folder exists on disk but is not registered in the vault; wait for Obsidian to index it');
        this._trackOwnChange(cur, 'folder');
        await this.app.vault.createFolder(cur);
        const folder = this.app.vault.getAbstractFileByPath(cur);
        if (!folder || !Array.isArray(folder.children) || !(await this.ad.exists(cur))) throw new Error('Created folder was not registered in the vault');
      } catch (e) {
        this._ownChanges.delete(cur);
        const clash = (await this._isFile(cur)) ? this.t('local.nameClash') : '';
        throw new Error(this.t('local.couldNotCreate', { dir: cur, clash, msg: e.message || e }));
      }
    }
  }
  async writeBinary(p, buf, mtime, options = {}) {
    return this._writeBinary(p, buf, mtime, !!options.createOnly, options.expectedHash);
  }

  // Start the byte read at event dispatch, before any hashing or auto-sync settings checks.
  // Supplementary recovery, not CAS: a queued read can miss a transient external edit.
  // It must never be used as permission for an unchecked binary replacement.
  captureChange(p) {
    const write = this._writes.get(p);
    if (!write) return;
    try {
      const capture = this.ad.readBinary(p);
      write.captures.push(Promise.resolve(capture).then((data) => ({ data }), (error) => ({ error })));
    } catch (error) { write.captures.push(Promise.resolve({ error })); }
  }

  _trackOwnChange(p, hash) {
    for (const [path, change] of this._ownChanges) if (change.until < Date.now()) this._ownChanges.delete(path);
    this._ownChanges.set(p, { hash, until: Date.now() + 3000 });
  }

  async isOwnChange(p) {
    const change = this._ownChanges.get(p);
    if (!change || change.until < Date.now()) { this._ownChanges.delete(p); return false; }
    try {
      if (change.hash === null) return !(await this.ad.exists(p));
      if (change.hash === 'folder') return await this._isDir(p);
      return await sha256Hex(await this.ad.readBinary(p)) === change.hash;
    } catch (_) { return false; }
  }

  /** Recovery-backed, not an atomic replacement: retain old bytes until the supported vault commit is verified. */
  async _writeBinary(p, buf, mtime, createOnly, expectedHash) {
    if (expectedHash != null && !(await this.ad.exists(p))) throw Object.assign(new Error('Local file disappeared since it was reviewed; original was not replaced'), { code: 'LOCAL_CHANGED' });
    await this._mkdirs(p);
    const opts = Number.isFinite(mtime) ? { mtime } : undefined;
    const f = this.app.vault.getAbstractFileByPath(p);
    const exists = await this.ad.exists(p);
    if (createOnly && (f || exists)) {
      const error = new Error('Local file appeared during download: ' + p);
      error.code = 'LOCAL_FILE_EXISTS';
      throw error;
    }
    if (exists && !(f instanceof TFile)) throw new Error('Local file exists on disk but is not registered as a vault file: ' + p + '; wait for Obsidian to index it');
    if (f && !(f instanceof TFile)) throw new Error('Not a file: ' + p);
    const hash = await sha256Hex(buf);
    let backup = null;
    let backupVerified = false;
    let write = null;
    const recoveryPaths = [];
    const preserveChanges = async () => {
      if (!write) return false;
      // Events may arrive while recovery I/O is pending; drain every observed snapshot.
      while (write.processed < write.captures.length) {
        const snapshot = await write.captures[write.processed++];
        if (snapshot.error) throw new Error('Could not capture an interleaving local edit: ' + (snapshot.error.message || snapshot.error));
        const capturedHash = await sha256Hex(snapshot.data);
        if (capturedHash === hash || write.hashes.has(capturedHash)) continue;
        write.changed = true;
        const recovery = await this._tempPath(p, 'edit');
        recoveryPaths.push(recovery);
        await this.ad.writeBinary(recovery, snapshot.data);
        if (await sha256Hex(await this.ad.readBinary(recovery)) !== capturedHash) throw new Error('Interleaving edit recovery verification failed');
        write.hashes.add(capturedHash);
      }
      return write.changed;
    };
    try {
      if (f) {
        const old = await this.ad.readBinary(p);
        const oldHash = await sha256Hex(old);
        if (expectedHash != null && oldHash !== expectedHash) throw Object.assign(new Error('Local file changed since it was reviewed; original was not replaced'), { code: 'LOCAL_CHANGED' });
        backup = await this._tempPath(p, 'backup');
        await this.ad.writeBinary(backup, old);
        if (await sha256Hex(await this.ad.readBinary(backup)) !== oldHash) throw new Error('Recovery backup verification failed');
        backupVerified = true;
        if (this.app.vault.getAbstractFileByPath(p) !== f || await sha256Hex(await this.ad.readBinary(p)) !== oldHash) throw new Error('Local file changed while preparing its recovery backup');
        write = { captures: [], processed: 0, hashes: new Set([oldHash]), changed: false };
        if (this._writes.has(p)) throw new Error('Another local write is already in progress');
        this._writes.set(p, write);
        this._trackOwnChange(p, hash);
        // Installed desktop/mobile adapters queue process's read + synchronous transform + write.
        // It is text-only, not OS CAS. Event snapshots cannot protect arbitrary binary writes.
        let oldText, newText;
        try {
          const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
          oldText = decoder.decode(old); newText = decoder.decode(buf);
          if (await sha256Hex(new TextEncoder().encode(oldText)) !== oldHash || await sha256Hex(new TextEncoder().encode(newText)) !== hash) oldText = undefined;
        } catch (_) { oldText = undefined; }
        if (oldText === undefined || typeof this.app.vault.process !== 'function') {
          const reason = oldText === undefined
            ? 'Obsidian cannot safely compare and replace arbitrary binary content in its write queue'
            : 'Safe existing-file replacement requires Obsidian\'s queued text process API';
          const error = new Error(reason + '; original retained. Incoming replacement was not written; keep the local file or download the incoming version as a new file.');
          error.code = 'LOCAL_UNSAFE_REPLACEMENT';
          throw error;
        }
        await this.app.vault.process(f, (current) => {
          if (this.app.vault.getAbstractFileByPath(p) !== f || current !== oldText) throw Object.assign(new Error('Local file changed before the queued text commit'), { code: 'LOCAL_CHANGED' });
          return newText;
        }, opts);
      } else {
        if (await this.ad.exists(p) || this.app.vault.getAbstractFileByPath(p)) {
          const error = new Error('Local file appeared before creation: ' + p);
          error.code = 'LOCAL_FILE_EXISTS';
          throw error;
        }
        this._trackOwnChange(p, hash);
        try { await this.app.vault.createBinary(p, buf, opts); }
        catch (e) {
          if (e.code === 'EEXIST' || /^(EEXIST\b|File already exists\.?$)/.test(e.message || '')) e.code = 'LOCAL_FILE_EXISTS';
          throw e;
        }
      }
      const committed = this.app.vault.getAbstractFileByPath(p);
      const disk = await this.ad.stat(p);
      if (!(committed instanceof TFile) || !disk || committed.stat.size !== disk.size || committed.stat.mtime !== disk.mtime || disk.size !== buf.byteLength || await sha256Hex(await this.ad.readBinary(p)) !== hash) throw new Error('Write did not produce matching vault index, disk stats, and content');
      if (await preserveChanges()) throw new Error('Local file changed during replacement; interleaving bytes retained in recovery (no baseline committed)');
      this._trackOwnChange(p, hash);
      if (backup) { try { await this.ad.remove(backup); } catch (_) { /* a redundant recovery copy is safe */ } }
      return { size: committed.stat.size, mtime: committed.stat.mtime };
    } catch (e) {
      this._ownChanges.delete(p);
      try { await preserveChanges(); } catch (captureError) { e = new Error((e.message || e) + '; ' + (captureError.message || captureError)); }
      const recovery = backup ? (backupVerified ? '; previous bytes retained in recovery backup ' : '; original was not replaced; incomplete recovery backup at ') + backup : '';
      const error = new Error('Could not write local file ' + p + ': ' + (e.message || e) + recovery + (recoveryPaths.length ? '; interleaving edit recovery at ' + recoveryPaths.join(', ') : ''));
      if (e.code) error.code = e.code;
      if (backup) error.backupPath = backup;
      if (recoveryPaths.length) error.recoveryPaths = recoveryPaths;
      throw error;
    } finally { if (write && this._writes.get(p) === write) this._writes.delete(p); }
  }
  async _tempPath(p, kind = 'part') {
    const i = p.lastIndexOf('/');
    let tmp;
    do { tmp = p.slice(0, i + 1) + '.' + p.slice(i + 1) + '.tdrive-' + kind + '-' + randomUuid(); }
    while (await this.ad.exists(tmp));
    return tmp;
  }

  /** Engine compatibility name; this is a recovery-backed vault write, not a proven atomic replacement. */
  async writeBinaryAtomic(p, buf, mtime, options = {}) {
    return this.writeBinary(p, buf, mtime, options);
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
    if (!f) return !(await this.ad.exists(p));         // an unindexed disk folder must be kept
    if (!Array.isArray(f.children)) return false;      // a file replaced the planned folder
    try { const l = await this.ad.list(p); if (!l || !Array.isArray(l.files) || !Array.isArray(l.folders) || l.files.length || l.folders.length) return false; } catch (e) { return false; }
    this._trackOwnChange(p, null);
    try {
      if (this.app.fileManager.trashFile) await this.app.fileManager.trashFile(f);
      else await this.app.vault.trash(f, true);
    } catch (e) { this._ownChanges.delete(p); throw e; }
    return true;
  }
  async trash(p) {
    const f = this.app.vault.getAbstractFileByPath(p);
    if (!f) throw new Error('File not found: ' + p);
    if (!(f instanceof TFile)) throw new Error('Not a file: ' + p);
    this._trackOwnChange(p, null);
    try {
      if (this.app.fileManager.trashFile) await this.app.fileManager.trashFile(f);
      else await this.app.vault.trash(f, true);
    } catch (e) { this._ownChanges.delete(p); throw e; }
  }
  /** Staging is streamed, but the supported indexed create API requires a buffered final commit. */
  get canStream() { return typeof this.ad.appendBinary === 'function' && typeof this.ad.readBinary === 'function' && typeof this.app.vault.createBinary === 'function'; }
  /** Streams to a private staging file. close commits a new file; abort removes only that staging file. */
  async beginStreamWrite(p) {
    if (!this.canStream) throw new Error('Streaming requires appendBinary, readBinary, and vault.createBinary support');
    if (await this.ad.exists(p)) throw new Error('Local file already exists: ' + p);
    await this._mkdirs(p);
    const tmp = await this._tempPath(p);
    const self = this; let closed = false;
    const abort = async () => { closed = true; if (await self.ad.exists(tmp)) await self.ad.remove(tmp); };
    try { await this.ad.writeBinary(tmp, new ArrayBuffer(0)); }
    catch (e) { try { await abort(); } catch (_) { /* staging cleanup */ } throw e; }
    return {
      async write(chunk) {
        if (closed) throw new Error('Stream is closed');
        const buf = chunk instanceof ArrayBuffer ? chunk : (chunk.buffer ? chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.byteLength) : chunk);
        await self.ad.appendBinary(tmp, buf);
      },
      async close() {
        if (closed) throw new Error('Stream is closed');
        closed = true;
        try {
          if (await self.ad.exists(p)) throw new Error('Local file appeared during download: ' + p);
          await self._writeBinary(p, await self.ad.readBinary(tmp), undefined, true);
          try { await abort(); } catch (_) { /* verified commit succeeded; redundant staging is safe */ }
          return p;
        } catch (e) { try { await abort(); } catch (_) { /* staging cleanup */ } throw e; }
      },
      abort,
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
    contentEl.empty(); contentEl.addClass('tdrive-sync-view');
    this._closed = false; const previewToken = this._previewToken = {};
    contentEl.createEl('h3', { text: t('conflict.title', { path: this.path }) });
    const c = this.plugin.ledger.getConflict(this.path);
    if (!c) {
      contentEl.createEl('p', { text: t('conflict.none') });
      contentEl.createEl('button', { text: t('common.close') }).onclick = () => this.close();
      return;
    }
    const reviewed = JSON.parse(JSON.stringify(c));
    const reviewedScope = this.plugin.ledger.data.meta.syncScope;
    contentEl.createEl('p', { text: t('conflict.body', { copy: c ? c.copyPath : '?' }) });
    const openFile = async (path) => {
      try {
        const file = this.plugin.app.vault.getAbstractFileByPath(path);
        if (!file || Array.isArray(file.children)) throw new Error(t('conflict.fileUnavailable'));
        this.close();
        await this.plugin.app.workspace.getLeaf(false).openFile(file);
      } catch (e) { new Notice(t('conflict.resolveFailed', { msg: this.plugin.redact(e.message) }), 8000); }
    };
    const tools = contentEl.createDiv({ cls: 'tdrive-sync-row' });
    const openLocal = tools.createEl('button', { text: t('conflict.openLocal') }); openLocal.onclick = () => openFile(this.path);
    const openRemote = tools.createEl('button', { text: t('conflict.openRemote') }); openRemote.onclick = () => openFile(c.copyPath);
    const reload = tools.createEl('button', { text: t('conflict.refresh') });
    reload.onclick = () => { if (!this._resolving) this.onOpen(); };
    // Simple, read-only side-by-side preview (no line-by-line merge editor): the local file next to the
    // conflict copy of the remote version, so the user can decide. Text files only, first 40 lines each.
    try {
      const local = this.plugin.app.vault.getAbstractFileByPath(this.path);
      const copyFile = c && c.copyPath ? this.plugin.app.vault.getAbstractFileByPath(c.copyPath) : null;
      const isText = (f) => f && /\.(md|markdown|txt|text|json|csv|yml|yaml|js|ts|css|html|xml)$/i.test(f.path);
      if (local && copyFile && isText(local) && isText(copyFile)) {
        const read = async (f) => { try { return await this.plugin.app.vault.read(f); } catch (e) { return null; } };
        Promise.all([read(local), read(copyFile)]).then(([a, b]) => {
          if (this._closed || this._previewToken !== previewToken || a == null || b == null) return;
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
    const status = contentEl.createEl('p', { cls: 'tdrive-conflict-result' });
    if (typeof status.setAttribute === 'function') { status.setAttribute('role', 'status'); status.setAttribute('aria-live', 'polite'); }
    const resultActions = contentEl.createDiv({ cls: 'tdrive-sync-row' });
    if (this.plugin.settings.paused) status.setText(t('notice.pausedDone'));
    else if (this.plugin._busy()) status.setText(t('notice.busy'));
    const buttons = [];
    const base = this.plugin.ledger.getFile(this.path);
    const mergeAvailable = !!(base && typeof base.baseText === 'string');
    const disable = (busy) => {
      reload.disabled = openLocal.disabled = openRemote.disabled = busy;
      for (const [button, key] of buttons) {
        const disabled = busy || key !== 'cancel' && (this.plugin._busy() || this.plugin.settings.paused || key === 'merge' && !mergeAvailable);
        if (typeof button.setDisabled === 'function') button.setDisabled(disabled);
        else if (button.buttonEl) button.buttonEl.disabled = disabled;
      }
    };
    const opts = [
      ['merge', 'conflict.merge', mergeAvailable ? 'conflict.mergeDesc' : 'conflict.mergeUnavailable'],
      ['local', 'conflict.local', 'conflict.localDesc'],
      ['remote', 'conflict.remote', 'conflict.remoteDesc'],
      ['both', 'conflict.both', 'conflict.bothDesc'],
      ['cancel', 'conflict.cancel', 'conflict.cancelDesc'],
    ];
    for (const [key, labelKey, descKey] of opts) {
      new Setting(contentEl).setName(t(labelKey)).setDesc(t(descKey)).addButton((b) => {
        buttons.push([b, key]);
        b.setButtonText(t(labelKey)).onClick(async () => {
          if (this._resolving || this._closed) return;
          if (key === 'cancel') { this.close(); return; }
          if (this.plugin._busy() || this.plugin.settings.paused) {
            status.setText(t(this.plugin.settings.paused ? 'notice.pausedDone' : 'notice.busy')); disable(false); return;
          }
          if (key === 'merge' && !mergeAvailable) { status.setText(t('conflict.mergeUnavailable')); return; }
          this._resolving = true; disable(true); resultActions.empty(); status.setText(t('conflict.resolving'));
          try {
            const result = await this.plugin.resolveConflict(this.path, key, { conflict: reviewed, scope: reviewedScope });
            if (result && result.resolved) { new Notice(t('conflict.resolved')); if (!this._closed) this.close(); }
            else if (!this._closed) {
              status.setText(t(result && result.draft ? 'conflict.mergeNeedsReview' : 'conflict.reviewAgain', { draft: result && result.draft || '' }));
              if (result && result.draft) resultActions.createEl('button', { text: t('conflict.openDraft') }).onclick = () => openFile(result.draft);
            }
          } catch (e) {
            const message = t('conflict.resolveFailed', { msg: this.plugin.redact(e.message) });
            if (!this._closed) status.setText(message);
            new Notice(message, 8000);
          } finally { this._resolving = false; if (!this._closed) disable(false); }
        });
      });
    }
    disable(false);
  }
  onClose() { this._closed = true; this._previewToken = null; this.contentEl.empty(); }
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
    const approvedDeletes = items.map((item) => ({ ...item }));
    const approvedScope = this.plugin.ledger.data.meta.syncScope;
    row.createEl('button', { text: t('pending.approve') }).onclick = () => { this.close(); this.plugin.runSync({ approveDeletes: true, approvedDeletes, approvedScope }); };
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
  constructor(app, plugin, path, remoteId, versions, scope) { super(app); this.plugin = plugin; this.path = path; this.remoteId = remoteId; this.versions = versions; this.scope = scope; }
  async onOpen() {
    const { contentEl } = this; const p = this.plugin; const t = p.t;
    contentEl.empty(); contentEl.addClass('tdrive-sync-view');
    contentEl.createEl('h3', { text: t('versions.title', { path: this.path }) });
    const status = contentEl.createEl('p', { text: t('common.loading') });
    try {
      const list = this.versions;
      if (!list.length) { status.setText(t('versions.none')); return; }
      status.setText(t('versions.hint'));
      for (const v of list) {
        const row = contentEl.createDiv({ cls: 'tdrive-sync-row' });
        const vid = v.id || v.versionId || v.version;
        row.createSpan({ text: `${new Date(v.created_at || v.createdAt || 0).toLocaleString()} · ${v.size != null ? v.size + ' B' : ''}` });
        row.createEl('button', { text: t('versions.restore') }).onclick = async () => {
          try {
            if (p._unloaded || p.settings.paused) throw new Error(t('notice.pausedDone'));
            await p._withOperation(async () => {
              const { engine, id, scope } = await p._remoteFileContext(this.path.normalize('NFC'));
              if (id !== this.remoteId || scope !== this.scope) throw new Error('Sync destination changed; reopen file versions.');
              const lock = await engine.acquireOperationLease();
              if (lock && lock.blocked) throw new Error(t('notice.busy'));
              p._checkOperation(engine);
              const current = await engine.client.getFile(id);
              p._checkOperation(engine);
              if (engine._capabilities && engine._capabilities.expectedContentVersion && current.contentVersion == null) throw new Error('Current content version missing; restore stopped.');
              await engine.client.request({ method: 'POST', path: `/files/${encodeURIComponent(id)}/versions/${encodeURIComponent(String(vid))}/restore`, headers: { 'Content-Type': 'application/json' }, body: new TextEncoder().encode(JSON.stringify(current.contentVersion == null ? {} : { expectedVersion: current.contentVersion })).buffer });
              engine._markRemoteMutated();
              p._checkOperation(engine);
            });
            new Notice(t('versions.restored')); this.close(); p.runSync({ silent: true });
          }
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
        const r = await p._withOperation(async () => {
          const engine = p.makeEngine();
          p._checkOperation(engine);
          const result = await engine.client.request({ path: '/search', query: { q, limit: 50 } });
          p._checkOperation(engine);
          return result;
        });
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
    line(p.lang === 'ar' ? 'نسخة الإضافة المحملة' : 'Loaded Plugin Version', meta.runtimeVersion || (p.manifest && p.manifest.version) || '?');
    line(t('status.state'), p.settings.paused ? t('status.paused') : (p._busy() ? t('status.running') : t('status.idle')));
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
    const blockedFolders = Object.values(meta.blockedFolders || {}).filter((b) => b && (!b.scope || b.scope === meta.syncScope));
    if (blockedFolders.length) {
      el.createEl('h4', { text: p.lang === 'ar' ? `حذف مجلدات مؤجل من الخادم (${blockedFolders.length})` : `Folder Deletions Blocked by Server (${blockedFolders.length})` });
      if (blockedFolders.some((b) => b.code === 'FOLDER_NOT_EMPTY')) el.createEl('p', { text: p.lang === 'ar' ? 'احتفظ Tdrive بالمجلدات لأن ملفات أو مجلدات جديدة وصلت قبل الحذف الذري. لم تحذف المحتويات الجديدة؛ أعد الفحص لمراجعتها.' : 'Tdrive preserved these folders because new files or folders arrived before the atomic empty-folder deletion. New arrivals were not deleted; recheck to review them.' });
      if (blockedFolders.some((b) => b.code !== 'FOLDER_NOT_EMPTY')) el.createEl('p', { text: p.lang === 'ar' ? 'يرفض Tdrive حذف هذه المجلدات بسبب جلسة رفع. تستمر مزامنة الملفات الأخرى؛ لن تلغى جلسة مكتملة للتحايل على الخطأ.' : 'Tdrive is refusing these folder deletions because of an upload session. Other files can still sync; committed sessions will not be aborted as a workaround.' });
      if (blockedFolders.some((b) => b.expiredUploadCount > 0)) el.createEl('p', { cls: 'mod-warning', text: p.lang === 'ar' ? 'الخادم ما زال يعد جلسات منتهية المدة نشطة. الحذف المؤجل يخص المجلد فقط، لا الملفات المتزامنة. لم تُلغَ هذه الجلسات أو تُحذف بياناتها؛ يلزم تنظيفها من خدمة API أو اتخاذ قرار صريح بشأنها.' : 'The server still classifies expired upload sessions as active. Only folder deletion is deferred, not synchronized files. These sessions and their data were not cancelled; API cleanup or an explicit decision is required.' });
      el.createEl('pre', { cls: 'tdrive-sync-log' }).setText(blockedFolders.map((b) => `${b.path}: ${b.code || 'UPLOAD_IN_PROGRESS'}${b.requestId ? ' [' + b.requestId + ']' : ''}\n${p.lang === 'ar' ? 'المحاولة التالية' : 'Next Retry'}: ${fmtTime(b.nextRetryAt)}`).join('\n'));
      for (const b of blockedFolders) for (const session of b.blockingUploads || []) el.createEl('div').setText(`${session.id}: ${session.status} · ${p.lang === 'ar' ? 'انتهاء المدة' : 'Expires'}: ${fmtTime(session.expiresAt)}${session.expired ? (p.lang === 'ar' ? ' (منتهية)' : ' (expired)') : ''}`);
      const recheck = el.createEl('button', { text: p.lang === 'ar' ? 'إعادة فحص API' : 'Recheck API' });
      recheck.disabled = p._busy();
      recheck.onclick = async () => {
        if (p._busy()) return;
        recheck.disabled = true;
        try { await p.runSync({ retryBlockedFolders: true }); }
        finally { recheck.disabled = p._busy(); }
      };
    }
    const pend = meta.blockedDeletes;
    if (pend && pend.items && pend.items.length) {
      el.createEl('h4', { text: t('status.blockedDeletes', { n: pend.items.length }) });
      const why = { limit: t('status.reasonLimit'), 'empty-local': t('status.reasonEmptyLocal'), 'empty-remote': t('status.reasonEmptyRemote') }[pend.reason] || pend.reason;
      el.createDiv().setText(t('status.reason', { why, at: fmtTime(pend.at) }));
      if (pend.reason === 'limit' || pend.reason === 'changed') el.createEl('button', { text: t('common.reviewApprove') }).onclick = () => new ApproveDeletesModal(p.app, p, pend).open();
    }
    const cs = p.ledger.conflictKeys();
    el.createEl('h4', { text: t('status.conflicts', { n: cs.length }) });
    if (cs.length) el.createEl('p', { text: t('conflict.pendingHint') });
    for (const path of cs) {
      const r = el.createDiv({ cls: 'tdrive-sync-row' }); r.createSpan({ text: path });
      const resolve = r.createEl('button', { text: t('status.resolve') }); resolve.disabled = p._busy();
      resolve.onclick = () => { if (!p._busy()) new ConflictModal(p.app, p, path).open(); };
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
    this._timer = null; this._changeTimer = null; this._startupTimer = null; this._unloaded = false; this._quietUntil = 0; this._crypto = null; this._cryptoTag = '';
    this._operation = null; this._pendingChange = false;
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
    if (this.manifest && typeof this.manifest.version === 'string') this.ledger.data.meta.runtimeVersion = this.manifest.version;
    await this._bindDeviceIdentity();
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
    cmd('view-conflicts', 'cmd.viewConflicts', () => this.viewConflicts());
    cmd('view-sync-log', 'cmd.viewLog', () => this.activateView());
    cmd('export-log', 'cmd.exportLog', () => this.exportLog());
    cmd('pause-sync', 'cmd.pause', () => this.pause());
    cmd('resume-sync', 'cmd.resume', () => this.resume());
    this.registerChangeSync();
    this.setupAutoSync(); this.refreshStatus();
    if (this.settings.syncOnStartup) this.app.workspace.onLayoutReady(() => {
      if (this._unloaded) return;
      this._startupTimer = window.setTimeout(() => { this._startupTimer = null; if (!this._unloaded && this.settings.syncOnStartup && this.withinSchedule()) this.runSync({ silent: true, reason: 'startup' }); }, 5000);
    });
  }

  /** Installation-local ownership must not be inherited from a copied plugin ledger. */
  async _bindDeviceIdentity() {
    const meta = this.ledger.data.meta;
    let id = typeof meta.deviceId === 'string' && meta.deviceId ? meta.deviceId : randomUuid();
    try {
      const storage = typeof window !== 'undefined' && window.localStorage;
      if (storage && typeof storage.getItem === 'function' && typeof storage.setItem === 'function') {
        const ad = this.app.vault.adapter;
        let path = null;
        try { if (typeof ad.getBasePath === 'function') path = ad.getBasePath(); } catch (_) { /* try the adapter property */ }
        if (typeof path !== 'string' || !path.trim()) { try { path = ad.basePath; } catch (_) { /* use the vault name */ } }
        const installation = typeof path === 'string' && path.trim() ? 'path:' + path.replace(/\\/g, '/').replace(/\/+$/, '') : 'name:' + this.app.vault.getName();
        const key = 'tdrive-sync:device-id:' + encodeURIComponent(installation);
        const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
        let stored = storage.getItem(key);
        if (typeof stored !== 'string' || !uuid.test(stored)) {
          storage.setItem(key, randomUuid());
          stored = storage.getItem(key);
          if (typeof stored !== 'string' || !uuid.test(stored)) throw new Error('Installation identity was not persisted');
        }
        id = stored;
      }
    } catch (_) { /* unavailable local storage retains the existing per-ledger owner */ }
    if (meta.deviceId !== id) {
      meta.deviceId = id;
      // Do not enable sync under an identity that could not be durably recorded.
      await this.ledger.save();
    }
  }

  onunload() {
    this._unloaded = true;
    if (this._timer) window.clearInterval(this._timer);
    if (this._changeTimer) window.clearTimeout(this._changeTimer);
    if (this._startupTimer) window.clearTimeout(this._startupTimer);
    this._timer = this._changeTimer = this._startupTimer = null;
    this._pendingChange = false;
    const releasing = this._cancelOperation();
    this._crypto = null; this._cryptoTag = '';
    return releasing;
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

  _busy() { return !!this._operation || !!(this.engine && this.engine.running); }

  /** The plugin guard spans all awaits, including the gap between metadata lookup and a mutation. */
  async _withOperation(fn) {
    if (this._unloaded || this.settings.paused) throw new Error(this.t('notice.pausedDone'));
    if (!this.settings.apiKey) throw new Error(this.t('notice.noApiKey'));
    if (this._busy()) throw new Error(this.t('notice.busy'));
    const operation = {};
    this._operation = operation;
    try { this.notify(); return await fn(); }
    finally {
      try { if (operation.engine) await operation.engine.releaseOperationLease(); }
      finally {
        if (operation.engine) operation.engine.running = false;
        if (this._operation === operation) this._operation = null;
        this._scheduleChangeSync();
        this.notify();
      }
    }
  }

  _cancelOperation() {
    const engine = this._operation && this._operation.engine;
    if (!engine) return Promise.resolve();
    engine.pause();
    return engine.releaseOperationLease().catch(() => { /* engine clears local lease state even if the server is unavailable */ });
  }

  _checkOperation(engine) {
    if (this._unloaded || this.settings.paused || engine.stopRequested) throw new Error(this.t('notice.pausedDone'));
    if (!this._operation || this._operation.engine !== engine || this.engine !== engine) throw new Error('Sync operation owner changed; retry the operation.');
    const before = engine.getSettings();
    if (Object.keys(before).some((k) => k in this.settings && JSON.stringify(before[k]) !== JSON.stringify(k === 'remoteFolder' ? folderPathFor(this.settings.remoteFolder || this.defaultRemoteFolder(), '') : this.settings[k]))) throw new Error('Sync settings changed; retry the operation with the current destination.');
  }

  _scheduleChangeSync() {
    if (this._changeTimer) { window.clearTimeout(this._changeTimer); this._changeTimer = null; }
    if (this._unloaded || !(this.settings.syncOnChange || this.settings.instantSync)) { this._pendingChange = false; return; }
    if (!this._pendingChange || this.settings.paused || !this.settings.apiKey || this._busy()) return;
    const secs = this.settings.instantSync ? 1 : Math.max(1, Number(this.settings.syncOnChangeDelaySeconds) || 2);
    const delay = this.withinSchedule() ? Math.max(secs * 1000, this._quietUntil - Date.now()) : 60000;
    this._changeTimer = window.setTimeout(() => {
      this._changeTimer = null;
      if (this._unloaded || !(this.settings.syncOnChange || this.settings.instantSync)) { this._pendingChange = false; return; }
      if (this.settings.paused || !this.settings.apiKey || this._busy() || Date.now() < this._quietUntil || !this.withinSchedule()) { this._scheduleChangeSync(); return; }
      this._pendingChange = false;
      this.runSync({ silent: true, reason: 'change' });
    }, delay);
  }

  /** Ignore only verified sync-owned content events; external edits remain pending while busy or quiet. */
  registerChangeSync() {
    const vault = this.app && this.app.vault;
    if (!vault || typeof vault.on !== 'function') return;   // adapter without events (tests/older builds)
    const schedule = (file) => {
      if (this._unloaded) return;
      const p = file && file.path ? String(file.path) : '';
      if (this.local) this.local.captureChange(p);
      if (!(this.settings.syncOnChange || this.settings.instantSync)) return;
      if (p.split('/').some((part) => part.startsWith('.'))) return;
      const changed = () => { if (!this._unloaded) { this._pendingChange = true; this._scheduleChangeSync(); } };
      if (this.local && this.local._ownChanges.has(p)) return this.local.isOwnChange(p).then((own) => { if (!own) changed(); }).catch(changed);
      else changed();
    };
    this.registerEvent(this.app.vault.on('modify', schedule));
    this.registerEvent(this.app.vault.on('create', schedule));
    this.registerEvent(this.app.vault.on('delete', schedule));
    // A rename/move fires with the OLD path. Recording it makes the move explicit and deterministic, so the file
    // can be moved in place on the server and then UPDATED (only changed parts) even if the user keeps editing it
    // right after the move — instead of being re-created at the new path and leaving a stale duplicate behind.
    this.registerEvent(this.app.vault.on('rename', (f, oldPath) => { this.rememberMove(oldPath, f && f.path); schedule(f); }));
    // Near-instant: catch up as soon as the app is focused again (covers edits made while backgrounded).
    if (typeof window !== 'undefined' && window.addEventListener) {
      const onFocus = () => { if (!this._unloaded && this.settings.instantSync) { this._pendingChange = true; this._scheduleChangeSync(); } };
      window.addEventListener('focus', onFocus);
      if (this.register) this.register(() => window.removeEventListener('focus', onFocus));
    }
  }

  defaultRemoteFolder() { return '/Obsidian/' + this.app.vault.getName() + '/'; }
  redact(str) {
    const client = this.engine && this.engine.client;
    if (client && typeof client.redact === 'function') return client.redact(str);
    const key = this.settings && this.settings.apiKey;
    const text = key ? String(str).split(key).join('***') : String(str);
    return text.replace(/tdk_[A-Za-z0-9_\-]+/g, 'tdk_***');
  }
  async saveSettings() {
    const engine = this._operation && this._operation.engine;
    if (engine) {
      const before = engine.getSettings();
      if (Object.keys(before).some((k) => k in this.settings && JSON.stringify(before[k]) !== JSON.stringify(k === 'remoteFolder' ? folderPathFor(this.settings.remoteFolder || this.defaultRemoteFolder(), '') : this.settings[k]))) this._cancelOperation();
    }
    await this.saveData(this.settings);
    this._scheduleChangeSync();
  }

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

  makeClient(settings = this.settings) {
    const client = new TdriveClient({ baseUrl: settings.baseUrl, apiKey: settings.apiKey, transport: makeThrottledTransport(() => settings) });
    const known = this.ledger && this.ledger.data && this.ledger.data.meta && this.ledger.data.meta.uploadChunkSize;
    if (known) client._uploadChunk = known;   // reuse the server's enforced part size (avoids a re-opened session)
    // Retry notice (once per run, so notifications stay concise). Pure formatting; no engine change.
    client.onRetry = (info) => { if (this._retryNotified) return; this._retryNotified = true; this.showNotice(NOTIFY.formatRetry(this.settings.notifyStyle, this.t, info)); };
    return client;
  }

  /** Returns a cached CryptoManager when E2EE is on; missing passphrases block the operation.
   *  The salt is a constant (or a user-set value) so all devices with the same passphrase agree on the key. */
  makeCrypto(s = this.settings) {
    if (!s.e2eeEnabled) { this._crypto = null; this._cryptoTag = ''; return null; }
    if (typeof s.e2eePassphrase !== 'string' || !s.e2eePassphrase.length) {
      this._crypto = null; this._cryptoTag = '';
      throw new Error('Encryption is enabled but the passphrase is empty');
    }
    const salt = s.e2eeSalt || CryptoManager.DEFAULT_SALT || 'tdrive-sync-e2ee-v1';
    const tag = JSON.stringify([s.e2eePassphrase, salt]);
    if (!this._crypto || this._cryptoTag !== tag) { this._crypto = new CryptoManager(s.e2eePassphrase, salt); this._cryptoTag = tag; }
    return this._crypto;
  }

  /** Returns a cached Compressor when compression is enabled, else null (store bytes as-is). */
  makeCompressor(s = this.settings) {
    if (!s.compressTextFiles) return null;
    return new Compressor({ minBytes: Math.max(4, Number(s.compressMinKB) || 64) * 1024 });
  }

  makeEngine() {
    const s = Object.freeze({
      ...this.settings,
      excludePatterns: Object.freeze([...(this.settings.excludePatterns || [])]),
      syncFolders: Object.freeze([...(this.settings.syncFolders || [])]),
      remoteFolder: folderPathFor(this.settings.remoteFolder || this.defaultRemoteFolder(), ''),
      maxFileBytes: this.settings.maxFileSizeMB > 0 ? this.settings.maxFileSizeMB * 1024 * 1024 : 0,
      cellularMaxFileBytes: this.settings.cellularMaxFileMB > 0 ? this.settings.cellularMaxFileMB * 1024 * 1024 : 0,
      streamThresholdBytes: this.settings.streamThresholdMB > 0 ? this.settings.streamThresholdMB * 1024 * 1024 : 8 * 1024 * 1024,
      dedupeUploads: this.settings.dedupeUploads,
      networkIsCellular: networkLooksCellular,
    });
    const crypto = this.makeCrypto(s);
    const client = this.makeClient(s);
    const getSettings = () => s;
    let lastTick = 0;
    const engine = new SyncEngine({
      client, local: this.local, manifest: this.ledger, getSettings, crypto, compressor: this.makeCompressor(s),
      operationValidity: () => { this._checkOperation(engine); return true; },
      onProgress: (p) => { this.progress = p; const now = Date.now(); if (now - lastTick > 400) { lastTick = now; this.notify(); } },
    });
    this.engine = engine;
    this._engineSettings = s;
    if (this._operation) this._operation.engine = engine;
    return engine;
  }

  async testConnection() {
    try {
      await this._withOperation(async () => {
        const engine = this.makeEngine(); const client = engine.client;
        await client.me(); this._checkOperation(engine);
        const lim = await client.limits(); this._checkOperation(engine);
        let part = null;
        try { part = await client.uploadChunkSize(); } catch (e) { /* optional */ }
        this._checkOperation(engine);
        this.connection = `✓ ${new Date().toLocaleTimeString()} — ${Math.round((part || lim.chunkBytes) / 1024)} KB`;
        new Notice(this.t('notice.connectionOk'));
      });
    } catch (e) {
      this.connection = '✗ ' + this.redact(e.message);
      new Notice(this.t('notice.connectionFail', { msg: this.redact(e.message) }), 8000);
    }
    this.notify();
  }

  async runSync(opts = {}) {
    if (this._unloaded) return null;
    const style = this.settings.notifyStyle;
    if (this.settings.paused) { if (!opts.silent) this.showNotice(NOTIFY.formatState(style, this.t, 'paused')); return null; }
    if (!this.settings.apiKey) { if (!opts.silent) this.showNotice(NOTIFY.formatState(style, this.t, 'noApiKey')); return null; }
    if (this._busy()) { if (!opts.silent) this.showNotice(NOTIFY.formatState(style, this.t, 'busy')); return null; }
    if (opts.reason && (!this.withinSchedule() || (opts.reason === 'change' && !(this.settings.syncOnChange || this.settings.instantSync)) || (opts.reason === 'startup' && !this.settings.syncOnStartup) || (opts.reason === 'interval' && !(this.settings.autoSyncMinutes > 0)))) return null;
    return this._withOperation(async () => {
      this._retryNotified = false;
      const started = Date.now();
      let engine;
      let rep;
      try { engine = this.makeEngine(); this._operation.engine = engine; rep = await engine.syncNow(opts); }
      catch (e) { new Notice('Tdrive: ' + this.redact(e.message), 8000); this.notify(); return null; }
      rep.durationMs = Date.now() - started;
      if (rep.status === 'locked') {
        if (!opts.silent) this.showNotice(NOTIFY.formatState(style, this.t, 'locked'));
        this.notify();
        return rep;
      }
      const blockedNotice = NOTIFY.formatWarning(style, this.t, rep);
      if (blockedNotice && !opts.silent) this.showNotice(blockedNotice);
      if (rep.status === 'blocked' && !opts.silent) {
        const n = (rep.blockedFolders || []).length;
        const codes = [...new Set((rep.blockedFolders || []).map((b) => b.code || 'UPLOAD_IN_PROGRESS'))].join(', ');
        const arrivals = (rep.blockedFolders || []).some((b) => b.code === 'FOLDER_NOT_EMPTY');
        this.showNotice({ text: this.lang === 'ar' ? `Tdrive: حذف ${n} مجلد مؤجل من الخادم (${codes}). ${arrivals ? 'احتفظ بالمحتويات الجديدة؛ راجع الحالة.' : 'تستمر مزامنة الملفات الأخرى؛ راجع الحالة.'}` : `Tdrive: ${n} folder deletion(s) deferred (${codes}). ${arrivals ? 'New arrivals are preserved' : 'Other files can still sync'}; see sync status.`, timeout: 9000 });
      } else if (rep.status === 'deferred') {
        // The server is briefly busy (retried, still deferred). Treat it as "try again soon", not a failure.
        if (!opts.silent) this.showNotice({ style, text: (style === 'technical' ? `Tdrive: ${this.t('notify.state.deferred')}` : this.t('notify.state.deferred')), rows: style === 'technical' ? null : [[{ icon: 'retry', text: this.t('notify.state.deferred') }]], timeout: 9000 });
      } else if (!opts.silent) this.showNotice(NOTIFY.formatResult(style, this.t, rep));
      this._quietUntil = Date.now() + 3000;
      const ch = engine.client._uploadChunk;
      if (ch && ch !== this.ledger.data.meta.uploadChunkSize) { this.ledger.data.meta.uploadChunkSize = ch; try { await this.ledger.save(); } catch (e) { /* non-fatal */ } }
      this.notify();
      return rep;
    });
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
    if (this._unloaded) return;
    const style = this.settings.notifyStyle;
    if (this.settings.paused) { this.showNotice(NOTIFY.formatState(style, this.t, 'paused')); return; }
    if (!this.settings.apiKey) { this.showNotice(NOTIFY.formatState(style, this.t, 'noApiKey')); return; }
    if (this._busy()) { this.showNotice(NOTIFY.formatState(style, this.t, 'busy')); return; }
    this._retryNotified = false;
    try {
      await this._withOperation(async () => {
        const rep = await this.makeEngine().syncNow({ dryRun: true });
        new PreviewModal(this.app, this, rep).open();
      });
    } catch (e) { new Notice('Tdrive: ' + this.redact(e.message), 8000); }
  }

  /** Restores the remote files deleted by the most recent sync from the Tdrive trash. */
  async restoreDeletes() {
    if (this._unloaded) return;
    if (!this.settings.apiKey) { new Notice(this.t('notice.noApiKey')); return; }
    if (this._busy()) { new Notice(this.t('notice.busy')); return; }
    const audit = (this.ledger.data.meta.deletions || []);
    if (!audit.length) { new Notice(this.t('notice.noDeletes')); return; }
    try {
      await this._withOperation(async () => {
        const rep = await this.makeEngine().restoreLastDeletions();
        let m = this.t('notice.restored', { restored: rep.restored });
        if (rep.failed) m += this.t('notice.restoredFailed', { failed: rep.failed });
        if (rep.skipped) m += this.t('notice.restoredSkipped', { skipped: rep.skipped });
        new Notice(m, 10000);
      });
    } catch (e) { new Notice('Tdrive: ' + this.redact(e.message), 8000); }
    this.notify();
  }

  approveDeletes() {
    const pend = this.ledger.data.meta.blockedDeletes;
    if (!pend || !pend.items || !pend.items.length) { new Notice(this.t('notice.noPendingDeletes')); return; }
    if (pend.reason !== 'limit' && pend.reason !== 'changed') { new Notice(this.t('notice.deleteBlockedReason', { reason: pend.reason }), 8000); return; }
    new ApproveDeletesModal(this.app, this, pend).open();
  }

  /** One-off cleanup of empty folders that older versions left on Tdrive (they never deleted folders). */
  async cleanEmptyRemoteFolders() {
    if (this._unloaded) return;
    if (!this.settings.apiKey) { new Notice(this.t('notice.noApiKey')); return; }
    if (this._busy()) { new Notice(this.t('notice.busy')); return; }
    try {
      await this._withOperation(async () => {
        const rep = await this.makeEngine().cleanEmptyRemoteFolders();
        await this.ledger.save();
        if ((rep.blockedFolders || []).length) new Notice(this.lang === 'ar' ? 'حذف بعض المجلدات مؤجل من خادم Tdrive؛ راجع حالة المزامنة.' : 'Some folder deletions are blocked by Tdrive; see sync status.', 8000);
        else new Notice(rep.failures.length ? this.t('notice.cleanFailed', { deleted: rep.deleted, failed: rep.failures.length, msg: this.redact(rep.failures[0].error) }) : this.t('notice.cleaned', { deleted: rep.deleted }), 8000);
      });
    } catch (e) { new Notice('Tdrive: ' + this.redact(e.message), 8000); }
    this.notify();
  }

  async uploadCurrent() {
    const f = this.app.workspace.getActiveFile();
    if (!f) { new Notice(this.t('notice.noOpenFile')); return; }
    await this.runSync({ only: [f.path.normalize('NFC')] });
  }

  async viewConflicts() {
    const paths = this.ledger.conflictKeys();
    const active = this.app.workspace.getActiveFile();
    const path = active && paths.includes(active.path.normalize('NFC')) ? active.path.normalize('NFC') : paths.length === 1 ? paths[0] : null;
    if (path) return new ConflictModal(this.app, this, path).open();
    return this.activateView();
  }

  async resolveConflict(path, choice, reviewed) {
    if (choice === 'cancel') return { resolved: false };
    if (this._unloaded || this.settings.paused) throw new Error(this.t('notice.pausedDone'));
    if (!this.settings.apiKey) throw new Error(this.t('notice.noApiKey'));
    const reviewState = (conflict) => { if (!conflict) return null; const { resolution, ...versions } = conflict; return JSON.stringify(versions); };
    try { return await this._withOperation(async () => {
      const eng = this.makeEngine();
      this._checkOperation(eng);
      if (reviewed && (reviewed.scope !== this.ledger.data.meta.syncScope || reviewState(reviewed.conflict) !== reviewState(this.ledger.getConflict(path)))) throw new Error('Conflict changed; reopen it before resolving.');
      try {
        const result = await eng.conflicts.resolve(path, choice, eng, { reviewed: reviewed && reviewed.conflict, checkActive: () => this._checkOperation(eng) });
        if (!result || !result.resolved) this.ledger.addLog('warn', this.redact(`Conflict remains open: ${path}${result && result.draft ? '; merge draft: ' + result.draft : ''}`));
        return result;
      }
      finally { this._quietUntil = Date.now() + 3000; this.notify(); }
    }); } catch (e) {
      this.ledger.addLog('error', this.redact(`Conflict resolution failed: ${path}: ${e.message}`));
      this.notify(); throw e;
    }
  }

  /** The remote id we recorded for a local path (needed for versions / share links). */
  async remoteIdFor(path) {
    if (this._unloaded) return null;
    return this._withOperation(async () => (await this._remoteFileContext(path)).id);
  }

  async _remoteFileContext(path) {
    const engine = this.makeEngine(); engine.running = true;
    this._operation.engine = engine;
    await engine.prepare({ acquireLease: false });
    this._checkOperation(engine);
    const e = this.ledger.getFile(path);
    return { engine, id: e && e.remoteId ? e.remoteId : null, scope: this.ledger.data.meta.syncScope };
  }

  /** Lists archived versions of the current file and lets the user roll back to one. */
  async viewVersions() {
    const f = this.app.workspace.getActiveFile();
    if (!f) { new Notice(this.t('notice.noOpenFile')); return; }
    try {
      await this._withOperation(async () => {
        const { engine, id, scope } = await this._remoteFileContext(f.path.normalize('NFC'));
        if (!id) { new Notice(this.t('versions.notSynced')); return; }
        const r = await engine.client.request({ path: `/files/${encodeURIComponent(id)}/versions` });
        this._checkOperation(engine);
        const versions = (r && (r.versions || r.items)) || (Array.isArray(r) ? r : []);
        new VersionsModal(this.app, this, f.path, id, versions, scope).open();
      });
    } catch (e) { new Notice(this.t('versions.failed', { msg: this.redact(e.message) }), 8000); }
  }

  /** Creates a share link for the current file and copies it to the clipboard. */
  async shareCurrent() {
    const f = this.app.workspace.getActiveFile();
    if (!f) { new Notice(this.t('notice.noOpenFile')); return; }
    try {
      const r = await this._withOperation(async () => {
        const { engine, id } = await this._remoteFileContext(f.path.normalize('NFC'));
        if (!id) { new Notice(this.t('versions.notSynced')); return null; }
        const lock = await engine.acquireOperationLease();
        if (lock && lock.blocked) throw new Error(this.t('notice.busy'));
        this._checkOperation(engine);
        const current = await engine.client.getFile(id);
        this._checkOperation(engine);
        const r = await engine.client.request({ method: 'POST', path: `/files/${encodeURIComponent(id)}/shares`, headers: { 'Content-Type': 'application/json' }, body: new TextEncoder().encode(JSON.stringify({ permission: 'download', ...(current.contentVersion == null ? {} : { expectedVersion: current.contentVersion }) })).buffer });
        engine._markRemoteMutated();
        this._checkOperation(engine);
        return r;
      });
      if (!r) return;
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

  async pause() { this.settings.paused = true; const releasing = this._cancelOperation(); this.notify(); await releasing; await this.saveSettings(); new Notice(this.t('notice.pausedDone')); }
  async resume() { this.settings.paused = false; await this.saveSettings(); new Notice(this.t('notice.resumedDone')); this.notify(); }

  /** Records a rename/move seen by the editor: old path -> new path. Bounded and persisted (best effort).
   *  The engine consumes these hints to perform an in-place move (same server file id) instead of a
   *  create-new + delete-old, which is what previously left stale duplicates after "move then edit". */
  rememberMove(oldPath, newPath) {
    if (this._unloaded) return;
    const from = String(oldPath || '').normalize('NFC');
    const to = String(newPath || '').normalize('NFC');
    if (!from || !to || from === to) return;
    if (from.startsWith('.') || to.startsWith('.')) return;         // hidden files are never synced
    try {
      const meta = this.ledger.data.meta;
      const hints = meta.moveHints || (meta.moveHints = {});
      hints[from] = { to, at: Date.now() };
      const keys = Object.keys(hints);
      if (keys.length > 500) {                                       // keep the map tiny
        keys.sort((a, b) => (hints[a].at || 0) - (hints[b].at || 0));
        for (const k of keys.slice(0, keys.length - 500)) delete hints[k];
      }
      this.ledger.save().catch(() => { /* best effort */ });
    } catch (e) { /* never break the editor on a bookkeeping failure */ }
  }

  setupAutoSync() {
    if (this._timer) { window.clearInterval(this._timer); this._timer = null; }
    if (this._unloaded) return;
    const m = this.settings.autoSyncMinutes;
    if (m > 0) {
      this._timer = window.setInterval(() => { if (this.settings.autoSyncMinutes > 0 && this.withinSchedule()) this.runSync({ silent: true, reason: 'interval' }); }, m * 60000);
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
      await this._withOperation(async () => {
        const engine = this.makeEngine(); const c = engine.client; const shape = (o) => (o && typeof o === 'object' ? Object.keys(o).join(', ') : typeof o);
        const info = await c.info(); this._checkOperation(engine);
        this.ledger.addLog('info', 'GET /api/v1 keys: ' + shape(info) + ' | upload keys: ' + shape(info && info.upload));
        const raw = await c.request({ path: '/files', query: { path: engine.getSettings().remoteFolder, limit: 100 } });
        this._checkOperation(engine);
        this.ledger.addLog('info', 'GET /files keys: ' + shape(raw));
        for (const k of Object.keys(raw || {})) if (Array.isArray(raw[k]) && raw[k][0] != null) this.ledger.addLog('info', `  ${k}: ${raw[k].length} item(s); [0] keys: ` + shape(raw[k][0]));
        await this.ledger.save(); this._checkOperation(engine); new Notice(this.t('notice.probed'));
      });
    } catch (e) { new Notice(this.t('notice.probeFailed', { msg: this.redact(e.message) }), 8000); }
    this.notify();
  }
}

module.exports = TdriveSyncPlugin;
