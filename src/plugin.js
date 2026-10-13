'use strict';
const obsidian = require('obsidian');
const { Plugin, PluginSettingTab, Setting, Notice, Modal, ItemView, requestUrl, TFile } = obsidian;
const { TdriveClient } = require('./TdriveClient');
const { ManifestStore } = require('./ManifestStore');
const { SyncEngine } = require('./SyncEngine');

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
};

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

class ObsidianLocalFS {
  constructor(app) { this.app = app; }
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
        const clash = (await this.ad.exists(cur)) ? ' (يوجد ملف بنفس الاسم)' : '';
        throw new Error('تعذر إنشاء المجلد المحلي "' + cur + '"' + clash + ': ' + (err.message || err));
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
      catch (e2) { throw new Error('تعذر كتابة الملف محليًا "' + p + '": ' + (e2.message || e.message)); }
    }
    if (!(await this.ad.exists(p))) throw new Error('الملف لم يظهر بعد الكتابة: ' + p);
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
}

const fmtTime = (ms) => (ms ? new Date(ms).toLocaleString() : '—');

class ConflictModal extends Modal {
  constructor(app, plugin, path) { super(app); this.plugin = plugin; this.path = path; }
  onOpen() {
    const { contentEl } = this;
    contentEl.createEl('h3', { text: 'تعارض في: ' + this.path });
    const c = this.plugin.ledger.getConflict(this.path);
    contentEl.createEl('p', { text: 'تغيّر الملف محليًا وعلى Tdrive منذ آخر مزامنة. النسخة البعيدة محفوظة محليًا باسم: ' + (c ? c.copyPath : '?') });
    const opts = [
      ['local', 'الاحتفاظ بالنسخة المحلية', 'يرفع المحلية، وتُنقل نسخة التعارض إلى سلة Obsidian. النسخة البعيدة القديمة تبقى في سلة Tdrive.'],
      ['remote', 'الاحتفاظ بالنسخة البعيدة', 'تُحفظ نسختك المحلية باسم "local backup" ثم تُطبَّق النسخة البعيدة.'],
      ['both', 'حفظ النسختين', 'تبقى المحلية في مكانها وتبقى نسخة التعارض كملف مستقل، وتُرفع المحلية.'],
      ['cancel', 'إلغاء', 'لا تغيير؛ يبقى التعارض معلقًا.'],
    ];
    for (const [key, label, desc] of opts) {
      new Setting(contentEl).setName(label).setDesc(desc).addButton((b) => b.setButtonText(label).onClick(async () => {
        this.close();
        if (key === 'cancel') return;
        try { await this.plugin.resolveConflict(this.path, key); new Notice('تم حل التعارض'); }
        catch (e) { new Notice('تعذر حل التعارض: ' + this.plugin.redact(e.message), 8000); }
      }));
    }
  }
  onClose() { this.contentEl.empty(); }
}

class ApproveDeletesModal extends Modal {
  constructor(app, plugin, pending) { super(app); this.plugin = plugin; this.pending = pending; }
  onOpen() {
    const { contentEl } = this; const items = this.pending.items || [];
    contentEl.createEl('h3', { text: `حذف معلّق (${items.length} ملف)` });
    contentEl.createEl('p', { text: 'هذه الملفات حُذفت في جهة واحدة فقط، وسيُنقل نظيرها في الجهة الأخرى إلى السلة (قابل للاسترجاع). راجع القائمة قبل الموافقة.' });
    const pre = contentEl.createEl('pre', { cls: 'tdrive-sync-log' });
    pre.setText(items.slice(0, 60).map((i) => (i.action === 'deleteRemote' ? '☁ حذف من Tdrive: ' : '💻 حذف محلي: ') + i.path).join('\n') + (items.length > 60 ? `\n… و${items.length - 60} أخرى` : ''));
    const row = contentEl.createDiv({ cls: 'tdrive-sync-row' });
    row.createEl('button', { text: 'موافقة وتنفيذ' }).onclick = () => { this.close(); this.plugin.runSync({ approveDeletes: true }); };
    row.createEl('button', { text: 'إلغاء' }).onclick = () => this.close();
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
    const p = this.plugin; const meta = p.ledger.data.meta; const last = meta.lastRun;
    el.createEl('h3', { text: 'Tdrive Sync' });
    const box = el.createDiv({ cls: 'tdrive-sync-box' });
    const line = (k, v) => box.createDiv().setText(`${k}: ${v}`);
    line('الاتصال', p.connection);
    line('الحالة', p.settings.paused ? 'متوقفة مؤقتًا' : (p.engine && p.engine.running ? 'جارية' : 'خاملة'));
    if (p.progress && p.engine && p.engine.running) line('التقدم', `${p.progress.done}/${p.progress.total} ${p.progress.current || ''}`);
    line('آخر مزامنة ناجحة', fmtTime(meta.lastSuccessAt));
    if (last) {
      line('آخر تشغيل', `${fmtTime(last.at)} — ${last.status}`);
      line('رُفع / نُزّل / تجاوز', `${last.uploaded} / ${last.downloaded} / ${last.skipped}`);
      line('تعارضات / أخطاء', `${p.ledger.conflictKeys().length} / ${last.errors}`);
    }
    const row = el.createDiv({ cls: 'tdrive-sync-row' });
    row.createEl('button', { text: 'مزامنة الآن' }).onclick = () => p.runSync();
    row.createEl('button', { text: p.settings.paused ? 'استئناف' : 'إيقاف مؤقت' }).onclick = () => (p.settings.paused ? p.resume() : p.pause());
    row.createEl('button', { text: 'اختبار الاتصال' }).onclick = () => p.testConnection();
    const pend = meta.blockedDeletes;
    if (pend && pend.items && pend.items.length) {
      el.createEl('h4', { text: `حذف محجوب (${pend.items.length})` });
      const why = { limit: 'تجاوز حد الحذف', 'empty-local': 'الـ Vault المحلي يبدو فارغًا', 'empty-remote': 'المجلد البعيد يبدو فارغًا' }[pend.reason] || pend.reason;
      el.createDiv().setText(`السبب: ${why} — آخر فحص ${fmtTime(pend.at)}`);
      if (pend.reason === 'limit') el.createEl('button', { text: 'مراجعة والموافقة…' }).onclick = () => new ApproveDeletesModal(p.app, p, pend).open();
    }
    const cs = p.ledger.conflictKeys();
    el.createEl('h4', { text: `التعارضات (${cs.length})` });
    for (const path of cs) {
      const r = el.createDiv({ cls: 'tdrive-sync-row' }); r.createSpan({ text: path });
      r.createEl('button', { text: 'حل…' }).onclick = () => new ConflictModal(p.app, p, path).open();
    }
    el.createEl('h4', { text: 'سجل المزامنة' });
    const pre = el.createEl('pre', { cls: 'tdrive-sync-log' });
    pre.setText(p.ledger.data.log.slice(-100).reverse().map((l) => `${new Date(l.t).toLocaleTimeString()} [${l.level}] ${l.msg}`).join('\n'));
  }
}

class TdriveSettingTab extends PluginSettingTab {
  constructor(app, plugin) { super(app, plugin); this.plugin = plugin; }
  display() {
    const { containerEl } = this; const p = this.plugin; const s = p.settings;
    containerEl.empty();
    containerEl.createEl('h2', { text: 'Tdrive Sync' });
    containerEl.createEl('p', { cls: 'mod-warning', text: 'تنبيه أمني: يُحفظ مفتاح API كنص عادي في ملف data.json داخل مجلد الإضافة. استخدم مفتاحًا مخصصًا لهذه الإضافة يمكن إلغاؤه من Tdrive، ولا تشارك مجلد .obsidian أو تضعه في مستودع عام.' });
    new Setting(containerEl).setName('عنوان خادم Tdrive').addText((t) => t.setValue(s.baseUrl).onChange(async (v) => { s.baseUrl = v.trim(); await p.saveSettings(); }));
    new Setting(containerEl).setName('API Key').setDesc('يبدأ عادةً بـ tdk_ — يُرسل فقط إلى عنوان الخادم أعلاه.')
      .addText((t) => { t.inputEl.type = 'password'; t.setValue(s.apiKey).onChange(async (v) => { s.apiKey = v.trim(); await p.saveSettings(); }); });
    new Setting(containerEl).setName('المجلد البعيد').setDesc('مثال: /Obsidian/MyVault/ — إن تُرك فارغًا يُستخدم /Obsidian/<اسم الـ Vault>/')
      .addText((t) => t.setPlaceholder(p.defaultRemoteFolder()).setValue(s.remoteFolder).onChange(async (v) => { s.remoteFolder = v.trim(); await p.saveSettings(); }));
    new Setting(containerEl).setName('نمط المزامنة').addDropdown((d) => d
      .addOptions({ 'two-way': 'ثنائية الاتجاه', 'upload-only': 'رفع فقط', 'download-only': 'تنزيل فقط' })
      .setValue(s.mode).onChange(async (v) => { s.mode = v; await p.saveSettings(); }));
    new Setting(containerEl).setName('سياسة الحذف').setDesc('"سلة": الحذف يُنقل إلى سلة Tdrive / سلة Obsidian (قابل للاسترجاع). "إيقاف": لا يُحذف شيء أبدًا.')
      .addDropdown((d) => d.addOptions({ trash: 'نقل إلى السلة', off: 'إيقاف الحذف' }).setValue(s.deletionPolicy).onChange(async (v) => { s.deletionPolicy = v; await p.saveSettings(); }));
    new Setting(containerEl).setName('حد الحذف لكل مزامنة').setDesc('إن تجاوز عدد الحذف هذا الحد تُحجب كل عمليات الحذف.')
      .addText((t) => t.setValue(String(s.maxDeletesPerRun)).onChange(async (v) => { const n = parseInt(v, 10); if (n >= 0) { s.maxDeletesPerRun = n; await p.saveSettings(); } }));
    new Setting(containerEl).setName('المزامنة التلقائية (بالدقائق)').setDesc('0 = معطلة. تعمل فقط أثناء فتح التطبيق (قيد من Obsidian، خاصة على أندرويد).')
      .addText((t) => t.setValue(String(s.autoSyncMinutes)).onChange(async (v) => { const n = parseInt(v, 10); if (n >= 0) { s.autoSyncMinutes = n; await p.saveSettings(); p.setupAutoSync(); } }));
    new Setting(containerEl).setName('مزامنة عند فتح التطبيق').addToggle((t) => t.setValue(s.syncOnStartup).onChange(async (v) => { s.syncOnStartup = v; await p.saveSettings(); }));
    new Setting(containerEl).setName('استثناءات (نمط في كل سطر)').setDesc('المجلدات والملفات المخفية (.obsidian و .git ...) مستثناة دائمًا. مثال: Private/**')
      .addTextArea((t) => t.setValue(s.excludePatterns.join('\n')).onChange(async (v) => { s.excludePatterns = v.split('\n').map((x) => x.trim()).filter(Boolean); await p.saveSettings(); }));
    new Setting(containerEl).setName('اختبار الاتصال').addButton((b) => b.setButtonText('اختبار').onClick(() => p.testConnection()));
    new Setting(containerEl).setName('الحالة والسجل').addButton((b) => b.setButtonText('فتح').onClick(() => p.activateView()));
    new Setting(containerEl).setName('إيقاف / استئناف المزامنة').addButton((b) => b.setButtonText(s.paused ? 'استئناف' : 'إيقاف').onClick(async () => { await (s.paused ? p.resume() : p.pause()); this.display(); }));
    new Setting(containerEl).setName('فحص شكل استجابات API').setDesc('يعرض أسماء الحقول التي ترجعها Tdrive (بدون محتوى ملفاتك) ضمن السجل — مفيد للإبلاغ عن مشاكل التوافق.')
      .addButton((b) => b.setButtonText('فحص').onClick(() => p.probeApi()));
  }
}

class TdriveSyncPlugin extends Plugin {
  async onload() {
    this.listeners = new Set(); this.progress = null; this.engine = null; this.connection = 'لم يُختبر بعد'; this._timer = null;
    this.settings = Object.assign({}, DEFAULTS, await this.loadData());
    const ad = this.app.vault.adapter; const statePath = this.manifest.dir + '/sync-state.json';
    this.ledger = new ManifestStore({
      read: async () => ((await ad.exists(statePath)) ? ad.read(statePath) : null),
      write: (s) => ad.write(statePath, s),
    });
    await this.ledger.load();
    this.local = new ObsidianLocalFS(this.app);

    this.registerView(VIEW_TYPE, (leaf) => new SyncStatusView(leaf, this));
    this.addSettingTab(new TdriveSettingTab(this.app, this));
    this.addRibbonIcon('refresh-cw', 'Tdrive Sync: Sync Now', () => this.runSync());
    this.statusEl = this.addStatusBarItem();
    const cmd = (id, name, cb) => this.addCommand({ id, name: 'Tdrive Sync: ' + name, callback: cb });
    cmd('test-connection', 'Test Connection', () => this.testConnection());
    cmd('sync-now', 'Sync Now', () => this.runSync());
    cmd('upload-current-file', 'Upload Current File', () => this.uploadCurrent());
    cmd('download-remote-changes', 'Download Remote Changes', () => this.runSync({ mode: 'download-only' }));
    cmd('clean-empty-remote-folders', 'Clean Empty Remote Folders', () => this.cleanEmptyRemoteFolders());
    cmd('approve-deletes', 'Approve Pending Deletions', () => this.approveDeletes());
    cmd('view-conflicts', 'View Conflicts', () => this.activateView());
    cmd('view-sync-log', 'View Sync Log', () => this.activateView());
    cmd('pause-sync', 'Pause Sync', () => this.pause());
    cmd('resume-sync', 'Resume Sync', () => this.resume());
    this.setupAutoSync(); this.refreshStatus();
    if (this.settings.syncOnStartup) this.app.workspace.onLayoutReady(() => window.setTimeout(() => this.runSync({ silent: true }), 5000));
  }

  onunload() { if (this._timer) window.clearInterval(this._timer); if (this.engine) this.engine.pause(); }

  defaultRemoteFolder() { return '/Obsidian/' + this.app.vault.getName() + '/'; }
  redact(t) { return this.engine ? this.engine.client.redact(t) : String(t).replace(/tdk_[A-Za-z0-9_\-]+/g, 'tdk_***'); }
  async saveSettings() { await this.saveData(this.settings); }
  notify() { this.listeners.forEach((h) => { try { h(); } catch (e) { /* view gone */ } }); this.refreshStatus(); }

  refreshStatus() {
    if (!this.statusEl) return;
    const m = this.ledger.data.meta; const c = this.ledger.conflictKeys().length;
    let t = 'Tdrive: ';
    if (this.settings.paused) t += 'متوقفة';
    else if (this.engine && this.engine.running && this.progress) t += `${this.progress.done}/${this.progress.total}`;
    else if (m.lastRun && m.lastRun.status !== 'ok') t += m.lastRun.status;
    else t += m.lastSuccessAt ? 'متزامنة' : '—';
    if (c) t += ` ⚠${c}`;
    this.statusEl.setText(t);
  }

  makeClient() {
    return new TdriveClient({ baseUrl: this.settings.baseUrl, apiKey: this.settings.apiKey, transport: obsidianTransport });
  }

  makeEngine() {
    const client = this.makeClient();
    const getSettings = () => ({ ...this.settings, remoteFolder: this.settings.remoteFolder || this.defaultRemoteFolder() });
    let lastTick = 0;
    this.engine = new SyncEngine({
      client, local: this.local, manifest: this.ledger, getSettings,
      onProgress: (p) => { this.progress = p; const now = Date.now(); if (now - lastTick > 400) { lastTick = now; this.notify(); } },
    });
    return this.engine;
  }

  async testConnection() {
    try {
      const client = this.makeClient();
      await client.me(); const lim = await client.limits();
      this.connection = `متصل ✓ (${new Date().toLocaleTimeString()}) — حجم الجزء ${Math.round(lim.chunkBytes / 1024)} KB`;
      new Notice('Tdrive: الاتصال ناجح');
    } catch (e) {
      this.connection = 'فشل: ' + this.redact(e.message);
      new Notice('Tdrive: فشل الاتصال — ' + this.redact(e.message), 8000);
    }
    this.notify();
  }

  async runSync(opts = {}) {
    if (this.settings.paused) { if (!opts.silent) new Notice('المزامنة متوقفة مؤقتًا'); return null; }
    if (!this.settings.apiKey) { if (!opts.silent) new Notice('أدخل API Key في إعدادات Tdrive Sync'); return null; }
    if (this.engine && this.engine.running) { if (!opts.silent) new Notice('مزامنة جارية بالفعل'); return null; }
    let rep;
    try { rep = await this.makeEngine().syncNow(opts); }
    catch (e) { new Notice('Tdrive: ' + this.redact(e.message), 8000); this.notify(); return null; }
    const msg = `↑${rep.uploaded} ↓${rep.downloaded} =${rep.skipped} ⚠${rep.conflicts} ✗${rep.errors}`;
    if (rep.blockedDeletes && rep.blockedDeletes.items.length && !opts.silent) new Notice(`Tdrive: حُجب حذف ${rep.blockedDeletes.items.length} ملف (${rep.blockedDeletes.reason}). راجعها من أمر "Approve Pending Deletions".`, 10000);
    if (rep.status === 'ok') { if (!opts.silent) new Notice('Tdrive: اكتملت المزامنة ' + msg); }
    else new Notice(`Tdrive: لم تكتمل المزامنة (${rep.status}) ${msg}` + (rep.failures[0] ? '\n' + rep.failures[0].error : ''), 10000);
    this.notify();
    return rep;
  }

  approveDeletes() {
    const pend = this.ledger.data.meta.blockedDeletes;
    if (!pend || !pend.items || !pend.items.length) { new Notice('لا يوجد حذف معلّق. شغّل المزامنة أولًا.'); return; }
    if (pend.reason !== 'limit') { new Notice('الحذف محجوب لسبب لا يُتجاوز بالموافقة: ' + pend.reason, 8000); return; }
    new ApproveDeletesModal(this.app, this, pend).open();
  }

  /** One-off cleanup of empty folders that older versions left on Tdrive (they never deleted folders). */
  async cleanEmptyRemoteFolders() {
    if (!this.settings.apiKey) { new Notice('أدخل API Key في إعدادات Tdrive Sync'); return; }
    if (this.engine && this.engine.running) { new Notice('مزامنة جارية بالفعل'); return; }
    try {
      const rep = await this.makeEngine().cleanEmptyRemoteFolders();
      await this.ledger.save();
      new Notice(rep.failures.length ? `حُذف ${rep.deleted} مجلد، وفشل ${rep.failures.length}: ${this.redact(rep.failures[0].error)}` : `تم حذف ${rep.deleted} مجلد فارغ من Tdrive (إلى السلة)`, 8000);
    } catch (e) { new Notice('Tdrive: ' + this.redact(e.message), 8000); }
    this.notify();
  }

  async uploadCurrent() {
    const f = this.app.workspace.getActiveFile();
    if (!f) { new Notice('لا يوجد ملف مفتوح'); return; }
    await this.runSync({ only: [f.path.normalize('NFC')] });
  }

  async resolveConflict(path, choice) {
    const eng = this.engine || this.makeEngine();
    await eng.conflicts.resolve(path, choice, eng);
    this.notify();
  }

  async pause() { this.settings.paused = true; await this.saveSettings(); if (this.engine) this.engine.pause(); new Notice('تم إيقاف المزامنة'); this.notify(); }
  async resume() { this.settings.paused = false; await this.saveSettings(); new Notice('تم استئناف المزامنة'); this.notify(); }

  setupAutoSync() {
    if (this._timer) { window.clearInterval(this._timer); this._timer = null; }
    const m = this.settings.autoSyncMinutes;
    if (m > 0) { this._timer = window.setInterval(() => this.runSync({ silent: true }), m * 60000); this.registerInterval(this._timer); }
  }

  async activateView() {
    let leaf = this.app.workspace.getLeavesOfType(VIEW_TYPE)[0];
    if (!leaf) { leaf = this.app.workspace.getRightLeaf(false); await leaf.setViewState({ type: VIEW_TYPE, active: true }); }
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
      await this.ledger.save(); new Notice('تم تسجيل شكل الاستجابات في السجل');
    } catch (e) { new Notice('فشل الفحص: ' + this.redact(e.message), 8000); }
    this.notify();
  }
}

module.exports = TdriveSyncPlugin;
