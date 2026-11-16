'use strict';

const MAX_LOG = 500;
const isRecord = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const own = (map, key) => Object.prototype.hasOwnProperty.call(map, key);

/** Local sync ledger: per-file state at last successful sync, open conflicts, and the operation log. */
class ManifestStore {
  constructor(storage, now = () => Date.now()) {
    this.storage = storage; // { read(): Promise<string|null>, write(str): Promise<void> }
    this.now = now;
    this.data = ManifestStore.empty();
    this._saveTail = Promise.resolve();
    this._readError = null;
    this._loading = false;
  }
  static empty() { return { version: 1, files: Object.create(null), folders: Object.create(null), conflicts: Object.create(null), log: [], meta: { lastRun: null, lastSuccessAt: null, deletions: [] } }; }

  async load() {
    if (this._loading) throw new Error('Sync ledger is already being read');
    this._loading = true;
    try {
      await this._saveTail;
      let raw = null;
      try { raw = await this.storage.read(); }
      catch (e) {
        this._readError = new Error('Could not read sync ledger; existing state must be recovered before saving: ' + (e.message || e));
        throw this._readError;
      }
      this._readError = null;
      if (!raw) { this.data = ManifestStore.empty(); return; }
      try {
        const d = JSON.parse(raw);
        if (!isRecord(d)) throw new Error('Invalid ledger');
        const data = ManifestStore.empty();
        for (const key of ['files', 'folders', 'conflicts']) {
          if (key in d && (!isRecord(d[key]) || Object.values(d[key]).some((entry) => !isRecord(entry)))) throw new Error('Invalid ledger ' + key);
          Object.assign(data[key], d[key]);
        }
        if ('meta' in d && !isRecord(d.meta)) throw new Error('Invalid ledger metadata');
        if ('log' in d && (!Array.isArray(d.log) || d.log.some((entry) => !isRecord(entry) || typeof entry.msg !== 'string'))) throw new Error('Invalid ledger log');
        if (d.meta && 'deletions' in d.meta && !Array.isArray(d.meta.deletions)) throw new Error('Invalid deletion audit');
        this.data = { ...data, version: d.version || 1, log: (d.log || []).slice(-MAX_LOG), meta: { ...data.meta, ...(d.meta || {}) } };
      } catch (e) {
        // A readable but corrupt ledger has no trustworthy destructive baseline.
        this.data = ManifestStore.empty();
        this.addLog('warn', 'Sync ledger was unreadable and has been reset; content will be compared on next sync.');
      }
    } finally { this._loading = false; }
  }
  async save() {
    if (this._loading) throw new Error('Sync ledger is being read; saving is blocked until loading finishes');
    if (this._readError) throw this._readError;
    const snapshot = JSON.stringify(this.data);
    const write = this._saveTail.then(() => {
      if (this._readError) throw this._readError;
      return this.storage.write(snapshot);
    });
    // A failed save rejects its caller, but must not poison later retries.
    this._saveTail = write.catch(() => {});
    await write;
  }

  getFile(rel) { return own(this.data.files, rel) ? this.data.files[rel] : undefined; }
  setFile(rel, e) { Object.defineProperty(this.data.files, rel, { value: e, enumerable: true, configurable: true, writable: true }); }
  deleteFile(rel) { delete this.data.files[rel]; }
  fileKeys() { return Object.keys(this.data.files); }
  count() { return this.fileKeys().length; }

  // Folders that existed on BOTH sides at the end of a successful run (so a later one-sided absence = a deletion).
  hasFolder(rel) { return own(this.data.folders, rel) && !!this.data.folders[rel]; }
  setFolder(rel, e) { Object.defineProperty(this.data.folders, rel, { value: e, enumerable: true, configurable: true, writable: true }); }
  deleteFolder(rel) { delete this.data.folders[rel]; }
  folderKeys() { return Object.keys(this.data.folders || {}); }

  getConflict(rel) { return own(this.data.conflicts, rel) ? this.data.conflicts[rel] : undefined; }
  setConflict(rel, c) { Object.defineProperty(this.data.conflicts, rel, { value: c, enumerable: true, configurable: true, writable: true }); }
  deleteConflict(rel) { delete this.data.conflicts[rel]; }
  conflictKeys() { return Object.keys(this.data.conflicts); }

  addLog(level, msg) {
    this.data.log.push({ t: this.now(), level, msg: String(msg) });
    if (this.data.log.length > MAX_LOG) this.data.log.splice(0, this.data.log.length - MAX_LOG);
  }
}

module.exports = { ManifestStore };
