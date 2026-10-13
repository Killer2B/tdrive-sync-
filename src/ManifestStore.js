'use strict';

const MAX_LOG = 500;

/** Local sync ledger: per-file state at last successful sync, open conflicts, and the operation log. */
class ManifestStore {
  constructor(storage, now = () => Date.now()) {
    this.storage = storage; // { read(): Promise<string|null>, write(str): Promise<void> }
    this.now = now;
    this.data = ManifestStore.empty();
  }
  static empty() { return { version: 1, files: {}, folders: {}, conflicts: {}, log: [], meta: { lastRun: null, lastSuccessAt: null, deletions: [] } }; }

  async load() {
    let raw = null;
    try { raw = await this.storage.read(); } catch (e) { raw = null; }
    if (!raw) { this.data = ManifestStore.empty(); return; }
    try {
      const d = JSON.parse(raw);
      this.data = { ...ManifestStore.empty(), ...d, meta: { ...ManifestStore.empty().meta, ...(d.meta || {}) } };
    } catch (e) {
      // Corrupt ledger: keep nothing destructive. Start empty; the engine then treats all files as
      // "unknown" and compares content instead of deleting or overwriting anything.
      this.data = ManifestStore.empty();
      this.addLog('warn', 'Sync ledger was unreadable and has been reset; content will be compared on next sync.');
    }
  }
  async save() { await this.storage.write(JSON.stringify(this.data)); }

  getFile(rel) { return this.data.files[rel]; }
  setFile(rel, e) { this.data.files[rel] = e; }
  deleteFile(rel) { delete this.data.files[rel]; }
  fileKeys() { return Object.keys(this.data.files); }
  count() { return this.fileKeys().length; }

  // Folders that existed on BOTH sides at the end of a successful run (so a later one-sided absence = a deletion).
  hasFolder(rel) { return !!this.data.folders[rel]; }
  setFolder(rel, e) { this.data.folders[rel] = e; }
  deleteFolder(rel) { delete this.data.folders[rel]; }
  folderKeys() { return Object.keys(this.data.folders || {}); }

  getConflict(rel) { return this.data.conflicts[rel]; }
  setConflict(rel, c) { this.data.conflicts[rel] = c; }
  deleteConflict(rel) { delete this.data.conflicts[rel]; }
  conflictKeys() { return Object.keys(this.data.conflicts); }

  addLog(level, msg) {
    this.data.log.push({ t: this.now(), level, msg: String(msg) });
    if (this.data.log.length > MAX_LOG) this.data.log.splice(0, this.data.log.length - MAX_LOG);
  }
}

module.exports = { ManifestStore };
