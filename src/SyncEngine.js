'use strict';
const { sha256Hex, nfc, splitRel, splitPath, folderPathFor, makeExcluder, bytesToHuman, randomUuid, sleep } = require('./util');
const { remoteSig, normalizeFileResponse } = require('./normalize');
const { ConflictManager } = require('./ConflictManager');
const { uploadFile, uploadRevision } = require('./UploadManager');
const { downloadFile, fetchManifest, downloadChangedRanges } = require('./DownloadManager');
const { normalizeChunker } = require('./Chunker');

const emptyReport = () => ({
  status: 'ok', uploaded: 0, downloaded: 0, skipped: 0, conflicts: 0, moved: 0,
  deletedLocal: 0, deletedRemote: 0, errors: 0, warnings: [], failures: [],
});

/** Pure decision table. See docs/SYNC-DESIGN.md. */
function decide({ hasLocal, hasRemote, hasEntry, localChanged, remoteChanged, conflictOpen }) {
  if (conflictOpen) return 'skip-conflict';
  if (!hasEntry) {
    if (hasLocal && hasRemote) return 'compare';
    if (hasLocal) return 'upload';
    if (hasRemote) return 'download';
    return 'forget';
  }
  if (hasLocal && hasRemote) {
    if (!localChanged && !remoteChanged) return 'skip';
    if (localChanged && !remoteChanged) return 'upload';
    if (!localChanged && remoteChanged) return 'download';
    return 'compare';
  }
  if (hasLocal) return localChanged ? 'upload' : 'deleteLocal';   // remote vanished
  if (hasRemote) return remoteChanged ? 'download' : 'deleteRemote'; // local vanished
  return 'forget';
}

const isNameConflict = (e) => e && (e.status === 409 || /exist|duplicate|conflict/i.test(e.code || ''));

const DELTA_MIN_BYTES = 64 * 1024;   // below this a full download is simpler than a delta round-trip
const MAX_MAP_CHUNKS = 4096;         // bound the chunk map kept in the ledger

const safePlatform = () => {
  try { return (typeof navigator !== 'undefined' && (navigator.platform || navigator.userAgent || '')) || 'unknown'; }
  catch (e) { return 'unknown'; }
};

class SyncEngine {
  constructor({ client, local, manifest, getSettings, onProgress, now, uploadFn, downloadFn, crypto, compressor, revisionFn, manifestFn, deltaFn }) {
    this.client = client; this.local = local; this.manifest = manifest; this.getSettings = getSettings;
    this.onProgress = onProgress || (() => {});
    this.now = now || (() => Date.now());
    this.uploadFn = uploadFn || uploadFile;
    this.revisionFn = revisionFn || uploadRevision;   // in-place update (only changed chunks)
    this.downloadFn = downloadFn || downloadFile;
    this.manifestFn = manifestFn || fetchManifest;            // {version, etag, chunks[]}
    this.deltaFn = deltaFn || downloadChangedRanges;          // download only the changed byte ranges
    this.crypto = crypto || null;   // optional { encrypt, decrypt } (E2EE); null = plaintext
    this.compressor = compressor || null;   // optional { maybeCompress, decompress }; null = store as-is
    this.conflicts = new ConflictManager({ manifest, local, now: this.now });
    this.running = false; this.stopRequested = false;
    this._folders = new Set();
    this._dirCache = new Map();
  }

  /** Transfer options (chunk size, parallel parts, encryption, compression, dedup) from the live settings. */
  _upOpts() { const s = this.getSettings(); return { chunkBytes: s.uploadChunkBytes, concurrency: s.maxConcurrentParts, crypto: this.crypto, compressor: this.compressor, dedupe: s.dedupeUploads !== false }; }
  _downOpts() { const s = this.getSettings(); return { chunkBytes: s.downloadChunkBytes, concurrency: s.maxConcurrentParts, crypto: this.crypto, compressor: this.compressor }; }

  /** Marks that this run changed the remote AND drops any cached remote listing, so a mutation made outside a
   *  normal run (conflict resolution, one-file upload, version restore) can never leave a stale snapshot behind. */
  _markRemoteMutated() {
    this._remoteMutated = true;
    if (this.manifest && this.manifest.data && this.manifest.data.meta) delete this.manifest.data.meta.remoteSnapshot;
  }

  /** Emits byte-level progress for the file currently being transferred. */
  _fileProgress(key, action) {
    return (done, total) => this.onProgress({ phase: 'sync', current: key, action, bytes: { done: done || 0, total: total || 0 } });
  }

  // ---------- incremental transfer (chunk manifest + delta) ----------
  /** The server's chunk manifest the way we store it: chunker + parallel size/hash arrays (offsets are the
   *  running sums of the sizes). Returns { notModified } when the caller's ETag is still current. */
  async _fetchChunkMap(remote, etag) {
    if (!this.manifestFn) return null;
    let m;
    try { m = await this.manifestFn(this.client, remote.id, etag); } catch (e) { return null; }
    if (!m) return null;
    if (m.notModified) return { notModified: true };
    const raw = (m.chunks || m.parts || []).slice().sort((a, b) => (a.idx ?? a.offset ?? 0) - (b.idx ?? b.offset ?? 0));
    if (!raw.length) return null;
    if (raw.length > MAX_MAP_CHUNKS) return { etag: m.etag, version: m.version, chunker: normalizeChunker(m.chunker), tooMany: true };
    return {
      etag: m.etag, version: m.version, chunker: normalizeChunker(m.chunker) || null, plan: raw,
      sizes: raw.map((c) => c.size), hashes: raw.map((c) => c.checksum),
      size: Number.isFinite(m.size) ? m.size : raw.reduce((n, c) => n + (c.size || 0), 0),   // authoritative total
    };
  }

  /** Stores a bounded chunk map on the ledger entry so a later change can be pulled incrementally. */
  _storeMap(rel, map, remote) {
    if (!map || map.tooMany || !Array.isArray(map.hashes) || !map.hashes.length) return;
    const entry = this.manifest.getFile(rel) || {};
    const sizes = map.sizes; const hashes = map.hashes;
    if (!sizes || sizes.length !== hashes.length) return;
    this.manifest.setFile(rel, { ...entry, remoteId: remote.id, remoteSize: remote.size, remoteMtime: remote.mtime, remoteSig: remote.sig || entry.remoteSig, syncedAt: this.now(), baseText: entry.baseText || null, chunkMap: { chunker: map.chunker, sizes, hashes }, etag: map.etag || null, version: map.version || null });
  }

  /** Ensures a chunk map exists for a remote file (used after a full download so the NEXT change is incremental). */

  /** Tries to fetch only what changed. Returns {unchanged} | {buffer, map} | null (=> do a full download). */
  async _deltaPull(rel, remote, E, onProgress) {
    if (!E || !E.chunkMap || !E.chunkMap.hashes || !this.manifestFn || !this.deltaFn) return null;
    let map;
    try { map = await this._fetchChunkMap(remote, E.etag); } catch (e) { return null; }
    if (!map) return null;
    if (map.notModified) return { unchanged: true };
    if (map.tooMany || !map.hashes) return null;
    const mine = new Set(E.chunkMap.hashes);
    const changed = map.plan.filter((c) => !mine.has(c.checksum));
    const full = remote.size || map.sizes.reduce((a, b) => a + b, 0);
    if (!changed.length) return { unchanged: true, map };
    const changedBytes = changed.reduce((n, c) => n + c.size, 0);
    if (changedBytes > full * 0.6) return null;                 // little saving: a plain download is simpler
    let local;
    try { local = new Uint8Array(await this.local.readBinary(rel)); } catch (e) { return null; }
    if (local.byteLength !== full) return null;                 // local is a different size: full download
    const out = new Uint8Array(local);                          // copy to splice in place
    try {
      const parts = await this.deltaFn(this.client, remote.id, changed, onProgress);
      for (const p of parts) out.set(new Uint8Array(p.data), p.offset);
    } catch (e) { return null; }                                // verification failed: full download
    return { buffer: out.buffer, map, savedBytes: changedBytes };
  }

  pause() { this.stopRequested = true; }
  log(level, msg) { this.manifest.addLog(level, this.client.redact(msg)); }

  /** Ensures a chunk map exists for a remote file (used after a full download so the NEXT change is incremental). */
  async _ensureMap(rel, remote) {
    const entry = this.manifest.getFile(rel);
    if (entry && entry.chunkMap && entry.remoteSize === remote.size) return entry.chunkMap;
    if (!(remote.size >= DELTA_MIN_BYTES)) return null;
    const map = await this._fetchChunkMap(remote, null);
    if (map && !map.notModified) { this._storeMap(rel, map, remote); return map; }
    return null;
  }

  /** For a small, single-chunk file: the authoritative size + whole-file SHA-256 from the server manifest,
   *  so a download can be verified byte-for-byte before it is written. Also returns the map for the ledger. */
  async _expectedSha(remote) {
    const lim = await this.client.limits();
    if (!Number.isFinite(remote.size) || remote.size <= 0 || remote.size > lim.oneShotMaxBytes) return { sha: null, map: null };
    const map = await this._fetchChunkMap(remote, null);
    if (!map || map.notModified || map.tooMany || !map.hashes || map.hashes.length !== 1) return { sha: null, map: null };
    if (map.sizes && map.sizes.length === 1 && map.sizes[0] !== remote.size) return { sha: null, map: null };
    return { sha: map.hashes[0], map };
  }

  /** The manifest is the server's authoritative description of the stored object. If the listing/metadata size
   *  disagrees with it (stale listing, duplicate-name collision), adopt the manifest size before transferring so
   *  a wrong advertised size can never trigger a past-EOF range or a false "Incomplete download". */
  async _adoptManifestSize(remote) {
    if (!this.manifestFn) return;
    try {
      const map = await this._fetchChunkMap(remote, null);
      if (map && !map.notModified && !map.tooMany && Number.isFinite(map.size) && map.size !== remote.size) {
        this.log('warn', `Size mismatch for ${remote.name || remote.id}: listing ${remote.size}, server ${map.size} — using the server value.`);
        try { remote.size = map.size; } catch (e) { /* frozen entry */ }
      }
    } catch (e) { /* manifest is optional */ }
  }

  /** Downloads a file, verifying integrity where possible and retrying transient/incomplete transfers with a
   *  FRESH remote entry (the listing may have been stale). Never returns unverified short data. */
  async _downloadVerified(key, remote, onProgress, expectSha) {
    let lastErr = null;
    let forceFull = false;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        return await this.downloadFn(this.client, remote, { ...this._downOpts(), onProgress, expectSha: expectSha || undefined, verifyLabel: `${key} [id=${remote.id}]`, forceFull });
      } catch (e) {
        lastErr = e;
        const retryable = e && (e.retryable === true || e.status >= 500 || e.status === 429);
        const checksumMismatch = /checksum mismatch/i.test(String(e && e.message));
        if ((!retryable && !checksumMismatch) || attempt === 2) break;
        if (checksumMismatch) forceFull = true;
        // Re-read the authoritative size and manifest hash before retrying; content may have changed
        // between the manifest request and the download.
        try {
          const fresh = await this.client.getFile(remote.id);
          if (fresh) { if (Number.isFinite(fresh.size)) remote.size = fresh.size; if (fresh.mtime) remote.mtime = fresh.mtime; }
          const exp = await this._expectedSha(remote);
          if (exp.sha) expectSha = exp.sha;
        } catch (e2) { /* keep the old size */ }
        await sleep(600 * (attempt + 1));
      }
    }
    throw lastErr;
  }

  // ---------- remote / local scanning ----------
  /** Selective sync: when settings.syncFolders lists folder prefixes, only those subtrees are synced on this
   *  device. Everything else is invisible AND untouched (so the other device can still own those files). */
  _scopeExcludes() {
    const s = this.getSettings();
    const list = (s.syncFolders || []).map((p) => String(p).replace(/^\/+|\/+$/g, '')).filter(Boolean);
    if (!list.length) return null;
    return list;
  }
  _inScope(rel) {
    const scopes = this._scopeExcludes();
    if (!scopes) return true;
    const r = nfc(String(rel || ''));
    return scopes.some((p) => r === p || r.startsWith(p + '/'));
  }

  async _listRemote(rootPath, rep, changed) {
    // Incremental fast path: when the server reports NO activity since the last full scan, reuse the cached
    // remote listing instead of walking the whole tree. Any doubt (no snapshot, stale, activity error) falls
    // back to a full scan. The cache is only ever written after a full, successful scan.
    if (changed === false) {
      const snap = this.manifest.data.meta.remoteSnapshot;
      if (snap && snap.files && snap.folders) {
        this._folders = new Set(snap.folders);
        this._remoteDirs = new Map(Object.entries(snap.dirs || {}));
        const files = new Map();
        for (const [k, v] of Object.entries(snap.files)) files.set(k, { id: v.id, name: v.name, size: v.size, mtime: v.mtime, sig: v.sig });
        this._snapshotUsed = true;
        return { files, rootMissing: false };
      }
    }
    const files = new Map(); this._folders = new Set(); this._remoteDirs = new Map(); let rootMissing = false;
    this._snapshotUsed = false;
    const rootSegs = splitPath(this.getSettings().remoteFolder);
    const excluded = makeExcluder(this.getSettings().excludePatterns);
    const stack = [''];
    while (stack.length) {
      const rel = stack.pop();
      let res;
      try { res = await this.client.listFolder(folderPathFor(this.getSettings().remoteFolder, rel)); }
      catch (e) {
        if (rel === '' && e.status === 404) { rootMissing = true; break; }
        throw e;
      }
      if (rel === '') for (let i = 1; i <= rootSegs.length; i++) this._folders.add(rootSegs.slice(0, i).join('/'));
      for (const d of res.folders) {
        const child = rel ? rel + '/' + d.name : d.name;
        stack.push(child);
        if (this._inScope(child)) { this._folders.add([...rootSegs, ...splitPath(child)].join('/')); this._remoteDirs.set(nfc(child), d); }
      }
      for (const f of res.files) {
        const key = nfc(rel ? rel + '/' + f.name : f.name);
        if (excluded(key) || !this._inScope(key)) continue;   // hidden/excluded/out-of-scope remote files are ignored
        f.sig = remoteSig(f);
        const prev = files.get(key);
        if (prev) {
          const entry = this.manifest.getFile(key);
          const keepNew = entry ? f.id === entry.remoteId : (f.mtime || 0) > (prev.mtime || 0);
          rep.warnings.push(`Duplicate remote name: ${key} (using ${keepNew ? f.id : prev.id})`);
          if (keepNew) files.set(key, f);
        } else files.set(key, f);
      }
    }
    this._pendingSnapshot = this._makeSnapshot(files);
    return { files, rootMissing };
  }

  _makeSnapshot(files) {
    const f = {};
    for (const [k, v] of files) f[k] = { id: v.id, name: v.name, size: v.size, mtime: v.mtime, sig: v.sig };
    const dirs = {};
    for (const [k, v] of this._remoteDirs) dirs[k] = { id: v && v.id, name: v && v.name };
    return { at: this.now(), files: f, folders: [...this._folders], dirs };
  }

  /** Latest activity timestamp the server reports (server clock), or null. Used instead of the local clock so
   *  a skew between this device and the server can never make every run look "changed". */
  async _latestActivityTime() {
    try {
      const r = await this.client.request({ path: '/activity', query: { limit: 50 } });
      const arr = (r && (r.activity || r.items || r.events)) || (Array.isArray(r) ? r : null);
      if (!Array.isArray(arr) || !arr.length) return null;
      let max = null;
      for (const x of arr) {
        const t = Date.parse((x && (x.created_at || x.createdAt || x.at || x.time)) || 0);
        if (Number.isFinite(t) && (max === null || t > max)) max = t;
      }
      return max;
    } catch (e) { return null; }
  }

  /** true = the remote may have changed (or we cannot tell) -> do a full scan. false = safe to reuse the cache. */
  async _remoteChangedSinceSnapshot() {
    const s = this.getSettings();
    if (s.incrementalScan === false) return true;
    const snap = this.manifest.data.meta.remoteSnapshot;
    if (!snap || !snap.at || !snap.files) return true;
    const maxAge = (Number.isFinite(s.fullScanMinutes) ? s.fullScanMinutes : 15) * 60000;
    const baseline = Number.isFinite(snap.activityAt) ? snap.activityAt : snap.at;
    if ((this.now() - snap.at) > maxAge) return true;
    try {
      const r = await this.client.request({ path: '/activity', query: { limit: 50 } });
      const arr = (r && (r.activity || r.items || r.events)) || (Array.isArray(r) ? r : null);
      if (!Array.isArray(arr)) return true;
      return arr.some((x) => {
        const t = Date.parse((x && (x.created_at || x.createdAt || x.at || x.time)) || 0);
        return Number.isFinite(t) ? t > baseline : true;   // unknown timestamp -> assume changed
      });
    } catch (e) { return true; }
  }

  async _listLocal() {
    const excluded = makeExcluder(this.getSettings().excludePatterns);
    const map = new Map();
    for (const f of await this.local.list()) {
      if (excluded(f.path) || !this._inScope(f.path)) continue;
      map.set(nfc(f.path), f);
    }
    return map;
  }

  async _localState(rel, L, E) {
    if (E && L.size === E.localSize && L.mtime === E.localMtime) {
      if (E.localHash != null) return { changed: false, hash: E.localHash };
      // Streamed download: the hash was not computed. Compute it once and record it WITHOUT treating it as a change.
      const hash = await sha256Hex(await this.local.readBinary(rel));
      E.localHash = hash;
      return { changed: false, hash };
    }
    const hash = await sha256Hex(await this.local.readBinary(rel));
    return { changed: !E || hash !== E.localHash, hash };
  }

  /** Text content of a small, text-like file — stored as the merge base so later conflicts can be auto-merged. */
  _baseTextFor(data) {
    if (this.getSettings().mergeTextFiles === false) return null;
    const u = new Uint8Array(data);
    if (u.byteLength > 65536) return null;
    for (let i = 0; i < u.byteLength; i++) if (u[i] === 0) return null;   // NUL => binary
    try { return new TextDecoder('utf-8').decode(u); } catch (e) { return null; }
  }

  // ---------- ledger ----------
  _record(rel, { lhash, lstat, remote, text }) {
    const prev = this.manifest.getFile(rel);
    this.manifest.setFile(rel, {
      localHash: lhash, localSize: lstat.size, localMtime: lstat.mtime,
      remoteId: remote.id, remoteSize: remote.size, remoteMtime: remote.mtime,
      remoteSig: remote.sig || remoteSig(remote), syncedAt: this.now(),
      baseText: text !== undefined ? text : (prev ? prev.baseText : null) || null,
    });
  }

  // ---------- remote folder helpers ----------
  /** One cached listing per remote folder per run: {files: name -> newest file, folders: Set(names)}. */
  async _dirIndex(folderPath) {
    if (!this._dirCache.has(folderPath)) {
      const idx = { files: new Map(), folders: new Set() };
      try {
        const r = await this.client.listFolder(folderPath);
        for (const f of r.files) {
          const k = nfc(f.name); const prev = idx.files.get(k);
          if (!prev || (f.mtime || 0) >= (prev.mtime || 0)) idx.files.set(k, f);
        }
        for (const d of r.folders) idx.folders.add(nfc(d.name));
      } catch (e) { if (e.status !== 404) throw e; }   // missing folder == empty
      this._dirCache.set(folderPath, idx);
    }
    return this._dirCache.get(folderPath);
  }

  /** Creates every missing folder on the way to relDir. Never blindly re-creates an existing one:
   *  some servers answer a repeated create with a renamed copy ("name (1)") instead of an error. */
  async _ensureFolders(relDir) {
    const segs = [...splitPath(this.getSettings().remoteFolder), ...splitPath(relDir)];
    const cur = [];
    for (const seg of segs) {
      const parent = folderPathFor('', cur.join('/'));
      cur.push(seg);
      const key = cur.join('/');
      if (this._folders.has(key)) continue;
      const idx = await this._dirIndex(parent);
      if (!idx.folders.has(nfc(seg))) {
        try { await this.client.createFolder(seg, parent); this._markRemoteMutated(); }
        catch (e) { if (!(e.status === 409 || /exist/i.test(e.code + ' ' + e.message))) throw e; }
        idx.folders.add(nfc(seg));
      }
      this._folders.add(key);
    }
  }

  // ---------- primitive operations ----------
  async _replaceUpload({ name, folderPath, data, mtime, old, onProgress, chunker }) {
    // In-place update first: when replacing a file we already have on the server, a revision sends ONLY the
    // changed chunks (unchanged chunks are reused server-side), commits atomically, and keeps the same file id.
    if (old && old.id && this.revisionFn) {
      try {
        const rev = await this.revisionFn(this.client, { id: old.id, name, folderPath, data, mtime, onProgress, chunker, ...this._upOpts() });
        if (rev && rev.id) {
          this._markRemoteMutated();
          if (rev.name && rev.name !== name) return { ...(await this.client.rename(rev.id, name)), ...rev, name };
          return rev;
        }
      } catch (e) {
        // Auth/network must propagate; any other rejection means "revision not usable right now".
        if (e && (e.status === 401 || e.status === 403 || e.isNetwork)) throw e;
      }
      // Small file (or a revision that did not apply): replace the content IN PLACE via the inline PATCH form,
      // which runs through the same atomic revision engine (no new file, no trash, previous version archived).
      try {
        const patched = await this.client.patchContent(old.id, data);
        if (patched && patched.id) {
          this._markRemoteMutated();
          if (patched.name && patched.name !== name) return { ...(await this.client.rename(patched.id, name)), ...patched, name };
          return patched;
        }
      } catch (e) {
        if (e && (e.status === 401 || e.status === 403 || e.isNetwork)) throw e;
      }
    }
    // Generic path (only when neither in-place form is available). Not an error, just the fallback.
    const attempt = () => this.uploadFn(this.client, { name, folderPath, data, mtime, onProgress, chunker, ...this._upOpts() });
    let trashed = false; let res;
    try { res = await attempt(); }
    catch (e) {
      if (!(old && isNameConflict(e))) throw e;
      await this.client.trash(old.id); trashed = true; // soft delete; restored below if the retry fails
      try { res = await attempt(); }
      catch (e2) { try { await this.client.restore(old.id); } catch (_) { /* best effort */ } throw e2; }
    }
    if (old && res.id !== old.id) {
      if (!trashed) {
        try { await this.client.trash(old.id); } catch (e) {
          if (e.status !== 404) this.log('warn', `Could not trash previous remote version of ${name}: ${e.message}`);
        }
      }
      if (res.name && res.name !== name) res = { ...(await this.client.rename(res.id, name)), ...res, name };
    }
    return res;
  }

  async pushLocal(rel, oldRemote, onProgress) {
    const lstat = await this.local.stat(rel); // stat BEFORE read: a concurrent edit is then detected next run
    const data = await this.local.readBinary(rel);
    const lhash = await sha256Hex(data);
    const { dir, name } = splitRel(rel);
    await this._ensureFolders(dir);
    this._markRemoteMutated();
    const folderPath = folderPathFor(this.getSettings().remoteFolder, dir);
    // No known remote copy: a file with this name may still be there (listing gap, lost ledger).
    // Replace it rather than adding a second copy next to it.
    if (!oldRemote) oldRemote = (await this._dirIndex(folderPath)).files.get(nfc(name));
    // Reuse the chunker we already know for this file so a later edit keeps the same boundaries (best reuse).
    const prev = this.manifest.getFile(rel);
    const cdc = this.getSettings().cdcFiles !== false;    // false = plain fixed chunks (revisable base)
    const chunker = cdc ? ((prev && prev.chunkMap && prev.chunkMap.chunker) || undefined) : false;
    const res = await this._replaceUpload({ name, folderPath, data, mtime: lstat.mtime, old: oldRemote, onProgress, chunker });
    (await this._dirIndex(folderPath)).files.set(nfc(name), res);
    this._record(rel, { lhash, lstat, remote: res, text: this._baseTextFor(data) });
    // Record the resulting chunk map (the server's own, if it re-chunked) so the next change is incremental.
    if (res.chunks && res.chunker) this._storeMap(rel, { chunker: res.chunker, sizes: res.chunks.map((c) => c.size), hashes: res.chunks.map((c) => c.hash), etag: null }, res);
    else if (res.size >= DELTA_MIN_BYTES) await this._ensureMap(rel, res);
    this._touched && this._touched.set(rel, res.id);
    return res;
  }

  async pullRemote(rel, remote, onProgress) {
    const data = await this.downloadFn(this.client, remote, { ...this._downOpts(), onProgress });
    return this._writeDownloaded(rel, remote, data, null, null);
  }

  async _writeDownloaded(rel, remote, data, L, E) {
    const rhash = await sha256Hex(data);
    remote.sig = remote.sig || remoteSig(remote);
    if (L && E) {
      const cur = await sha256Hex(await this.local.readBinary(rel));
      if (cur !== E.localHash) return { conflict: true, data };           // local changed since the decision
      if (rhash === E.localHash) {                                          // only remote metadata changed
        this.manifest.setFile(rel, { ...E, remoteId: remote.id, remoteSize: remote.size, remoteMtime: remote.mtime, remoteSig: remote.sig, syncedAt: this.now(), baseText: E.baseText || this._baseTextFor(data) });
        return { unchanged: true };
      }
    } else if (L) {
      // File exists locally with no ledger entry: never overwrite without an explicit policy.
      if ((await sha256Hex(await this.local.readBinary(rel))) !== rhash) return { conflict: true, data };
    }
    // Replace the local file only with fully-verified bytes, atomically when the adapter supports it.
    if (typeof this.local.writeBinaryAtomic === 'function') await this.local.writeBinaryAtomic(rel, data, remote.mtime);
    else await this.local.writeBinary(rel, data, remote.mtime);
    this._record(rel, { lhash: rhash, lstat: await this.local.stat(rel), remote, text: this._baseTextFor(data) });
    return { written: true };
  }
  // ---------- main entry ----------
  async syncNow(opts = {}) {
    const rep = emptyReport();
    if (this.running) { rep.status = 'busy'; return rep; }
    this.running = true; this.stopRequested = false; this._touched = new Map(); this._dirCache = new Map();
    this._runId = this.now(); this._dryRun = false; this._pendingSnapshot = null; this._remoteMutated = false;
    const s = this.getSettings();
    const mode = opts.mode || s.mode || 'two-way';
    let netFails = 0; let serverFails = 0; let lock = null;
    try {
      await this.client.limits();               // validates the key + discovers upload limits
      const remoteChanged = await this._remoteChangedSinceSnapshot();   // read BEFORE the lock churns /activity
      lock = await this._acquireLock();
      if (lock && lock.blocked) {
        rep.status = 'locked';
        // Report each distinct lock once; repeated attempts against the same lock stay quiet.
        if (!lock.repeat) {
          const msg = `Another device appears to be syncing (since ${new Date(lock.at || 0).toLocaleString()}). Try again later.`;
          rep.warnings.push(msg); this.log('warn', msg);
        }
        return rep;
      }
      const rootPath = folderPathFor(s.remoteFolder, '');
      const { files: remote, rootMissing } = await this._listRemote(rootPath, rep, remoteChanged);
      const local = await this._listLocal();
      await this._rescueInvisible(local, remote, rep);
      await this._rescueLocalInvisible(local, rep);
      const knownDirs = this._knownDirs(s.excludePatterns);
      const ledgerCount0 = this.manifest.count();
      const folderGuard = ledgerCount0 > 0 && (local.size === 0 || remote.size === 0);
      let keys = new Set([...local.keys(), ...remote.keys(), ...this.manifest.fileKeys().filter((k) => !makeExcluder(s.excludePatterns)(k) && this._inScope(k))]);
      if (opts.only) keys = new Set([...keys].filter((k) => opts.only.includes(k)));   // single-file runs

      // ---- plan ----
      const plan = [];
      let i = 0;
      for (const key of [...keys].sort()) {
        if (this.stopRequested) { rep.status = 'paused'; break; }
        this.onProgress({ phase: 'scan', done: ++i, total: keys.size, current: key });
        const L = local.get(key); const R = remote.get(key); const E = this.manifest.getFile(key);
        let ls = { changed: false };
        try { if (L) ls = await this._localState(key, L, E); }
        catch (e) { rep.failures.push({ path: key, error: e.message }); continue; }
        const remoteChanged = !!(E && R && R.sig !== E.remoteSig);
        const action = decide({ hasLocal: !!L, hasRemote: !!R, hasEntry: !!E, localChanged: ls.changed, remoteChanged, conflictOpen: this.conflicts.has(key) });
        plan.push({ key, L, R, E, action, lhash: ls.hash });
      }

      // ---- deletion safety ----
      const dels = plan.filter((p) => p.action === 'deleteLocal' || p.action === 'deleteRemote');
      let blockDeletes = false; let blockReason = null;
      const fullRun = !opts.only && mode === 'two-way';
      if (dels.length) {
        const limit = s.maxDeletesPerRun == null ? 10 : s.maxDeletesPerRun;
        if (opts.only || mode !== 'two-way' || s.deletionPolicy === 'off') blockDeletes = true;
        else if (dels.length > limit && !opts.approveDeletes) {
          blockDeletes = true; blockReason = 'limit';
          rep.warnings.push(`Deletions blocked: ${dels.length} files would be deleted (limit ${limit}). Use "Approve Pending Deletions" after reviewing.`);
        } else if (local.size === 0 && this.manifest.count() > 0) {
          blockDeletes = true; blockReason = 'empty-local'; rep.warnings.push('Deletions blocked: the local vault looks empty.');
        } else if (remote.size === 0 && this.manifest.count() > 0) {
          blockDeletes = true; blockReason = 'empty-remote'; rep.warnings.push('Deletions blocked: the remote folder looks empty.');
        }
      }
      // Remember what is waiting for approval so the UI can show it (it used to vanish silently every run).
      if (fullRun) {
        this.manifest.data.meta.blockedDeletes = blockReason
          ? { at: this.now(), reason: blockReason, items: dels.map((p) => ({ path: p.key, action: p.action })) } : null;
        rep.blockedDeletes = this.manifest.data.meta.blockedDeletes;
      }
      if (rootMissing) this.log('info', 'Remote folder does not exist yet; it will be created on first upload.');

      // ---- rename/move detection ----
      // A moved/renamed file must be moved IN PLACE on the server (same file id, existing chunks reused) and then
      // have its new content uploaded as a revision — never re-created at the new path while the old one lingers
      // (that is what produced stale duplicates after "move the note, then keep editing it").
      //
      // Detection is layered, strongest evidence first:
      //   1. an explicit hint captured from the editor's rename event (deterministic; works even with edits);
      //   2. identical content hash (a move made while the app was closed);
      //   3. same basename + an empty destination (a move+edit made while the app was closed).
      const pairs = []; const pairByUp = new Map(); const pairBySrc = new Map();
      const moveDone = new Set(); const moveFailed = new Set();
      const consumedHints = [];
      if (!opts.only && mode === 'two-way' && s.deletionPolicy !== 'off' && !blockDeletes) {
        const byKey = new Map(plan.map((p) => [p.key, p]));
        const srcFor = (fromKey, upKey) => {
          const src = byKey.get(fromKey);
          return (src && src.action === 'deleteRemote' && src.R && !pairBySrc.has(src)) ? src : null;
        };
        const addPair = (up, src) => {
          if (!up || !src || pairByUp.has(up) || pairBySrc.has(src)) return;
          // Content changed when the new local hash differs from what we last synced for the source path.
          const oldHash = src.E && src.E.localHash;
          const contentChanged = !(oldHash && up.lhash === oldHash);
          const pair = { up, src, contentChanged };
          pairs.push(pair); pairByUp.set(up, pair); pairBySrc.set(src, pair);
        };

        // 1) explicit rename hints from the editor
        const hints = this.manifest.data.meta.moveHints || {};
        for (const [from, info] of Object.entries(hints)) {
          const to = info && info.to;
          const up = to && byKey.get(to);
          if (!up || up.action !== 'upload') continue;
          const src = srcFor(from, to);
          if (!src) continue;
          addPair(up, src); consumedHints.push(from);
        }

        // 2) identical content hash
        const byHash = new Map();
        for (const p of plan) {
          if (p.action === 'deleteRemote' && p.E && p.E.localHash && !pairBySrc.has(p)) {
            if (!byHash.has(p.E.localHash)) byHash.set(p.E.localHash, []);
            byHash.get(p.E.localHash).push(p);
          }
        }
        for (const p of plan) {
          if (p.action !== 'upload' || !p.lhash || !p.L || pairByUp.has(p)) continue;
          const list = byHash.get(p.lhash);
          if (!list || !list.length) continue;
          addPair(p, list.pop());
        }

        // 3) same basename + empty destination (last resort for a move+edit made while the app was closed)
        const nameOf = (k) => { const parts = splitRel(k); return nfc(parts.name).toLowerCase(); };
        const byName = new Map();
        for (const p of plan) {
          if (p.action === 'deleteRemote' && p.R && !pairBySrc.has(p)) {
            const n = nameOf(p.key);
            if (!byName.has(n)) byName.set(n, []);
            byName.get(n).push(p);
          }
        }
        for (const p of plan) {
          if (p.action !== 'upload' || !p.L || p.R || pairByUp.has(p)) continue;   // destination must be free
          const list = byName.get(nameOf(p.key));
          if (!list || !list.length) continue;
          addPair(p, list.pop());
        }
        // Drop the hints we just used so they cannot match a later, unrelated file.
        for (const from of consumedHints) { try { delete this.manifest.data.meta.moveHints[from]; } catch (e) { /* ignore */ } }
      }

      // ---- size + network guards (skip heavy transfers without ever touching data) ----
      const cellular = typeof s.networkIsCellular === 'function' ? !!s.networkIsCellular() : false;
      const netBlockAll = cellular && !!s.wifiOnly;
      const netMaxBytes = cellular && !s.wifiOnly && Number.isFinite(s.cellularMaxFileBytes) && s.cellularMaxFileBytes > 0 ? s.cellularMaxFileBytes : 0;
      const maxFileBytes = Number.isFinite(s.maxFileBytes) && s.maxFileBytes > 0 ? s.maxFileBytes : 0;
      const blockedByGuard = (p) => {
        const netAction = p.action === 'upload' || p.action === 'download' || p.action === 'compare' || p.action === 'deleteRemote';
        if (netBlockAll && netAction) return 'wifi-only';
        const bytes = p.action === 'upload' ? (p.L && p.L.size) : (p.R && p.R.size);
        if (maxFileBytes && bytes && bytes > maxFileBytes) return 'size-limit';
        if (netMaxBytes && netAction && bytes && bytes > netMaxBytes) return 'cellular-defer';
        return null;
      };

      // ---- dry run: classify everything, change nothing ----
      if (opts.dryRun) {
        const todo0 = plan.filter((p) => p.action !== 'skip' && p.action !== 'forget');
        const moveKeys = new Set();
        for (const pair of pairs) moveKeys.add(pair.src.key);
        const buckets = { upload: [], download: [], compare: [], deleteRemote: [], deleteLocal: [], move: [], blocked: [], skipConflict: [] };
        for (const p of todo0) {
          if (!this._allowed(p.action, mode, blockDeletes)) { if (p.action === 'skip-conflict') buckets.skipConflict.push(p.key); else buckets.blocked.push({ path: p.key, why: 'not-allowed' }); continue; }
          const why = blockedByGuard(p);
          if (why) { buckets.blocked.push({ path: p.key, why }); continue; }
          if (pairBySrc.has(p)) { if (!moveKeys.has(p.key)) { /* handled with its upload half */ } buckets.move.push({ from: p.key, to: pairBySrc.get(p).up.key }); continue; }
          if (pairByUp.has(p)) continue;   // counted via its src entry
          buckets[p.action] && buckets[p.action].push(p.key);
        }
        rep.plan = buckets;
        rep.status = 'ok';
        this._dryRun = true;
        return rep;
      }

      // ---- execute ----
      const todo = plan.filter((p) => p.action !== 'skip' && p.action !== 'forget');
      for (const p of plan) {
        if (p.action === 'forget') this.manifest.deleteFile(p.key);
        else if (p.action === 'skip') rep.skipped++;
      }
      let done = 0; let sinceSave = 0;
      const sizeWarned = new Set(); const netWarned = { all: false, cellular: false };
      if (netBlockAll) rep.warnings.push('Wi-Fi only is on and this connection looks cellular: only local changes were handled this run.');
      for (const p of todo) {
        if (this.stopRequested) { rep.status = 'paused'; break; }
        this.onProgress({ phase: 'sync', done: done++, total: todo.length, current: p.key, action: p.action });
        const allowed = this._allowed(p.action, mode, blockDeletes);
        if (!allowed) { rep.skipped++; if (p.action === 'skip-conflict') rep.conflicts++; continue; }
        const guard = blockedByGuard(p);
        if (guard === 'wifi-only') { rep.skipped++; if (!netWarned.all) { netWarned.all = true; this.log('warn', 'Skipped network transfers (Wi-Fi only on a cellular connection).'); } continue; }
        if (guard === 'size-limit') { rep.skipped++; if (!sizeWarned.has('max')) { sizeWarned.add('max'); const msg = `Skipped file(s) larger than the ${bytesToHuman(maxFileBytes)} size limit (first: ${p.key}).`; rep.warnings.push(msg); this.log('warn', msg); } continue; }
        if (guard === 'cellular-defer') { rep.skipped++; if (!netWarned.cellular) { netWarned.cellular = true; const msg = `On cellular: deferred file(s) larger than ${bytesToHuman(netMaxBytes)}.`; rep.warnings.push(msg); this.log('warn', msg); } continue; }
        try {
          // Rename/move: PATCH the existing remote file instead of upload+trash. Works from either half.
          const pair = pairByUp.get(p) || pairBySrc.get(p);
          if (pair && moveDone.has(pair)) { rep.skipped++; continue; }   // the other half of a completed move
          if (pair && !moveFailed.has(pair)) {
            try {
              if (await this._executeMove(pair.up, pair.src, rep, pair.contentChanged)) { moveDone.add(pair); netFails = 0; continue; }
              moveFailed.add(pair);   // target name already exists -> upload will replace it instead
            } catch (e) {
              moveFailed.add(pair);
              this.log('warn', `Move failed for ${pair.up.key} (${this.client.redact(e.message)}); uploading instead.`);
            }
          }
          await this._execute(p, rep);
          netFails = 0; serverFails = 0;
          // Persist progress regularly: if Obsidian is killed mid-run (Android) finished files are not re-compared.
          if (++sinceSave >= 20) { sinceSave = 0; try { await this.manifest.save(); } catch (_) { /* retried at the end */ } }
        } catch (e) {
          rep.failures.push({ path: p.key, error: this.client.redact(e.message) });
          this.log('error', `${p.action} ${p.key}: ${e.message}`);
          if (e.status === 401 || e.status === 403) { rep.status = 'auth'; break; }
          if (e.isNetwork && ++netFails >= 3) { rep.status = 'offline'; break; }
          // Repeated server backpressure (503 "freeing storage"): stop hammering and retry later.
          if (e.status >= 500 && ++serverFails >= 3) { rep.status = 'deferred'; break; }
        }
      }

      // ---- folders: propagate folder deletions (files are already handled above) ----
      if (!opts.only && !this.stopRequested && (rep.status === 'ok' || rep.status === 'errors')) {
        try { await this._syncFolders({ known: knownDirs, mode, rep, rootMissing, blockDeletes, guard: folderGuard, approve: !!opts.approveDeletes }); }
        catch (e) {
          if (e.status === 401 || e.status === 403) rep.status = 'auth';
          rep.failures.push({ path: '', error: this.client.redact(e.message) });
          this.log('error', `Folder sync: ${e.message}`);
        }
      }

      // ---- refresh remote signatures of what we touched (list-shape == future-listing shape) ----
      if (this._touched.size && rep.status !== 'auth' && rep.status !== 'offline') await this._reconcile(rep);
    } catch (e) {
      const transient = e.status >= 500 || e.status === 429;
      rep.status = e.status === 401 || e.status === 403 ? 'auth' : (transient ? 'deferred' : (e.isNetwork ? 'offline' : 'aborted'));
      rep.failures.push({ path: '', error: this.client.redact(e.message) });
      // Transient server backpressure is expected and retried: log it as a warning, not a scary error.
      if (transient) this.log('warn', `Server busy; sync will retry later: ${this.client.redact(e.message)}`);
      else this.log('error', `Sync aborted: ${e.message}`);
    } finally {
      await this._releaseLock(lock);
      if (this._dryRun) { this.running = false; return rep; }
      if (rep.status === 'locked') { this.running = false; return rep; }
      // Persist the remote snapshot only after a FULL scan (never after reusing the cache) AND only when the run
      // did not itself change the remote (otherwise the cached listing would be stale). Stamped after the lock
      // is released so this run's lock activity is not seen as a remote change next time.
      if (this._remoteMutated) delete this.manifest.data.meta.remoteSnapshot;   // cache is stale -> full scan next run
      else if (this._pendingSnapshot) {
        this._pendingSnapshot.at = this.now();
        const act = await this._latestActivityTime();          // server-clock baseline for the next run
        if (act != null) this._pendingSnapshot.activityAt = act;
        this.manifest.data.meta.remoteSnapshot = this._pendingSnapshot;
      }
      for (const w of rep.warnings) this.log('warn', w);
      rep.errors = rep.failures.length;
      if (rep.status === 'ok' && rep.errors > 0) rep.status = 'errors';
      this.manifest.data.meta.lastRun = { at: this.now(), status: rep.status, uploaded: rep.uploaded, downloaded: rep.downloaded, skipped: rep.skipped, conflicts: rep.conflicts, errors: rep.errors };
      if (rep.status === 'ok') this.manifest.data.meta.lastSuccessAt = this.now();
      this.log(rep.status === 'ok' ? 'info' : 'warn', `Sync ${rep.status}: ↑${rep.uploaded} ↓${rep.downloaded} =${rep.skipped} conflicts:${rep.conflicts} errors:${rep.errors}`);
      try { await this.manifest.save(); } catch (e) { /* ledger save failure must not mask the report */ }
      this.running = false;
    }
    return rep;
  }

  /** Renames/moves an existing remote file in place (no re-upload), then — when the user kept editing after the
   *  move — uploads the new content as an in-place revision so only the changed chunks cross the wire and the
   *  server file keeps its id. `up` is the new-path plan item, `src` the old-path one. Returns false when the
   *  destination name is already taken (the normal upload handles that). */
  async _executeMove(up, src, rep, contentChanged) {
    if (!src.R || !src.R.id) return false;
    const { dir, name } = splitRel(up.key);
    await this._ensureFolders(dir);
    const folderPath = folderPathFor(this.getSettings().remoteFolder, dir);
    const idx = await this._dirIndex(folderPath);
    const existing = idx.files.get(nfc(name));
    if (existing && existing.id !== src.R.id) return false;
    let res = await this.client.move(src.R.id, { name, path: folderPath });
    this._remoteMutated = true;

    // The move itself is done (same id, new path). Now sync any content the user changed afterwards, in place.
    let lhash = up.lhash;
    if (contentChanged) {
      const data = await this.local.readBinary(up.key);
      const lstat = await this.local.stat(up.key);
      try {
        const revised = await this._replaceUpload({ name, folderPath, data, mtime: lstat.mtime, old: res, onProgress: null });
        if (revised && revised.id) { res = revised; lhash = await sha256Hex(data); }
      } catch (e) {
        // The path change already succeeded; if the content update fails, leave the file at the new path and let
        // the next run upload it normally (the ledger below records the move so nothing is duplicated).
        this.log('warn', `Moved ${src.key} to ${up.key} but could not update its content (${this.client.redact(e.message)}); will retry.`);
      }
    }

    const lstat = await this.local.stat(up.key);
    this._record(up.key, { lhash, lstat, remote: res });
    this.manifest.deleteFile(src.key);
    idx.files.set(nfc(name), res);
    try {
      const { dir: sdir, name: sname } = splitRel(src.key);
      (await this._dirIndex(folderPathFor(this.getSettings().remoteFolder, sdir))).files.delete(nfc(sname));
    } catch (e) { /* cache only */ }
    this._touched.set(up.key, res.id);
    rep.moved++;
    this.log('info', `Moved in place: ${src.key} -> ${up.key}${contentChanged ? ' (with edits)' : ''}`);
    return true;
  }

  /** Records what was deleted (which side, and the remote id when there is one) so the user can undo it.
   *  Kept small: the newest 200 entries, tagged with the run id so "restore last" can target one batch. */
  _auditDelete(rel, side, remoteId) {
    const meta = this.manifest.data.meta;
    const list = Array.isArray(meta.deletions) ? meta.deletions : (meta.deletions = []);
    list.push({ runId: this._runId, at: this.now(), path: rel, side, remoteId: remoteId || null });
    if (list.length > 200) list.splice(0, list.length - 200);
  }

  /** Restores the most recent run's remote deletions from the Tdrive trash. Local trashes are reported but
   *  cannot be undone through the API (Obsidian keeps them in its own trash). */
  async restoreLastDeletions() {
    const meta = this.manifest.data.meta;
    const list = Array.isArray(meta.deletions) ? meta.deletions : [];
    if (!list.length) return { restored: 0, failed: 0, skipped: 0 };
    const lastRun = Math.max(...list.map((x) => x.runId || 0));
    const batch = list.filter((x) => (x.runId || 0) === lastRun);
    let restored = 0; let failed = 0; let skipped = 0;
    for (const d of batch) {
      if (d.side !== 'remote' || !d.remoteId) { skipped++; continue; }
      try { await this.client.restore(d.remoteId); restored++; this.log('info', `Restored from trash: ${d.path}`); }
      catch (e) { failed++; this.log('warn', `Could not restore ${d.path}: ${this.client.redact(e.message)}`); }
    }
    meta.deletions = list.filter((x) => (x.runId || 0) !== lastRun);
    await this.manifest.save();
    return { restored, failed, skipped };
  }

  _allowed(action, mode, blockDeletes) {
    if (action === 'skip-conflict') return false;
    if (action === 'deleteLocal' || action === 'deleteRemote') return !blockDeletes;
    if (mode === 'upload-only') return action === 'upload' || action === 'compare';
    if (mode === 'download-only') return action === 'download' || action === 'compare';
    return true;
  }

  async _execute(p, rep) {
    const { key, L, R, E } = p;
    const onP = this._fileProgress(key, p.action);
    switch (p.action) {
      case 'upload': await this.pushLocal(key, R, onP); rep.uploaded++; break;
      case 'download': {
        // Stream very large, plaintext downloads straight to disk when the adapter supports appending, so the
        // whole file never sits in memory. Files present locally, encrypted transfers and conflicts keep the
        // in-memory path (they need the bytes for comparison / decryption).
        const s0 = this.getSettings();
        const threshold = Number.isFinite(s0.streamThresholdBytes) && s0.streamThresholdBytes > 0 ? s0.streamThresholdBytes : 8 * 1024 * 1024;
        const canStream = s0.streamLargeDownloads !== false && this.local && typeof this.local.beginStreamWrite === 'function' && this.local.canStream !== false
          && !this.crypto && !this.compressor && !L && Number.isFinite(R.size) && R.size >= threshold;
        if (canStream) {
          const sink = await this.local.beginStreamWrite(key);
          try {
            await this.downloadFn(this.client, R, { ...this._downOpts(), onProgress: onP, sink });
            await sink.close();
          } catch (e) { try { await this.local.trash(key); } catch (_) { /* best effort */ } throw e; }
          const st = await this.local.stat(key);
          this.manifest.setFile(key, {
            localHash: null, localSize: st ? st.size : R.size, localMtime: st ? st.mtime : this.now(),
            remoteId: R.id, remoteSize: R.size, remoteMtime: R.mtime, remoteSig: R.sig || remoteSig(R), syncedAt: this.now(), baseText: null,
          });
          await this._ensureMap(key, R);          // store the manifest so the NEXT change can be a delta pull
          this._touched.set(key, R.id);
          rep.downloaded++;
          break;
        }
        // Incremental pull first: when we hold a chunk map for the old content, fetch only the changed byte
        // ranges. Falls through to a normal (full) download whenever a delta is not applicable.
        // Adopt the server's authoritative size first so a stale listing cannot cause a false failure.
        await this._adoptManifestSize(R);
        const delta = (L && E && R.size >= DELTA_MIN_BYTES) ? await this._deltaPull(key, R, E, onP) : null;
        if (delta && delta.unchanged) { if (delta.map) this._storeMap(key, delta.map, R); rep.skipped++; break; }
        let data;
        let verifiedMap = delta && delta.map ? delta.map : null;
        if (delta && delta.buffer) {
          data = delta.buffer;
        } else {
          // Integrity: for a small single-chunk file, verify the downloaded bytes against the server's own
          // manifest checksum before writing. The same manifest is then reused for the ledger (no extra request).
          const expect = await this._expectedSha(R);
          if (expect.map) verifiedMap = expect.map;
          data = await this._downloadVerified(key, R, onP, expect.sha);
        }
        const r = await this._writeDownloaded(key, R, data, L, E);
        if (r.conflict) { await this.conflicts.record(key, R, data); rep.conflicts++; }
        else {
          if (verifiedMap) this._storeMap(key, verifiedMap, R);
          else await this._ensureMap(key, R);                    // so the NEXT change can be incremental
          if (r.unchanged) rep.skipped++; else rep.downloaded++;
        }
        break;
      }
      case 'compare': {
        const ldata = await this.local.readBinary(key); const lhash = await sha256Hex(ldata);
        const lstat = await this.local.stat(key);
        // Verify the remote content against the server's own manifest checksum before comparing, so a wrong or
        // short body (mis-sized listing, duplicate-name collision) can never be treated as the file's content.
        const expect = await this._expectedSha(R);
        if (expect.map) this._storeMap(key, expect.map, R);
        const rdata = await this._downloadVerified(key, R, onP, expect.sha);
        if ((await sha256Hex(rdata)) === lhash) { this._record(key, { lhash, lstat, remote: R }); rep.skipped++; }
        else { await this.conflicts.record(key, R, rdata); rep.conflicts++; }
        break;
      }
      case 'deleteRemote': await this.client.trash(R.id); this._markRemoteMutated(); this.manifest.deleteFile(key); rep.deletedRemote++; this._auditDelete(key, 'remote', R.id); break;
      case 'deleteLocal': await this.local.trash(key); this.manifest.deleteFile(key); rep.deletedLocal++; this._auditDelete(key, 'local', null); break;
      default: break;
    }
  }

  // ---------- remote lock ----------
  /** A tiny hidden file in the remote root. If another device is mid-sync (fresh lock, different device id),
   *  this run is skipped to avoid two devices racing. Best-effort: any error just means "no lock". The file is
   *  dot-prefixed so the engine itself always ignores it.
   *
   *  Robustness rules learned from a stuck-lock report:
   *   - a lock is ALWAYS released at the end of the run (a leftover lock from a killed run is the main cause of
   *     "Another device appears to be syncing" persisting), so we never leave orphans behind;
   *   - a stale marker (older than the TTL) is dropped and taken over;
   *   - a negative age (server clock ahead of ours) is clamped to 0 so clock skew cannot make an old lock look
   *     fresh forever;
   *   - the "another device" warning is emitted at most once per lock (not on every attempt). */
  async _acquireLock() {
    const s = this.getSettings();
    if (s.remoteLock === false) return null;
    const meta = this.manifest.data.meta;
    if (!meta.deviceId) meta.deviceId = randomUuid();
    const root = folderPathFor(s.remoteFolder, '');
    const mine = meta.deviceId;
    const prefix = '.tdrive-sync.lock';              // a small hidden marker file, named "...-<deviceId>"
    const ttlMs = Math.max(1, Number.isFinite(s.remoteLockMinutes) ? s.remoteLockMinutes : 5) * 60000;
    const now = this.now();
    try {
      const listing = await this.client.listFolder(root);
      const locks = (listing.files || []).filter((f) => f && typeof f.name === 'string' && f.name.startsWith(prefix));
      for (const f of locks) {
        const isMine = f.name === prefix + '-' + mine;
        const age = Number.isFinite(f.mtime) ? Math.max(0, now - f.mtime) : 0;   // clamp clock skew
        const fresh = !Number.isFinite(f.mtime) || age < ttlMs;
        if (isMine) { try { await this.client.trash(f.id); } catch (e) { /* replace our own marker */ } continue; }
        if (fresh) {
          // Remember that we already reported this exact lock, so repeated attempts stay quiet.
          const stamp = `${f.name}@${f.mtime}`;
          const seen = meta.lockWarned === stamp;
          if (!seen) meta.lockWarned = stamp;
          return { blocked: true, by: String(f.name).slice(prefix.length + 1) || 'another device', at: f.mtime, repeat: seen };
        }
        try { await this.client.trash(f.id); } catch (e) { /* stale marker: drop it */ }
      }
      meta.lockWarned = null;
      const body = new TextEncoder().encode(JSON.stringify({ device: mine, at: now, platform: safePlatform() })).buffer;
      const up = await this.client.request({
        method: 'POST', path: '/files', query: { name: prefix + '-' + mine, path: root },
        headers: { 'Content-Type': 'application/octet-stream', 'Idempotency-Key': randomUuid() }, body,
      });
      const f = normalizeFileResponse(up);
      return { id: f && f.id };
    } catch (e) { return null; }
  }

  /** Always releases the marker we created (best effort), so a finished run never blocks the next device. */
  async _releaseLock(lock) {
    if (!lock || !lock.id) return;
    try { await this.client.trash(lock.id); } catch (e) { /* best effort; the TTL is the backstop */ }
  }

  /** Ledger says we synced this file, the recursive listing does not show it. Before believing it is gone
   *  (and deleting the local copy, or uploading yet another copy), ask the server for that exact id. */
  async _liveById(id) {
    if (!id) return null;
    try {
      const f = await this.client.getFile(id);
      return f && f.id && !f.trashed ? f : null;
    } catch (e) { if (e.status === 404) return null; throw e; }
  }

  async _rescueInvisible(local, remote, rep) {
    let rescued = 0;
    for (const [key] of local) {
      if (remote.has(key)) continue;
      const E = this.manifest.getFile(key);
      if (!E || !E.remoteId || this.conflicts.has(key)) continue;
      const f = await this._liveById(E.remoteId);
      if (!f) continue;
      f.sig = remoteSig(f); remote.set(key, f); rescued++;
    }
    if (rescued) rep.warnings.push(`${rescued} file(s) exist on the server but were missing from the folder listing; matched by id.`);
  }

  /** Ledger says we have this file, the local index does not list it (e.g. written to disk but not yet indexed by
   *  Obsidian). Never read that as "deleted locally" (which would trash the remote copy): ask the disk itself. */
  async _rescueLocalInvisible(local, rep) {
    const excluded = makeExcluder(this.getSettings().excludePatterns);
    let n = 0;
    for (const key of this.manifest.fileKeys()) {
      if (local.has(key) || excluded(key) || !this._inScope(key)) continue;
      let st = null;
      try { st = (await this.local.exists(key)) ? await this.local.stat(key) : null; } catch (e) { st = null; }
      if (st) { local.set(key, { path: key, size: st.size, mtime: st.mtime }); n++; }
    }
    if (n) rep.warnings.push(`${n} file(s) exist on disk but were missing from the vault index; kept (not treated as deleted).`);
  }

  // ---------- folders ----------
  /** Folders we have synced before: those in the folder ledger plus the parents of every ledger file
   *  (so folders from before folder tracking existed are covered too). */
  _knownDirs(excludePatterns) {
    const excluded = makeExcluder(excludePatterns);
    const out = new Set(this.manifest.folderKeys());
    for (const k of this.manifest.fileKeys()) {
      const segs = splitPath(k); segs.pop();
      for (let i = 1; i <= segs.length; i++) out.add(segs.slice(0, i).join('/'));
    }
    return new Set([...out].filter((d) => d && !excluded(d) && this._inScope(d)));
  }

  async _localDirSet() {
    if (typeof this.local.listFolders !== 'function') return null;   // adapter cannot list folders: do nothing
    const arr = await this.local.listFolders();
    if (!arr) return null;
    return new Set(arr.map((p) => nfc(String(p)).replace(/^\/+|\/+$/g, '')).filter(Boolean).filter((p) => this._inScope(p)));
  }

  /** A folder missing on exactly one side was deleted there:
   *   - gone locally  -> soft-delete the remote folder, but only if the server says it is empty right now;
   *   - gone remotely -> trash the local folder, but only if nothing is left inside it.
   *  A folder that still holds anything is never deleted. Folders present on both sides are (re)recorded. */
  async _syncFolders({ known, mode, rep, rootMissing, blockDeletes, guard, approve }) {
    const s = this.getSettings();
    const localDirs = await this._localDirSet();
    if (!localDirs) return;
    const excluded = makeExcluder(s.excludePatterns);
    const rootSegs = splitPath(s.remoteFolder);
    const full = (rel) => [...rootSegs, ...splitPath(rel)].join('/');
    const rawFiles = (await this.local.list()).map((f) => nfc(f.path));
    const hasFilesUnder = (d) => rawFiles.some((p) => p.startsWith(d + '/'));
    const hasDirsUnder = (d) => { for (const x of localDirs) if (x.startsWith(d + '/')) return true; return false; };
    const deletedRemote = new Set();
    const inRemote = (d) => this._folders.has(full(d)) && !deletedRemote.has(d);
    const canDelete = !blockDeletes && !guard && !rootMissing && mode === 'two-way' && s.deletionPolicy !== 'off' && rep.status === 'ok';
    const limit = s.maxDeletesPerRun == null ? 10 : s.maxDeletesPerRun;
    const depth = (d) => splitPath(d).length;
    let dels = 0;

    for (const d of [...known].sort((a, b) => depth(b) - depth(a))) {
      if (this.stopRequested) break;
      let L = localDirs.has(d); const R = inRemote(d);
      if (!L && R && (await this.local.exists(d))) { localDirs.add(d); L = true; }   // index lag: the disk has it
      if (L && R) continue;
      if (!L && !R) { this.manifest.deleteFolder(d); continue; }
      if (!canDelete) continue;
      if (dels >= limit && !approve) { rep.warnings.push(`Folder deletions blocked: more than ${limit} folders would be deleted. Review manually.`); break; }
      const remotePath = folderPathFor(s.remoteFolder, d);
      try {
        if (!L) {                                           // deleted locally
          let live = null;
          try { live = await this.client.listFolder(remotePath); }
          catch (e) { if (e.status !== 404) throw e; }
          if (live && (live.files.some((f) => !f.trashed) || live.folders.length)) continue;   // not empty: keep
          if (live) { const meta = this._remoteDirs && this._remoteDirs.get(d); await this.client.deleteFolder(remotePath, meta && meta.id); this._markRemoteMutated(); }
          deletedRemote.add(d); this.manifest.deleteFolder(d); rep.deletedRemote++; dels++;
          this.log('info', `Deleted remote folder (to trash): ${d}`);
        } else {                                            // deleted remotely
          if (hasFilesUnder(d) || hasDirsUnder(d)) continue;                                   // not empty: keep
          if ((await this.local.trashFolder(d)) === false) { rep.warnings.push(`Local folder kept (still holds hidden/untracked items): ${d}`); continue; }
          localDirs.delete(d); this.manifest.deleteFolder(d); rep.deletedLocal++; dels++;
          this.log('info', `Deleted local folder (to trash): ${d}`);
        }
      } catch (e) {
        rep.failures.push({ path: d + '/', error: this.client.redact(e.message) });
        this.log('error', `delete folder ${d}: ${e.message}`);
        if (e.status === 401 || e.status === 403) { rep.status = 'auth'; break; }
        if (e.isNetwork) { rep.status = 'offline'; break; }
      }
    }
    // New folders (never synced before), including EMPTY ones, are mirrored to the other side.
    const canUp = mode === 'two-way' || mode === 'upload-only';
    const canDown = mode === 'two-way' || mode === 'download-only';
    if (canUp) {
      for (const d of [...localDirs].sort((a, b) => depth(a) - depth(b))) {
        if (this.stopRequested) break;
        if (excluded(d) || known.has(d) || inRemote(d)) continue;
        try { await this._ensureFolders(d); this.log('info', `Created remote folder: ${d}`); }
        catch (e) { rep.failures.push({ path: d + '/', error: this.client.redact(e.message) }); this.log('error', `create remote folder ${d}: ${e.message}`); if (e.status === 401 || e.status === 403) { rep.status = 'auth'; break; } }
      }
    }
    if (canDown && typeof this.local.mkdir === 'function' && this._remoteDirs) {
      for (const d of [...this._remoteDirs.keys()].sort((a, b) => depth(a) - depth(b))) {
        if (this.stopRequested) break;
        if (excluded(d) || known.has(d) || localDirs.has(d) || deletedRemote.has(d)) continue;
        try { await this.local.mkdir(d); localDirs.add(d); this.log('info', `Created local folder: ${d}`); }
        catch (e) { rep.failures.push({ path: d + '/', error: e.message }); this.log('error', `create local folder ${d}: ${e.message}`); }
      }
    }
    for (const d of localDirs) if (!excluded(d) && inRemote(d)) this.manifest.setFolder(d, { syncedAt: this.now() });
  }

  /** Manual cleanup for folders orphaned by versions before 1.0.4: remote folders that are EMPTY (no files, no
   *  sub-folders) and do not exist locally are soft-deleted, deepest first. Nothing that holds a file is touched. */
  async cleanEmptyRemoteFolders() {
    const rep = { deleted: 0, failures: [] };
    const s = this.getSettings();
    const { files, rootMissing } = await this._listRemote(folderPathFor(s.remoteFolder, ''), emptyReport());
    if (rootMissing) return rep;
    const localDirs = await this._localDirSet();
    if (!localDirs) return rep;
    const rawFiles = (await this.local.list()).map((f) => nfc(f.path));
    const withFiles = new Set();
    for (const k of files.keys()) { const segs = splitPath(k); segs.pop(); for (let i = 1; i <= segs.length; i++) withFiles.add(segs.slice(0, i).join('/')); }
    const dirs = [...this._remoteDirs.keys()].sort((a, b) => splitPath(b).length - splitPath(a).length);
    const gone = new Set();
    for (const d of dirs) {
      if (withFiles.has(d) || localDirs.has(d) || rawFiles.some((p) => p.startsWith(d + '/'))) continue;
      if (dirs.some((x) => x !== d && x.startsWith(d + '/') && !gone.has(x))) continue;      // still has a sub-folder
      if (await this.local.exists(d)) continue;
      const path = folderPathFor(s.remoteFolder, d);
      try {
        const live = await this.client.listFolder(path);                                     // re-check right before deleting
        if (live.files.some((f) => !f.trashed) || live.folders.length) continue;
        await this.client.deleteFolder(path, this._remoteDirs.get(d).id);
        gone.add(d); rep.deleted++; this.log('info', `Removed empty remote folder: ${d}`);
      } catch (e) { rep.failures.push({ path: d + '/', error: this.client.redact(e.message) }); if (e.status === 401 || e.status === 403 || e.isNetwork) break; }
    }
    return rep;
  }

  async prepare() {
    const rep = emptyReport();
    await this._listRemote(folderPathFor(this.getSettings().remoteFolder, ''), rep);
  }

  async refreshSigs(touched) { return this._reconcile(emptyReport(), touched); }

  async _reconcile(rep, touched = this._touched) {
    try {
      const { files } = await this._listRemote(folderPathFor(this.getSettings().remoteFolder, ''), emptyReport());
      for (const [rel, id] of touched) {
        const E = this.manifest.getFile(rel); const R = files.get(rel);
        if (!E) continue;
        if (R && R.id === id) { E.remoteId = R.id; E.remoteSize = R.size; E.remoteMtime = R.mtime; E.remoteSig = R.sig; }
        else {
          const live = await this._liveById(id);
          if (live) {   // the upload is fine; only the listing does not show it. Keep the ledger entry (no re-upload).
            live.sig = remoteSig(live);
            E.remoteId = live.id; E.remoteSize = live.size; E.remoteMtime = live.mtime; E.remoteSig = live.sig;
            rep.warnings.push(`Uploaded file not visible in folder listing (verified by id): ${rel}`);
          } else {
            rep.failures.push({ path: rel, error: 'Uploaded file not visible in remote listing' });
            this.manifest.deleteFile(rel);
            rep.uploaded = Math.max(0, rep.uploaded - 1);
          }
        }
      }
    } catch (e) {
      rep.warnings.push('Could not refresh remote listing after upload; next sync may re-verify some files.');
      for (const rel of touched.keys()) { const E = this.manifest.getFile(rel); if (E) E.remoteSig = null; }
    }
  }
}

module.exports = { SyncEngine, decide, emptyReport };
