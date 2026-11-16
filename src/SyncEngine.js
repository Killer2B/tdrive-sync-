'use strict';
const { sha256Hex, nfc, splitRel, splitPath, folderPathFor, makeExcluder, bytesToHuman, randomUuid, sleep } = require('./util');
const { remoteSig, normalizeFileResponse, normalizeReceipt } = require('./normalize');
const { ConflictManager } = require('./ConflictManager');
const { uploadFile, uploadRevision, recoverCommittedUpload, validateUploadReceipt } = require('./UploadManager');
const { downloadFile, fetchManifest, downloadChangedRanges, validateManifestEvidence } = require('./DownloadManager');
const { normalizeChunker, fingerprintOf } = require('./Chunker');

const emptyReport = () => ({
  status: 'ok', uploaded: 0, downloaded: 0, skipped: 0, conflicts: 0, moved: 0, moveConflicts: [],
  deletedLocal: 0, deletedRemote: 0, deferredFolders: 0, blockedFolders: [], errors: 0, warnings: [], failures: [],
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

const DELTA_MIN_BYTES = 64 * 1024;   // below this a full download is simpler than a delta round-trip
const MAX_MAP_CHUNKS = 4096;         // bound the chunk map kept in the ledger
const CONFLICT_COMPARE_MAX_BYTES = 8 * 1024 * 1024; // larger conflicts stay available for manual review

const safePlatform = () => {
  try { return (typeof navigator !== 'undefined' && (navigator.platform || navigator.userAgent || '')) || 'unknown'; }
  catch (e) { return 'unknown'; }
};

class SyncEngine {
  constructor({ client, local, manifest, getSettings, onProgress, now, uploadFn, downloadFn, crypto, compressor, revisionFn, manifestFn, deltaFn, operationValidity }) {
    this.client = client; this.local = local; this.manifest = manifest;
    this._settingsSource = getSettings;
    this.getSettings = () => this._runSettings || this._settingsSource();
    this.onProgress = onProgress || (() => {});
    this.now = now || (() => Date.now());
    this.uploadFn = uploadFn || uploadFile;
    this.revisionFn = revisionFn || uploadRevision;   // in-place update (only changed chunks)
    this.downloadFn = downloadFn || downloadFile;
    this.manifestFn = manifestFn || fetchManifest;            // {version, etag, chunks[]}
    this.deltaFn = deltaFn || downloadChangedRanges;          // download only the changed byte ranges
    this.crypto = crypto || null;   // optional { encrypt, decrypt } (E2EE); null = plaintext
    this.compressor = compressor || null;   // optional { maybeCompress, decompress }; null = store as-is
    this.operationValidity = operationValidity || null;
    this.conflicts = new ConflictManager({ manifest, local, now: this.now });
    this.running = false; this.stopRequested = false;
    this._folders = new Set();
    this._dirCache = new Map();
  }

  /** Transfer options (chunk size, parallel parts, encryption, compression, dedup) from the live settings. */
  _upOpts() {
    const s = this.getSettings();
    const meta = this.manifest.data.meta;
    const owner = meta.deviceId || (meta.deviceId = randomUuid());
    let scope = meta.syncScope;
    const checkScope = async () => {
      if (!scope || !meta.uploadSessionsScope) { await this._bindScope(); scope = meta.syncScope; }
      if (this.manifest.data.meta !== meta || meta.deviceId !== owner || meta.syncScope !== scope || meta.uploadSessionsScope !== scope) throw new Error('Upload journal destination changed; transfer stopped');
      if (this.operationValidity && await this.operationValidity() === false) throw new Error('Sync operation is no longer valid; transfer stopped');
    };
    const uploadJournal = {
      owner,
      get: async (key) => {
        await checkScope();
        const record = Object.prototype.hasOwnProperty.call(meta.uploadSessions, key) ? meta.uploadSessions[key] : null;
        return record == null ? null : JSON.parse(JSON.stringify(record));
      },
      set: async (key, record) => {
        await checkScope();
        if (!record || record.owner !== owner) throw new Error('Upload journal ownership mismatch; transfer stopped');
        const value = {};
        for (const field of ['owner', 'scope', 'target', 'sourceHash', 'source', 'fingerprint', 'size', 'generation', 'clientKey', 'sessionId', 'phase', 'fileId', 'contentVersion']) {
          if (Object.prototype.hasOwnProperty.call(record, field)) value[field] = record[field];
        }
        value.body = {};
        for (const field of ['name', 'path', 'baseId', 'mode', 'size', 'fingerprint', 'contentType', 'lastModified', 'totalChunks', 'clientKey', 'expectedVersion']) {
          if (record.body && Object.prototype.hasOwnProperty.call(record.body, field)) value.body[field] = record.body[field];
        }
        if (record.body && record.body.chunker) {
          value.body.chunker = {};
          for (const field of ['algo', 'min', 'avg', 'max', 'window']) if (Object.prototype.hasOwnProperty.call(record.body.chunker, field)) value.body.chunker[field] = record.body.chunker[field];
        }
        if (record.receipt) {
          value.receipt = {};
          for (const field of ['contentVersion', 'checksum', 'etag', 'version']) if (Object.prototype.hasOwnProperty.call(record.receipt, field)) value.receipt[field] = record.receipt[field];
        }
        const stored = JSON.parse(JSON.stringify(value));
        const sessions = meta.uploadSessions;
        const hadPrevious = Object.prototype.hasOwnProperty.call(sessions, key);
        const previous = sessions[key];
        Object.defineProperty(sessions, key, { value: stored, enumerable: true, configurable: true, writable: true });
        try { await this.manifest.save(); }
        catch (e) {
          // An unsaved opening phase must not authorize a POST on the next retry.
          if (sessions[key] === stored) {
            if (hadPrevious) Object.defineProperty(sessions, key, { value: previous, enumerable: true, configurable: true, writable: true });
            else delete sessions[key];
          }
          throw e;
        }
        await checkScope();
      },
    };
    return { chunkBytes: s.uploadChunkBytes, concurrency: s.maxConcurrentParts, crypto: this.crypto, compressor: this.compressor, dedupe: s.dedupeUploads !== false, uploadJournal };
  }
  _downOpts() { const s = this.getSettings(); return { chunkBytes: s.downloadChunkBytes, concurrency: s.maxConcurrentParts, crypto: this.crypto, compressor: this.compressor }; }

  /** Marks that this run changed the remote AND drops any cached remote listing, so a mutation made outside a
   *  normal run (conflict resolution, one-file upload, version restore) can never leave a stale snapshot behind. */
  _markRemoteMutated() {
    this._remoteMutated = true;
    if (this.manifest && this.manifest.data && this.manifest.data.meta) delete this.manifest.data.meta.remoteSnapshot;
  }

  async _bindScope() {
    const s = this.getSettings();
    const scope = await sha256Hex(JSON.stringify([this.client.baseUrl || s.baseUrl, this.client.apiKey || '', folderPathFor(nfc(s.remoteFolder), '')]));
    const meta = this.manifest.data.meta;
    if (meta.syncScope !== scope) {
      if (this.manifest.count() || this.manifest.folderKeys().length || this.manifest.conflictKeys().length || Object.keys(meta.uploadSessions || {}).length) {
        meta.previousScopeLedger = { scope: meta.syncScope || null, files: this.manifest.data.files, folders: this.manifest.data.folders, conflicts: this.manifest.data.conflicts, uploadSessions: meta.uploadSessions || null };
        this.manifest.data.files = Object.create(null); this.manifest.data.folders = Object.create(null); this.manifest.data.conflicts = Object.create(null);
        this.log('warn', 'Sync destination changed or legacy ledger migrated; baselines reset for safe content comparison.');
      }
      delete meta.remoteSnapshot; delete meta.blockedDeletes; delete meta.blockedFolders; delete meta.moveHints; delete meta.uploadChunkSize;
      delete meta.uploadBaselineSources;
      delete this.client._uploadChunk;
      meta.uploadSessions = Object.create(null);
      meta.deletions = []; meta.lastRun = null; meta.lastSuccessAt = null; meta.syncScope = scope;
    }
    if (!meta.uploadSessions || typeof meta.uploadSessions !== 'object' || Array.isArray(meta.uploadSessions)) meta.uploadSessions = Object.create(null);
    meta.uploadSessionsScope = scope;
    if (!meta.deviceId) { meta.deviceId = randomUuid(); this._identityNeedsSave = true; }
    const owner = meta.deviceId;
    if (meta.baselineOwner !== owner) {
      // Keep inherited state and journals, but absence on this installation is not deletion evidence.
      meta.unverifiedBaselineFiles = Object.fromEntries(this.manifest.fileKeys().map((k) => [k, true]));
      meta.unverifiedBaselineFolders = Object.fromEntries(this.manifest.folderKeys().map((k) => [k, true]));
      if (this.manifest.count() || this.manifest.folderKeys().length) this.log('warn', 'Inherited sync baselines require content comparison on this installation before deletions are enabled.');
      delete meta.remoteSnapshot; delete meta.blockedDeletes; delete meta.blockedFolders; delete meta.moveHints;
      meta.baselineOwner = owner;
    }
    const scanScope = JSON.stringify([s.syncFolders || [], s.excludePatterns || []]);
    if (meta.scanScope !== scanScope) { delete meta.remoteSnapshot; delete meta.blockedFolders; meta.scanScope = scanScope; }
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
    try { m = await this.manifestFn(this.client, remote.id, etag); }
    catch (e) { if (e.code === 'content_changed') throw e; return null; }
    if (!m) return null;
    const raw = (m.chunks || m.parts || []).slice().sort((a, b) => (a.idx ?? a.offset ?? 0) - (b.idx ?? b.offset ?? 0));
    const evidence = normalizeReceipt(m);
    if (m.notModified) {
      if (evidence.etag == null) evidence.etag = etag;
      await validateManifestEvidence(remote, evidence);
      return { notModified: true, ...evidence };
    }
    const declaredChecksum = evidence.checksum;
    evidence.checksum = await fingerprintOf(raw.map((c) => c.checksum));
    await validateManifestEvidence(remote, evidence);
    if (declaredChecksum != null && declaredChecksum !== evidence.checksum) throw Object.assign(new Error('Chunk manifest checksum contradicts its chunk hashes'), { code: 'bad_response' });
    if (!raw.length) return null;
    if (raw.length > MAX_MAP_CHUNKS) return { ...evidence, chunker: normalizeChunker(m.chunker), tooMany: true };
    return {
      ...evidence, chunker: normalizeChunker(m.chunker) || null, plan: raw,
      sizes: raw.map((c) => c.size), hashes: raw.map((c) => c.checksum),
      size: Number.isFinite(m.size) ? m.size : raw.reduce((n, c) => n + (c.size || 0), 0),   // authoritative total
    };
  }

  /** Stores a bounded chunk map on the ledger entry so a later change can be pulled incrementally. */
  _storeMap(rel, map, remote) {
    if (!map || map.tooMany || !Array.isArray(map.hashes) || !map.hashes.length) return;
    const entry = this.manifest.getFile(rel);
    if (!entry) return;
    const sizes = map.sizes; const hashes = map.hashes;
    if (!sizes || sizes.length !== hashes.length) return;
    // A fetched map is a cache, not proof that its content has been synced.
    this.manifest.setFile(rel, { ...entry, chunkMap: { chunker: map.chunker, sizes, hashes, contentVersion: map.contentVersion ?? null, checksum: map.checksum ?? null }, etag: map.etag || null, version: map.version || null });
  }

  /** Ensures a chunk map exists for a remote file (used after a full download so the NEXT change is incremental). */

  /** Tries to fetch only what changed. Returns {unchanged} | {buffer, map} | null (=> do a full download). */
  async _deltaPull(rel, remote, E, onProgress) {
    if (this.crypto || this.compressor || !E || !E.chunkMap || !E.chunkMap.hashes || !this.manifestFn || !this.deltaFn) return null;
    let map;
    try { map = await this._fetchChunkMap(remote, E.etag); }
    catch (e) { if (e.code === 'content_changed') throw e; return null; }
    if (!map) return null;
    if (!map.notModified && (map.tooMany || !map.hashes)) return null;
    let local;
    try { local = new Uint8Array(await this.local.readBinary(rel)); } catch (e) { return null; }
    if (await sha256Hex(local) !== E.localHash) return null;
    const available = new Map(); let offset = 0;
    for (let i = 0; i < E.chunkMap.hashes.length; i++) {
      const size = E.chunkMap.sizes && E.chunkMap.sizes[i];
      if (!Number.isSafeInteger(size) || size < 0 || offset + size > local.length) return null;
      const bytes = local.subarray(offset, offset + size);
      if (await sha256Hex(bytes) !== E.chunkMap.hashes[i]) return null;
      available.set(E.chunkMap.hashes[i], bytes); offset += size;
    }
    if (offset !== local.length) return null;
    if (map.notModified) {
      await validateManifestEvidence(remote, { etag: E.etag, contentVersion: E.chunkMap.contentVersion ?? E.remoteContentVersion,
        checksum: await fingerprintOf(E.chunkMap.hashes) });
      return { unchanged: true };
    }
    if (!Number.isSafeInteger(map.size) || map.size < 0) return null;
    const changed = map.plan.filter((c) => !available.has(c.checksum));
    const changedBytes = changed.reduce((n, c) => n + c.size, 0);
    if (changedBytes > map.size * 0.6) return null;
    const out = new Uint8Array(map.size);
    try {
      const parts = await this.deltaFn(this.client, remote.id, changed, onProgress);
      const downloaded = new Map(parts.map((p) => [p.offset, new Uint8Array(p.data)]));
      offset = 0;
      for (const c of map.plan) {
        if (c.offset !== offset || !Number.isSafeInteger(c.size) || c.size < 0 || offset + c.size > out.length) return null;
        const bytes = available.get(c.checksum) || downloaded.get(c.offset);
        if (!bytes || bytes.length !== c.size || await sha256Hex(bytes) !== c.checksum) return null;
        // Reused chunks can move or repeat after a CDC edit; place every chunk at its NEW offset.
        out.set(bytes, offset); offset += c.size;
      }
      if (offset !== out.length) return null;
    } catch (e) { if (e.code === 'content_changed') throw e; return null; } // verification failed: full download
    return { buffer: out.buffer, map, savedBytes: changedBytes };
  }

  pause() { this.stopRequested = true; }
  _redact(msg) {
    let text = String(msg || '');
    if (this._lease && this._lease.token) text = text.split(this._lease.token).join('[lease redacted]');
    return this.client.redact(text);
  }
  log(level, msg) { this.manifest.addLog(level, this._redact(msg)); }

  /** Ensures a chunk map exists for a remote file (used after a full download so the NEXT change is incremental). */
  async _ensureMap(rel, remote) {
    const entry = this.manifest.getFile(rel);
    if (entry && entry.chunkMap && entry.remoteSize === remote.size) {
      try {
        await validateManifestEvidence(remote, { etag: entry.etag, contentVersion: entry.chunkMap.contentVersion ?? entry.remoteContentVersion,
          checksum: await fingerprintOf(entry.chunkMap.hashes) });
        return entry.chunkMap;
      } catch (e) { if (e.code !== 'content_changed') throw e; } // A cache for another generation needs a fresh map.
    }
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
    } catch (e) { if (e.code === 'content_changed') throw e; /* manifest is optional */ }
  }

  /** Retries transient/incomplete transfers against the original planned content evidence. */
  async _downloadVerified(key, remote, onProgress, expectSha) {
    let lastErr = null;
    let forceFull = false;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const attemptRemote = { ...remote };
        const data = await this.downloadFn(this.client, attemptRemote, { ...this._downOpts(), onProgress, expectSha: expectSha || undefined, verifyLabel: `${key} [id=${remote.id}]`, forceFull });
        await validateManifestEvidence(remote, attemptRemote);
        if (Number.isFinite(attemptRemote.size) && attemptRemote.size !== remote.size) { remote.size = attemptRemote.size; delete remote.sig; }
        return data;
      } catch (e) {
        lastErr = e;
        if (e.code === 'content_changed') break;
        const retryable = e && (e.retryable === true || e.status >= 500 || e.status === 429);
        const checksumMismatch = /checksum mismatch/i.test(String(e && e.message));
        if ((!retryable && !checksumMismatch) || attempt === 2) break;
        if (checksumMismatch) forceFull = true;
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
    const list = (s.syncFolders || []).map((p) => nfc(String(p)).replace(/^\/+|\/+$/g, '')).filter(Boolean);
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
      if (snap && snap.files && snap.folders && snap.scope === this.manifest.data.meta.syncScope) {
        this._folders = new Set(snap.folders);
        this._remoteDirs = new Map(Object.entries(snap.dirs || {}));
        const files = new Map();
        for (const [k, v] of Object.entries(snap.files)) files.set(k, { ...v });
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
      for (const d of res.folders) {
        const child = rel ? rel + '/' + d.name : d.name;
        if (excluded(child) || excluded(child + '/') || excluded(child + '/_')) continue;
        const scopes = this._scopeExcludes();
        if (scopes && !this._inScope(child) && !scopes.some((p) => p.startsWith(child + '/'))) continue;
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
    const f = Object.create(null);
    for (const [k, v] of files) f[k] = { ...v };
    const dirs = Object.create(null);
    for (const [k, v] of this._remoteDirs) dirs[k] = { id: v && v.id, name: v && v.name };
    return { at: this.now(), scope: this.manifest.data.meta.syncScope, files: f, folders: [...this._folders], dirs };
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
    if (!Number.isFinite(snap.activityAt)) return true;
    const baseline = snap.activityAt;
    if ((this.now() - snap.at) > maxAge) return true;
    try {
      const r = await this.client.request({ path: '/activity', query: { limit: 50 } });
      const arr = (r && (r.activity || r.items || r.events)) || (Array.isArray(r) ? r : null);
      if (!Array.isArray(arr)) return true;
      return arr.some((x) => {
        const t = Date.parse((x && (x.created_at || x.createdAt || x.at || x.time)) || 0);
         return Number.isFinite(t) ? t >= baseline : true;   // equal timestamps can hide concurrent events
      });
    } catch (e) { return true; }
  }

  async _listLocal() {
    const excluded = makeExcluder(this.getSettings().excludePatterns);
    const map = new Map();
    for (const f of await this.local.list()) {
      if (excluded(f.path) || !this._inScope(f.path)) continue;
      const key = nfc(f.path);
      if (map.has(key)) throw new Error(`Local names collide after Unicode normalization: ${key}`);
      map.set(key, f);
    }
    return map;
  }

  async _localState(rel, L, E) {
    if (E && L.size === E.localSize && L.mtime === E.localMtime) {
      if (E.localHash != null) return { changed: false, hash: E.localHash };
      // Streamed download: the hash was not computed. Compute it once and record it WITHOUT treating it as a change.
        const hash = await sha256Hex(await this.local.readBinary(L.path || rel));
      E.localHash = hash;
      return { changed: false, hash };
    }
    const hash = await sha256Hex(await this.local.readBinary(L.path || rel));
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
  _trustedEntry(rel) {
    const meta = this.manifest.data.meta;
    return meta.baselineOwner === meta.deviceId && !Object.prototype.hasOwnProperty.call(meta.unverifiedBaselineFiles || {}, rel)
      ? this.manifest.getFile(rel) : undefined;
  }

  _verifyBaseline(rel) {
    const pending = this.manifest.data.meta.unverifiedBaselineFiles;
    if (pending) delete pending[rel];
  }

  _remoteBaseline(remote) {
    return {
      remoteId: remote.id, remoteSize: remote.size, remoteMtime: remote.mtime,
      remoteSig: remote.sig || remoteSig(remote), remoteChecksum: remote.checksum || null,
      remoteVersion: remote.version == null ? null : remote.version, remoteEtag: remote.etag || null,
      remoteContentVersion: remote.contentVersion == null ? null : remote.contentVersion,
      remoteSha256: remote.sha256 || null, remotePath: remote.path || null, remoteName: remote.name || null,
    };
  }

  _remoteChanged(remote, entry) {
    if (!entry.remoteSig || remote.id !== entry.remoteId || remote.size !== entry.remoteSize) return true;
    // A hydrated weak receipt has no generation or timestamp anchor for a later metadata-only GET.
    if (entry.remoteContentVersion == null && entry.remoteMtime == null) return true;
    const oldSig = String(entry.remoteSig).split('|');
    const different = ([a, b]) => a != null && a !== '' && b != null && b !== '' && String(a) !== String(b);
    const hashes = [[remote.checksum, entry.remoteChecksum ?? oldSig[3]], [remote.sha256, entry.remoteSha256]];
    if (hashes.some(different)) return true;
    if (remote.contentVersion != null && entry.remoteContentVersion != null) {
      // Current generations describe bytes; legacy version is an archived receipt, not the head generation.
      return String(remote.contentVersion) !== String(entry.remoteContentVersion);
    }
    const fields = [
      [remote.mtime, entry.remoteMtime], [remote.checksum, entry.remoteChecksum ?? oldSig[3]],
      [remote.version, entry.remoteVersion ?? oldSig[4]], [remote.etag, entry.remoteEtag],
      [remote.sha256, entry.remoteSha256], [remote.path, entry.remotePath], [remote.name, entry.remoteName],
    ];
    // Listing and metadata endpoints need not expose the same optional fields.
    return fields.some(different);
  }

  _record(rel, { lhash, lstat, remote, text }) {
    const prev = this.manifest.getFile(rel);
    this.manifest.setFile(rel, {
      localHash: lhash, localSize: lstat.size, localMtime: lstat.mtime,
      ...this._remoteBaseline(remote), syncedAt: this.now(),
      baseText: text !== undefined ? text : (prev ? prev.baseText : null) || null,
    });
    this._verifyBaseline(rel);
  }

  _deleteSnapshot(p) {
    const folder = p.key.endsWith('/');
    const remote = p.R || null; const entry = p.E || null;
    const dir = folder && this._remoteDirs && this._remoteDirs.get(p.key.slice(0, -1));
    return {
      path: p.key, action: p.action, scope: this.manifest.data.meta.syncScope,
      remoteId: folder ? dir && dir.id || null : remote && remote.id || entry && entry.remoteId || null,
      remoteContentVersion: remote ? remote.contentVersion ?? null : (entry && entry.remoteContentVersion) ?? null,
      remoteSig: remote ? remote.sig || remoteSig(remote) : entry && entry.remoteSig || null,
      remoteHash: p.remoteHash || null,
      remotePath: remote && remote.path || null, remoteName: remote && remote.name || null,
      localHash: p.action === 'deleteLocal' && !folder ? p.lhash || null : null,
      localSize: p.action === 'deleteLocal' && p.L ? p.L.size : null,
      localMtime: p.action === 'deleteLocal' && p.L ? p.L.mtime : null,
    };
  }

  _deleteApproved(snapshot) {
    const reviewed = this._approvedDeletes && this._approvedDeletes.get(`${snapshot.action}:${snapshot.path}`);
    if (!reviewed || this._approvalScopeChanged) return false;
    return Object.keys(snapshot).every((field) => reviewed[field] === snapshot[field]);
  }

  async _recoverUploads(opts) {
    if (opts.dryRun) return false;
    const meta = this.manifest.data.meta;
    const root = folderPathFor(this.getSettings().remoteFolder, '');
    const excluded = makeExcluder(this.getSettings().excludePatterns);
    const byId = new Map();
    for (const rel of this.manifest.fileKeys()) {
      const E = this._trustedEntry(rel);
      if (!E || !E.remoteId) continue;
      if (!byId.has(E.remoteId)) byId.set(E.remoteId, []);
      byId.get(E.remoteId).push(rel);
    }
    let recovered = false;
    for (const [key, record] of Object.entries(meta.uploadSessions || {})) {
      if (!record || record.owner !== meta.deviceId || !['opening', 'uploading', 'completing', 'completed'].includes(record.phase)) continue;
      if (meta.uploadBaselineSources && meta.uploadBaselineSources[key] === record.source) continue;
      const body = record.body;
      if (!body) continue;
      let rel;
      if (body.baseId) {
        const paths = byId.get(String(body.baseId)) || [];
        if (paths.length !== 1) continue;
        rel = paths[0];
      } else {
        if (typeof body.path !== 'string' || !body.path.startsWith(root) || typeof body.name !== 'string') continue;
        rel = nfc(body.path.slice(root.length) + body.name);
      }
      if (excluded(rel) || !this._inScope(rel) || opts.only && !opts.only.includes(rel) || this.conflicts.has(rel)) continue;
      const E = this._trustedEntry(rel);
      if (E && record.fileId && E.remoteId !== record.fileId) continue;
      if (E && body.expectedVersion != null && E.remoteContentVersion != null && String(body.expectedVersion) !== String(E.remoteContentVersion)) continue;
      if (record.phase === 'completed' && E && E.localHash === record.sourceHash) {
        if (!meta.uploadBaselineSources) meta.uploadBaselineSources = Object.create(null);
        Object.defineProperty(meta.uploadBaselineSources, key, { value: record.source, enumerable: true, configurable: true, writable: true });
        continue;
      }
      await this._assertOperation();
      const result = await recoverCommittedUpload(this.client, { key, uploadJournal: this._upOpts().uploadJournal });
      if (!result) continue;
      await this._assertOperation();
      const { file, record: committed } = result;
      if (E && E.remoteId !== file.id) throw new Error('Recovered upload identity changed; baseline kept');
      // Receipt recovery acknowledges the original plaintext, never the newer local edit or GET head.
      const exists = await this.local.exists(rel);
      if (!exists && !E) continue;
      let lstat = { size: -1, mtime: -1 };
      if (exists) {
        const stat = await this.local.stat(rel);
        const hash = await sha256Hex(await this.local.readBinary(rel));
        if (hash === committed.sourceHash && stat) lstat = stat;
      }
      const location = splitRel(rel);
      this._record(rel, { lhash: committed.sourceHash, lstat,
        remote: { ...file, name: location.name, path: folderPathFor(this.getSettings().remoteFolder, location.dir) }, text: null });
      if (!byId.has(file.id)) byId.set(file.id, [rel]);
      if (!meta.uploadBaselineSources) meta.uploadBaselineSources = Object.create(null);
      Object.defineProperty(meta.uploadBaselineSources, key, { value: committed.source, enumerable: true, configurable: true, writable: true });
      await this.manifest.save();
      recovered = true;
    }
    return recovered;
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
        if (this.operationValidity && await this.operationValidity() === false) throw new Error('Sync operation is no longer valid; folder creation stopped');
        try { await this.client.createFolder(seg, parent); this._markRemoteMutated(); }
        catch (e) {
          if (!(e.status === 409 || /exist/i.test(e.code + ' ' + e.message))) throw e;
          // ALREADY_EXISTS also covers trash. It does NOT prove the requested folder is live.
          this._dirCache.delete(parent);
          if (!(await this._dirIndex(parent)).folders.has(nfc(seg))) throw e;
        }
        idx.folders.add(nfc(seg));
      }
      this._folders.add(key);
    }
  }

  // ---------- primitive operations ----------
  async _replaceUpload({ name, folderPath, data, mtime, old, onProgress, chunker, beforeReplace, expectedVersion }) {
    // In-place update first: when replacing a file we already have on the server, a revision sends ONLY the
    // changed chunks (unchanged chunks are reused server-side), commits atomically, and keeps the same file id.
    if (old && old.id && this.revisionFn) {
      try {
        if (beforeReplace) expectedVersion = await beforeReplace();
        const rev = await this.revisionFn(this.client, { id: old.id, name, folderPath, data, mtime, onProgress, chunker, ...this._upOpts(), expectedVersion });
        if (rev && rev.id) {
          this._markRemoteMutated();
          return rev;
        }
      } catch (e) { throw e; }
      // Small file (or a revision that did not apply): replace the content IN PLACE via the inline PATCH form,
      // which runs through the same atomic revision engine (no new file, no trash, previous version archived).
      try {
        let payload = data;
        if (this.compressor) payload = await this.compressor.maybeCompress(name, payload);
        if (this.crypto) payload = await this.crypto.encrypt(payload);
        const lim = await this.client.limits();
        if (payload.byteLength > lim.oneShotMaxBytes) throw new Error('Content replacement requires a resumable revision');
        if (beforeReplace) expectedVersion = await beforeReplace();
        const patched = await this.client.patchContent(old.id, payload, { expectedVersion });
        if (patched && patched.id) {
          this._markRemoteMutated();
          return validateUploadReceipt(this.client, { file: patched, receipt: normalizeReceipt(patched.raw || patched),
            size: payload.byteLength, fingerprint: await fingerprintOf([await sha256Hex(payload)]), data: payload });
        }
      } catch (e) { throw e; }
      throw new Error('In-place update did not return a file; previous version was kept');
    }
    if (old) throw new Error('Cannot replace a remote file without in-place update support; previous version kept');
    return this.uploadFn(this.client, { name, folderPath, data, mtime, onProgress, chunker, ...this._upOpts() });
  }

  async pushLocal(rel, oldRemote, onProgress, localPath = rel, { expectedContentVersion, expectedLocalHash, createOnly = false } = {}) {
    if (createOnly && oldRemote) throw new Error('Create-only upload cannot replace an existing remote file');
    const lstat = await this.local.stat(localPath); // stat BEFORE read: a concurrent edit is then detected next run
    const data = new Uint8Array(await this.local.readBinary(localPath)).slice().buffer;
    const lhash = await sha256Hex(data);
    if (expectedLocalHash !== undefined && lhash !== expectedLocalHash) throw Object.assign(new Error('Local file changed after review; upload stopped'), { code: 'local_changed' });
    const { dir, name } = splitRel(rel);
    const conflict = this.manifest.getConflict(rel);
    if (conflict && conflict.movePending && oldRemote && (oldRemote.path !== folderPathFor(this.getSettings().remoteFolder, dir) || nfc(oldRemote.name || '') !== name)) throw new Error('Rename conflict still has its remote source at another path; complete the path move before resolving');
    await this._ensureFolders(dir);
    this._markRemoteMutated();
    const folderPath = folderPathFor(this.getSettings().remoteFolder, dir);
    const compareOccupant = async (occupant) => {
      const expected = await this._expectedSha(occupant);
      const remoteData = await this._downloadVerified(rel, occupant, onProgress, expected.sha);
      if (await sha256Hex(remoteData) !== lhash) {
        if (createOnly) throw Object.assign(new Error('Create-only upload destination is occupied by different content'), { code: 'remote_collision' });
        await this.conflicts.record(rel, occupant, remoteData);
        return { conflict: true };
      }
      this._record(rel, { lhash, lstat, remote: occupant, text: this._baseTextFor(data) });
      if (expected.map) this._storeMap(rel, expected.map, occupant);
      return { unchanged: true, ...occupant };
    };
    // No known remote copy: a file with this name may still be there (listing gap, lost ledger).
    // A newly discovered occupant was never part of the upload decision. Do not overwrite it.
    if (!oldRemote) {
      let occupant;
      if (createOnly) {
        const listing = await this.client.listFolder(folderPath);
        const matches = listing.files.filter((f) => nfc(f.name) === nfc(name));
        if (matches.length > 1 || listing.folders.some((f) => nfc(f.name) === nfc(name))) throw Object.assign(new Error('Create-only upload destination is ambiguous or occupied by a folder'), { code: 'remote_collision' });
        occupant = matches[0];
      } else occupant = (await this._dirIndex(folderPath)).files.get(nfc(name));
      if (occupant) return compareOccupant(occupant);
    }
    // Reuse the chunker we already know for this file so a later edit keeps the same boundaries (best reuse).
    const prev = this.manifest.getFile(rel);
    const cdc = this.getSettings().cdcFiles !== false;    // false = plain fixed chunks (revisable base)
    const chunker = cdc ? ((prev && prev.chunkMap && prev.chunkMap.chunker) || undefined) : false;
    let changedRemote = null;
    let beforeReplace = null;
    if (oldRemote) {
      const planned = this._remoteBaseline(oldRemote);
      if (expectedContentVersion != null) planned.remoteContentVersion = expectedContentVersion;
      if (!conflict && prev && prev.remoteId === oldRemote.id) {
        for (const field of ['remoteChecksum', 'remoteVersion', 'remoteEtag', 'remoteSha256', 'remoteContentVersion']) if (planned[field] == null) planned[field] = prev[field];
      }
      beforeReplace = async () => {
        if (this.operationValidity && await this.operationValidity() === false) throw new Error('Sync operation is no longer valid; replacement stopped');
        const live = await this._liveById(oldRemote.id);
        if (this.operationValidity && await this.operationValidity() === false) throw new Error('Sync operation is no longer valid; replacement stopped');
        if (!live) throw new Error('Remote file disappeared before replacement; local copy kept');
        if ((live.path && live.path !== folderPath) || (live.name && nfc(live.name) !== name)) throw new Error('Remote file moved before replacement; both copies kept for replanning');
        if (this._remoteChanged(live, planned)) {
          changedRemote = live;
          throw Object.assign(new Error('Remote content changed after planning; replacement stopped for review'), { code: 'remote_changed' });
        }
        if (this._capabilities && this._capabilities.expectedContentVersion) {
          if (live.contentVersion == null) throw new Error('Current content version missing; replacement stopped');
          if (planned.remoteContentVersion == null) planned.remoteContentVersion = live.contentVersion;
          return planned.remoteContentVersion;
        }
      };
    }
    let res;
    try { res = await this._replaceUpload({ name, folderPath, data, mtime: lstat.mtime, old: oldRemote, onProgress, chunker, beforeReplace }); }
    catch (e) {
      if (oldRemote && (this._isVersionConflict(e) || e.code === 'remote_changed' && conflict && this._capabilities && this._capabilities.expectedContentVersion)) {
        const live = changedRemote || await this._liveById(oldRemote.id);
        if (!live) throw e;
        // A rejected precondition is not permission to overwrite the new head, including reviewed conflicts.
        if (conflict) {
          const expected = await this._expectedSha(live);
          await this.conflicts.record(rel, live, await this._downloadVerified(rel, live, onProgress, expected.sha));
          throw Object.assign(new Error('Remote content changed during replacement; review the preserved versions'), { code: 'remote_changed' });
        }
        return compareOccupant(live);
      }
      if (e.code !== 'remote_changed' || !changedRemote || conflict) throw e;
      return compareOccupant(changedRemote);
    }
    const relocated = oldRemote && ((res.name && nfc(res.name) !== name) || (res.path && res.path !== folderPath));
    const index = await this._dirIndex(folderPath);
    if (relocated) index.files.delete(nfc(name)); else index.files.set(nfc(name), res);
    // Content was committed, but a concurrent rename has not yet been applied to the local path.
    this._record(rel, { lhash, lstat, remote: relocated ? { ...res, name, path: folderPath } : res, text: this._baseTextFor(data) });
    // Record the resulting chunk map (the server's own, if it re-chunked) so the next change is incremental.
    if (res.chunks && res.chunker) this._storeMap(rel, { chunker: res.chunker, sizes: res.chunks.map((c) => c.size), hashes: res.chunks.map((c) => c.hash), etag: null }, res);
    else if (res.size >= DELTA_MIN_BYTES) await this._ensureMap(rel, res);
    this._touched && this._touched.set(rel, res.id);
    return res;
  }

  async pullRemote(rel, remote, onProgress) {
    const exists = await this.local.exists(rel);
    const L = exists ? await this.local.stat(rel) : null;
    const E = L ? { ...(this.manifest.getFile(rel) || {}), localHash: await sha256Hex(await this.local.readBinary(rel)) } : null;
    const data = await this.downloadFn(this.client, remote, { ...this._downOpts(), onProgress });
    return this._writeDownloaded(rel, remote, data, L, E);
  }

  async _writeDownloaded(rel, remote, data, L, E) {
    const conflict = this.manifest.getConflict(rel);
    if (conflict && conflict.movePending && (remote.path !== folderPathFor(this.getSettings().remoteFolder, splitRel(rel).dir) || nfc(remote.name || '') !== splitRel(rel).name)) throw new Error('Rename conflict still has its remote source at another path; complete the path move before resolving');
    const localPath = L && L.path || rel;
    const rhash = await sha256Hex(data);
    remote.sig = remote.sig || remoteSig(remote);
    if (L && E) {
      const cur = await sha256Hex(await this.local.readBinary(localPath));
      if (cur !== E.localHash) return { conflict: true, data };           // local changed since the decision
      if (rhash === E.localHash && E.localSize != null) {                    // only remote metadata changed
        this.manifest.setFile(rel, { ...E, ...this._remoteBaseline(remote), syncedAt: this.now(), baseText: E.baseText || this._baseTextFor(data) });
        return { unchanged: true };
      }
    } else if (L || await this.local.exists(rel)) {
      // File exists locally with no ledger entry: never overwrite without an explicit policy.
      if ((await sha256Hex(await this.local.readBinary(localPath))) !== rhash) return { conflict: true, data };
    }
    // Replace the local file only with fully-verified bytes, atomically when the adapter supports it.
    const writeOptions = L ? { expectedHash: E ? E.localHash : rhash } : { createOnly: true };
    if (typeof this.local.writeBinaryAtomic === 'function') await this.local.writeBinaryAtomic(localPath, data, remote.mtime, writeOptions);
    else await this.local.writeBinary(localPath, data, remote.mtime, writeOptions);
    this._record(rel, { lhash: rhash, lstat: await this.local.stat(localPath), remote, text: this._baseTextFor(data) });
    return { written: true };
  }

  /** Equality acknowledges no choice between versions. Copies/history and unfinished reviews stay untouched. */
  async _reconcileConflict({ key, L, R }) {
    const conflict = this.manifest.getConflict(key);
    if (!conflict || !conflict.remoteId || conflict.movePending || conflict.resolution || !L || R && R.id !== conflict.remoteId) return false;
    const reviewed = JSON.stringify(conflict);
    const ledger = this.manifest.getFile(key);
    const meta = this.manifest.data.meta;
    const scope = meta.syncScope;
    const { dir, name } = splitRel(key);
    const folderPath = folderPathFor(this.getSettings().remoteFolder, dir);
    const localPath = L.path || key;
    const unchanged = () => {
      const s = this.getSettings();
      return !this.stopRequested && !s.paused && this.manifest.data.meta === meta && meta.syncScope === scope
        && this.manifest.getConflict(key) === conflict && JSON.stringify(conflict) === reviewed
        && this.manifest.getFile(key) === ledger && this._inScope(key) && !makeExcluder(s.excludePatterns)(key);
    };
    const active = async () => { await this._assertOperation(); return unchanged(); };
    const limit = () => {
      const s = this.getSettings();
      const cellular = typeof s.networkIsCellular === 'function' && s.networkIsCellular();
      if (cellular && s.wifiOnly) return 0;
      return Math.min(CONFLICT_COMPARE_MAX_BYTES, s.maxFileBytes > 0 ? s.maxFileBytes : Infinity,
        cellular && s.cellularMaxFileBytes > 0 ? s.cellularMaxFileBytes : Infinity);
    };
    const bounded = (stat) => stat && Number.isSafeInteger(stat.size) && stat.size >= 0 && stat.size <= limit() && limit() > 0;
    const atPath = (remote) => remote && remote.id === conflict.remoteId && nfc(remote.path || '') === folderPath && nfc(remote.name || '') === name;
    const pinned = async (remote, evidence) => {
      await validateManifestEvidence(remote, evidence);
      const generation = remote.contentVersion != null && remote.contentVersion === evidence.contentVersion
        || remote.etag != null && remote.etag === evidence.etag;
      return generation || ['contentVersion', 'etag', 'checksum'].every((field) => remote[field] == null || evidence[field] != null);
    };
    if (!await active() || !bounded(L) || R && !bounded(R)) return false;
    const remote = await this._liveById(conflict.remoteId);
    if (!await active() || !atPath(remote) || !bounded(remote)) return false;
    if (R && !await pinned(R, remote)) return false;
    const lstat = await this.local.stat(localPath);
    if (!bounded(lstat) || !await active()) return false;
    const data = new Uint8Array(await this.local.readBinary(localPath)).slice().buffer;
    if (data.byteLength !== lstat.size || !bounded({ size: data.byteLength })) return false;
    const lhash = await sha256Hex(data);
    const map = await this._fetchChunkMap(remote, null);
    if (!await active() || map && (map.notModified || map.tooMany || !bounded(map) || map.size !== remote.size)) return false;
    const expected = { map, sha: map && map.hashes && map.hashes.length === 1 ? map.hashes[0] : null };
    const proof = { ...remote };
    for (const [field, value] of Object.entries(normalizeReceipt(expected.map))) if (value != null) proof[field] = value;
    if (proof.contentVersion == null && proof.etag == null && proof.checksum == null) return false;
    // One pinned attempt, not a retry/refresh loop that could silently acknowledge a different head.
    const attemptRemote = { ...proof };
    const remoteData = await this.downloadFn(this.client, attemptRemote, { ...this._downOpts(), expectSha: expected.sha || undefined,
      onProgress: this._fileProgress(key, 'compare'), verifyLabel: `${key} [id=${remote.id}]` });
    if (!await active() || attemptRemote.id !== remote.id || !bounded(attemptRemote) || !bounded({ size: remoteData.byteLength }) || !await pinned(proof, attemptRemote)) return false;
    if (await sha256Hex(remoteData) !== lhash) return false;
    const latest = await this._liveById(remote.id);
    if (!await active() || !atPath(latest) || !bounded(latest) || !await pinned(proof, latest)) return false;
    // Without generation evidence, even a metadata change needs a fresh run, not baseline hydration.
    if (this._remoteChanged(latest, this._remoteBaseline(remote))) return false;
    const stat = await this.local.stat(localPath);
    if (!bounded(stat) || !await active()) return false;
    const current = await this.local.readBinary(localPath);
    if (current.byteLength !== stat.size || !bounded({ size: current.byteLength }) || await sha256Hex(current) !== lhash || !unchanged()) return false;
    const unverified = meta.unverifiedBaselineFiles;
    const wasUnverified = unverified && Object.prototype.hasOwnProperty.call(unverified, key);
    const previousUnverified = wasUnverified && unverified[key];
    this._record(key, { lhash, lstat: stat, remote: proof, text: this._baseTextFor(data) });
    const adopted = this.manifest.getFile(key);
    this.manifest.deleteConflict(key);
    try { await this.manifest.save(); }
    catch (e) {
      if (!this.manifest.getConflict(key)) this.manifest.setConflict(key, conflict);
      if (this.manifest.getFile(key) === adopted) {
        if (ledger) this.manifest.setFile(key, ledger); else this.manifest.deleteFile(key);
        if (wasUnverified && meta.unverifiedBaselineFiles === unverified) unverified[key] = previousUnverified;
      }
      throw e;
    }
    // Never add this path to _touched: a later listing is not proof of the equality just saved.
    this.log('info', `Conflict cleared: identical verified local and remote content (${key}); conflict copies kept.`);
    return true;
  }
  // ---------- main entry ----------
  async syncNow(opts = {}) {
    const rep = emptyReport();
    if (this.running) { rep.status = 'busy'; return rep; }
    this.running = true; this.stopRequested = false; this._touched = new Map(); this._dirCache = new Map();
    this._runId = this.now(); this._dryRun = !!opts.dryRun; this._pendingSnapshot = null; this._remoteMutated = false; this._rescanNeeded = false;
    const beforeDryRun = opts.dryRun ? JSON.stringify(this.manifest.data) : null;
    this._runSettings = { ...this._settingsSource() };
    for (const k of ['excludePatterns', 'syncFolders']) this._runSettings[k] = [...(this._runSettings[k] || [])];
    const s = this.getSettings();
    const mode = opts.mode || s.mode || 'two-way';
    let netFails = 0; let serverFails = 0; let lock = null;
    try {
      if (s.e2eeEnabled && !this.crypto) throw new Error('Encryption is enabled but no encryption key is configured');
      if (s.wifiOnly && typeof s.networkIsCellular === 'function' && s.networkIsCellular()) {
        rep.status = 'deferred'; rep.warnings.push('Wi-Fi only: sync deferred on a cellular connection.'); return rep;
      }
      await this._bindScope();
      const meta = this.manifest.data.meta;
      const reviewed = opts.approvedDeletes || (opts.approveDeletes && meta.blockedDeletes && meta.blockedDeletes.items) || [];
      this._approvedDeletes = opts.approveDeletes ? new Map(reviewed.map((p) => [`${p.action}:${p.path}`, { ...p }])) : null;
      this._approvalScopeChanged = opts.approvedScope != null && opts.approvedScope !== meta.syncScope;
      await this.client.limits();               // validates the key + discovers upload limits
      await this._discoverCapabilities();
      const remoteChanged = await this._remoteChangedSinceSnapshot();   // read BEFORE the lock churns /activity
      const activityAt = await this._latestActivityTime();
      if (!opts.dryRun) lock = await this.acquireOperationLease();
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
      const recovered = await this._recoverUploads(opts);
      const { files: remote, rootMissing } = await this._listRemote(rootPath, rep, recovered || remoteChanged);
      if (this._pendingSnapshot) this._pendingSnapshot.activityAt = activityAt;
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
      const pendingMoveSources = new Map(this.conflicts.list().filter((c) => c.movePending && c.moveFrom).map((c) => [c.moveFrom, c.remoteId]));
      const pendingMoveIds = new Set(pendingMoveSources.values());
      let i = 0;
      for (const key of [...keys].sort()) {
        if (this.stopRequested) { rep.status = 'paused'; break; }
        this.onProgress({ phase: 'scan', done: ++i, total: keys.size, current: key });
        const L = local.get(key); const R = remote.get(key); const E = this._trustedEntry(key);
        const conflictOpen = this.conflicts.has(key) || !!(E && pendingMoveSources.get(key) === E.remoteId) || !!(R && pendingMoveIds.has(R.id));
        let ls = { changed: false };
        try { if (L && !conflictOpen) ls = await this._localState(key, L, E); }
        catch (e) { rep.failures.push({ path: key, error: e.message }); continue; }
        const remoteChanged = !!(E && R && this._remoteChanged(R, E));
        let action = decide({ hasLocal: !!L, hasRemote: !!R, hasEntry: !!E, localChanged: ls.changed, remoteChanged, conflictOpen });
        if (action !== 'skip-conflict' && E && E.moveVerify && L && R) action = 'compare';
        plan.push({ key, L, R, E, action, lhash: ls.hash });
      }

      // A known identity at another path is not a new local file plus an unrelated download.
      const remoteById = new Map([...remote].map(([key, value]) => [value.id, { key, remote: value }]));
      const planByKey = new Map(plan.map((p) => [p.key, p]));
      for (const p of plan) {
        if (!p.L || !p.E || p.action === 'skip-conflict') continue;
        const moved = remoteById.get(p.E.remoteId);
        if (!moved || moved.key === p.key) continue;
        const hash = await sha256Hex(await this.local.readBinary(p.L.path || p.key));
        if (hash === p.E.localHash && !p.R) continue;
        p.action = 'compare'; p.R = moved.remote; p.lhash = hash; p.remoteMoveTo = moved.key;
        const destination = planByKey.get(moved.key);
        if (destination) destination.action = 'skip-conflict';
      }

      // Legacy metadata cannot identify a content generation; prove the bytes before reviewing deletion.
      for (const p of plan) if (p.action === 'deleteRemote' && p.R.contentVersion == null) {
        const cellular = typeof s.networkIsCellular === 'function' && s.networkIsCellular();
        const limit = Math.min(s.maxFileBytes > 0 ? s.maxFileBytes : Infinity, cellular && s.cellularMaxFileBytes > 0 ? s.cellularMaxFileBytes : Infinity);
        if (p.R.size > limit) continue; // The execution guard will defer this operation without reading content.
        const expected = await this._expectedSha(p.R);
        p.remoteHash = await sha256Hex(await this._downloadVerified(p.key, p.R, null, expected.sha));
        if (!p.E.localHash || p.remoteHash !== p.E.localHash) p.action = 'download';
      }

      // ---- deletion safety ----
      const dels = plan.filter((p) => p.action === 'deleteLocal' || p.action === 'deleteRemote');
      const byPlanKey = new Map(plan.map((p) => [p.key, p]));
      const expandedHints = Object.create(null);
      for (const [from, info] of Object.entries(this.manifest.data.meta.moveHints || {})) {
        if (!info || typeof info.to !== 'string') continue;
        const candidates = byPlanKey.has(from) ? [from] : plan.filter((p) => p.key.startsWith(from + '/')).map((p) => p.key);
        for (const key of candidates) {
          const to = info.to + key.slice(from.length); const src = byPlanKey.get(key); const up = byPlanKey.get(to);
          if (src && !src.L && src.R && src.E && src.E.remoteId && ['deleteRemote', 'download'].includes(src.action) && up && up.L && ['upload', 'compare'].includes(up.action)) expandedHints[key] = { to, hint: from };
        }
      }
      const hintedMoves = mode === 'two-way' && !opts.only && s.deletionPolicy !== 'off' ? new Set(Object.keys(expandedHints)) : new Set();
      const localDirs = await this._localDirSet();
      const folderDels = localDirs ? [...knownDirs].filter((d) => localDirs.has(d) !== this._folders.has([...splitPath(s.remoteFolder), ...splitPath(d)].join('/')))
        .map((d) => ({ key: d + '/', action: localDirs.has(d) ? 'deleteLocal' : 'deleteRemote' })) : [];
      const allDels = [...dels.filter((p) => !hintedMoves.has(p.key)), ...folderDels];
      for (const p of allDels) {
        if (p.action === 'deleteLocal' && p.L) p.lhash = await sha256Hex(await this.local.readBinary(p.L.path || p.key));
        p.deleteSnapshot = this._deleteSnapshot(p);
      }
      let blockDeletes = false; let blockReason = null;
      const fullRun = !opts.only && mode === 'two-way';
      if (allDels.length) {
        const limit = s.maxDeletesPerRun == null ? 10 : s.maxDeletesPerRun;
        if (opts.only || mode !== 'two-way' || s.deletionPolicy === 'off') blockDeletes = true;
        else if (local.size === 0 && this.manifest.count() > 0) {
          blockDeletes = true; blockReason = 'empty-local'; rep.warnings.push('Deletions blocked: the local vault looks empty.');
        } else if (remote.size === 0 && this.manifest.count() > 0) {
          blockDeletes = true; blockReason = 'empty-remote'; rep.warnings.push('Deletions blocked: the remote folder looks empty.');
        } else if (this._approvedDeletes && (this._approvalScopeChanged || this._approvedDeletes.size !== allDels.length || allDels.some((p) => !this._deleteApproved(p.deleteSnapshot)))) {
          blockDeletes = true; blockReason = 'changed'; rep.warnings.push('Deletion plan changed after review; review the new list before approving.');
        } else if (allDels.length > limit && !this._approvedDeletes) {
          blockDeletes = true; blockReason = 'limit';
          rep.warnings.push(`Deletions blocked: ${allDels.length} files/folders would be deleted (limit ${limit}). Use "Approve Pending Deletions" after reviewing.`);
        }
      }
      // Remember what is waiting for approval so the UI can show it (it used to vanish silently every run).
      if (fullRun) {
        this.manifest.data.meta.blockedDeletes = blockReason
          ? { at: this.now(), reason: blockReason, items: allDels.map((p) => ({ ...p.deleteSnapshot })) } : null;
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
      if (!opts.only && mode === 'two-way' && s.deletionPolicy !== 'off') {
        const byKey = new Map(plan.map((p) => [p.key, p]));
        const srcFor = (fromKey, trusted = false) => {
          const src = byKey.get(fromKey);
          return (src && !src.L && src.R && (src.action === 'deleteRemote' || trusted && src.action === 'download') && !pairBySrc.has(src)) ? src : null;
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
        const hints = expandedHints;
        for (const [from, info] of Object.entries(hints)) {
          const to = info && info.to;
          const up = to && byKey.get(to);
          if (!up || !['upload', 'compare'].includes(up.action)) continue;
          const src = srcFor(from, true);
          if (!src) continue;
          addPair(up, src); consumedHints.push(info.hint || from);
        }

        // Cleanup limits must not turn a trusted identity-preserving move into upload+trash.
        // Heuristic moves retain the ordinary deletion approval requirement.
        if (!blockDeletes) {
          // 2) identical content hash
          const byHash = new Map();
          const uploadsByHash = new Map();
          for (const p of plan) {
            if (p.action === 'deleteRemote' && p.E && p.E.localHash && !pairBySrc.has(p)) {
              if (!byHash.has(p.E.localHash)) byHash.set(p.E.localHash, []);
              byHash.get(p.E.localHash).push(p);
            }
          }
          for (const p of plan) if (p.action === 'upload' && p.L && p.lhash && !pairByUp.has(p)) uploadsByHash.set(p.lhash, (uploadsByHash.get(p.lhash) || 0) + 1);
          for (const p of plan) {
            if (p.action !== 'upload' || !p.lhash || !p.L || pairByUp.has(p)) continue;
            const list = byHash.get(p.lhash);
            if (!list || list.length !== 1 || uploadsByHash.get(p.lhash) !== 1) continue;
            addPair(p, list.pop());
          }

          // 3) same basename + empty destination (last resort for a move+edit made while the app was closed)
          const nameOf = (k) => { const parts = splitRel(k); return nfc(parts.name).toLowerCase(); };
          const byName = new Map();
          const uploadsByName = new Map();
          for (const p of plan) {
            if (p.action === 'deleteRemote' && p.R && !pairBySrc.has(p)) {
              const n = nameOf(p.key);
              if (!byName.has(n)) byName.set(n, []);
              byName.get(n).push(p);
            }
          }
          for (const p of plan) if (p.action === 'upload' && p.L && !p.R && !pairByUp.has(p)) {
            const name = nameOf(p.key); uploadsByName.set(name, (uploadsByName.get(name) || 0) + 1);
          }
          for (const p of plan) {
            if (p.action !== 'upload' || !p.L || p.R || pairByUp.has(p)) continue;   // destination must be free
            const list = byName.get(nameOf(p.key));
            if (!list || list.length !== 1 || uploadsByName.get(nameOf(p.key)) !== 1) continue;
            addPair(p, list.pop());
          }
        }
        // Hints are only cleared after every associated move succeeds, so failures remain retryable.
      }

      // ---- size + network guards (skip heavy transfers without ever touching data) ----
      const cellular = typeof s.networkIsCellular === 'function' ? !!s.networkIsCellular() : false;
      const netBlockAll = cellular && !!s.wifiOnly;
      const netMaxBytes = cellular && !s.wifiOnly && Number.isFinite(s.cellularMaxFileBytes) && s.cellularMaxFileBytes > 0 ? s.cellularMaxFileBytes : 0;
      const maxFileBytes = Number.isFinite(s.maxFileBytes) && s.maxFileBytes > 0 ? s.maxFileBytes : 0;
      const blockedByGuard = (p) => {
        const netAction = p.action === 'upload' || p.action === 'download' || p.action === 'compare' || p.action === 'deleteRemote' || p.action === 'skip-conflict';
        if (netBlockAll && netAction) return 'wifi-only';
        const bytes = p.action === 'skip-conflict' ? Math.max(p.L && p.L.size || 0, p.R && p.R.size || 0) : p.action === 'upload' ? (p.L && p.L.size) : (p.R && p.R.size);
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
          const pair = pairByUp.get(p) || pairBySrc.get(p);
          if (!pair && !this._allowed(p.action, mode, blockDeletes)) { if (p.action === 'skip-conflict') buckets.skipConflict.push(p.key); else buckets.blocked.push({ path: p.key, why: 'not-allowed' }); continue; }
          const why = pair ? blockedByGuard({ ...pair.up, action: 'upload' }) || blockedByGuard({ ...pair.src, action: 'download' }) : blockedByGuard(p);
          if (why) { buckets.blocked.push({ path: p.key, why }); continue; }
          if (pairBySrc.has(p)) { if (!moveKeys.has(p.key)) { /* handled with its upload half */ } buckets.move.push({ from: p.key, to: pairBySrc.get(p).up.key }); continue; }
          if (pairByUp.has(p)) continue;   // counted via its src entry
          buckets[p.action] && buckets[p.action].push(p.key);
        }
        rep.plan = buckets;
        rep.errors = rep.failures.length; rep.status = rep.errors ? 'errors' : 'ok';
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
        const pair = pairByUp.get(p) || pairBySrc.get(p);
        const allowed = !!pair || p.action === 'skip-conflict' && this.conflicts.has(p.key) || this._allowed(p.action, mode, blockDeletes);
        if (!allowed) { rep.skipped++; if (p.action === 'skip-conflict' && this.conflicts.has(p.key)) rep.conflicts++; continue; }
        const guard = pair ? blockedByGuard({ ...pair.up, action: 'upload' }) || blockedByGuard({ ...pair.src, action: 'download' }) : blockedByGuard(p);
        if (guard === 'wifi-only') { rep.skipped++; if (!netWarned.all) { netWarned.all = true; this.log('warn', 'Skipped network transfers (Wi-Fi only on a cellular connection).'); } continue; }
        if (guard === 'size-limit') { rep.skipped++; if (!sizeWarned.has('max')) { sizeWarned.add('max'); const msg = `Skipped file(s) larger than the ${bytesToHuman(maxFileBytes)} size limit (first: ${p.key}).`; rep.warnings.push(msg); this.log('warn', msg); } continue; }
        if (guard === 'cellular-defer') { rep.skipped++; if (!netWarned.cellular) { netWarned.cellular = true; const msg = `On cellular: deferred file(s) larger than ${bytesToHuman(netMaxBytes)}.`; rep.warnings.push(msg); this.log('warn', msg); } continue; }
        try {
          // Rename/move: PATCH the existing remote file instead of upload+trash. Works from either half.
          if (pair && moveDone.has(pair)) { rep.skipped++; continue; }   // the other half of a completed move
          if (pair && moveFailed.has(pair)) { rep.skipped++; continue; }
          if (pair && !moveFailed.has(pair)) {
            try {
              const result = await this._executeMove(pair.up, pair.src, rep, pair.contentChanged);
              if (result === 'conflict') { moveFailed.add(pair); netFails = 0; continue; }
              if (result) { moveDone.add(pair); netFails = 0; continue; }
              throw new Error('Move destination is no longer free; source kept');
            } catch (e) {
              moveFailed.add(pair);
              throw e;
            }
          }
          await this._execute(p, rep);
          netFails = 0; serverFails = 0;
          // Persist progress regularly: if Obsidian is killed mid-run (Android) finished files are not re-compared.
          if (++sinceSave >= 20) { sinceSave = 0; try { await this.manifest.save(); } catch (_) { /* retried at the end */ } }
        } catch (e) {
          if (e.code === 'content_changed') { this._pendingSnapshot = null; delete this.manifest.data.meta.remoteSnapshot; }
          rep.failures.push({ path: p.key, error: this._redact(e.message) });
          this.log('error', `${p.action} ${p.key}: ${e.message}`);
          if (e.status === 401 || e.status === 403) { rep.status = 'auth'; break; }
          if (e.status === 423) { rep.status = 'locked'; break; }
          if (this.stopRequested) { rep.status = 'paused'; break; }
          if (e.isNetwork && ++netFails >= 3) { rep.status = 'offline'; break; }
          // Repeated server backpressure (503 "freeing storage"): stop hammering and retry later.
          if (e.status >= 500 && ++serverFails >= 3) { rep.status = 'deferred'; break; }
        }
      }
      for (const from of new Set(consumedHints)) {
        const related = pairs.filter((pair) => expandedHints[pair.src.key] && expandedHints[pair.src.key].hint === from);
        if (related.length && related.every((pair) => moveDone.has(pair))) delete this.manifest.data.meta.moveHints[from];
      }

      // ---- folders: propagate folder deletions (files are already handled above) ----
      if (!opts.only && !this.stopRequested && (rep.status === 'ok' || rep.status === 'errors')) {
        const protectedMoveDirs = new Set();
        for (const from of pendingMoveSources.keys()) {
          const dirs = splitPath(splitRel(from).dir);
          for (let i = 1; i <= dirs.length; i++) protectedMoveDirs.add(dirs.slice(0, i).join('/'));
        }
        for (const pair of pairs) if (!moveDone.has(pair)) {
          const dirs = splitPath(splitRel(pair.src.key).dir);
          for (let i = 1; i <= dirs.length; i++) protectedMoveDirs.add(dirs.slice(0, i).join('/'));
        }
        try { await this._syncFolders({ known: knownDirs, mode, rep, rootMissing, blockDeletes, guard: folderGuard, approve: !!opts.approveDeletes, retryBlocked: !!opts.retryBlockedFolders, protectedMoveDirs }); }
        catch (e) {
          if (e.status === 401 || e.status === 403) rep.status = 'auth';
          if (e.status === 423) rep.status = 'locked';
          rep.failures.push({ path: '', error: this._redact(e.message) });
          this.log('error', `Folder sync: ${e.message}`);
        }
      }

      // ---- refresh remote signatures of what we touched (list-shape == future-listing shape) ----
      if (this._touched.size && !['auth', 'offline', 'locked', 'paused'].includes(rep.status)) await this._reconcile(rep);
    } catch (e) {
      if (e.code === 'content_changed') { this._pendingSnapshot = null; delete this.manifest.data.meta.remoteSnapshot; }
      const transient = e.status >= 500 || e.status === 429;
      rep.status = this.stopRequested ? 'paused' : e.status === 423 ? 'locked' : e.status === 401 || e.status === 403 ? 'auth' : (transient ? 'deferred' : (e.isNetwork ? 'offline' : 'aborted'));
      rep.failures.push({ path: '', error: this._redact(e.message) });
      // Transient server backpressure is expected and retried: log it as a warning, not a scary error.
      if (transient) this.log('warn', `Server busy; sync will retry later: ${this._redact(e.message)}`);
      else this.log('error', `Sync aborted: ${e.message}`);
    } finally {
      await this.releaseOperationLease();
      if (this._dryRun) { this.manifest.data = JSON.parse(beforeDryRun); this._dryRun = false; this.running = false; this._runSettings = null; return rep; }
      if (rep.status === 'locked' && !this._remoteMutated) { this.running = false; this._runSettings = null; return rep; }
      rep.errors = rep.failures.length;
      if (rep.status === 'ok' && rep.errors > 0) rep.status = 'errors';
      this._reportBlockedFolders(rep);
      // Persist the remote snapshot only after a FULL scan (never after reusing the cache) AND only when the run
      // did not itself change the remote (otherwise the cached listing would be stale). Stamped after the lock
      // is released so this run's lock activity is not seen as a remote change next time.
      if (this._remoteMutated) delete this.manifest.data.meta.remoteSnapshot;   // cache is stale -> full scan next run
      else if (this._pendingSnapshot && rep.status === 'ok' && !rep.failures.length && !this.stopRequested && !this._rescanNeeded) {
        this._pendingSnapshot.at = this.now();
        this.manifest.data.meta.remoteSnapshot = this._pendingSnapshot;
      }
      for (const w of rep.warnings) this.log('warn', w);
      rep.errors = rep.failures.length;
      if (rep.status === 'ok' && rep.errors > 0) rep.status = 'errors';
      this.manifest.data.meta.lastRun = { at: this.now(), status: rep.status, uploaded: rep.uploaded, downloaded: rep.downloaded, skipped: rep.skipped, conflicts: rep.conflicts, errors: rep.errors, deferredFolders: rep.deferredFolders };
      if (rep.status === 'ok') this.manifest.data.meta.lastSuccessAt = this.now();
      this.log(rep.status === 'ok' ? 'info' : 'warn', `Sync ${rep.status}: ↑${rep.uploaded} ↓${rep.downloaded} =${rep.skipped} conflicts:${rep.conflicts} errors:${rep.errors}`);
      const saveWasOk = rep.status === 'ok';
      try { await this.manifest.save(); } catch (e) {
        rep.failures.push({ path: '', error: 'Could not save sync ledger: ' + this._redact(e.message) }); rep.errors++; rep.status = 'errors';
        this.manifest.data.meta.lastRun.status = 'errors'; this.manifest.data.meta.lastRun.errors = rep.errors;
        if (saveWasOk) this.manifest.data.meta.lastSuccessAt = null;
      }
      this.running = false;
      this._runSettings = null;
    }
    return rep;
  }

  /** Moves one tracked identity, then verifies its bytes before any content replacement. A failed path move
   *  freezes both halves as a linked conflict; it must never fall back to upload+trash. */
  async _executeMove(up, src, rep, contentChanged) {
    if (!src.R || !src.R.id || !src.E || src.E.remoteId !== src.R.id) throw new Error(`Move source identity changed: ${src.key} -> ${up.key}; copies kept`);
    const excluded = makeExcluder(this.getSettings().excludePatterns);
    if (!this._inScope(src.key) || !this._inScope(up.key) || excluded(src.key) || excluded(up.key)) throw new Error('Move crossed the sync scope; copies kept');
    if (await this.local.exists(src.key)) throw new Error('Move source reappeared locally; remote copy kept');
    const localPath = up.L && up.L.path || up.key;
    const beforeStat = await this.local.stat(localPath);
    const data = await this.local.readBinary(localPath);
    const currentHash = await sha256Hex(data);
    if (!beforeStat || currentHash !== up.lhash) throw new Error('Move destination changed after scan; copies kept');
    const settings = this.getSettings();
    const cellular = typeof settings.networkIsCellular === 'function' && settings.networkIsCellular();
    const transferLimit = Math.min(settings.maxFileBytes > 0 ? settings.maxFileBytes : Infinity, cellular && settings.cellularMaxFileBytes > 0 ? settings.cellularMaxFileBytes : Infinity);
    if (cellular && settings.wifiOnly || data.byteLength > transferLimit) throw new Error('Move blocked by current transfer guard; copies kept');
    if (this.operationValidity && await this.operationValidity() === false) throw new Error('Sync operation is no longer valid; move stopped');
    contentChanged = currentHash !== (src.E && src.E.localHash);
    const { dir, name } = splitRel(up.key);
    await this._ensureFolders(dir);
    const folderPath = folderPathFor(this.getSettings().remoteFolder, dir);
    const idx = await this._dirIndex(folderPath);
    const existing = idx.files.get(nfc(name));
    let res = await this._liveById(src.R.id);
    if (!res) throw new Error(`Move source no longer exists: ${src.key} -> ${up.key}; local copy kept`);
    if (res.size > transferLimit) throw new Error('Move source grew beyond the transfer limit; copies kept');
    const source = splitRel(src.key);
    let moveError = existing && existing.id !== src.R.id ? new Error('Move destination is occupied; source kept') : null;
    const atDestination = (remote) => remote && remote.path && nfc(remote.path) === folderPath && nfc(remote.name || '') === name;
    if (!atDestination(res) && ((res.path && nfc(res.path) !== folderPathFor(this.getSettings().remoteFolder, source.dir)) || (res.name && nfc(res.name) !== source.name))) moveError = new Error('Move source changed location; copy kept');
    if (await this.local.exists(src.key) || await sha256Hex(await this.local.readBinary(localPath)) !== currentHash) throw new Error('Local files changed before move; copies kept');
    if (this.operationValidity && await this.operationValidity() === false) throw new Error('Sync operation is no longer valid; move stopped');
    if (!moveError && !atDestination(res)) {
      try {
        const moved = await this.client.move(src.R.id, { name, path: folderPath });
        if (!moved || moved.id !== src.R.id || !atDestination(moved)) throw new Error('Move acknowledgement did not match the source identity and destination');
        res = moved;
      } catch (e) {
        if (e.isNetwork || e.status === 401 || e.status === 403 || e.status === 423) throw e;
        moveError = e;
        res = await this._liveById(src.R.id);
        if (!res) throw e;
      }
    }
    const moved = atDestination(res);
    if (moved) {
      this._markRemoteMutated();
      // Changing a path does not acknowledge remote content. Keep the original merge base and force
      // verification even if the subsequent download fails or the listing omits content evidence.
      this.manifest.setFile(up.key, { ...src.E, remotePath: folderPath, remoteName: name, remoteSig: null, localSize: -1, localMtime: -1, moveVerify: true, moveFrom: src.key });
      this.manifest.deleteFile(src.key);
      idx.files.set(nfc(name), res);
      const { dir: sdir, name: sname } = splitRel(src.key);
      try { (await this._dirIndex(folderPathFor(this.getSettings().remoteFolder, sdir))).files.delete(nfc(sname)); } catch (_) { /* cache only */ }
      this._touched.set(up.key, res.id);
      rep.moved++;
    }
    let remoteData;
    try {
      if (res.size > transferLimit) throw new Error('Moved remote content exceeds the transfer limit; verification deferred');
      const expected = await this._expectedSha(res);
      remoteData = await this._downloadVerified(up.key, res, this._fileProgress(up.key, 'compare'), expected.sha);
      if (this.operationValidity && await this.operationValidity() === false) throw new Error('Sync operation is no longer valid; move verification stopped');
    } catch (e) {
      if (!moved) throw e;
      if (e.status === 423) throw e;
      rep.failures.push({ path: up.key, error: this._redact(e.message) });
      return true;  // Path committed; its unknown content baseline remains retryable without a second ID.
    }
    const remoteHash = await sha256Hex(remoteData);
    const plannedGeneration = src.R.contentVersion ?? src.E.remoteContentVersion;
    const changedRemote = (plannedGeneration != null && res.contentVersion != null && String(plannedGeneration) !== String(res.contentVersion)) || (src.E.localHash ? remoteHash !== src.E.localHash : remoteHash !== currentHash);
    if (!moved || moveError || changedRemote || await sha256Hex(await this.local.readBinary(localPath)) !== currentHash) {
      try { await this.conflicts.record(up.key, res, remoteData); }
      catch (e) {
        if (!moved) throw e;
        rep.failures.push({ path: up.key, error: this._redact(e.message) });
        return true;
      }
      const conflict = this.manifest.getConflict(up.key);
      conflict.moveFrom = src.key; conflict.movePending = !moved;
      rep.conflicts++;
      const reason = moveError ? this._redact(moveError.message) : 'Remote content or local destination changed';
      rep.moveConflicts.push({ from: src.key, to: up.key, remoteId: res.id, moved, reason, destinationId: existing && existing.id !== src.R.id ? existing.id : null });
      rep.warnings.push(`Rename conflict: ${src.key} -> ${up.key}; ${moved ? 'original remote identity moved' : 'remote source kept at its current path'} (${reason}), local version kept and remote bytes preserved for review.`);
      return moved ? true : 'conflict';
    }

    contentChanged = remoteHash !== currentHash;
    let lhash = contentChanged ? src.E.localHash : currentHash;
    let updateError = null;
    if (contentChanged) {
      try {
        const planned = this._remoteBaseline(res);
        const beforeReplace = async () => {
          await this._assertOperation();
          const live = await this._liveById(res.id);
          if (!live || !atDestination(live) || this._remoteChanged(live, planned)) throw Object.assign(new Error('Moved remote content changed before replacement'), { code: 'remote_changed' });
          if (this._capabilities && this._capabilities.expectedContentVersion) {
            if (live.contentVersion == null) throw new Error('Current content version missing; moved replacement stopped');
            if (planned.remoteContentVersion == null) planned.remoteContentVersion = live.contentVersion;
            return planned.remoteContentVersion;
          }
        };
        const revised = await this._replaceUpload({ name, folderPath, data, mtime: beforeStat.mtime, old: res, onProgress: null, beforeReplace });
        if (revised && revised.id) { res = revised; lhash = await sha256Hex(data); }
      } catch (e) {
        if (e.status === 423) throw e;
        if (e.code === 'remote_changed' || this._isVersionConflict(e)) {
          const live = await this._liveById(res.id);
          if (live) {
            const expected = await this._expectedSha(live);
            await this.conflicts.record(up.key, live, await this._downloadVerified(up.key, live, null, expected.sha));
            const conflict = this.manifest.getConflict(up.key);
            conflict.moveFrom = src.key; conflict.movePending = false;
            rep.conflicts++;
          }
        }
        // The path change already succeeded; if the content update fails, leave the file at the new path and let
        // the next run upload it normally (the ledger below records the move so nothing is duplicated).
        this.log('warn', `Moved ${src.key} to ${up.key} but could not update its content (${this._redact(e.message)}); will retry.`);
        updateError = e;
      }
    }

    this._record(up.key, { lhash, lstat: updateError ? { size: -1, mtime: -1 } : beforeStat, remote: res, text: updateError ? src.E.baseText : this._baseTextFor(data) });
    idx.files.set(nfc(name), res);
    this.log('info', `Moved in place: ${src.key} -> ${up.key}${contentChanged ? ' (with edits)' : ''}`);
    if (updateError) { rep.failures.push({ path: up.key, error: this._redact(updateError.message) }); }
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
    await this._bindScope();
    await this._discoverCapabilities();
    const meta = this.manifest.data.meta;
    const list = Array.isArray(meta.deletions) ? meta.deletions : [];
    if (!list.length) return { restored: 0, failed: 0, skipped: 0 };
    const lastRun = Math.max(...list.map((x) => x.runId || 0));
    const batch = list.filter((x) => (x.runId || 0) === lastRun);
    let restored = 0; let failed = 0; let skipped = 0; const done = new Set();
    const ownLease = !this._operationLock;
    try {
      if (batch.some((d) => d.side === 'remote' && d.remoteId)) {
        const lock = await this.acquireOperationLease();
        if (lock && lock.blocked) throw this._leaseFailure(null, 'Another device is syncing; restore stopped');
      }
      for (const d of batch) {
        if (d.side !== 'remote' || !d.remoteId) { skipped++; continue; }
        try { await this.client.restore(d.remoteId); this._markRemoteMutated(); done.add(d); restored++; this.log('info', `Restored from trash: ${d.path}`); }
        catch (e) { failed++; this.log('warn', `Could not restore ${d.path}: ${this._redact(e.message)}`); if (e.status === 423 || e.status === 401 || e.status === 403 || this.stopRequested) break; }
      }
      meta.deletions = list.filter((x) => !done.has(x));
      await this.manifest.save();
      return { restored, failed, skipped };
    } finally { if (ownLease) await this.releaseOperationLease(); }
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
      case 'skip-conflict': {
        if (!await this._reconcileConflict(p) && this.conflicts.has(key)) rep.conflicts++;
        rep.skipped++;
        break;
      }
      case 'upload': {
        const result = await this.pushLocal(key, R, onP, L && L.path || key);
        if (result.conflict) rep.conflicts++;
        else if (result.unchanged) rep.skipped++;
        else rep.uploaded++;
        break;
      }
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
          } catch (e) { try { if (sink.abort) await sink.abort(); } catch (_) { /* temporary file only */ } throw e; }
          const st = await this.local.stat(key);
          this.manifest.setFile(key, {
            localHash: null, localSize: st ? st.size : R.size, localMtime: st ? st.mtime : this.now(),
            ...this._remoteBaseline(R), syncedAt: this.now(), baseText: null,
          });
          this._verifyBaseline(key);
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
        if (delta && delta.unchanged) {
          this.manifest.setFile(key, { ...E, ...this._remoteBaseline(R), syncedAt: this.now() });
          rep.skipped++; break;
        }
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
        const lstat = await this.local.stat(L.path || key);
        const ldata = await this.local.readBinary(L.path || key); const lhash = await sha256Hex(ldata);
        // Verify the remote content against the server's own manifest checksum before comparing, so a wrong or
        // short body (mis-sized listing, duplicate-name collision) can never be treated as the file's content.
        const expect = await this._expectedSha(R);
        const rdata = await this._downloadVerified(key, R, onP, expect.sha);
        const rhash = await sha256Hex(rdata);
        if (rhash === lhash) {
          this._record(key, { lhash, lstat, remote: R });
          if (expect.map) this._storeMap(key, expect.map, R);
          rep.skipped++;
        }
        else if (E && E.remoteContentVersion == null && E.remoteMtime == null && rhash === E.localHash && !E.moveVerify && !p.remoteMoveTo) {
          // The remote still contains the proven upload, not a competing edit. Keep the newer local edit dirty;
          // the next ordinary upload plan applies it with all transfer guards and the newly verified head.
          this.manifest.setFile(key, { ...E, ...this._remoteBaseline(R), localSize: -1, localMtime: -1, syncedAt: this.now() });
          rep.skipped++;
        }
        else {
          await this.conflicts.record(key, R, rdata);
          if (p.remoteMoveTo) {
            const conflict = this.manifest.getConflict(key);
            conflict.moveFrom = p.remoteMoveTo; conflict.movePending = true;
            rep.moveConflicts.push({ from: p.remoteMoveTo, to: key, remoteId: R.id, moved: false });
            rep.warnings.push(`Rename conflict: ${key} -> ${p.remoteMoveTo}; known remote identity and both versions kept for review.`);
          } else if (E && E.moveVerify) {
            const conflict = this.manifest.getConflict(key);
            conflict.moveFrom = E.moveFrom; conflict.movePending = false;
            rep.moveConflicts.push({ from: E.moveFrom, to: key, remoteId: R.id, moved: true });
            rep.warnings.push(`Rename conflict: ${E.moveFrom} -> ${key}; original remote identity retained and both versions preserved.`);
          }
          rep.conflicts++;
        }
        break;
      }
      case 'deleteRemote': {
        if (await this.local.exists(key)) throw new Error('Local file reappeared before deletion; remote copy kept');
        const live = await this._liveById(R.id);
        if (live && (this._remoteChanged(live, this._remoteBaseline(R)) || live.path && R.path && live.path !== R.path || live.name && R.name && live.name !== R.name)) throw new Error('Remote file changed before deletion; copy kept');
        if (live && p.remoteHash) {
          const expected = await this._expectedSha(live);
          if (await sha256Hex(await this._downloadVerified(key, live, onP, expected.sha)) !== p.remoteHash) throw new Error('Remote bytes changed after deletion review; copy kept');
        }
        if (await this.local.exists(key)) throw new Error('Local file reappeared before deletion; remote copy kept');
        if (live) await this.client.trash(R.id);
        this._markRemoteMutated(); this.manifest.deleteFile(key); rep.deletedRemote++; this._auditDelete(key, 'remote', R.id); break;
      }
      case 'deleteLocal': {
        const path = L.path || key;
        const live = await this._liveById(E.remoteId);
        if (live && (!live.path || (nfc(live.path) === folderPathFor(this.getSettings().remoteFolder, splitRel(key).dir) && nfc(live.name) === splitRel(key).name))) throw new Error('Remote file still exists; local copy kept');
        if (await sha256Hex(await this.local.readBinary(path)) !== E.localHash) throw new Error('Local file changed before deletion; copy kept');
        await this.local.trash(path); this.manifest.deleteFile(key); rep.deletedLocal++; this._auditDelete(key, 'local', null); break;
      }
      default: break;
    }
  }

  // ---------- remote lock ----------
  async _discoverCapabilities() {
    const advertised = typeof this.client.capabilities === 'function' ? await this.client.capabilities() : {};
    const caps = {};
    for (const key of ['syncLeases', 'atomicIfEmpty', 'expectedContentVersion', 'contentVersions', 'uploadSessionListing']) caps[key] = advertised && advertised[key] === true;
    this._capabilities = caps;
    const meta = this.manifest.data.meta;
    const fingerprint = JSON.stringify(caps);
    if (meta.apiContract !== fingerprint) {
      delete meta.remoteSnapshot;
      // Old backend blockers must get one safe recheck when the server contract changes, not disappear.
      for (const b of Object.values(meta.blockedFolders || {})) {
        if (b && b.scope === meta.syncScope) { b.nextRetryAt = 0; b.serverRetryAt = 0; }
      }
      meta.apiContract = fingerprint;
    }
    return caps;
  }

  _isVersionConflict(e) {
    return !!e && (e.status === 412 || /^(contentversionconflict|expectedversionmismatch|versionconflict|uploadsourcemismatch|contentchanged)$/.test(String(e.code || '').replace(/[^a-z0-9]/gi, '').toLowerCase()));
  }

  _leaseFailure(e, message = 'Remote sync lease is no longer valid; mutations stopped') {
    // Lease control responses may echo secrets. Never propagate their message, raw response or cause.
    return Object.assign(new Error(message), { status: e && [401, 403].includes(e.status) ? e.status : 423, code: 'sync_lease_invalid' });
  }

  async _assertOperation() {
    if (this.stopRequested || this.operationValidity && await this.operationValidity() === false) {
      throw Object.assign(new Error('Sync operation cancelled; mutations stopped'), { code: 'operation_cancelled' });
    }
  }

  _acceptLease(response, started, previous) {
    const value = response && (response.lease || response);
    const token = value && value.token || previous && previous.token;
    const timestamp = (v) => typeof v === 'number' ? v : Date.parse(v);
    const serverTime = timestamp(response && response.serverTime != null ? response.serverTime : value && value.serverTime);
    const expiresAt = timestamp(value && value.expiresAt);
    const elapsed = Math.max(0, this.now() - started);
    const duration = expiresAt - serverTime;
    const path = previous && previous.path || this._lease && this._lease.path || folderPathFor(this.getSettings().remoteFolder, '');
    if (value && value.path != null && folderPathFor(value.path, '') !== path) throw this._leaseFailure();
    if (typeof token !== 'string' || !token || !Number.isFinite(duration) || duration <= elapsed) throw this._leaseFailure();
    const ttlSeconds = this._leaseTtlSeconds;
    // Use a duration on our clock, never the server's absolute clock; charge the entire request round trip.
    const deadline = this.now() + Math.min(duration, ttlSeconds * 1000) - elapsed - 100;
    if (deadline <= this.now()) throw this._leaseFailure();
    this._lease = { path, token, deadline, ttlSeconds };
    this.client.setLease({ path: this._lease.path, token }, () => this._ensureLeaseValid());
    this._scheduleLeaseRenewal();
  }

  _scheduleLeaseRenewal() {
    if (this._leaseTimer) clearTimeout(this._leaseTimer);
    if (!this._lease || this._leaseInvalid) return;
    const delay = Math.max(1, this._lease.deadline - this.now() - this._lease.ttlSeconds * 1000 / 3);
    this._leaseTimer = setTimeout(() => {
      this._leaseTimer = null;
      if (!this._lease || this.stopRequested) return;
      this._renewOperationLease().catch(() => {});
    }, delay);
    this._leaseTimer.unref?.();
  }

  async _renewOperationLease() {
    if (this._leaseRenewal) return this._leaseRenewal;
    const lease = this._lease;
    if (!lease || this._leaseInvalid || lease.deadline <= this.now()) throw this._leaseInvalid || this._leaseFailure();
    this._leaseRenewal = (async () => {
      try {
        await this._assertOperation();
        const started = this.now();
        const renewed = await this.client.renewLease({ path: lease.path, token: lease.token, ttlSeconds: lease.ttlSeconds });
        if (this._lease !== lease) throw this._leaseFailure();
        if (lease.deadline <= this.now()) throw this._leaseFailure();
        this._acceptLease(renewed, started, lease);
      } catch (e) {
        const failure = e.code === 'operation_cancelled' ? e : this._leaseFailure(e);
        if (this._lease === lease) this._leaseInvalid = failure;
        if (this._leaseTimer) clearTimeout(this._leaseTimer);
        this._leaseTimer = null;
        throw failure;
      }
    })();
    try { await this._leaseRenewal; } finally { this._leaseRenewal = null; }
  }

  async _ensureLeaseValid() {
    await this._assertOperation();
    if (this._leaseRenewal) await this._leaseRenewal;
    if (this._leaseInvalid) throw this._leaseInvalid;
    const lease = this._lease;
    if (!lease || lease.deadline <= this.now()) throw this._leaseFailure();
    if (lease.deadline - this.now() <= lease.ttlSeconds * 1000 / 3) await this._renewOperationLease();
    await this._assertOperation();
    if (!this._lease || this._lease.deadline <= this.now()) throw this._leaseFailure();
  }

  async acquireOperationLease({ dryRun = false } = {}) {
    const s = this.getSettings();
    if (dryRun || this._dryRun || s.wifiOnly && typeof s.networkIsCellular === 'function' && s.networkIsCellular()) return null;
    if (this._leaseRelease) await this._leaseRelease;
    if (this._operationLock) { if (this._lease) await this._ensureLeaseValid(); return this._operationLock; }
    if (this._operationAcquire) return this._operationAcquire;
    this._operationAcquire = (async () => {
      await this._bindScope();
      await this._discoverCapabilities();
      await this._assertOperation();
      try { this._operationLock = await this._acquireLock(); return this._operationLock; }
      catch (e) { await this.releaseOperationLease(); throw e; }
    })();
    try { return await this._operationAcquire; } finally { this._operationAcquire = null; }
  }

  async releaseOperationLease() {
    if (this._leaseRelease) return this._leaseRelease;
    if (this._leaseTimer) clearTimeout(this._leaseTimer);
    this._leaseTimer = null;
    const lease = this._lease; const lock = this._operationLock;
    const renewal = this._leaseRenewal;
    // Invalidate immediately, even while native HTTP is pending; a late renewal cannot reinstall this lease.
    this._lease = null; this._operationLock = null; this._leaseInvalid = null;
    if (typeof this.client.clearLease === 'function') this.client.clearLease();
    this._leaseRelease = (async () => {
      if (renewal) { try { await renewal; } catch (_) { /* invalid already */ } }
      if (lease) {
        try { await this.client.releaseLease({ path: lease.path, token: lease.token }); } catch (_) { /* TTL backstop; no secret-bearing errors */ }
      } else await this._releaseLock(lock);
    })();
    try { await this._leaseRelease; } finally { this._leaseRelease = null; }
  }

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
    if (!meta.deviceId) { meta.deviceId = randomUuid(); await this.manifest.save(); }
    if (this._identityNeedsSave) { await this.manifest.save(); this._identityNeedsSave = false; }
    const root = folderPathFor(s.remoteFolder, '');
    if (this._capabilities && this._capabilities.syncLeases) {
      this._leaseTtlSeconds = Math.max(15, Math.min(300, Math.floor(Number.isFinite(s.remoteLockMinutes) ? s.remoteLockMinutes * 60 : 60)));
      // The API needs a live scope folder. Bootstrap only empty ancestor/root folders, never contents.
      await this._ensureFolders('');
      await this._assertOperation();
      const started = this.now();
      let response;
      try { response = await this.client.acquireLease({ path: root, owner: meta.deviceId, ttlSeconds: this._leaseTtlSeconds }); }
      catch (e) {
        if (e.isNetwork || e.status >= 500 || e.status === 429) {
          throw Object.assign(new Error('Remote lease acquisition was not confirmed; no mutations will be attempted'), { isNetwork: !!e.isNetwork, status: e.status || 0, code: 'sync_lease_unconfirmed' });
        }
        throw this._leaseFailure(e, 'Remote folder is locked or its lease could not be acquired');
      }
      // Retain a returned token only in memory so even invalid/cancelled acknowledgements can be released.
      const value = response && (response.lease || response);
      if (value && typeof value.token === 'string') this._lease = { path: root, token: value.token };
      this._acceptLease(response, started);
      await this._assertOperation();
      return { apiLease: true };
    }
    const mine = meta.deviceId;
    const prefix = '.tdrive-sync.lock';              // a small hidden marker file, named "...-<deviceId>"
    const ttlMs = Math.max(1, Number.isFinite(s.remoteLockMinutes) ? s.remoteLockMinutes : 5) * 60000;
    const now = this.now();
    try {
      const listing = await this.client.listFolder(root);
      const locks = (listing.files || []).filter((f) => f && typeof f.name === 'string' && f.name.startsWith(prefix));
      for (const f of locks) {
        const isMine = f.name === prefix + '-' + mine || f.name.startsWith(prefix + '-' + mine + '-');
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
        method: 'POST', path: '/files', query: { name: prefix + '-' + mine + '-' + randomUuid(), path: root },
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
      if (!f || !f.id) return null;
      if (f.id !== id) throw new Error('Remote metadata returned a different file id; copy kept');
      const raw = f.raw || f;
      return f.trashed || f.isFolder || raw.trashed || raw.isTrashed || raw.is_trashed || raw.deleted || raw.deletedAt || raw.deleted_at || raw.inTrash || /^(trash|trashed|deleted)$/i.test(raw.status || raw.state || '') ? null : f;
    } catch (e) { if (e.status === 404) return null; throw e; }
  }

  async _rescueInvisible(local, remote, rep) {
    let rescued = 0;
    const excluded = makeExcluder(this.getSettings().excludePatterns);
    const root = folderPathFor(this.getSettings().remoteFolder, '');
    for (const key of new Set([...local.keys(), ...this.manifest.fileKeys()])) {
      const E = this.manifest.getFile(key);
      if (!E || !E.remoteId || this.conflicts.has(key)) continue;
      if (excluded(key) || !this._inScope(key) || remote.get(key)?.id === E.remoteId) continue;
      const f = await this._liveById(E.remoteId);
      if (!f) continue;
      const expected = splitRel(key);
      const path = nfc(f.path || folderPathFor(this.getSettings().remoteFolder, expected.dir));
      if (!path.startsWith(root)) continue;
      const target = nfc(path.slice(root.length) + (f.name || expected.name));
      if (excluded(target) || !this._inScope(target)) continue;
      if (remote.has(target) && remote.get(target).id !== f.id) throw new Error(`Remote names collide while rescuing ${target}; copies kept`);
      f.sig = remoteSig(f); remote.set(target, f); rescued++;
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
      st = (await this.local.exists(key)) ? await this.local.stat(key) : null;
      if (st) { local.set(key, { path: key, size: st.size, mtime: st.mtime }); n++; }
    }
    if (n) rep.warnings.push(`${n} file(s) exist on disk but were missing from the vault index; kept (not treated as deleted).`);
  }

  // ---------- folders ----------
  /** Folders we have synced before: those in the folder ledger plus the parents of every ledger file
   *  (so folders from before folder tracking existed are covered too). */
  _knownDirs(excludePatterns) {
    const excluded = makeExcluder(excludePatterns);
    const unverified = this.manifest.data.meta.unverifiedBaselineFolders || {};
    const out = new Set(this.manifest.folderKeys().filter((d) => !Object.prototype.hasOwnProperty.call(unverified, d)));
    for (const blocked of Object.values(this.manifest.data.meta.blockedFolders || {})) {
      if (blocked && typeof blocked.path === 'string' && blocked.scope === this.manifest.data.meta.syncScope) out.add(blocked.path);
    }
    for (const k of this.manifest.fileKeys()) {
      if (!this._trustedEntry(k)) continue;
      const segs = splitPath(k); segs.pop();
      for (let i = 1; i <= segs.length; i++) out.add(segs.slice(0, i).join('/'));
    }
    return new Set([...out].filter((d) => d && !excluded(d) && !excluded(d + '/') && !excluded(d + '/_') && this._inScope(d)));
  }

  async _localDirSet() {
    if (typeof this.local.listFolders !== 'function') return null;   // adapter cannot list folders: do nothing
    const arr = await this.local.listFolders();
    if (!arr) return null;
    const excluded = makeExcluder(this.getSettings().excludePatterns);
    return new Set(arr.map((p) => nfc(String(p)).replace(/^\/+|\/+$/g, '')).filter(Boolean).filter((p) => this._inScope(p) && !excluded(p) && !excluded(p + '/') && !excluded(p + '/_')));
  }

  _reportBlockedFolders(rep) {
    const meta = this.manifest.data.meta;
    const excluded = makeExcluder(this.getSettings().excludePatterns);
    rep.blockedFolders = Object.values(meta.blockedFolders || {}).filter((b) => b && typeof b.path === 'string' && b.scope === meta.syncScope && this._inScope(b.path) && !excluded(b.path) && !excluded(b.path + '/_')).map((b) => ({ ...b }));
    rep.deferredFolders = rep.blockedFolders.length;
    if (rep.status === 'ok' && rep.deferredFolders) rep.status = 'blocked';
  }

  async _describeBlockedUploads(block, path) {
    if (!this._capabilities || !this._capabilities.uploadSessionListing || typeof this.client.listUploads !== 'function') return;
    if (block.sessionsCheckedAt && this.now() - block.sessionsCheckedAt < 5 * 60000) return;
    try {
      const sessions = await this.client.listUploads({ path, active: true, limit: 100 });
      block.blockingUploads = sessions.slice(0, 100).map((s) => {
        const expiresAt = typeof s.expiresAt === 'number' ? s.expiresAt : Date.parse(s.expiresAt);
        return { id: s.uploadId || s.id, status: s.status, expiresAt: Number.isFinite(expiresAt) ? expiresAt : null, expired: Number.isFinite(expiresAt) && expiresAt <= this.now() };
      });
      block.activeUploadCount = sessions.length;
      block.expiredUploadCount = block.blockingUploads.filter((s) => s.expired).length;
      block.sessionsCheckedAt = this.now();
    } catch (e) { block.sessionInspectionError = this._redact(e.message); }
  }

  async _deleteRemoteFolder(rel, path, id, rep, retryBlocked = false) {
    const meta = this.manifest.data.meta;
    const blocks = meta.blockedFolders || (meta.blockedFolders = Object.create(null));
    const previous = Object.prototype.hasOwnProperty.call(blocks, rel) ? blocks[rel] : null;
    const now = this.now();
    if (previous && previous.scope === meta.syncScope && (previous.serverRetryAt > now || (!retryBlocked && previous.nextRetryAt > now))) {
      await this._describeBlockedUploads(previous, path);
      return false;
    }
    try {
      await this.client.deleteFolder(path, id, { ifEmpty: !!(this._capabilities && this._capabilities.atomicIfEmpty) });
      delete blocks[rel];
      this._markRemoteMutated();
      return true;
    } catch (e) {
      const code = String(e.code || '').replace(/[^a-z0-9]/gi, '').toLowerCase();
      if (code === 'foldernotempty') {
        delete meta.remoteSnapshot; this._pendingSnapshot = null; this._rescanNeeded = true;
        Object.defineProperty(blocks, rel, { value: { path: rel, scope: meta.syncScope, code: 'FOLDER_NOT_EMPTY', at: now, nextRetryAt: 0, serverRetryAt: 0 }, enumerable: true, configurable: true, writable: true });
        await this._describeBlockedUploads(blocks[rel], path);
        rep.warnings.push(`Remote folder kept: ${rel} (new contents arrived; rescan required).`);
        return false;
      }
      if (e.status !== 409 || !(code === 'uploadinprogress' || /upload[\s_-]*in[\s_-]*progress/i.test(e.message || ''))) throw e;
      const attempts = previous && previous.scope === meta.syncScope ? (previous.attempts || 0) + 1 : 1;
      const retryAfter = Number(e.retryAfter);
      const serverDelay = Number.isFinite(retryAfter) && retryAfter > 0 && Number.isFinite(retryAfter * 1000) ? retryAfter * 1000 : 0;
      const delay = Math.max(serverDelay, Math.min(6 * 60 * 60000, 15 * 60000 * Math.pow(2, Math.min(attempts - 1, 5))));
      const blocked = {
        path: rel, scope: meta.syncScope, code: 'UPLOAD_IN_PROGRESS', at: previous && previous.at || now,
        lastAttemptAt: now, nextRetryAt: Math.min(Number.MAX_SAFE_INTEGER, now + delay), attempts,
        serverRetryAt: serverDelay ? Math.min(Number.MAX_SAFE_INTEGER, now + serverDelay) : 0,
        error: this._redact(e.message), requestId: this._redact(e.requestId || ''),
      };
      Object.defineProperty(blocks, rel, { value: blocked, enumerable: true, configurable: true, writable: true });
      await this._describeBlockedUploads(blocked, path);
      if (!previous || previous.scope !== meta.syncScope) rep.warnings.push(`Remote folder deletion deferred: ${rel} (UPLOAD_IN_PROGRESS). Other sync work will continue; no upload sessions were aborted.`);
      return false;
    }
  }

  /** A folder missing on exactly one side was deleted there:
   *   - gone locally  -> soft-delete the remote folder, but only if the server says it is empty right now;
   *   - gone remotely -> trash the local folder, but only if nothing is left inside it.
   *  A folder that still holds anything is never deleted. Folders present on both sides are (re)recorded. */
  async _syncFolders({ known, mode, rep, rootMissing, blockDeletes, guard, approve, retryBlocked, protectedMoveDirs }) {
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
    const canDelete = !blockDeletes && !guard && !rootMissing && mode === 'two-way' && s.deletionPolicy !== 'off' && rep.status === 'ok' && !rep.failures.length;
    const limit = s.maxDeletesPerRun == null ? 10 : s.maxDeletesPerRun;
    const depth = (d) => splitPath(d).length;
    let dels = 0;

    for (const d of [...known].sort((a, b) => depth(b) - depth(a))) {
      if (this.stopRequested) break;
      if (protectedMoveDirs && protectedMoveDirs.has(d)) continue;
      let L = localDirs.has(d); const R = inRemote(d);
      if (!L && R && (await this.local.exists(d))) { localDirs.add(d); L = true; }   // index lag: the disk has it
      if (L && R) { if (this.manifest.data.meta.blockedFolders) delete this.manifest.data.meta.blockedFolders[d]; continue; }
      if (!L && !R) { this.manifest.deleteFolder(d); if (this.manifest.data.meta.blockedFolders) delete this.manifest.data.meta.blockedFolders[d]; continue; }
      if (!canDelete) continue;
      if (this._approvedDeletes && !this._deleteApproved(this._deleteSnapshot({ key: d + '/', action: L ? 'deleteLocal' : 'deleteRemote' }))) {
        rep.warnings.push(`Folder deletion changed after review: ${d}; folder kept.`); continue;
      }
      if (dels >= limit && !approve) { rep.warnings.push(`Folder deletions blocked: more than ${limit} folders would be deleted. Review manually.`); break; }
      const remotePath = folderPathFor(s.remoteFolder, d);
      try {
        if (!L) {                                           // deleted locally
          let live = null;
          try { live = await this.client.listFolder(remotePath); }
          catch (e) { if (e.status !== 404) throw e; }
          if (live && (live.files.some((f) => !f.trashed) || live.folders.length)) {
            if (this.manifest.data.meta.blockedFolders) delete this.manifest.data.meta.blockedFolders[d];
            continue;
          }
          if (live) { const meta = this._remoteDirs && this._remoteDirs.get(d); if (!await this._deleteRemoteFolder(d, remotePath, meta && meta.id, rep, retryBlocked)) continue; }
          else if (this.manifest.data.meta.blockedFolders) delete this.manifest.data.meta.blockedFolders[d];
          deletedRemote.add(d); this.manifest.deleteFolder(d); rep.deletedRemote++; dels++;
          this.log('info', `Deleted remote folder (to trash): ${d}`);
        } else {                                            // deleted remotely
          if (hasFilesUnder(d) || hasDirsUnder(d)) continue;                                   // not empty: keep
          if ((await this.local.trashFolder(d)) === false) { rep.warnings.push(`Local folder kept (still holds hidden/untracked items): ${d}`); continue; }
          localDirs.delete(d); this.manifest.deleteFolder(d); rep.deletedLocal++; dels++;
          this.log('info', `Deleted local folder (to trash): ${d}`);
        }
      } catch (e) {
        rep.failures.push({ path: d + '/', error: this._redact(e.message) });
        this.log('error', `delete folder ${d}: ${e.message}`);
        if (e.status === 401 || e.status === 403) { rep.status = 'auth'; break; }
        if (e.status === 423) { rep.status = 'locked'; break; }
        if (e.isNetwork) { rep.status = 'offline'; break; }
      }
    }
    // New folders (never synced before), including EMPTY ones, are mirrored to the other side.
    const canUp = mode === 'two-way' || mode === 'upload-only';
    const canDown = mode === 'two-way' || mode === 'download-only';
    if (canUp && !['auth', 'offline', 'locked', 'paused'].includes(rep.status)) {
      for (const d of [...localDirs].sort((a, b) => depth(a) - depth(b))) {
        if (this.stopRequested) break;
        if (excluded(d) || excluded(d + '/') || excluded(d + '/_') || known.has(d) || inRemote(d)) continue;
        try { await this._ensureFolders(d); this.log('info', `Created remote folder: ${d}`); }
        catch (e) { rep.failures.push({ path: d + '/', error: this._redact(e.message) }); this.log('error', `create remote folder ${d}: ${e.message}`); if (e.status === 401 || e.status === 403) { rep.status = 'auth'; break; } if (e.status === 423) { rep.status = 'locked'; break; } }
      }
    }
    if (canDown && typeof this.local.mkdir === 'function' && this._remoteDirs) {
      for (const d of [...this._remoteDirs.keys()].sort((a, b) => depth(a) - depth(b))) {
        if (this.stopRequested) break;
        if (excluded(d) || excluded(d + '/') || excluded(d + '/_') || known.has(d) || localDirs.has(d) || deletedRemote.has(d)) continue;
        try { await this.local.mkdir(d); localDirs.add(d); this.log('info', `Created local folder: ${d}`); }
        catch (e) { rep.failures.push({ path: d + '/', error: e.message }); this.log('error', `create local folder ${d}: ${e.message}`); }
      }
    }
    for (const d of localDirs) if (!excluded(d) && inRemote(d)) {
      this.manifest.setFolder(d, { syncedAt: this.now() });
      if (this.manifest.data.meta.unverifiedBaselineFolders) delete this.manifest.data.meta.unverifiedBaselineFolders[d];
    }
  }

  /** Manual cleanup for folders orphaned by versions before 1.0.4: remote folders that are EMPTY (no files, no
   *  sub-folders) and do not exist locally are soft-deleted, deepest first. Nothing that holds a file is touched. */
  async cleanEmptyRemoteFolders() {
    const rep = { status: 'ok', deleted: 0, failures: [], warnings: [], blockedFolders: [], deferredFolders: 0 };
    const ownLease = !this._operationLock;
    try {
      await this._bindScope();
      await this._discoverCapabilities();
      const s = this.getSettings();
      const { files, rootMissing } = await this._listRemote(folderPathFor(s.remoteFolder, ''), emptyReport());
      if (rootMissing) { delete this.manifest.data.meta.blockedFolders; await this.manifest.save(); return rep; }
      const localDirs = await this._localDirSet();
      if (!localDirs) { this._reportBlockedFolders(rep); await this.manifest.save(); return rep; }
      const rawFiles = (await this.local.list()).map((f) => nfc(f.path));
      const withFiles = new Set();
      for (const k of files.keys()) { const segs = splitPath(k); segs.pop(); for (let i = 1; i <= segs.length; i++) withFiles.add(segs.slice(0, i).join('/')); }
      const dirs = [...this._remoteDirs.keys()].sort((a, b) => splitPath(b).length - splitPath(a).length);
      if (dirs.length) {
        const lock = await this.acquireOperationLease();
        if (lock && lock.blocked) throw this._leaseFailure(null, 'Another device is syncing; cleanup stopped');
      }
      const gone = new Set();
      for (const d of dirs) {
        if (this.stopRequested) { rep.status = 'paused'; break; }
        if (makeExcluder(s.excludePatterns)(d) || makeExcluder(s.excludePatterns)(d + '/')) continue;
        if (withFiles.has(d) || localDirs.has(d) || rawFiles.some((p) => p.startsWith(d + '/'))) continue;
        if (dirs.some((x) => x !== d && x.startsWith(d + '/') && !gone.has(x))) continue;      // still has a sub-folder
        if (await this.local.exists(d)) continue;
        const path = folderPathFor(s.remoteFolder, d);
        try {
          const live = await this.client.listFolder(path);                                     // re-check right before deleting
          if (live.files.some((f) => !f.trashed) || live.folders.length) continue;
          if (!await this._deleteRemoteFolder(d, path, this._remoteDirs.get(d).id, rep)) continue;
          gone.add(d); rep.deleted++; this.log('info', `Removed empty remote folder: ${d}`);
        } catch (e) {
          rep.failures.push({ path: d + '/', error: this._redact(e.message) });
          if (e.status === 423) rep.status = 'locked';
          if (e.status === 401 || e.status === 403) rep.status = 'auth';
          if (e.isNetwork) rep.status = 'offline';
          if (rep.status !== 'ok') break;
        }
      }
      if (rep.failures.length && rep.status === 'ok') rep.status = 'errors';
      this._reportBlockedFolders(rep);
      for (const warning of rep.warnings) this.log('warn', warning);
      await this.manifest.save();
      return rep;
    } finally { if (ownLease) await this.releaseOperationLease(); }
  }

  async prepare({ acquireLease = true, dryRun = false } = {}) {
    await this._bindScope();
    await this._discoverCapabilities();
    if (acquireLease && !dryRun) {
      const lock = await this.acquireOperationLease();
      if (lock && lock.blocked) throw this._leaseFailure(null, 'Another device is syncing; operation stopped');
    }
    const rep = emptyReport();
    try { await this._listRemote(folderPathFor(this.getSettings().remoteFolder, ''), rep); }
    catch (e) { await this.releaseOperationLease(); throw e; }
  }

  async refreshSigs(touched) { return this._reconcile(emptyReport(), touched); }

  async _reconcile(rep, touched = this._touched) {
    try {
      const { files } = await this._listRemote(folderPathFor(this.getSettings().remoteFolder, ''), emptyReport());
      for (const [rel, id] of touched) {
        const E = this.manifest.getFile(rel); const R = files.get(rel);
        if (!E) continue;
        if (R && R.id === id) {
          if (this._remoteChanged(R, E)) {
            rep.warnings.push(`Remote file changed after transfer: ${rel}; committed baseline kept for the next sync.`);
            continue;
          }
          for (const [key, value] of Object.entries(this._remoteBaseline(R))) if (key !== 'remoteContentVersion' && (value != null || E[key] == null)) E[key] = value;
        }
        else {
          const live = await this._liveById(id);
          if (live) {   // the upload is fine; only the listing does not show it. Keep the ledger entry (no re-upload).
            if (this._remoteChanged(live, E)) {
              rep.warnings.push(`Remote file changed after transfer: ${rel}; committed baseline kept for the next sync.`);
              continue;
            }
            live.sig = remoteSig(live);
            for (const [key, value] of Object.entries(this._remoteBaseline(live))) if (key !== 'remoteContentVersion' && (value != null || E[key] == null)) E[key] = value;
            rep.warnings.push(`Uploaded file not visible in folder listing (verified by id): ${rel}`);
          } else {
            rep.warnings.push(`Remote file missing after transfer: ${rel}; committed baseline kept for deletion verification on the next sync.`);
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
