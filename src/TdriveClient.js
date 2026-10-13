'use strict';
const { sleep, redact, randomUuid } = require('./util');
const { normalizeFileResponse, normalizeList } = require('./normalize');

class TdriveError extends Error {
  constructor(message, o = {}) {
    super(message);
    this.name = 'TdriveError';
    this.status = o.status || 0;
    this.code = o.code || '';
    this.retryable = !!o.retryable;
    this.isNetwork = !!o.isNetwork;
    this.retryAfter = o.retryAfter == null ? null : o.retryAfter;
  }
}

function parseRetryAfter(v) {
  if (v == null || v === '') return null;
  if (/^\d+(\.\d+)?$/.test(String(v))) return Number(v);
  const t = Date.parse(v);
  return Number.isNaN(t) ? null : Math.max(0, (t - Date.now()) / 1000);
}

// The API rejects any request body larger than 5 MB, so every upload part is capped below it
// (5,000,000 bytes is under the limit whether "MB" means 10^6 or 2^20). Larger files are split into parts.
const MAX_PART_BYTES = 5000000;

/**
 * The only module that talks HTTP. `transport({url, method, headers, body})` must resolve to
 * `{status, headers (lower-case keys), body: ArrayBuffer}` and reject on network failure.
 * The API key is only ever sent to the configured origin.
 */
class TdriveClient {
  constructor(opts) {
    let u;
    try { u = new URL(opts.baseUrl); } catch (e) { throw new TdriveError('Invalid server URL', { code: 'bad_config' }); }
    const loopback = ['127.0.0.1', 'localhost', '[::1]'].includes(u.hostname);
    if (u.protocol !== 'https:' && !opts.allowInsecure && !loopback) throw new TdriveError('Server URL must use HTTPS', { code: 'bad_config' });
    if (!opts.apiKey) throw new TdriveError('API key is empty', { code: 'bad_config' });
    this.origin = u.origin;
    this.apiKey = opts.apiKey;
    this.transport = opts.transport;
    this.sleep = opts.sleep || sleep;
    this.maxRetries = opts.maxRetries == null ? 4 : opts.maxRetries;
    this.baseDelayMs = opts.baseDelayMs == null ? 500 : opts.baseDelayMs;
    this.maxDelayMs = opts.maxDelayMs == null ? 30000 : opts.maxDelayMs;
    this._info = null;
  }

  redact(t) { return redact(t, this.apiKey); }

  _url(path, query) {
    const url = new URL(this.origin + '/api/v1' + path);
    for (const [k, v] of Object.entries(query || {})) if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
    if (url.origin !== this.origin) throw new TdriveError('Refusing cross-origin request', { code: 'bad_config' });
    return url.toString();
  }

  async _wait(attempt, retryAfter) {
    const d = retryAfter != null ? Math.min(retryAfter * 1000, this.maxDelayMs)
      : Math.min(this.baseDelayMs * Math.pow(2, attempt), this.maxDelayMs);
    await this.sleep(d);
  }

  _httpError(res) {
    let code = 'http_' + res.status; let message = 'HTTP ' + res.status;
    try {
      const j = JSON.parse(new TextDecoder().decode(res.body));
      if (j && j.error) { code = j.error.code || code; message = j.error.message || message; }
    } catch (e) { /* non-JSON error body */ }
    return new TdriveError(this.redact(message), {
      status: res.status, code, retryable: res.status === 429 || res.status >= 500,
      retryAfter: parseRetryAfter(res.headers && res.headers['retry-after']),
    });
  }

  async request({ method = 'GET', path = '', query, headers = {}, body, binary = false }) {
    const url = this._url(path, query);
    for (let attempt = 0; ; attempt++) {
      let res;
      try {
        res = await this.transport({ url, method, headers: { Authorization: 'Bearer ' + this.apiKey, ...headers }, body });
      } catch (e) {
        const err = new TdriveError('Network error: ' + this.redact(e && e.message), { isNetwork: true, retryable: true });
        if (attempt >= this.maxRetries) throw err;
        if (this.onRetry) { try { this.onRetry({ attempt: attempt + 1, status: 0, network: true }); } catch (_) { /* never break on a hook */ } }
        await this._wait(attempt, null);
        continue;
      }
      if (res.status === 429 || res.status >= 500) {
        if (res.status === 429) {
          // Back off the parallelism, not just this request: the server is rate limiting us.
          const cur = Number.isFinite(this._concurrencyHint) ? this._concurrencyHint : 3;
          this._concurrencyHint = Math.max(1, cur - 1);
        }
        const err = this._httpError(res);
        if (attempt >= this.maxRetries) throw err;
        if (this.onRetry) { try { this.onRetry({ attempt: attempt + 1, status: res.status, retryAfter: err.retryAfter }); } catch (_) { /* hook only */ } }
        await this._wait(attempt, err.retryAfter);
        continue;
      }
      if (res.status === 304) return { status: 304, notModified: true, headers: res.headers || {}, body: new ArrayBuffer(0) };
      if (res.status >= 400) throw this._httpError(res);
      if (binary) return { status: res.status, headers: res.headers || {}, body: res.body };
      let json;
      try { json = JSON.parse(new TextDecoder().decode(res.body)); } catch (e) {
        if (attempt >= this.maxRetries) throw new TdriveError('Invalid JSON response', { code: 'bad_response', retryable: true });
        await this._wait(attempt, null);
        continue;
      }
      if (!json || json.ok !== true) {
        const er = (json && json.error) || {};
        throw new TdriveError(this.redact(er.message || 'API returned ok:false'), { status: res.status, code: er.code || 'api_error' });
      }
      return json.data;
    }
  }

  info() { return this.request({ path: '' }); }
  me() { return this.request({ path: '/me' }); }

  /** Upload limits from GET /api/v1 (upload.oneShotMaxBytes, upload.chunkBytes). Never guessed. */
  async limits() {
    if (this._info) return this._info;
    const d = await this.info();
    const u = (d && d.upload) || d || {};
    const srvChunk = Number(u.chunkBytes);
    if (!Number.isFinite(srvChunk) || srvChunk <= 0) throw new TdriveError('Server did not report upload.chunkBytes', { code: 'bad_response' });
    const srvOne = u.oneShotMaxBytes != null ? Number(u.oneShotMaxBytes) : srvChunk;
    const chunk = Math.min(srvChunk, MAX_PART_BYTES);
    const one = Math.min(Number.isFinite(srvOne) ? srvOne : chunk, MAX_PART_BYTES);
    this._info = { chunkBytes: chunk, oneShotMaxBytes: one };
    return this._info;
  }

  /** The REAL resumable part size. `GET /api/v1` may advertise a different `chunkBytes`, and the server only
   *  reveals its true `chunkSize` in an upload session — so open a throwaway session (valid-format fingerprint)
   *  and abort it. Cached per client. Returns null when the server does not report one. */
  async uploadChunkSize() {
    if (this._uploadChunkSize) return this._uploadChunkSize;
    const lim = await this.limits();
    const size = Math.max(1, lim.oneShotMaxBytes + 1);
    const body = new TextEncoder().encode(JSON.stringify({
      name: 'probe', path: '/', size, fingerprint: '0'.repeat(64), clientKey: randomUuid(),
      contentType: 'application/octet-stream', lastModified: Date.now(),
    })).buffer;
    const sess = await this.request({ method: 'POST', path: '/uploads', headers: { 'Content-Type': 'application/json' }, body });
    const id = sess && (sess.id || sess.uploadId);
    const cs = Number(sess && sess.chunkSize);
    try { if (id) await this.request({ method: 'DELETE', path: '/uploads/' + encodeURIComponent(id) }); } catch (e) { /* abort is best effort */ }
    if (Number.isFinite(cs) && cs > 0) this._uploadChunkSize = cs;
    return this._uploadChunkSize || null;
  }

  /** Lists one folder (all pages). Returns {files, folders} (normalized, de-duplicated by id). */
  async listFolder(path) {
    const limit = 100;
    const files = []; const folders = []; const seen = new Set();
    for (let offset = 0; ; offset += limit) {
      const data = await this.request({ path: '/files', query: { path, limit, offset } });
      const page = normalizeList(data);
      let fresh = 0;
      for (const f of page.files) { const k = 'f' + f.id; if (!seen.has(k)) { seen.add(k); files.push(f); fresh++; } }
      for (const f of page.folders) { const k = 'd' + (f.id || f.name); if (!seen.has(k)) { seen.add(k); folders.push(f); fresh++; } }
      if (page.files.length + page.folders.length < limit || fresh === 0) break;
    }
    return { files, folders };
  }

  async getFile(id) { return normalizeFileResponse(await this.request({ path: '/files/' + encodeURIComponent(id) })); }

  /** Which of these chunk SHA-256 hashes does the server already store for this account? (≤500 per call) */
  async chunksLookup(checksums) {
    return this.request({ method: 'POST', path: '/chunks/lookup', headers: { 'Content-Type': 'application/json' }, body: new TextEncoder().encode(JSON.stringify({ checksums })).buffer });
  }

  /** Soft delete (goes to Tdrive trash, recoverable). Permanent deletion is never used. */
  trash(id) { return this.request({ method: 'DELETE', path: '/files/' + encodeURIComponent(id), query: { permanent: 0 } }); }
  restore(id) { return this.request({ method: 'POST', path: '/trash/' + encodeURIComponent(id) + '/restore' }); }

  /** Soft-deletes an (already verified empty) folder. The folder-delete contract is not documented, and a wrong
   *  path form (trailing slash, query vs body, `permanent` flag) answers 400, so several forms are tried in order;
   *  the first 2xx wins and is remembered. Auth/rate/network errors are never masked.
   *  Server expects: { name: "folderName", path: "/parent/path/" } (parent path + folder name). */
  async deleteFolder(path, id) {
    const enc = encodeURIComponent;
    const noSlash = path === '/' ? '/' : path.replace(/\/+$/, '');
    const withSlash = noSlash === '/' ? '/' : noSlash + '/';
    const json = (o) => ({ headers: { 'Content-Type': 'application/json' }, body: new TextEncoder().encode(JSON.stringify(o)).buffer });

    // Parse folder name and parent path from the full path
    // e.g., "/Vault/Folder/" -> folderName="Folder", parentPath="/Vault/"
    let folderName = '';
    let parentPath = '/';
    if (noSlash !== '/') {
      const lastSlash = noSlash.lastIndexOf('/');
      if (lastSlash >= 0) {
        folderName = noSlash.substring(lastSlash + 1);
        parentPath = noSlash.substring(0, lastSlash + 1);  // includes trailing slash
      } else {
        folderName = noSlash;
        parentPath = '/';
      }
    }

    // If no ID provided, try to resolve it by listing the parent folder
    if (!id && folderName) {
      try {
        const parentListing = await this.listFolder(parentPath);
        // Check folders array first
        let found = parentListing.folders.find((f) => f.name === folderName);
        // Some servers return folders in files array with mimeType
        if (!found || !found.id) {
          found = parentListing.files.find((f) => f.name === folderName && f.isFolder);
        }
        if (found && found.id) {
          id = found.id;
        }
      } catch (e) {
        // Ignore errors, fall back to path-based attempts
      }
    }

    // Try relative path variants (without leading slash) in case server expects relative paths
    const relNoSlash = noSlash.startsWith('/') ? noSlash.substring(1) : noSlash;
    const relWithSlash = relNoSlash + '/';
    const relParentPath = parentPath.startsWith('/') ? parentPath.substring(1) : parentPath;

    const attempts = [];
    if (id) {
      attempts.push({ k: 'id+perm0', method: 'DELETE', path: '/folders/' + enc(id), query: { permanent: 0 } });
      attempts.push({ k: 'id', method: 'DELETE', path: '/folders/' + enc(id) });
    }
    // Correct format: name + parent path (what the real Tdrive server expects)
    attempts.push({ k: 'name+parentPath', method: 'DELETE', path: '/folders', ...json({ name: folderName, path: parentPath }) });
    attempts.push({ k: 'name+parentPath+perm0', method: 'DELETE', path: '/folders', query: { name: folderName, path: parentPath, permanent: 0 } });
    attempts.push({ k: 'name+parentPathNoSlash', method: 'DELETE', path: '/folders', ...json({ name: folderName, path: parentPath.replace(/\/+$/, '') }) });
    attempts.push({ k: 'name+parentPathNoSlash+perm0', method: 'DELETE', path: '/folders', query: { name: folderName, path: parentPath.replace(/\/+$/, ''), permanent: 0 } });
    // Relative path variants
    attempts.push({ k: 'rel-name+parentPath', method: 'DELETE', path: '/folders', ...json({ name: folderName, path: relParentPath }) });
    attempts.push({ k: 'rel-name+parentPath+perm0', method: 'DELETE', path: '/folders', query: { name: folderName, path: relParentPath, permanent: 0 } });
    // Absolute path variants (legacy - some servers may expect full path)
    attempts.push({ k: 'path/', method: 'DELETE', path: '/folders', query: { path: withSlash, permanent: 0 } });
    attempts.push({ k: 'path', method: 'DELETE', path: '/folders', query: { path: noSlash, permanent: 0 } });
    attempts.push({ k: 'path-noperm', method: 'DELETE', path: '/folders', query: { path: noSlash } });
    attempts.push({ k: 'body-path', method: 'DELETE', path: '/folders', ...json({ path: noSlash }) });
    attempts.push({ k: 'body-path/', method: 'DELETE', path: '/folders', ...json({ path: withSlash }) });
    // Relative path variants (some servers expect paths relative to root)
    attempts.push({ k: 'rel-path/', method: 'DELETE', path: '/folders', query: { path: relWithSlash, permanent: 0 } });
    attempts.push({ k: 'rel-path', method: 'DELETE', path: '/folders', query: { path: relNoSlash, permanent: 0 } });
    attempts.push({ k: 'rel-body-path', method: 'DELETE', path: '/folders', ...json({ path: relNoSlash }) });
    attempts.push({ k: 'rel-body-path/', method: 'DELETE', path: '/folders', ...json({ path: relWithSlash }) });
    if (id) {
      attempts.push({ k: 'body-id', method: 'DELETE', path: '/folders', ...json({ id }) });
      attempts.push({ k: 'file-id', method: 'DELETE', path: '/files/' + enc(id), query: { permanent: 0 } });
    }
    if (this._delVariant) { const i = attempts.findIndex((x) => x.k === this._delVariant); if (i > 0) attempts.unshift(attempts.splice(i, 1)[0]); }
    const tried = []; let last = null;
    for (const a of attempts) {
      const { k, ...req } = a;
      try {
        const res = await this.request({ ...req, binary: true });   // 204/empty bodies are fine
        try {
          const j = JSON.parse(new TextDecoder().decode(res.body));
          if (j && j.ok === false) throw new TdriveError(this.redact((j.error && j.error.message) || 'API returned ok:false'), { status: res.status, code: (j.error && j.error.code) || 'api_error' });
        } catch (e) { if (e instanceof TdriveError) throw e; /* empty / non-JSON success body */ }
        this._delVariant = k;
        return true;
      } catch (e) {
        last = e; tried.push(`${k}→${e.status || e.code || 'err'}${e.status === 400 && e.message ? ' "' + e.message.slice(0, 80) + '"' : ''}`);
        if (e.isNetwork || e.retryable || e.status === 401 || e.status === 403) throw e;
      }
    }
    throw new TdriveError('Folder delete failed (' + tried.join(', ') + '): ' + (last && last.message), { status: last && last.status, code: last && last.code });
  }

  async _patch(id, body) {
    return normalizeFileResponse(await this.request({
      method: 'PATCH', path: '/files/' + encodeURIComponent(id),
      headers: { 'Content-Type': 'application/json' }, body: new TextEncoder().encode(JSON.stringify(body)).buffer,
    }));
  }

  async rename(id, name) { return this._patch(id, { name }); }

  /** Renames and/or moves a file in place (no re-upload). The real server handles `name` and `path` reliably only
   *  as SEPARATE PATCH calls (a combined body can silently ignore one of them), so they are issued one by one. */
  async move(id, { name, path } = {}) {
    let f = null;
    if (path) f = await this._patch(id, { path });
    if (name) f = await this._patch(id, { name });
    if (!f) f = await this.getFile(id);
    return f;
  }

  /** Creates folder `name` under parent `path` ("/a/b/"). The body shape is undocumented and a wrong one answers 400,
   *  so the forms below are tried in order; the one that works is remembered. "Already exists" is never retried. */
  async createFolder(name, path) {
    const noSlash = path === '/' ? '/' : path.replace(/\/+$/, '');
    const parent = noSlash === '/' ? '' : noSlash;
    const variants = [
      ['name+path/', { name, path }],
      ['name+path', { name, path: noSlash }],
      ['fullpath', { path: parent + '/' + name }],
      ['fullpath/', { path: parent + '/' + name + '/' }],
      ['name+parentPath', { name, parentPath: noSlash }],
    ];
    if (this._crVariant) { const i = variants.findIndex((x) => x[0] === this._crVariant); if (i > 0) variants.unshift(variants.splice(i, 1)[0]); }
    const tried = []; let last = null;
    for (const [k, body] of variants) {
      try {
        const r = await this.request({
          method: 'POST', path: '/folders', headers: { 'Content-Type': 'application/json' },
          body: new TextEncoder().encode(JSON.stringify(body)).buffer,
        });
        this._crVariant = k;
        return r;
      } catch (e) {
        last = e; tried.push(`${k}→${e.status || e.code || 'err'}${e.message ? ' "' + e.message.slice(0, 80) + '"' : ''}`);
        if (e.status === 409 || /exist/i.test(e.code + ' ' + e.message)) throw e;
        if (!(e.status === 400 || e.status === 404 || e.status === 405 || e.status === 415 || e.status === 422)) throw e;
      }
    }
    throw new TdriveError('Folder create failed (' + tried.join(', ') + ')', { status: last && last.status, code: last && last.code });
  }
}

module.exports = { TdriveClient, TdriveError, MAX_PART_BYTES };
