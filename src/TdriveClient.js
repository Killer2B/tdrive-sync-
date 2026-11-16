'use strict';
const { sleep, redact, sha256Hex } = require('./util');
const { normalizeFileResponse, normalizeList, normalizeReceipt, normalizeName, normalizePath } = require('./normalize');

class TdriveError extends Error {
  constructor(message, o = {}) {
    super(message);
    this.name = 'TdriveError';
    this.status = o.status || 0;
    this.code = o.code || '';
    this.retryable = !!o.retryable;
    this.isNetwork = !!o.isNetwork;
    this.retryAfter = o.retryAfter == null ? null : o.retryAfter;
    this.requestId = o.requestId || '';
    this.details = o.details == null ? null : o.details;
  }
}

function parseRetryAfter(v) {
  if (v == null || v === '') return null;
  if (/^\d+(\.\d+)?$/.test(String(v))) return Number(v);
  const t = Date.parse(v);
  return Number.isNaN(t) ? null : Math.max(0, (t - Date.now()) / 1000);
}

const MAX_PART_BYTES = 5242880;

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
    this.maxServerRetries = opts.maxServerRetries == null ? 12 : opts.maxServerRetries;   // transient 5xx: be patient
    this.baseDelayMs = opts.baseDelayMs == null ? 500 : opts.baseDelayMs;
    this.maxDelayMs = opts.maxDelayMs == null ? 30000 : opts.maxDelayMs;
    this._info = null;
    this._rawInfo = null;
    this._capabilities = null;
    this._lease = null;
    this._ensureLease = null;
    this._leaseTokens = new Set();
    this._leaseEpoch = 0;
  }

  redact(t) {
    let s = String(t == null ? '' : t);
    for (const secret of [this.apiKey, ...this._leaseTokens]) if (secret) s = s.split(secret).join('***');
    return redact(s);
  }

  _sanitize(value, seen = new Set()) {
    if (typeof value === 'string') return this.redact(value);
    if (!value || typeof value !== 'object') return value;
    if (seen.has(value)) return '[Circular]';
    seen.add(value);
    const result = Array.isArray(value) ? value.map((v) => this._sanitize(v, seen)) : Object.fromEntries(Object.entries(value).map(([k, v]) =>
      [this.redact(k), /^(authorization|api[_-]?key|(?:lease[_-]?)?token)$/i.test(k) && v != null ? '***' : this._sanitize(v, seen)]));
    seen.delete(value);
    return result;
  }

  setLease(lease, ensureValidFn) {
    if (!lease || typeof lease.token !== 'string' || !lease.token) throw new TdriveError('Invalid sync lease', { code: 'bad_response' });
    if (ensureValidFn !== undefined && typeof ensureValidFn !== 'function') throw new TdriveError('Invalid sync lease guard', { code: 'bad_config' });
    const path = normalizePath(lease.path);
    this._leaseTokens.add(lease.token);
    this._lease = { ...lease, path };
    if (ensureValidFn !== undefined) {
      this._ensureLease = ensureValidFn;
    }
    return this._lease;
  }

  clearLease() { this._lease = null; this._ensureLease = null; this._leaseEpoch++; }

  _leaseWrite(method, path) {
    if (['POST', 'PATCH', 'DELETE'].includes(method) && /^\/folders(?:\/[^/]+)?$/.test(path)) return true;
    if (method === 'POST' && path === '/files') return true;
    if (['PATCH', 'DELETE'].includes(method) && /^\/files\/[^/]+$/.test(path)) return true;
    if (method === 'POST' && /^\/files\/[^/]+\/(?:shares|versions\/[^/]+\/restore)$/.test(path)) return true;
    if (method === 'POST' && /^\/trash\/[^/]+\/restore$/.test(path)) return true;
    if (method === 'POST' && /^\/uploads(?:\/[^/]+\/(?:complete|abort))?$/.test(path)) return true;
    if (method === 'PUT' && /^\/uploads\/[^/]+\/parts\/\d+$/.test(path)) return true;
    if (method === 'DELETE' && /^\/uploads\/[^/]+$/.test(path)) return true;
    return (method === 'DELETE' && /^\/(?:shares\/[^/]+|files\/[^/]+\/shares\/[^/]+)$/.test(path)) ||
      (method === 'POST' && /^\/(?:shares\/[^/]+|files\/[^/]+\/shares\/[^/]+)\/revoke$/.test(path));
  }

  _url(path, query) {
    const url = new URL(this.origin + '/api/v1' + path);
    for (const [k, v] of Object.entries(query || {})) if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
    if (url.origin !== this.origin) throw new TdriveError('Refusing cross-origin request', { code: 'bad_config' });
    return url.toString();
  }

  async _wait(attempt, retryAfter) {
    // Honour the server's Retry-After as a FLOOR, but keep growing exponentially so a long outage (small
    // Retry-After repeated many times, e.g. ControlUnavailable) does not hammer the server.
    const fromHeader = Number.isFinite(retryAfter) && retryAfter >= 0 ? retryAfter * 1000 : 0;
    const backoff = Math.min(this.baseDelayMs * Math.pow(2, attempt), this.maxDelayMs);
    await this.sleep(Math.max(fromHeader, backoff));
  }

  _httpError(res) {
    let code = 'http_' + res.status; let message = 'HTTP ' + res.status; let bodyRetry = null; let details = null;
    try {
      const j = JSON.parse(new TextDecoder().decode(res.body));
      if (j) {
        // Two shapes are seen live: the documented { ok:false, error:{ code, message } } and a bare
        // { error:"Code", code, retryAfter } (used by transient ControlUnavailable / storage backpressure).
        if (j.error && typeof j.error === 'object') { code = j.error.code || code; message = j.error.message || message; }
        else if (j.code && j.error) { code = j.code; message = typeof j.error === 'string' ? j.error : (j.message || message); }
        else if (j.error && typeof j.error === 'string') { code = j.code || j.error; message = j.message || j.error; }
        if (j.retryAfter != null) bodyRetry = Number(j.retryAfter);
        details = j.error && typeof j.error === 'object' && j.error.details != null ? j.error.details : j.details;
        const sessionIds = (j.error && j.error.sessionIds) || j.sessionIds;
        if (sessionIds != null) details = { ...(details && typeof details === 'object' ? details : {}), sessionIds };
      }
    } catch (e) { /* non-JSON error body */ }
    const headerRetry = parseRetryAfter(res.headers && res.headers['retry-after']);
    const requestId = this.redact(res.headers && res.headers['x-request-id']);
    return new TdriveError(this.redact(`HTTP ${res.status} ${code}: ${message}${requestId ? ' [request ' + requestId + ']' : ''}`), {
      status: res.status, code: this.redact(code), retryable: res.status === 429 || res.status >= 500,
      retryAfter: headerRetry != null ? headerRetry : bodyRetry,
      requestId,
      details: this._sanitize(details),
    });
  }

  async request({ method = 'GET', path = '', query, headers = {}, body, binary = false, retry = true }) {
    method = method.toUpperCase();
    const url = this._url(path, query);
    const leasedWrite = !!this._lease && this._leaseWrite(method, path);
    const leaseEpoch = this._leaseEpoch;
    for (let attempt = 0; ; attempt++) {
      if (leasedWrite && (leaseEpoch !== this._leaseEpoch || !this._lease)) throw new TdriveError('Sync lease was released before the write', { status: 423, code: 'LEASE_CONFLICT' });
      let requestHeaders = { ...headers };
      if (this._lease && this._leaseWrite(method, path)) {
        if (this._ensureLease) {
          try { await this._ensureLease(); }
          catch (e) { throw new TdriveError(this.redact(e && e.message), { ...e, code: this.redact(e && e.code), details: this._sanitize(e && e.details) }); }
        }
        if (!this._lease || (leasedWrite && leaseEpoch !== this._leaseEpoch)) throw new TdriveError('Sync lease was released before the write', { status: 423, code: 'LEASE_CONFLICT' });
        requestHeaders = { ...requestHeaders, 'X-Sync-Path': this._lease.path, 'X-Sync-Lease': this._lease.token };
      }
      let res;
      try {
        res = await this.transport({ url, method, headers: { ...requestHeaders, Authorization: 'Bearer ' + this.apiKey }, body });
      } catch (e) {
        const err = new TdriveError('Network error: ' + this.redact(e && e.message), { isNetwork: true, retryable: true });
        if (!retry || attempt >= this.maxRetries) throw err;
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
        // 5xx is transient server backpressure ("Freeing storage space — retrying shortly"): be more patient
        // than for other errors and always honour Retry-After, so a short outage does not fail the whole run.
        const cap = res.status >= 500 ? this.maxServerRetries : this.maxRetries;
        if (!retry || attempt >= cap) throw err;
        if (this.onRetry) { try { this.onRetry({ attempt: attempt + 1, status: res.status, retryAfter: err.retryAfter }); } catch (_) { /* hook only */ } }
        await this._wait(attempt, err.retryAfter);
        continue;
      }
      if (res.status === 304) return { status: 304, notModified: true, headers: res.headers || {}, body: new ArrayBuffer(0) };
      // A ranged request past EOF answers 416 with "Content-Range: bytes */<total>" (RFC 7233). That total is
      // authoritative, so binary callers must receive it (the download layer reconciles the size) rather than
      // see it converted into an error.
      if (res.status === 416 && binary) return { status: 416, headers: res.headers || {}, body: res.body || new ArrayBuffer(0) };
      if (res.status >= 400) throw this._httpError(res);
      if (binary) return { status: res.status, headers: res.headers || {}, body: res.body };
      if (res.status === 204 && method === 'DELETE') return {};
      let json;
      try { json = JSON.parse(new TextDecoder().decode(res.body)); } catch (e) {
        if (!retry || attempt >= this.maxRetries || !['GET', 'HEAD'].includes(method)) throw new TdriveError('Invalid JSON response (HTTP ' + res.status + ')', { status: res.status, code: 'bad_response', retryable: ['GET', 'HEAD'].includes(method) });
        await this._wait(attempt, null);
        continue;
      }
      if (!json || json.ok !== true) {
        throw this._httpError(res);
      }
      return Object.prototype.hasOwnProperty.call(json, 'data') ? json.data : json;
    }
  }

  async info() {
    if (!this._rawInfo) this._rawInfo = this.request({ path: '' }).then((data) => {
      if (data && data.capabilities != null && (typeof data.capabilities !== 'object' || Array.isArray(data.capabilities))) throw new TdriveError('Invalid server capabilities', { code: 'bad_response' });
      this._capabilities = data && data.capabilities || {};
      return data;
    }).catch((e) => { this._rawInfo = null; throw e; });
    return this._rawInfo;
  }
  async capabilities() { await this.info(); return this._capabilities; }
  me() { return this.request({ path: '/me' }); }

  async _leaseControl(method, { path, owner, token, ttlSeconds }) {
    path = normalizePath(path);
    if (method === 'POST' && ttlSeconds === undefined) ttlSeconds = 60;
    if (method === 'POST' && (typeof owner !== 'string' || !owner)) throw new TdriveError('Invalid sync lease owner', { code: 'bad_request' });
    if (method !== 'POST' && (typeof token !== 'string' || !token)) throw new TdriveError('Invalid sync lease token', { code: 'bad_request' });
    if (method !== 'DELETE' && !(method === 'PATCH' && ttlSeconds === undefined) && (!Number.isSafeInteger(ttlSeconds) || ttlSeconds < 15 || ttlSeconds > 300)) throw new TdriveError('Invalid sync lease ttlSeconds', { code: 'bad_request' });
    if (token) this._leaseTokens.add(token);
    const epoch = this._leaseEpoch;
    const response = await this.request({ method, path: '/sync/leases', retry: false, headers: { 'Content-Type': 'application/json' },
      body: new TextEncoder().encode(JSON.stringify({ path, ...(method === 'POST' ? { owner } : { token }), ...(method === 'DELETE' ? {} : { ttlSeconds }) })).buffer });
    if (response && typeof response.token === 'string' && response.token) this._leaseTokens.add(response.token);
    if (method === 'POST' && (!response || typeof response.token !== 'string' || !response.token)) throw new TdriveError('Invalid acquired sync lease', { code: 'bad_response' });
    if (epoch === this._leaseEpoch && this._lease && this._lease.token === token) {
      if (method === 'DELETE') this.clearLease();
      else if (method === 'PATCH') this.setLease({ ...this._lease, ...response, path, token: response && response.token || token });
    }
    return response;
  }

  acquireLease(lease) { return this._leaseControl('POST', lease); }
  renewLease(lease) { return this._leaseControl('PATCH', lease); }
  releaseLease(lease) { return this._leaseControl('DELETE', lease); }

  /** Upload limits from GET /api/v1 (upload.oneShotMaxBytes, upload.chunkBytes). Never guessed. */
  async limits() {
    if (this._info) return this._info;
    const d = await this.info();
    const u = (d && d.upload) || d || {};
    const srvChunk = Number(u.chunkBytes);
    if (!Number.isSafeInteger(srvChunk) || srvChunk <= 0) throw new TdriveError('Server did not report valid upload.chunkBytes', { code: 'bad_response' });
    const srvOne = Number(u.oneShotMaxBytes);
    if (!Number.isSafeInteger(srvOne) || srvOne <= 0 || srvOne > srvChunk) throw new TdriveError('Server did not report valid upload.oneShotMaxBytes', { code: 'bad_response' });
    this._info = { chunkBytes: srvChunk, oneShotMaxBytes: srvOne };
    return this._info;
  }

  /** Discovery is read-only; real upload sessions are validated by the upload manager. */
  async uploadChunkSize() {
    return (await this.limits()).chunkBytes;
  }

  /** Lists one folder (all pages). Returns {files, folders} (normalized, de-duplicated by id). */
  async listFolder(path) {
    path = normalizePath(path);
    const limit = 100;
    const files = []; const folders = []; const seen = new Set();
    for (let offset = 0; ;) {
      const data = await this.request({ path: '/files', query: { path, limit, offset } });
      const page = normalizeList(data);
      let freshFiles = 0;
      for (const f of page.files) { const k = 'f' + f.id; if (!seen.has(k)) { seen.add(k); files.push(f); freshFiles++; } }
      for (const f of page.folders) { const k = 'd' + f.name; if (!seen.has(k)) { seen.add(k); folders.push(f); } }
      if (Object.prototype.hasOwnProperty.call(data, 'nextOffset')) {
        if (data.nextOffset === null) break;
        if (!Number.isSafeInteger(data.nextOffset) || data.nextOffset <= offset || data.nextOffset > 100000) throw new TdriveError('Invalid listing nextOffset', { code: 'bad_response' });
        offset = data.nextOffset;
      } else {
        if (page.files.length < limit) break;
        if (!freshFiles || offset + limit > 100000) throw new TdriveError('Remote listing pagination did not complete', { code: 'bad_response' });
        offset += limit;
      }
    }
    return { files, folders };
  }

  async getFile(id) {
    const f = normalizeFileResponse(await this.request({ path: '/files/' + encodeURIComponent(id) }));
    if (!f || f.id !== String(id) || !f.name || !Number.isSafeInteger(f.size)) throw new TdriveError('Invalid file metadata response', { code: 'bad_response' });
    return f;
  }

  /** Which of these chunk SHA-256 hashes does the server already store for this account? (≤500 per call) */
  async chunksLookup(checksums) {
    return this.request({ method: 'POST', path: '/chunks/lookup', headers: { 'Content-Type': 'application/json' }, body: new TextEncoder().encode(JSON.stringify({ checksums })).buffer });
  }

  getUpload(id) { return this.request({ path: '/uploads/' + encodeURIComponent(id) }); }
  abortUpload(id) { return this.request({ method: 'DELETE', path: '/uploads/' + encodeURIComponent(id) }); }

  /** Account-owned sessions only; enumeration never authorizes automatic cancellation. */
  async listUploads({ path, limit = 100, offset = 0, ...filters } = {}) {
    if (!Number.isSafeInteger(limit) || limit <= 0 || limit > 500 || !Number.isSafeInteger(offset) || offset < 0 || offset > 100000) throw new TdriveError('Invalid upload listing pagination', { code: 'bad_response' });
    if (filters.active !== undefined && ![false, true, 0, 1].includes(filters.active)) throw new TdriveError('Invalid active upload filter', { code: 'bad_request' });
    if (filters.status !== undefined && !['pending', 'ready', 'committed', 'deleting'].includes(filters.status)) throw new TdriveError('Invalid upload status filter', { code: 'bad_request' });
    if (path !== undefined) path = normalizePath(path);
    if (typeof filters.active === 'boolean') filters.active = filters.active ? 1 : 0;
    const uploads = []; const seen = new Set();
    for (;;) {
      const data = await this.request({ path: '/uploads', query: { ...filters, path, limit, offset } });
      if (!data || typeof data !== 'object' || !Array.isArray(data.sessions) || !Object.prototype.hasOwnProperty.call(data, 'nextOffset')) throw new TdriveError('Invalid upload listing response', { code: 'bad_response' });
      for (const session of data.sessions) {
        const id = session && (session.uploadId || session.id);
        if (!session || typeof session !== 'object' || typeof id !== 'string' || !id || typeof session.status !== 'string' || !session.status) throw new TdriveError('Invalid upload listing session', { code: 'bad_response' });
        if (!seen.has(id)) { seen.add(id); uploads.push(session); }
      }
      if (data.nextOffset === null) return uploads;
      if (!Number.isSafeInteger(data.nextOffset) || data.nextOffset <= offset || data.nextOffset > 100000) throw new TdriveError('Invalid upload listing nextOffset', { code: 'bad_response' });
      offset = data.nextOffset;
    }
  }

  /** Soft delete (goes to Tdrive trash, recoverable). Permanent deletion is never used. */
  trash(id) { return this.request({ method: 'DELETE', path: '/files/' + encodeURIComponent(id), query: { permanent: 0 } }); }
  restore(id) { return this.request({ method: 'POST', path: '/trash/' + encodeURIComponent(id) + '/restore' }); }

  /** Soft delete by parent path and name only. The caller must verify emptiness first. */
  async deleteFolder(path) {
    const full = normalizePath(path);
    if (full === '/') throw new TdriveError('Refusing to delete the root folder', { code: 'invalid_path' });
    const target = full.slice(0, -1);
    const i = target.lastIndexOf('/');
    await this.request({ method: 'DELETE', path: '/folders', query: { path: target.slice(0, i + 1), name: target.slice(i + 1), ifEmpty: this._capabilities && this._capabilities.atomicIfEmpty === true ? 1 : undefined } });
    return true;
  }

  async _patch(id, body, contentSize) {
    if (Object.prototype.hasOwnProperty.call(body, 'content') && ['name', 'path', 'favorite'].some((k) => Object.prototype.hasOwnProperty.call(body, k))) throw new TdriveError('Cannot combine metadata and content changes', { code: 'bad_request' });
    const response = await this.request({
      method: 'PATCH', path: '/files/' + encodeURIComponent(id),
      headers: { 'Content-Type': 'application/json' }, body: new TextEncoder().encode(JSON.stringify(body)).buffer,
    });
    const ack = normalizeFileResponse(response);
    if (ack && ack.id !== String(id)) throw new TdriveError('PATCH returned a different file id', { code: 'bad_response' });
    // PATCH acknowledgments may omit metadata; never retry an already committed mutation to obtain it.
    const head = await this.getFile(id);
    const receipt = normalizeReceipt(response);
    const result = Object.assign(head, receipt);
    if (Object.prototype.hasOwnProperty.call(body, 'content')) {
      if (!Object.prototype.hasOwnProperty.call(receipt, 'contentVersion')) result.contentVersion = null;
      if (contentSize !== undefined && receipt.contentVersion != null) {
        const raw = response && (response.file || response.item) || response;
        if (raw && raw.size != null && raw.size !== contentSize) throw new TdriveError('PATCH content size mismatch', { code: 'bad_response' });
        result.size = contentSize;
      }
      const raw = response && (response.file || response.item) || response;
      const committed = normalizeFileResponse({ ...raw, id: String(id) });
      result.mtime = committed ? committed.mtime : null;
      result.sha256 = committed ? committed.sha256 : null;
      result.raw = { ...raw, ...receipt, id: String(id), contentVersion: result.contentVersion,
        checksum: receipt.checksum == null ? null : receipt.checksum, etag: receipt.etag == null ? null : receipt.etag,
        version: receipt.version == null ? null : receipt.version };
    }
    return result;
  }

  /** Replaces a file's content in place when it fits in one chunk (≤ oneShotMaxBytes). Runs through the same
   *  revision engine as the resumable path: atomic swap, previous content kept as a version, id/name/path kept.
   *  Returns the updated file, or null when the payload is too large for the inline form. */
  async patchContent(id, data, { expectedVersion } = {}) {
    if (expectedVersion !== undefined && (!Number.isSafeInteger(expectedVersion) || expectedVersion <= 0)) throw new TdriveError('Invalid expectedVersion', { code: 'bad_request' });
    const lim = await this.limits();
    const buf = data instanceof ArrayBuffer ? new Uint8Array(data) : new Uint8Array(data.buffer, data.byteOffset || 0, data.byteLength);
    if (buf.byteLength > lim.oneShotMaxBytes) return null;
    let b64 = '';
    try { b64 = btoa(String.fromCharCode(...buf)); }
    catch (e) { // large-ish payloads exceed the argument limit: encode in chunks
      let s = ''; for (let i = 0; i < buf.length; i += 0x8000) s += String.fromCharCode.apply(null, buf.subarray(i, i + 0x8000));
      b64 = btoa(s);
    }
    const r = await this._patch(id, { ...(expectedVersion === undefined ? {} : { expectedVersion }), content: { inline: b64 } }, buf.byteLength);
    const committed = () => Object.assign(r, normalizeReceipt(r.raw));
    if (r.contentVersion != null) {
      const { fingerprintOf } = require('./Chunker');
      const receipt = normalizeReceipt(r.raw);
      if (receipt.checksum != null && receipt.checksum !== await fingerprintOf([await sha256Hex(buf)])) throw new TdriveError('PATCH content checksum mismatch', { code: 'bad_response' });
      return committed();
    }
    if (r.size !== buf.byteLength) throw new TdriveError('PATCH content size mismatch', { code: 'bad_response' });
    // Hydrated metadata may belong to a newer writer, not the PATCH that just completed.
    try {
      const { fetchManifest } = require('./DownloadManager');
      const { fingerprintOf } = require('./Chunker');
      const map = await fetchManifest(this, id);
      if (!map) {
        const stored = await this.request({ path: '/files/' + encodeURIComponent(id) + '/content', binary: true });
        if (await sha256Hex(stored.body) !== await sha256Hex(buf)) throw new TdriveError('PATCH content does not match stored bytes', { code: 'content_changed' });
        return committed();
      }
      const fingerprint = map.checksum || await fingerprintOf(map.chunks.map((c) => c.checksum));
      if (map.size !== buf.byteLength || (r.checksum && r.checksum !== fingerprint)) throw new TdriveError('Remote content changed before PATCH acknowledgement', { code: 'content_changed' });
      for (const chunk of map.chunks) {
        if (await sha256Hex(buf.subarray(chunk.offset, chunk.offset + chunk.size)) !== chunk.checksum) throw new TdriveError('PATCH content does not match the committed version', { code: 'content_changed' });
      }
    } catch (e) {
      if (e.status !== 404) throw e;
      const stored = await this.request({ path: '/files/' + encodeURIComponent(id) + '/content', binary: true });
      if (await sha256Hex(stored.body) !== await sha256Hex(buf)) throw new TdriveError('PATCH content does not match stored bytes', { code: 'content_changed' });
    }
    return committed();
  }

  async rename(id, name) { return this._patch(id, { name }); }
  /** V2 supports atomic metadata PATCH; concrete older servers require separate path/name calls. */
  async move(id, { name, path } = {}) {
    if (this._capabilities && this._capabilities.expectedContentVersion === true && (name || path)) return this._patch(id, { ...(name ? { name } : {}), ...(path ? { path } : {}) });
    let f = null;
    if (path) f = await this._patch(id, { path });
    if (name) f = await this._patch(id, { name });
    if (!f) f = await this.getFile(id);
    return f;
  }

  /** Creates a single folder segment under its parent. Trash name conflicts remain errors. */
  async createFolder(name, path) {
    return this.request({ method: 'POST', path: '/folders', headers: { 'Content-Type': 'application/json' },
      body: new TextEncoder().encode(JSON.stringify({ name: normalizeName(name), path: normalizePath(path) })).buffer });
  }
}

module.exports = { TdriveClient, TdriveError, MAX_PART_BYTES };
