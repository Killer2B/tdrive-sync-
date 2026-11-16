'use strict';
// In-process Tdrive contract server. Alternative legacy listing shapes remain explicit fixtures.
const http = require('http');
const crypto = require('crypto');

const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');
const normFolder = (p) => { p = p || '/'; if (!p.startsWith('/')) p = '/' + p; if (!p.endsWith('/')) p += '/'; return p; };

function createMockServer(opts = {}) {
  const st = {
    key: 'tdk_testkey1234567890', files: new Map(), folders: new Set(['/']), uploads: new Map(), idem: new Map(),
    nextId: 1, clock: 1_700_000_000_000, log: [], rules: [],
    dup: opts.dup || 'allow', requireFolders: opts.requireFolders !== false,
    maxBody: opts.maxBody || 0, maxSeen: 0,
    folderShape: opts.folderShape || 'default', hideSubfolderFiles: !!opts.hideSubfolderFiles, dupFolders: !!opts.dupFolders,
    folderDelete: opts.folderDelete || 'any',
    oneShotMax: opts.oneShotMax || 5242880, chunk: opts.chunk || 5242880,
    legacyFeatures: !!opts.legacyFeatures, chunkManifests: opts.chunkManifests !== false,
    noRevisions: !!opts.noRevisions, chunkStore: new Map(), activity: [], faultHits: {},
  };
  const tick = () => (st.clock += 1000);
  const fileJson = (f, listing = false) => ({ id: f.id, name: f.name, path: f.path,
    size: listing && st.listingSizeOverride && st.listingSizeOverride[f.id] != null ? st.listingSizeOverride[f.id] : f.bytes.length,
    lastModified: f.lastModified, trashed: f.trashed,
    ...(st.legacyFeatures ? { contentVersion: f.version || 1, etag: etagOf(f), checksum: fingerprintOf(f) } : {}) });
  const live = () => [...st.files.values()].filter((f) => !f.trashed);
  const note = (action, target) => { if (st.legacyFeatures) st.activity.push({ action, target, detail: '{}', created_at: new Date(tick()).toISOString() }); };
  const hit = (fault) => { st.faultHits[fault] = (st.faultHits[fault] || 0) + 1; };
  function planOf(f) {
    if (f.chunks) return f.chunks;
    return [...Array(Math.max(1, Math.ceil(f.bytes.length / st.chunk))).keys()].map((idx) => {
      const offset = idx * st.chunk; const bytes = f.bytes.subarray(offset, offset + st.chunk);
      return { idx, offset, size: bytes.length, checksum: sha(bytes) };
    });
  }
  const fingerprintOf = (f) => sha(planOf(f).map((c) => c.checksum).join('\n'));
  const etagOf = (f) => 'manifest-' + fingerprintOf(f);
  function storePlan(f) { if (st.legacyFeatures) for (const c of planOf(f)) st.chunkStore.set(c.checksum, Buffer.from(f.bytes.subarray(c.offset, c.offset + c.size))); }
  function replaceFile(f, bytes, chunks, chunker) {
    if (st.legacyFeatures) { (f.versions || (f.versions = [])).push({ version: f.version || 1, bytes: Buffer.from(f.bytes) }); f.version = (f.version || 1) + 1; }
    f.bytes = Buffer.from(bytes); f.chunks = chunks || null; f.chunker = chunker || null; f.lastModified = tick(); storePlan(f); note('revision', f.id);
  }

  function addFolder(p) { p = normFolder(p); const parts = p.split('/').filter(Boolean); let cur = '/'; for (const s of parts) { cur += s + '/'; st.folders.add(cur); } }
  function createFile(name, path, bytes, lastModified) {
    path = normFolder(path);
    const existing = live().find((f) => f.name === name && f.path === path);
    if (existing && st.dup === 'reject409') return { err: [409, 'conflict', 'name exists'] };
    if (existing && st.dup === 'autorename') {
      const dot = name.lastIndexOf('.'); const base = dot > 0 ? name.slice(0, dot) : name; const ext = dot > 0 ? name.slice(dot) : '';
      let n = 1; while (live().some((f) => f.name === `${base} (${n})${ext}` && f.path === path)) n++;
      name = `${base} (${n})${ext}`;
    }
    const f = { id: 'f' + st.nextId++, name, path, bytes: Buffer.from(bytes), lastModified: lastModified || tick(), trashed: false };
    st.files.set(f.id, f); storePlan(f); note('upload', f.id); return { f };
  }
  st.addFile = (path, name, data) => { addFolder(path); return createFile(name, path, Buffer.from(data)).f; };
  st.modifyFile = (id, data) => { const f = st.files.get(id); replaceFile(f, data); return f; };
  st.removeFile = (id) => { st.files.get(id).trashed = true; note('trash', id); };
  st.findLive = (path, name) => live().find((f) => f.path === normFolder(path) && f.name === name);
  st.liveFiles = live;
  st.failRule = (match, action, times = 1) => st.rules.push({ match, action, times });
  st.removeFolder = (p) => { p = normFolder(p); for (const f of [...st.folders]) if (f.startsWith(p)) st.folders.delete(f); for (const f of st.files.values()) if (f.path.startsWith(p)) f.trashed = true; };
  st.opsLog = () => st.log.filter((l) => l.method !== 'GET');
  st.addActivity = (action, target) => note(action, target);

  const send = (res, status, body, headers = {}) => {
    const buf = Buffer.isBuffer(body) ? body : Buffer.from(JSON.stringify(body));
    res.writeHead(status, { 'Content-Type': Buffer.isBuffer(body) ? 'application/octet-stream' : 'application/json', 'Content-Length': buf.length, ...headers });
    res.end(buf);
  };
  const ok = (res, data, status = 200, h) => send(res, status, { ok: true, data }, h);
  const fail = (res, status, code, message, h) => send(res, status, { ok: false, error: { code, message } }, h);

  const server = http.createServer(async (req, res) => {
    const chunks = []; for await (const c of req) chunks.push(c);
    const body = Buffer.concat(chunks);
    const u = new URL(req.url, 'http://x');
    const route = u.pathname.replace(/^\/api\/v1/, '') || '';
    const partM = /^\/uploads\/([^/]+)\/parts\/(\d+)$/.exec(route);
    const info = { method: req.method, route, query: Object.fromEntries(u.searchParams), range: req.headers.range, partIdx: partM ? Number(partM[2]) : undefined, name: u.searchParams.get('name') };
    st.log.push(info);
    st.maxSeen = Math.max(st.maxSeen, body.length);
    if (st.maxBody && body.length > st.maxBody) return fail(res, 413, 'too_large', 'body over limit');
    for (const r of st.rules) {
      if (r.times > 0 && r.match(info)) {
        r.times--;
        if (r.action === 'drop') return req.socket.destroy();
        return fail(res, r.action.status, 'injected', 'injected failure', r.action.retryAfter != null ? { 'Retry-After': String(r.action.retryAfter) } : {});
      }
    }
    if (req.headers.authorization !== 'Bearer ' + st.key) return fail(res, 401, 'unauthorized', 'bad key');
    const m = req.method;
    try {
      if (route === '' && m === 'GET') return ok(res, { upload: { oneShotMaxBytes: st.oneShotMax, chunkBytes: st.chunk } });
      if (route === '/me') return ok(res, { name: 'tester' });
      if (st.legacyFeatures && route === '/activity' && m === 'GET') return ok(res, { activity: st.activity.slice(-50).reverse(), nextOffset: null });
      if (st.legacyFeatures && route === '/search' && m === 'GET') {
        const q = (u.searchParams.get('q') || '').toLowerCase();
        return ok(res, { files: live().filter((f) => f.name.toLowerCase().includes(q)).map((f) => fileJson(f)), nextOffset: null });
      }
      if (st.legacyFeatures && route === '/chunks/lookup' && m === 'POST') {
        const j = JSON.parse(body.toString()); return ok(res, { available: (j.checksums || []).filter((h) => st.chunkStore.has(h)), chunkBytes: st.chunk });
      }
      let featureMatch;
      if (st.legacyFeatures && (featureMatch = /^\/files\/([^/]+)\/shares$/.exec(route)) && m === 'POST') {
        if (!st.files.has(featureMatch[1])) return fail(res, 404, 'not_found', 'no file');
        const token = 'sh' + st.nextId++; return ok(res, { token, url: st.url + '/s/' + token, permission: 'download' }, 201);
      }
      if (st.legacyFeatures && (featureMatch = /^\/files\/([^/]+)\/versions$/.exec(route)) && m === 'GET') {
        const f = st.files.get(featureMatch[1]); if (!f) return fail(res, 404, 'not_found', 'no file');
        return ok(res, { versions: (f.versions || []).map((v, i) => ({ id: 'v' + (i + 1), version: v.version, size: v.bytes.length })).reverse() });
      }
      if (st.legacyFeatures && (featureMatch = /^\/files\/([^/]+)\/versions\/v(\d+)\/restore$/.exec(route)) && m === 'POST') {
        const f = st.files.get(featureMatch[1]); const v = f && (f.versions || [])[Number(featureMatch[2]) - 1];
        if (!v) return fail(res, 404, 'not_found', 'no version'); replaceFile(f, v.bytes); return ok(res, { file: fileJson(f) });
      }
      if (st.legacyFeatures && (featureMatch = /^\/files\/([^/]+)\/(chunks|delta)$/.exec(route))) {
        const f = st.files.get(featureMatch[1]); if (!f) return fail(res, 404, 'not_found', 'no file');
        const chunks = planOf(f); const etag = etagOf(f);
        const manifest = { id: f.id, size: f.bytes.length, contentVersion: f.version || 1, etag, checksum: fingerprintOf(f), chunker: f.chunker || null, chunkSize: st.chunk, totalChunks: chunks.length, chunks };
        if (featureMatch[2] === 'chunks' && m === 'GET') {
          if (!st.chunkManifests) return fail(res, 404, 'not_found', 'chunk manifests unavailable');
          if (req.headers['if-none-match'] === etag) { res.writeHead(304, { ETag: etag }); return res.end(); }
          return ok(res, manifest);
        }
        if (featureMatch[2] === 'delta' && m === 'POST') {
          const mine = new Set(JSON.parse(body.toString()).checksums || []); const changed = chunks.filter((c) => !mine.has(c.checksum));
          const changedRanges = []; for (const c of changed) { const last = changedRanges[changedRanges.length - 1]; if (last && last[0] + last[1] === c.offset) last[1] += c.size; else changedRanges.push([c.offset, c.size]); }
          return ok(res, { ...manifest, changed, changedRanges, unchanged: chunks.filter((c) => mine.has(c.checksum)).map((c) => c.checksum), checksums: chunks.map((c) => c.checksum) });
        }
      }
      if (route === '/files' && m === 'GET') {
        const p = normFolder(u.searchParams.get('path'));
        if (!st.folders.has(p) && p !== '/') return fail(res, 404, 'not_found', 'no such folder');
        const depth = p.split('/').filter(Boolean).length;
        const files = live().filter((f) => f.path === p).filter(() => !(st.hideSubfolderFiles && depth > 1)).map((f) => fileJson(f, true));
        const subs = [...st.folders].filter((f) => f !== p && f.startsWith(p) && f.slice(p.length).split('/').filter(Boolean).length === 1);
        const shape = {
          default: (f) => ({ id: 'd' + f, name: f.slice(p.length, -1), type: 'folder' }),
          pathOnly: (f) => ({ id: 'd' + f, path: f, type: 'folder' }),          // no `name` field
          strings: (f) => f.slice(p.length, -1),                                // bare names
          kindDir: (f) => ({ id: 'd' + f, name: f.slice(p.length, -1), kind: 'directory' }),
        }[st.folderShape] || (() => null);
        const folders = subs.map(shape);
        const off = Number(u.searchParams.get('offset') || 0); const lim = Number(u.searchParams.get('limit') || 100);
        if (st.folderShape === 'inFiles') {     // folders mixed into `files`, flagged only by mimeType
          const mixed = [...subs.map((f) => ({ id: 'd' + f, name: f.slice(p.length, -1), mimeType: 'application/vnd.tdrive.folder' })), ...files];
          return ok(res, { files: mixed.slice(off, off + lim) });
        }
        return ok(res, { files: files.slice(off, off + lim), folders: off === 0 ? folders : [] });
      }
      if (route === '/files' && m === 'POST') {
        const key = req.headers['idempotency-key'];
        if (key && st.idem.has(key)) return ok(res, fileJson(st.files.get(st.idem.get(key))), 201);
        const path = normFolder(u.searchParams.get('path'));
        if (st.requireFolders && !st.folders.has(path)) return fail(res, 404, 'folder_not_found', 'folder missing');
        if (body.length > st.oneShotMax) return fail(res, 413, 'too_large', 'use resumable upload');
        const r = createFile(u.searchParams.get('name'), path, body);
        if (r.err) return fail(res, ...r.err);
        if (key) st.idem.set(key, r.f.id);
        return ok(res, fileJson(r.f), 201);
      }
      let mm;
      if ((mm = /^\/files\/([^/]+)\/content$/.exec(route)) && m === 'GET') {
        const f = st.files.get(mm[1]); if (!f) return fail(res, 404, 'not_found', 'nope');
        const headers = st.legacyFeatures ? { ETag: etagOf(f) } : {};
        if (st.legacyFeatures && st.busyFirst > 0) { st.busyFirst--; hit('busyFirst'); return fail(res, 503, 'Busy', 'try later'); }
        const bytes = st.legacyFeatures && st.contentOverride ? st.contentOverride : f.bytes;
        if (bytes !== f.bytes) hit('contentOverride');
        const rg = /bytes=(\d+)-(\d*)/.exec(req.headers.range || '');
        const a = rg ? Number(rg[1]) : 0; const b = rg ? Math.min(rg[2] ? Number(rg[2]) : bytes.length - 1, bytes.length - 1) : bytes.length - 1;
        if (rg && a >= bytes.length) return send(res, 416, Buffer.alloc(0), { 'Content-Range': `bytes */${bytes.length}`, ...headers });
        if (st.legacyFeatures && rg && st.contentTruncate && st.contentTruncate.times > 0) {
          st.contentTruncate.times--; hit('contentTruncate');
          return send(res, 206, bytes.subarray(a, Math.min(a + st.contentTruncate.at, b + 1)), { 'Content-Range': `bytes ${a}-${b}/${bytes.length}`, ...headers });
        }
        if (st.legacyFeatures && rg && st.staleContent && a === 0) {
          hit('staleContent'); const n = Math.min(st.staleContent.partial, bytes.length);
          return send(res, 206, bytes.subarray(0, n), { 'Content-Range': `bytes 0-${n - 1}/${n}`, ...headers });
        }
        const content = Buffer.from(bytes.subarray(a, b + 1));
        if (st.legacyFeatures && st.corruptContent) { if (content.length) content[0] ^= 0xff; hit('corruptContent'); }
        return send(res, rg ? 206 : 200, content, { ...(rg ? { 'Content-Range': `bytes ${a}-${b}/${bytes.length}` } : {}), ...headers });
      }
      if ((mm = /^\/files\/([^/]+)$/.exec(route))) {
        const f = st.files.get(mm[1]); if (!f) return fail(res, 404, 'not_found', 'nope');
        if (m === 'GET') return ok(res, fileJson(f));
        if (m === 'DELETE') { if (u.searchParams.get('permanent') === '1') return fail(res, 400, 'refused', 'test'); f.trashed = true; note('trash', f.id); return ok(res, { id: f.id }); }
        if (m === 'PATCH') {
          const j = JSON.parse(body.toString()); if (j.name) f.name = j.name; if (j.path) f.path = normFolder(j.path);
          if (j.content && typeof j.content.inline === 'string') {
            const bytes = Buffer.from(j.content.inline, 'base64');
            if (st.legacyFeatures && bytes.length > st.oneShotMax) return fail(res, 413, 'too_large', 'inline content exceeds limit');
            replaceFile(f, bytes, st.legacyFeatures ? [{ idx: 0, offset: 0, size: bytes.length, checksum: sha(bytes) }] : null);
          } else note('rename', f.id);
          return ok(res, { file: fileJson(f) });
        }
      }
      if ((mm = /^\/trash\/([^/]+)\/restore$/.exec(route)) && m === 'POST') { const f = st.files.get(mm[1]); if (f) f.trashed = false; return ok(res, { id: mm[1] }); }
      if (m === 'DELETE' && (route === '/folders' || /^\/folders\/[^/]+$/.test(route))) {
        if (st.folderDelete === 'none') return fail(res, 405, 'method_not_allowed', 'no');
        let p;
        if (route === '/folders') {
          if (!u.searchParams.get('name')) return fail(res, 400, 'invalid_request', 'Folder name is required');
          p = normFolder(u.searchParams.get('path')) + u.searchParams.get('name') + '/';
        }
        else { if (st.folderDelete === 'byPath') return fail(res, 404, 'not_found', 'unknown route'); p = decodeURIComponent(route.slice('/folders/'.length)).replace(/^d/, ''); }
        p = normFolder(p);
        if (!st.folders.has(p) || p === '/') return fail(res, 404, 'not_found', 'no such folder');
        if (u.searchParams.get('permanent') === '1') return fail(res, 400, 'refused', 'test');
        for (const f of [...st.folders]) if (f.startsWith(p)) st.folders.delete(f);       // sub-folders go too
        for (const f of st.files.values()) if (f.path.startsWith(p)) f.trashed = true;    // soft delete of contents
        return ok(res, {});
      }
      if (route === '/folders' && m === 'POST') {
        const j = JSON.parse(body.toString()); const p = normFolder(normFolder(j.path) + j.name);
        if (st.folders.has(p) && st.dupFolders) { st.folders.add(normFolder(normFolder(j.path) + j.name + ' (1)')); return ok(res, { name: j.name + ' (1)', path: j.path }, 201); }
        if (st.folders.has(p)) return fail(res, 409, 'exists', 'folder exists');
        if (!st.folders.has(normFolder(j.path))) return fail(res, 404, 'parent_missing', 'parent missing');
        st.folders.add(p); return ok(res, { name: j.name, path: j.path }, 201);
      }
      if (route === '/uploads' && m === 'POST') {
        const j = JSON.parse(body.toString());
        if (!/^[0-9a-f-]{36}$/.test(j.clientKey)) return fail(res, 400, 'bad_key', 'clientKey must be uuid');
        if (st.legacyFeatures && j.baseId && st.noRevisions) return fail(res, 400, 'invalid_request', 'revisions not supported');
        let s = [...st.uploads.values()].find((x) => x.clientKey === j.clientKey);
        if (!s) { s = { id: 'u' + st.nextId++, ...j, parts: new Map(), offsets: new Map(), done: null }; st.uploads.set(s.id, s); }
        const base = j.baseId && st.files.get(j.baseId);
        if (j.baseId && !base) return fail(res, 404, 'NOT_FOUND', 'Base not found');
        return ok(res, { id: s.id, status: s.done ? (base ? 'committed' : 'ready') : s.parts.size ? 'uploading' : 'pending', fileId: s.done || undefined, clientKey: s.clientKey,
          chunkSize: st.chunk, totalChunks: s.chunker ? s.totalChunks : Math.max(1, Math.ceil(s.size / st.chunk)), received: [...s.parts.keys()], revision: !!base, ...(base ? { base: { parts: st.legacyFeatures ? planOf(base) : [] } } : {}), ...(s.chunker ? { chunker: s.chunker } : {}) }, 201);
      }
      if ((mm = /^\/uploads\/([^/]+)\/parts\/(\d+)$/.exec(route)) && m === 'PUT') {
        const s = st.uploads.get(mm[1]); if (!s) return fail(res, 404, 'not_found', 'no session');
        let bytes = body;
        if (st.legacyFeatures && req.headers['x-chunk-reuse'] === '1') {
          if (body.length) return fail(res, 400, 'INVALID_PART', 'reuse must have no body');
          bytes = st.chunkStore.get(req.headers['x-content-sha256']);
          if (!bytes) return fail(res, 409, 'ChunkNotAvailable', 'chunk unavailable');
        }
        if (sha(bytes) !== req.headers['x-content-sha256']) return fail(res, 400, 'checksum', 'bad checksum');
        const idx = Number(mm[2]);
        const n = s.chunker ? s.totalChunks : Math.max(1, Math.ceil(s.size / st.chunk));
        if (idx >= n || bytes.length > st.chunk) return fail(res, 400, 'INVALID_PART', 'Part exceeds plan');
        if (!s.chunker && bytes.length !== Math.min(st.chunk, s.size - idx * st.chunk)) return fail(res, 400, 'INVALID_PART', 'Incorrect fixed part length');
        if (st.legacyFeatures && Number(req.headers['x-chunk-size']) !== bytes.length) return fail(res, 400, 'INVALID_PART', 'Incorrect declared part size');
        s.offsets.set(idx, Number(req.headers['x-chunk-offset'] || idx * st.chunk));
        s.parts.set(idx, Buffer.from(bytes)); if (st.legacyFeatures) st.chunkStore.set(sha(bytes), Buffer.from(bytes));
        return ok(res, { idx, ...(bytes !== body ? { reused: true } : {}) });
      }
      if ((mm = /^\/uploads\/([^/]+)\/complete$/.exec(route)) && m === 'POST') {
        const s = st.uploads.get(mm[1]); if (!s) return fail(res, 404, 'not_found', 'no session');
        if (s.done) return ok(res, fileJson(st.files.get(s.done)));
        const n = s.chunker ? s.totalChunks : Math.max(1, Math.ceil(s.size / st.chunk));
        for (let i = 0; i < n; i++) if (!s.parts.has(i)) return fail(res, 409, 'incomplete', 'missing part ' + i);
        const all = Buffer.concat([...Array(n).keys()].map((i) => s.parts.get(i)));
        if (all.length !== s.size) return fail(res, 409, 'UploadIncomplete', 'Wrong file size');
        let offset = 0;
        for (let i = 0; i < n; i++) { if (s.offsets.get(i) !== offset) return fail(res, 400, 'INVALID_PART', 'Non-contiguous chunks'); offset += s.parts.get(i).length; }
        const fingerprint = sha(Buffer.from([...Array(n).keys()].map((i) => sha(s.parts.get(i))).join('\n')));
        if (fingerprint !== s.fingerprint) return fail(res, 409, 'UploadChecksumMismatch', 'Wrong fingerprint');
        const plan = [...Array(n).keys()].map((idx) => ({ idx, offset: s.offsets.get(idx), size: s.parts.get(idx).length, checksum: sha(s.parts.get(idx)) }));
        if (s.baseId) { const f = st.files.get(s.baseId); replaceFile(f, all, st.legacyFeatures ? plan : null, s.chunker); s.done = f.id; return ok(res, { id: f.id, revision: true, committed: true, file: fileJson(f), checksum: fingerprint }); }
        const r = createFile(s.name, s.path, all, s.lastModified);
        if (r.err) return fail(res, ...r.err);
        if (st.legacyFeatures) { r.f.chunks = plan; r.f.chunker = s.chunker || null; storePlan(r.f); }
        s.done = r.f.id; return ok(res, { file: fileJson(r.f), checksum: fingerprint });
      }
      if ((mm = /^\/uploads\/([^/]+)$/.exec(route)) && m === 'GET') {
        const s = st.uploads.get(mm[1]); if (!s) return fail(res, 404, 'not_found', 'no session');
        return ok(res, { id: s.id, status: s.done ? (s.baseId ? 'committed' : 'ready') : s.parts.size ? 'uploading' : 'pending', fileId: s.done || undefined, clientKey: s.clientKey,
          chunkSize: st.chunk, totalChunks: s.chunker ? s.totalChunks : Math.max(1, Math.ceil(s.size / st.chunk)), received: [...s.parts.keys()], uncertain: [], inProgress: [] });
      }
      return fail(res, 404, 'not_found', 'unknown route ' + route);
    } catch (e) { return fail(res, 500, 'server_error', String(e)); }
  });

  st.start = () => new Promise((r) => server.listen(0, '127.0.0.1', () => { st.url = 'http://127.0.0.1:' + server.address().port; r(st); }));
  st.stop = () => new Promise((r) => { server.closeAllConnections && server.closeAllConnections(); server.close(r); });
  return st;
}

module.exports = { createMockServer, sha };
