'use strict';
// In-process fake Tdrive. It implements the documented endpoint list with *assumed* JSON shapes
// (the public docs do not give them). It tests our logic, NOT the real server.
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
    folderDelete: opts.folderDelete || 'any',   // 'any' | 'byPath' (only DELETE /folders?path=) | 'none' (405)
    oneShotMax: opts.oneShotMax || 1024 * 1024, chunk: opts.chunk || 1024 * 1024,
  };
  const tick = () => (st.clock += 1000);
  const fileJson = (f) => ({ id: f.id, name: f.name, path: f.path, size: f.bytes.length, lastModified: f.lastModified, trashed: f.trashed });
  const live = () => [...st.files.values()].filter((f) => !f.trashed);
  // Activity recorded by requests during a sync must look OLDER than the engine's snapshot stamp; activity
  // simulating another device (the helpers below) must look NEWER. The fake clock is far in the past.
  const note = (action, target, at) => { st.activity.push({ action, target, detail: '{}', created_at: new Date(at || st.clock).toISOString() }); };
  const noteExternal = (action, target) => note(action, target, Date.now() + 1000);

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
    st.files.set(f.id, f); return { f };
  }
  st.addFile = (path, name, data) => { addFolder(path); const f = createFile(name, path, Buffer.from(data)).f; noteExternal('upload', f.id); return f; };
  st.modifyFile = (id, data) => { const f = st.files.get(id); f.bytes = Buffer.from(data); f.lastModified = tick(); noteExternal('update', id); return f; };
  st.removeFile = (id) => { st.files.get(id).trashed = true; noteExternal('trash', id); };
  st.removeFile = (id) => { st.files.get(id).trashed = true; noteExternal('trash', id); };
  st.findLive = (path, name) => live().find((f) => f.path === normFolder(path) && f.name === name);
  st.liveFiles = live;
  st.failRule = (match, action, times = 1) => st.rules.push({ match, action, times });
  st.removeFolder = (p) => { p = normFolder(p); for (const f of [...st.folders]) if (f.startsWith(p)) st.folders.delete(f); for (const f of st.files.values()) if (f.path.startsWith(p)) f.trashed = true; noteExternal('move', p); };
  st.opsLog = () => st.log.filter((l) => l.method !== 'GET');
  st.activity = [];
  st.addActivity = (action, target, detail) => noteExternal(action, target);

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
      if (route === '/activity') return ok(res, { activity: st.activity, nextOffset: null });
      if (route === '/files' && m === 'GET') {
        const p = normFolder(u.searchParams.get('path'));
        if (!st.folders.has(p) && p !== '/') return fail(res, 404, 'not_found', 'no such folder');
        const depth = p.split('/').filter(Boolean).length;
        const files = live().filter((f) => f.path === p).filter(() => !(st.hideSubfolderFiles && depth > 1)).map(fileJson);
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
        note('upload', r.f.id);
        return ok(res, fileJson(r.f), 201);
      }
      let mm;
      if ((mm = /^\/files\/([^/]+)\/content$/.exec(route)) && m === 'GET') {
        const f = st.files.get(mm[1]); if (!f) return fail(res, 404, 'not_found', 'nope');
        const rg = /bytes=(\d+)-(\d*)/.exec(req.headers.range || '');
        if (!rg) return send(res, 200, f.bytes);
        const a = Number(rg[1]); const b = Math.min(rg[2] ? Number(rg[2]) : f.bytes.length - 1, f.bytes.length - 1);
        if (a >= f.bytes.length) return fail(res, 416, 'range', 'bad range');
        return send(res, 206, f.bytes.subarray(a, b + 1), { 'Content-Range': `bytes ${a}-${b}/${f.bytes.length}` });
      }
      if ((mm = /^\/files\/([^/]+)$/.exec(route))) {
        const f = st.files.get(mm[1]); if (!f) return fail(res, 404, 'not_found', 'nope');
        if (m === 'GET') return ok(res, fileJson(f));
        if (m === 'DELETE') { if (u.searchParams.get('permanent') === '1') return fail(res, 400, 'refused', 'test'); f.trashed = true; note('trash', f.id); return ok(res, { id: f.id }); }
        if (m === 'PATCH') { const j = JSON.parse(body.toString()); if (j.path) f.path = normFolder(j.path); if (j.name) f.name = j.name; note('rename', f.id); return ok(res, fileJson(f)); }
      }
      if ((mm = /^\/trash\/([^/]+)\/restore$/.exec(route)) && m === 'POST') { const f = st.files.get(mm[1]); if (f) f.trashed = false; note('restore', mm[1]); return ok(res, { id: mm[1] }); }
      if (m === 'DELETE' && (route === '/folders' || /^\/folders\/[^/]+$/.test(route))) {
        if (st.folderDelete === 'none') return fail(res, 405, 'method_not_allowed', 'no');
        let p;
        if (route === '/folders') p = u.searchParams.get('path');
        else { if (st.folderDelete === 'byPath') return fail(res, 404, 'not_found', 'unknown route'); p = decodeURIComponent(route.slice('/folders/'.length)).replace(/^d/, ''); }
        p = normFolder(p);
        if (!st.folders.has(p) || p === '/') return fail(res, 404, 'not_found', 'no such folder');
        if (u.searchParams.get('permanent') === '1') return fail(res, 400, 'refused', 'test');
        for (const f of [...st.folders]) if (f.startsWith(p)) st.folders.delete(f);       // sub-folders go too
        for (const f of st.files.values()) if (f.path.startsWith(p)) f.trashed = true;    // soft delete of contents
        note('move', p);
        return send(res, 204, Buffer.alloc(0));      // empty body on purpose: real servers often do this
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
        let s = [...st.uploads.values()].find((x) => x.clientKey === j.clientKey);
        if (!s) { s = { id: 'u' + st.nextId++, ...j, parts: new Map(), done: null }; st.uploads.set(s.id, s); }
        return ok(res, { id: s.id }, 201);
      }
      if ((mm = /^\/uploads\/([^/]+)\/parts\/(\d+)$/.exec(route)) && m === 'PUT') {
        const s = st.uploads.get(mm[1]); if (!s) return fail(res, 404, 'not_found', 'no session');
        if (sha(body) !== req.headers['x-content-sha256']) return fail(res, 400, 'checksum', 'bad checksum');
        s.parts.set(Number(mm[2]), body); return ok(res, { idx: Number(mm[2]) });
      }
      if ((mm = /^\/uploads\/([^/]+)\/complete$/.exec(route)) && m === 'POST') {
        const s = st.uploads.get(mm[1]); if (!s) return fail(res, 404, 'not_found', 'no session');
        if (s.done) return ok(res, fileJson(st.files.get(s.done)));
        const n = st.maxBody ? s.parts.size : Math.ceil(s.size / st.chunk);
        for (let i = 0; i < n; i++) if (!s.parts.has(i)) return fail(res, 409, 'incomplete', 'missing part ' + i);
        const all = Buffer.concat([...Array(n).keys()].map((i) => s.parts.get(i)));
        const r = createFile(s.name, s.path, all, s.lastModified);
        if (r.err) return fail(res, ...r.err);
        s.done = r.f.id; note('upload', r.f.id); return ok(res, fileJson(r.f));
      }
      if ((mm = /^\/uploads\/([^/]+)$/.exec(route)) && m === 'GET') {
        const s = st.uploads.get(mm[1]); if (!s) return fail(res, 404, 'not_found', 'no session');
        return ok(res, { received: [...s.parts.keys()], uncertain: [], inProgress: [] });
      }
      return fail(res, 404, 'not_found', 'unknown route ' + route);
    } catch (e) { return fail(res, 500, 'server_error', String(e)); }
  });

  st.start = () => new Promise((r) => server.listen(0, '127.0.0.1', () => { st.url = 'http://127.0.0.1:' + server.address().port; r(st); }));
  st.stop = () => new Promise((r) => { server.closeAllConnections && server.closeAllConnections(); server.close(r); });
  return st;
}

module.exports = { createMockServer, sha };
