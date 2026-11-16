'use strict';
// Exercise the repaired contract using only uniquely-owned generated remote data. Secrets stay in memory.
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const cfg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'data.json'), 'utf8'));
const origin = new URL(cfg.baseUrl || 'https://tdriver.x2o.workers.dev').origin;
if (!origin.startsWith('https://')) throw new Error('HTTPS is required');
const name = '.tdrive-api-repairs-' + crypto.randomUUID();
const root = '/' + name + '/';
const owner = crypto.randomUUID();
const secrets = new Set([cfg.apiKey]);
const report = []; const pending = new Set(); const files = new Set();
let lease = null; let deadline = 0; let created = false;
const hash = (b) => crypto.createHash('sha256').update(b).digest('hex');
const clean = (s) => { let text = String(s); for (const secret of secrets) if (secret) text = text.split(secret).join('[redacted]'); return text; };
async function call(method, route, options = {}) {
  if (lease && !route.startsWith('/sync/leases') && !['GET', 'HEAD'].includes(method) && Date.now() > deadline - 20000) await renew();
  const url = new URL('/api/v1' + route, origin);
  for (const [k, v] of Object.entries(options.query || {})) url.searchParams.set(k, String(v));
  const headers = { Authorization: 'Bearer ' + cfg.apiKey, ...(options.headers || {}) };
  if (lease && options.guarded !== false && !route.startsWith('/sync/leases') && !['GET', 'HEAD'].includes(method)) {
    headers['X-Sync-Path'] = root; headers['X-Sync-Lease'] = lease.token;
  }
  const body = options.json === undefined ? options.body : JSON.stringify(options.json);
  if (options.json !== undefined) headers['Content-Type'] = 'application/json';
  const r = await fetch(url, { method, headers, body, redirect: 'error', signal: AbortSignal.timeout(45000) });
  const bytes = Buffer.from(await r.arrayBuffer()); let json = null;
  try { json = JSON.parse(bytes.toString('utf8')); } catch (_) { /* binary content */ }
  if (json && json.data && json.data.token) secrets.add(json.data.token);
  const result = { status: r.status, data: json && json.data, code: json && json.error && json.error.code, requestId: r.headers.get('x-request-id'), bytes };
  report.push({ operation: method + ' ' + route.replace(/[0-9a-f]{8}-[0-9a-f-]{27,}/g, ':id'), status: result.status, code: result.code, requestId: result.requestId });
  return result;
}
function expect(label, result, statuses) {
  const ok = statuses.includes(result.status);
  report.push({ check: label, ok, status: result.status, code: result.code });
  if (!ok) process.exitCode = 1;
  return ok;
}
function rememberLease(data) {
  if (!data || !data.token || !Number.isFinite(data.expiresAt) || !Number.isFinite(data.serverTime)) throw new Error('Invalid live lease response');
  lease = data; secrets.add(data.token); deadline = Date.now() + Math.max(0, data.expiresAt - data.serverTime - 1500);
}
async function renew() {
  const r = await call('PATCH', '/sync/leases', { json: { path: root, token: lease.token, ttlSeconds: 60 }, guarded: false });
  if (!expect('renew owned lease', r, [200])) throw new Error('Lease renewal failed');
  report.push({ check: 'renew response shape', keys: Object.keys(r.data || {}), ttlSeconds: r.data && r.data.ttlSeconds });
  rememberLease({ ...lease, ...r.data });
}
(async () => {
  try {
    const acquired = await call('POST', '/sync/leases', { json: { path: root, owner, ttlSeconds: 60 }, guarded: false });
    if (!expect('acquire scoped lease', acquired, [200, 201])) return;
    rememberLease(acquired.data);
    report.push({ check: 'acquire response shape', keys: Object.keys(acquired.data), ttlSeconds: acquired.data.ttlSeconds, fence: acquired.data.fence, tokenPresent: true });
    expect('second owner cannot acquire same scope', await call('POST', '/sync/leases', { json: { path: root, owner: crypto.randomUUID(), ttlSeconds: 60 }, guarded: false }), [423]);
    expect('ancestor owner cannot acquire overlapping scope', await call('POST', '/sync/leases', { json: { path: '/', owner: crypto.randomUUID(), ttlSeconds: 60 }, guarded: false }), [423]);
    await renew();
    const folder = await call('POST', '/folders', { json: { path: '/', name } });
    if (!expect('create owned folder under lease', folder, [200, 201])) return;
    created = true;
    expect('tokenless write cannot overlap lease', await call('POST', '/files', { query: { path: root, name: 'tokenless.txt' }, body: Buffer.from('generated'), guarded: false }), [423]);
    const child = root + 'child/';
    expect('create scoped child', await call('POST', '/folders', { json: { path: root, name: 'child' } }), [200, 201]);
    const initial = await call('POST', '/files', { query: { path: root, name: 'note.md' }, headers: { 'Content-Type': 'application/octet-stream', 'Idempotency-Key': crypto.randomUUID() }, body: Buffer.from('BASE') });
    if (!expect('create generated file', initial, [200, 201])) return;
    const id = initial.data.file.id; files.add(id);
    expect('combined metadata move and rename', await call('PATCH', '/files/' + id, { json: { name: 'renamed.md', path: child, favorite: true } }), [200]);
    const meta = await call('GET', '/files/' + id);
    const f = meta.data.file; const generation = f.contentVersion ?? f.content_version;
    report.push({ check: 'metadata identity and generation', ok: f.name === 'renamed.md' && f.path === child && generation === 1, contentVersion: generation });
    if (generation !== 1) process.exitCode = 1;
    const patch = await call('PATCH', '/files/' + id, { json: { expectedVersion: generation, content: { inline: Buffer.from('NEXT').toString('base64') } } });
    if (!expect('conditional inline content update', patch, [200])) return;
    report.push({ check: 'inline exact receipt shape', keys: Object.keys(patch.data || {}), contentVersion: patch.data.contentVersion, archivedVersion: patch.data.version });
    expect('stale generation cannot overwrite', await call('PATCH', '/files/' + id, { json: { expectedVersion: generation, content: { inline: Buffer.from('BAD!').toString('base64') } } }), [409, 412]);
    const nextMeta = await call('GET', '/files/' + id); const current = nextMeta.data.file.contentVersion ?? nextMeta.data.file.content_version;
    const content = await call('GET', '/files/' + id + '/content');
    report.push({ check: 'newer content survived stale update', ok: content.bytes.equals(Buffer.from('NEXT')) && current === 2, contentVersion: current });
    if (!content.bytes.equals(Buffer.from('NEXT')) || current !== 2) process.exitCode = 1;
    expect('empty-only deletion preserves live file', await call('DELETE', '/folders', { query: { path: root, name: 'child', ifEmpty: 1 } }), [409]);
    const bytes = Buffer.from('GENERATED REVISION'); const fingerprint = hash(Buffer.from(hash(bytes)));
    const opened = await call('POST', '/uploads', { json: { baseId: id, mode: 'replace', expectedVersion: current, size: bytes.length, fingerprint, clientKey: crypto.randomUUID(), lastModified: 1 } });
    if (!expect('prepare conditional revision', opened, [200, 201])) return;
    const sid = opened.data.id; pending.add(sid);
    const listing = await call('GET', '/uploads', { query: { path: child, active: 1, limit: 100, offset: 0 } });
    report.push({ check: 'active session enumeration', ok: !!(listing.data && listing.data.sessions.some((s) => s.id === sid && s.active && s.cancellable)), retentionSeconds: listing.data && listing.data.retentionSeconds });
    expect('upload generated revision bytes', await call('PUT', '/uploads/' + sid + '/parts/0', { headers: { 'Content-Type': 'application/octet-stream', 'X-Content-SHA256': hash(bytes), 'X-Chunk-Offset': '0', 'X-Chunk-Size': String(bytes.length) }, body: bytes }), [200, 201]);
    const complete = await call('POST', '/uploads/' + sid + '/complete', { json: { expectedVersion: current } });
    if (!expect('commit conditional revision', complete, [200, 201])) return;
    pending.delete(sid);
    report.push({ check: 'revision exact receipt', contentVersion: complete.data.contentVersion, archivedVersion: complete.data.version, resolvesBase: complete.data.id === id });
    const terminal = await call('GET', '/uploads/' + sid);
    report.push({ check: 'revision terminal status', status: terminal.data.status, revision: terminal.data.revision });
    if (process.argv.includes('--restore-precondition')) {
      const versions = await call('GET', '/files/' + id + '/versions');
      const entries = versions.data && (versions.data.versions || versions.data.items || versions.data);
      const versionId = Array.isArray(entries) && entries[0] && (entries[0].id || entries[0].versionId);
      if (!versionId) throw new Error('No generated archived version returned');
      const staleRestore = await call('POST', '/files/' + id + '/versions/' + encodeURIComponent(versionId) + '/restore', { json: { expectedVersion: 1 } });
      report.push({ check: 'version restore expectedVersion enforcement (additional capability)', enforced: [409, 412].includes(staleRestore.status), status: staleRestore.status, code: staleRestore.code });
    }
    await call('DELETE', '/files/' + id, { query: { permanent: 0 } }); files.delete(id);
    expect('committed revision no longer blocks empty-only trash', await call('DELETE', '/folders', { query: { path: root, name: 'child', ifEmpty: 1 } }), [200, 204]);
    const old = lease;
    expect('release current lease', await call('DELETE', '/sync/leases', { json: { path: root, token: old.token }, guarded: false }), [200, 204]); lease = null;
    const successor = await call('POST', '/sync/leases', { json: { path: root, owner, ttlSeconds: 60 }, guarded: false });
    if (!expect('acquire successor lease', successor, [200, 201])) return;
    rememberLease(successor.data);
    report.push({ check: 'fence increments across release', ok: successor.data.fence > old.fence });
    expect('former token cannot publish', await call('POST', '/files', { query: { path: root, name: 'stale.txt' }, body: Buffer.from('generated'), guarded: false, headers: { 'X-Sync-Path': root, 'X-Sync-Lease': old.token } }), [423]);
  } catch (e) { report.push({ check: 'exception', error: clean(e.message) }); process.exitCode = 1; }
  finally {
    for (const sid of pending) { try { await call('DELETE', '/uploads/' + sid); } catch (e) { report.push({ check: 'owned pending cleanup', error: clean(e.message) }); } }
    for (const id of files) { try { await call('DELETE', '/files/' + id, { query: { permanent: 0 } }); } catch (e) { report.push({ check: 'owned file cleanup', error: clean(e.message) }); } }
    if (created && lease) { try { expect('cleanup owned empty root', await call('DELETE', '/folders', { query: { path: '/', name, ifEmpty: 1 } }), [200, 204]); } catch (e) { report.push({ check: 'owned folder cleanup', error: clean(e.message) }); } }
    if (lease) { try { await call('DELETE', '/sync/leases', { json: { path: root, token: lease.token }, guarded: false }); } catch (e) { report.push({ check: 'owned lease release', error: clean(e.message) }); } lease = null; }
    console.log(clean(JSON.stringify({ checkedAt: new Date().toISOString(), generatedDataOnly: true, diagnosticRoot: root, report }, null, 2)));
  }
})();
