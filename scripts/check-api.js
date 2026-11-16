'use strict';
// Read-only live diagnostics. Credentials and remote filenames never appear in output.
const fs = require('fs');
const path = require('path');
const config = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'data.json'), 'utf8'));
const base = new URL(config.baseUrl || 'https://tdriver.x2o.workers.dev');
if (base.protocol !== 'https:' || base.username || base.password) throw new Error('A credential-free HTTPS origin is required');
const vaultName = path.basename(path.resolve(__dirname, '..', '..', '..', '..'));
const pathIndex = process.argv.indexOf('--path');
const root = pathIndex >= 0 ? process.argv[pathIndex + 1] : config.remoteFolder || '/Obsidian/' + vaultName + '/';
if (typeof root !== 'string' || !root.startsWith('/') || !root.endsWith('/') || /[\\\x00-\x1f\x7f]/.test(root) || root.split('/').some((s) => s === '.' || s === '..')) throw new Error('Invalid diagnostic path');
const results = [];
const deep = process.argv.includes('--deep');
let scannedFolders = 0; let scanTruncated = false;
async function get(route, query = {}) {
  const url = new URL('/api/v1' + route, base.origin);
  for (const [k, v] of Object.entries(query)) url.searchParams.set(k, v);
  const r = await fetch(url, { headers: { Authorization: 'Bearer ' + config.apiKey }, redirect: 'error', signal: AbortSignal.timeout(30000) });
  let j;
  try { j = await r.json(); } catch (_) { j = null; }
  results.push({ endpoint: route, status: r.status, ok: j && j.ok === true, code: j && j.error && j.error.code, requestId: r.headers.get('x-request-id'), retryAfter: r.headers.get('retry-after') });
  return { r, j, data: j && j.data };
}
async function list(dir) {
  scannedFolders++;
  const files = await get('/files', { path: dir, limit: 100, offset: 0 });
  const folders = await get('/folders', { path: dir });
  const fd = files.data; const dd = folders.data;
  results.push({ check: 'listing-contract', filesArray: Array.isArray(fd && fd.files), foldersArray: Array.isArray(fd && fd.folders), namesOnly: Array.isArray(dd && dd.folders) && dd.folders.every((f) => typeof f === 'string'), foldersAgree: Array.isArray(fd && fd.folders) && Array.isArray(dd && dd.folders) && JSON.stringify([...fd.folders].sort()) === JSON.stringify([...dd.folders].sort()), fileCount: fd && fd.files && fd.files.length, folderCount: dd && dd.folders && dd.folders.length, nextOffset: fd && fd.nextOffset });
  return fd;
}
(async () => {
  const lim = await get('');
  results.push({ check: 'live-limits', upload: lim.data && lim.data.upload });
  results.push({ check: 'capabilities', indexKeys: Object.keys(lim.data || {}), capabilities: lim.data && lim.data.capabilities, sync: lim.data && lim.data.sync, links: Object.keys(lim.data && lim.data.links || {}) });
  const me = await get('/me');
  if (me.r.status === 200) {
    const first = await list(root);
    const children = (data, dir) => ((data && data.folders) || [])
      .filter((s) => typeof s === 'string' && s && !s.startsWith('.') && !/[\/\\\x00-\x1f\x7f]/.test(s))
      .map((name) => dir.replace(/\/?$/, '/') + name + '/');
    const queue = children(first, root); const seen = new Set([root]);
    if (deep) {
      while (queue.length && scannedFolders < 128) {
        const dir = queue.shift(); if (seen.has(dir)) continue; seen.add(dir);
        const data = await list(dir); queue.push(...children(data, dir));
      }
      scanTruncated = queue.length > 0;
    } else for (const dir of queue.slice(0, 3)) await list(dir);
    if (first && first.files && first.files[0] && first.files[0].id) {
      const id = first.files[0].id;
      const metadata = await get('/files/' + encodeURIComponent(id));
      const manifest = await get('/files/' + encodeURIComponent(id) + '/chunks');
      // Replace ids in endpoint labels before printing the diagnostics.
      for (const entry of results) if (entry.endpoint && entry.endpoint.includes(id)) entry.endpoint = entry.endpoint.replace(id, ':id');
      const f = metadata.data && metadata.data.file; const m = manifest.data;
      results.push({ check: 'file-contract', metadataKeys: f ? Object.keys(f).sort() : [], manifestKeys: m ? Object.keys(m).sort() : [], sizesAgree: !!f && !!m && f.size === m.size, chunkCount: m && m.chunks && m.chunks.length });
    }
  }
  // Recheck any failing read once. Do not retry authentication or mutate remote state.
  if (results.some((r) => r.status >= 500)) await get('');
  if (results.some((r) => r.status >= 400 || (r.check === 'listing-contract' && (!r.filesArray || !r.foldersArray || !r.namesOnly || !r.foldersAgree)))) process.exitCode = 1;
  console.log(JSON.stringify({ checkedAt: new Date().toISOString(), requestsAreReadOnly: true, scannedFolders, scanTruncated, results }, null, 2));
})().catch((e) => { console.error('Read-only API check failed:', e.name); process.exitCode = 1; });
