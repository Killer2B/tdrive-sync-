'use strict';
// Real HTTP with two in-memory vaults. Only generated data in a unique remote folder is modified.
const fs = require('fs');
const path = require('path');
const { TdriveClient } = require('../src/TdriveClient');
const { SyncEngine } = require('../src/SyncEngine');
const { ManifestStore } = require('../src/ManifestStore');
const { MemFS } = require('../test/helpers');
const { randomUuid } = require('../src/util');
const cfg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'data.json'), 'utf8'));
const name = '.tdrive-sync-repaired-' + randomUuid(); const root = '/' + name + '/';
const report = []; const requests = [];
const settings = { remoteFolder: root, mode: 'two-way', deletionPolicy: 'trash', maxDeletesPerRun: 20, remoteLock: true, remoteLockMinutes: 1, cdcFiles: true, dedupeUploads: false, excludePatterns: [], incrementalScan: false, maxConcurrentParts: 2 };
function client() {
  return new TdriveClient({ baseUrl: cfg.baseUrl, apiKey: cfg.apiKey, maxRetries: 1, maxServerRetries: 2,
    transport: async (r) => {
      let body = r.body;
      if (ArrayBuffer.isView(body)) body = new Uint8Array(body.buffer, body.byteOffset, body.byteLength).slice().buffer;
      const res = await fetch(r.url, { method: r.method, headers: r.headers, body, redirect: 'error', signal: AbortSignal.timeout(45000) });
      requests.push({ method: r.method, route: new URL(r.url).pathname.replace(/[0-9a-f]{8}-[0-9a-f-]{27,}/g, ':id'), status: res.status, requestId: res.headers.get('x-request-id'), leaseHeader: !!r.headers['X-Sync-Lease'] });
      return { status: res.status, headers: Object.fromEntries(res.headers), body: await res.arrayBuffer() };
    },
  });
}
function device() {
  const local = new MemFS(); const manifest = new ManifestStore({ read: async () => null, write: async () => {} });
  const engine = new SyncEngine({ client: client(), local, manifest, getSettings: () => settings });
  return { local, manifest, engine };
}
const a = device(); const b = device();
async function run(label, device) {
  const rep = await device.engine.syncNow();
  report.push({ check: label, status: rep.status, uploaded: rep.uploaded, downloaded: rep.downloaded, moved: rep.moved, errors: rep.errors, failures: rep.failures });
  if (rep.status !== 'ok' || rep.errors) process.exitCode = 1;
  return rep;
}
function verify(label, ok) { report.push({ check: label, ok }); if (!ok) process.exitCode = 1; }
(async () => {
  try {
    a.local.put('folder/note.md', 'Generated synchronization base'); a.local.put('keep.md', 'Generated sentinel');
    await run('first-device initial upload', a); await run('second-device initial pull', b);
    verify('initial exact content', b.local.text('folder/note.md') === a.local.text('folder/note.md'));
    const beforeId = a.manifest.getFile('folder/note.md') && a.manifest.getFile('folder/note.md').remoteId;
    a.local.put('folder/note.md', 'Generated synchronization edit');
    await run('expected-generation content update', a); await run('second-device updated pull', b);
    verify('updated exact content', b.local.text('folder/note.md') === a.local.text('folder/note.md'));
    const content = a.local.text('folder/note.md'); a.local.removeDir('folder'); a.local.put('moved/note.md', content);
    a.manifest.data.meta.moveHints = { folder: { to: 'moved' } };
    await run('folder move after committed revision', a); await run('second-device folder convergence', b);
    verify('file id preserved', a.manifest.getFile('moved/note.md') && a.manifest.getFile('moved/note.md').remoteId === beforeId);
    verify('old path absent and new exact content', b.local.text('folder/note.md') === undefined && b.local.text('moved/note.md') === content);
    const at = requests.length; await run('unchanged first device', a); await run('unchanged second device', b);
    verify('no file or folder mutations on unchanged runs', !requests.slice(at).some((r) => !['GET', 'HEAD'].includes(r.method) && !r.route.includes('/sync/leases')));
    verify('all protected writes carry lease headers', requests.filter((r) => !['GET', 'HEAD'].includes(r.method) && !r.route.includes('/sync/leases') && !r.route.includes('/chunks/lookup') && !r.route.endsWith('/delta')).every((r) => r.leaseHeader || (r.route.endsWith('/folders') && r.method === 'POST')));
  } catch (e) { report.push({ check: 'exception', error: a.engine.client.redact(e.message), code: e.code }); process.exitCode = 1; }
  finally {
    const cleanup = client(); let lease = null;
    try {
      lease = await cleanup.acquireLease({ path: root, owner: randomUuid(), ttlSeconds: 60 }); cleanup.setLease(lease);
      const dirs = []; const queue = [root];
      while (queue.length) {
        const dir = queue.shift(); const list = await cleanup.listFolder(dir);
        for (const f of list.files) { if (!['note.md', 'keep.md'].includes(f.name)) throw new Error('Unknown data in generated folder; cleanup refused'); await cleanup.trash(f.id); }
        for (const d of list.folders) { const p = dir + d.name + '/'; queue.push(p); dirs.push(p); }
      }
      for (const dir of dirs.reverse()) await cleanup.deleteFolder(dir);
      await cleanup.deleteFolder(root); report.push({ check: 'generated folder cleaned', ok: true });
    } catch (e) { report.push({ check: 'generated folder cleanup', ok: false, error: cleanup.redact(e.message), code: e.code }); }
    finally { if (lease) { try { await cleanup.releaseLease({ path: root, token: lease.token }); } catch (_) { /* server expiry remains the safety boundary */ } cleanup.clearLease(); } }
    console.log(a.engine.client.redact(JSON.stringify({ checkedAt: new Date().toISOString(), generatedDataOnly: true, diagnosticRoot: root, report, requests }, null, 2)));
  }
})();
