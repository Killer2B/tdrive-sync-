'use strict';
// Uses only generated data in a uniquely-owned hidden remote diagnostic folder; never writes the real vault.
const fs = require('fs');
const path = require('path');
const { TdriveClient } = require('../src/TdriveClient');
const { SyncEngine } = require('../src/SyncEngine');
const { ManifestStore } = require('../src/ManifestStore');
const { MemFS } = require('../test/helpers');
const { randomUuid, folderPathFor, redact } = require('../src/util');
const { uploadFile, uploadResumable, uploadRevision } = require('../src/UploadManager');
const { cdcPlan } = require('../src/Chunker');
const root = path.join(__dirname, '..');
const config = JSON.parse(fs.readFileSync(path.join(root, 'data.json'), 'utf8'));
const vaultName = path.basename(path.resolve(root, '..', '..', '..'));
const parent = folderPathFor(config.remoteFolder || '/Obsidian/' + vaultName + '/', '');
const cleanupIndex = process.argv.indexOf('--cleanup');
const cleanupName = cleanupIndex >= 0 ? process.argv[cleanupIndex + 1] : null;
if (cleanupIndex >= 0 && !/^\.tdrive-diagnostic-[0-9a-f-]{36}$/.test(cleanupName || '')) throw new Error('Cleanup is restricted to a generated diagnostic folder name');
const name = cleanupName || '.tdrive-diagnostic-' + randomUuid();
const diagnosticRoot = folderPathFor(parent, name);
const sessions = new Set(); const report = []; let owned = false;
const client = new TdriveClient({
  baseUrl: config.baseUrl, apiKey: config.apiKey, maxRetries: 1, maxServerRetries: 1,
  transport: async (request) => {
    let sent = request.body;
    if (ArrayBuffer.isView(sent)) sent = new Uint8Array(sent.buffer, sent.byteOffset, sent.byteLength).slice().buffer;
    const res = await fetch(request.url, { method: request.method, headers: request.headers, body: sent, redirect: 'error', signal: AbortSignal.timeout(45000) });
    const body = await res.arrayBuffer();
    const route = new URL(request.url).pathname.replace(/^\/api\/v1/, '');
    if (route === '/uploads' && request.method === 'POST' && res.status < 300) {
      const json = JSON.parse(new TextDecoder().decode(body)); const data = json.data || {};
      if (data.id) sessions.add(data.id);
      report.push({ check: 'upload-session-shape', chunkSize: data.chunkSize, totalChunks: data.totalChunks, revision: data.revision, chunker: data.chunker, baseChunkSize: data.base && data.base.chunkSize, receivedCount: Array.isArray(data.received) ? data.received.length : null });
    }
    if (res.status >= 400) report.push({ check: 'http-error', method: request.method, route: route.replace(/\/[^/]+(?=\/(parts|complete|content|chunks))/, '/:id'), status: res.status, requestId: res.headers.get('x-request-id') });
    if (request.method === 'PUT' && /\/parts\/\d+$/.test(route)) report.push({ check: 'part-wire-contract', bytes: sent && sent.byteLength, declared: Number(request.headers['X-Chunk-Size']), offset: Number(request.headers['X-Chunk-Offset']), status: res.status, arrayBuffer: sent instanceof ArrayBuffer });
    return { status: res.status, headers: Object.fromEntries(res.headers), body };
  },
});
const settings = { remoteFolder: diagnosticRoot, mode: 'two-way', deletionPolicy: 'trash', maxDeletesPerRun: 10, remoteLock: false, cdcFiles: true, dedupeUploads: false, compressTextFiles: false, incrementalScan: false, excludePatterns: [], maxConcurrentParts: 2, downloadChunkBytes: 4096 };
function device() {
  const local = new MemFS(); const manifest = new ManifestStore({ read: async () => null, write: async () => {} });
  const engine = new SyncEngine({ client, local, manifest, getSettings: () => settings });
  return { local, engine, manifest };
}
async function run(label, d) {
  const result = await d.engine.syncNow();
  if (result.status !== 'ok' || result.errors) process.exitCode = 1;
  report.push({ check: label, status: result.status, uploaded: result.uploaded, downloaded: result.downloaded, skipped: result.skipped, conflicts: result.conflicts, errors: result.errors, failures: result.failures.map((f) => ({ path: f.path, error: redact(f.error, config.apiKey) })) });
  return result;
}
(async () => {
  try {
    if (cleanupName) {
      const listed = await client.listFolder(diagnosticRoot);
      if (listed.folders.length || listed.files.some((f) => !['note.md', 'keep.md', 'generated.bin', 'cdc-new.bin', 'cdc-base.md'].includes(f.name))) throw new Error('Unexpected data in diagnostic folder; automatic cleanup refused');
      owned = true;
      for (const file of listed.files) {
        const meta = await client.getFile(file.id);
        report.push({ check: 'diagnostic-file-status', name: file.name, status: meta.raw.status, metadataKeys: Object.keys(meta.raw).sort() });
        try {
          const session = await client.request({ path: '/uploads/' + encodeURIComponent(file.id) });
          report.push({ check: 'diagnostic-upload-status', name: file.name, status: session.status, received: session.received, keys: Object.keys(session).sort() });
        } catch (e) { report.push({ check: 'diagnostic-upload-status', name: file.name, code: e.code }); }
        await client.trash(file.id);
      }
      return;
    }
    await client.createFolder(name, parent); owned = true;
    if (process.argv.includes('--cdc-probe')) {
      const chunker = { algo: 'buzhash', min: 1024, avg: 2048, max: 4096, window: 48 };
      let data; let plan; let seed;
      for (seed = 1; seed <= 512; seed++) {
        let x = seed; let text = '';
        while (Buffer.byteLength(text) < 14600) { x ^= x << 13; x ^= x >>> 17; x ^= x << 5; text += 'نص تجريبي للمزامنة ' + (x >>> 0) + '\n'; }
        const all = Buffer.from(text); let end = 14502;
        while ((all[end] & 0xc0) === 0x80) end--;
        data = Buffer.concat([all.subarray(0, end), Buffer.alloc(14502 - end, 120)]);
        plan = await cdcPlan(data, chunker);
        const tail = plan.chunks[plan.chunks.length - 1];
        if (plan.chunks[0].size === 4096 && tail.size >= chunker.min && tail.size < chunker.avg && (data[4095] & 0xe0) === 0xc0) break;
      }
      if (seed > 512) throw new Error('Could not prepare generated CDC range control');
      report.push({ check: 'generated-valid-cdc-plan', size: data.length, sizes: plan.chunks.map((c) => c.size), chunker, firstChunkSplitsUtf8: (data[4095] & 0xe0) === 0xc0 });
      const uploadOwner = randomUuid();
      try {
        const f = await uploadResumable(client, { name: 'cdc-new.bin', folderPath: diagnosticRoot, data, mtime: 1, chunker, dedupe: false, concurrency: 2, uploadOwner });
        report.push({ check: 'new-cdc-range-control', ok: true, size: f.size });
      } catch (e) { process.exitCode = 1; report.push({ check: 'new-cdc-range-control', ok: false, error: redact(e.message, config.apiKey) }); }
      const baseBytes = Buffer.from(('نص تجريبي قديم\n').repeat(1500)); let end = 14150;
      while ((baseBytes[end] & 0xc0) === 0x80) end--;
      const baseData = Buffer.concat([baseBytes.subarray(0, end), Buffer.alloc(14150 - end, 120)]);
      const base = await uploadFile(client, { name: 'cdc-base.md', folderPath: diagnosticRoot, data: baseData, mtime: 1, dedupe: false });
      const baseMap = await client.request({ path: '/files/' + encodeURIComponent(base.id) + '/chunks' });
      report.push({ check: 'revision-base-shape', size: baseMap.size, chunkSize: baseMap.chunkSize, chunker: baseMap.chunker, sizes: baseMap.chunks.map((c) => c.size) });
      try {
        const f = await uploadRevision(client, { id: base.id, name: 'cdc-base.md', folderPath: diagnosticRoot, data, mtime: 2, chunker, concurrency: 2, uploadOwner });
        report.push({ check: 'revision-cdc-range-control', ok: true, size: f.size });
      } catch (e) { process.exitCode = 1; report.push({ check: 'revision-cdc-range-control', ok: false, error: redact(e.message, config.apiKey) }); }
      return;
    }
    const a = device(); const b = device();
    const first = Array.from({ length: 1000 }, (_, i) => 'Diagnostic generated line ' + i).join('\n');
    a.local.put('note.md', first); a.local.put('keep.md', 'Generated diagnostic sentinel');
    await run('initial-upload', a); await run('initial-download-second-device', b);
    const firstMatches = b.local.text('note.md') === first;
    report.push({ check: 'first-content-match', matches: firstMatches });
    if (!firstMatches) process.exitCode = 1;
    if (!process.argv.includes('--basic')) {
      a.local.put('note.md', first.replace('line 500', 'edited line 500'));
      await run('revision-upload', a); await run('revision-download-second-device', b);
      const revisionMatches = b.local.text('note.md') === a.local.text('note.md');
      report.push({ check: 'revision-content-match', matches: revisionMatches });
      if (!revisionMatches) process.exitCode = 1;
    }
    await run('unchanged-sync-first-device', a); await run('unchanged-sync-second-device', b);
    // A forced multi-chunk first upload exposes real CDC session negotiation (generated bytes, no user content).
    if (process.argv.includes('--large')) {
      const data = Buffer.alloc(6 * 1024 * 1024); for (let i = 0; i < data.length; i++) data[i] = (i * 31 + 7) & 255;
      a.local.put('generated.bin', data); await run('large-cdc-upload', a);
    }
  } catch (e) { report.push({ check: 'exception', code: e.code, error: redact(e.message, config.apiKey) }); process.exitCode = 1; }
  finally {
    // We only own sessions opened by this process, never arbitrary pending uploads on the account.
    for (const id of sessions) {
      try {
        const session = await client.request({ path: '/uploads/' + encodeURIComponent(id) });
        report.push({ check: 'session-after-complete', id, status: session && session.status, revision: session && session.revision, keys: session ? Object.keys(session).sort() : [] });
        if (session && ['pending', 'uploading'].includes(session.status)) await client.request({ method: 'DELETE', path: '/uploads/' + encodeURIComponent(id) });
      } catch (e) { report.push({ check: 'diagnostic-session-cleanup', code: e.code, error: redact(e.message, config.apiKey) }); }
    }
    if (owned) {
      try { await client.deleteFolder(diagnosticRoot); report.push({ check: 'cleanup-owned-diagnostic-folder', ok: true }); }
      catch (e) {
        process.exitCode = 1;
        report.push({ check: 'cleanup-owned-diagnostic-folder', ok: false, error: redact(e.message, config.apiKey) });
        // This destructive abort is deliberately restricted to this process's generated test sessions.
        // It must NEVER be used as production cleanup for completed user revisions.
        for (const id of sessions) {
          try { await client.request({ method: 'DELETE', path: '/uploads/' + encodeURIComponent(id) }); report.push({ check: 'abort-owned-generated-session', id, ok: true }); }
          catch (err) { report.push({ check: 'abort-owned-generated-session', id, ok: false, code: err.code }); }
        }
        if (sessions.size) {
          try { await client.deleteFolder(diagnosticRoot); report.push({ check: 'cleanup-after-owned-session-abort', ok: true }); }
          catch (err) { report.push({ check: 'cleanup-after-owned-session-abort', ok: false, error: redact(err.message, config.apiKey) }); }
        }
      }
    }
    console.log(JSON.stringify({ checkedAt: new Date().toISOString(), generatedDataOnly: true, diagnosticRoot, diagnosticSessions: [...sessions], report }, null, 2));
  }
})();
