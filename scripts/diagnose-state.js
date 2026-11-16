'use strict';
// Read-only runtime diagnostics: never print credentials, note bodies, or merge-base text.
const fs = require('fs');
const path = require('path');
const { redact } = require('../src/util');
const root = path.join(__dirname, '..');
const config = JSON.parse(fs.readFileSync(path.join(root, 'data.json'), 'utf8'));
const state = JSON.parse(fs.readFileSync(path.join(root, 'sync-state.json'), 'utf8'));
const clean = (value) => redact(redact(String(value), config.apiKey), config.e2eePassphrase);
const files = Object.entries(state.files || {});
const log = Array.isArray(state.log) ? state.log : [];
const meta = state.meta || {};
const failures = log.filter((entry) => entry.level === 'error' || /failed|aborted|mismatch|not visible|no such|invalid|deferred|blocked|conflict|locked|another device|reset/i.test(entry.msg || ''));
const settings = {};
for (const key of ['baseUrl', 'remoteFolder', 'mode', 'deletionPolicy', 'maxDeletesPerRun', 'autoSyncMinutes', 'syncOnStartup', 'syncOnChange', 'instantSync', 'paused', 'e2eeEnabled', 'compressTextFiles', 'cdcFiles', 'dedupeUploads', 'maxConcurrentParts', 'uploadChunkBytes', 'downloadChunkBytes', 'remoteLock', 'remoteLockMinutes', 'incrementalScan', 'fullScanMinutes', 'streamLargeDownloads', 'syncFolders', 'excludePatterns']) if (key in config) settings[key] = config[key];
const ids = new Map();
for (const [name, entry] of files) if (entry && entry.remoteId) ids.set(entry.remoteId, [...(ids.get(entry.remoteId) || []), name]);
const summary = {
  checkedAt: new Date().toISOString(), settings, hasApiKey: !!config.apiKey,
  effectiveRemoteFolder: config.remoteFolder || '/Obsidian/' + path.basename(path.resolve(root, '..', '..', '..')) + '/',
  files: files.length, folders: Object.keys(state.folders || {}).length, conflicts: Object.keys(state.conflicts || {}).length,
  lastRun: meta.lastRun, lastSuccessAt: meta.lastSuccessAt ? new Date(meta.lastSuccessAt).toISOString() : null,
  scoped: !!meta.syncScope, previousScopeFiles: Object.keys(meta.previousScopeLedger && meta.previousScopeLedger.files || {}).length,
  blockedDeletes: meta.blockedDeletes ? { reason: meta.blockedDeletes.reason, count: (meta.blockedDeletes.items || []).length } : null,
  apiContract: meta.apiContract || null,
  runtimeVersion: meta.runtimeVersion || null,
  blockedFolders: Object.entries(meta.blockedFolders || {}).map(([name, b]) => ({ path: clean(name), code: b.code, message: clean(b.message || b.error || ''), scopeMatches: b.scope === meta.syncScope, attempts: b.attempts, at: b.at, lastAttemptAt: b.lastAttemptAt, nextRetryAt: b.nextRetryAt, serverRetryAt: b.serverRetryAt, requestId: b.requestId })),
  cache: meta.remoteSnapshot ? { at: meta.remoteSnapshot.at, activityAt: meta.remoteSnapshot.activityAt, files: Object.keys(meta.remoteSnapshot.files || {}).length } : null,
  sizeDisagreements: files.filter(([, e]) => e && e.localSize !== e.remoteSize).length,
  sameRemoteIdPaths: [...ids.values()].filter((names) => names.length > 1).map((names) => names.map(clean)),
  failureCount: failures.length,
  recentFailures: failures.slice(-45).map((e) => ({ at: new Date(e.t).toISOString(), level: e.level, message: clean(e.msg) })),
  recentLog: log.slice(-12).map((e) => ({ at: new Date(e.t).toISOString(), level: e.level, message: clean(e.msg) })),
};
(async () => {
if (process.argv.includes('--deletions')) {
  const { TdriveClient } = require('../src/TdriveClient');
  const { folderPathFor } = require('../src/util');
  const client = new TdriveClient({ baseUrl: config.baseUrl, apiKey: config.apiKey, maxRetries: 1, maxServerRetries: 1,
    transport: async (r) => { if (r.method !== 'GET') throw new Error('Read-only deletion diagnostics'); const res = await fetch(r.url, { headers: r.headers, redirect: 'error', signal: AbortSignal.timeout(30000) }); return { status: res.status, headers: Object.fromEntries(res.headers), body: await res.arrayBuffer() }; },
  });
  const owned = new Set(Object.values(meta.uploadSessions || {}).map((r) => r.sessionId).filter(Boolean));
  const checks = [];
  for (const b of summary.blockedFolders) {
    const full = folderPathFor(summary.effectiveRemoteFolder, b.path); const item = { ...b };
    try {
      const list = await client.listFolder(full); item.liveFiles = list.files.length; item.liveFolders = list.folders.length;
      const sessions = await client.listUploads({ path: full, active: true, limit: 100 });
      item.activeSessions = sessions.map((s) => ({ id: s.uploadId || s.id, status: s.status, active: s.active, cancellable: s.cancellable, createdAt: s.createdAt || s.created_at, expiresAt: s.expiresAt, revision: s.revision, knownOwned: owned.has(s.uploadId || s.id) }));
    } catch (e) { item.serverError = client.redact(e.message); }
    checks.push(item);
  }
  console.log(JSON.stringify({ checkedAt: summary.checkedAt, apiContract: summary.apiContract, runtimeVersion: summary.runtimeVersion, lastRun: summary.lastRun, checks }, null, 2)); return;
}
if (process.argv.includes('--chunks') || process.argv.includes('--server')) {
  const chunkMaps = files.filter(([, entry]) => entry && entry.chunkMap).map(([name, entry]) => {
    const map = entry.chunkMap; const sizes = map.sizes || []; const chunker = map.chunker;
    return { path: clean(name), localSize: entry.localSize, remoteSize: entry.remoteSize, chunker, sizes, invalidNonFinalIndices: chunker && Number.isFinite(chunker.min) ? sizes.map((size, i) => i < sizes.length - 1 && size < chunker.min ? i : -1).filter((i) => i >= 0) : [] };
  });
  const uploads = [];
  for (const record of Object.values(meta.uploadSessions || {})) {
    const item = { phase: record.phase, sessionId: record.sessionId, ownerMatches: record.owner === meta.deviceId, generation: record.generation, size: record.size, name: clean(record.body && record.body.name || ''), chunker: record.body && record.body.chunker, totalChunks: record.body && record.body.totalChunks };
    const entry = files.find(([, file]) => file && record.body && file.remoteId === record.body.baseId);
    if (entry && !config.e2eeEnabled) {
      const { sha256Hex } = require('../src/util');
      const { cdcPlan } = require('../src/Chunker');
      const { Compressor } = require('../src/Compressor');
      const vault = path.resolve(root, '..', '..', '..'); const rel = entry[0];
      if (!rel.split(/[\/\\]/).some((s) => !s || s === '.' || s === '..')) {
        const source = fs.readFileSync(path.join(vault, ...rel.split('/')));
        const wire = config.compressTextFiles ? await new Compressor().maybeCompress(rel, source) : source;
        item.name = clean(rel); item.baseStoredSize = entry[1].remoteSize;
        item.currentSourceMatchesRecord = await sha256Hex(source) === record.sourceHash;
        if (record.body.chunker) {
          const plan = await cdcPlan(wire, record.body.chunker);
          item.currentWireSize = wire.byteLength; item.currentPartSizes = plan.chunks.map((c) => c.size);
          item.shortNonFinalIndices = plan.chunks.filter((c, i) => i !== plan.chunks.length - 1 && c.size < plan.chunker.min).map((c) => c.idx);
          item.finalChunkIsShort = !!plan.chunks.length && plan.chunks[plan.chunks.length - 1].size < plan.chunker.min;
        }
      }
    }
    if (process.argv.includes('--server')) {
      const { TdriveClient } = require('../src/TdriveClient');
      const client = new TdriveClient({ baseUrl: config.baseUrl, apiKey: config.apiKey, maxRetries: 1, maxServerRetries: 1,
        transport: async (r) => { if (r.method !== 'GET') throw new Error('Read-only session diagnostics'); const res = await fetch(r.url, { headers: r.headers, redirect: 'error', signal: AbortSignal.timeout(30000) }); return { status: res.status, headers: Object.fromEntries(res.headers), body: await res.arrayBuffer() }; },
      });
      try {
        const session = await client.request({ path: '/uploads/' + encodeURIComponent(record.sessionId) });
        item.serverSession = { status: session.status, chunkSize: session.chunkSize, totalChunks: session.totalChunks, chunker: session.chunker, received: session.received, uncertain: session.uncertain, inProgress: session.inProgress };
        const base = await client.request({ path: '/files/' + encodeURIComponent(record.body.baseId) + '/chunks' });
        item.serverBase = { size: base.size, chunkSize: base.chunkSize, totalChunks: base.totalChunks, chunker: base.chunker, partSizes: base.chunks && base.chunks.map((c) => c.size) };
      } catch (e) { item.serverError = clean(e.message); }
    }
    uploads.push(item);
  }
  console.log(JSON.stringify({ checkedAt: summary.checkedAt, chunkMaps, uploads }, null, 2));
} else console.log(JSON.stringify(summary, null, 2));
})().catch((e) => { console.error('Sanitized state diagnostics failed:', e.name); process.exitCode = 1; });
