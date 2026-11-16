'use strict';
// Read-only content comparison. Only hashes/booleans are reported; no note body or credential is printed.
const fs = require('fs');
const path = require('path');
const { TdriveClient } = require('../src/TdriveClient');
const { downloadFile } = require('../src/DownloadManager');
const { Compressor } = require('../src/Compressor');
const { CryptoManager } = require('../src/CryptoManager');
const { sha256Hex, folderPathFor, splitRel, makeExcluder, redact, nfc } = require('../src/util');
const pluginRoot = path.join(__dirname, '..');
const vaultRoot = path.resolve(pluginRoot, '..', '..', '..');
const config = JSON.parse(fs.readFileSync(path.join(pluginRoot, 'data.json'), 'utf8'));
const state = JSON.parse(fs.readFileSync(path.join(pluginRoot, 'sync-state.json'), 'utf8'));
const remoteRoot = config.remoteFolder || '/Obsidian/' + path.basename(vaultRoot) + '/';
const client = new TdriveClient({ baseUrl: config.baseUrl, apiKey: config.apiKey, maxRetries: 1, maxServerRetries: 1,
  transport: async (r) => { if (r.method !== 'GET') throw new Error('Integrity checks must be read-only'); const res = await fetch(r.url, { method: r.method, headers: r.headers, redirect: 'error', signal: AbortSignal.timeout(30000) }); return { status: res.status, headers: Object.fromEntries(res.headers), body: await res.arrayBuffer() }; },
});
const excluded = makeExcluder(config.excludePatterns);
function diskFiles(dir, prefix = '') {
  const files = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const rel = prefix + entry.name;
    if (excluded(rel) || entry.isSymbolicLink()) continue;
    if (entry.isDirectory()) files.push(...diskFiles(path.join(dir, entry.name), rel + '/'));
    else if (entry.isFile()) files.push(rel);
  }
  return files;
}
(async () => {
  const disk = diskFiles(vaultRoot); const ledger = Object.entries(state.files || {}); const checks = [];
  let remaining = 20 * 1024 * 1024;
  for (const [rel, entry] of ledger) {
    if (rel.split(/[\/\\]/).some((s) => !s || s === '.' || s === '..') || excluded(rel)) continue;
    const localPath = path.join(vaultRoot, ...rel.split('/'));
    const item = { path: rel, localPresent: fs.existsSync(localPath) };
    try {
      if (!item.localPresent) { checks.push(item); continue; }
      const localBytes = fs.readFileSync(localPath); const localHash = await sha256Hex(localBytes);
      item.localMatchesLedger = localHash === entry.localHash;
      const remote = await client.getFile(entry.remoteId); const { dir, name } = splitRel(rel);
      item.remoteIdentityMatches = remote.name === nfc(name) && remote.path === folderPathFor(remoteRoot, dir);
      item.remoteMetadataMatchesLedger = remote.size === entry.remoteSize && remote.mtime === entry.remoteMtime;
      if (!item.remoteIdentityMatches || remote.size > remaining || remote.size > 8 * 1024 * 1024) { item.contentComparisonSkipped = true; checks.push(item); continue; }
      remaining -= remote.size;
      const crypto = config.e2eeEnabled ? new CryptoManager(config.e2eePassphrase, config.e2eeSalt) : null;
      const bytes = await downloadFile(client, remote, { concurrency: 1, crypto, compressor: new Compressor() });
      item.remoteContentMatchesLocal = await sha256Hex(bytes) === localHash;
    } catch (e) { item.error = redact(e.message, config.apiKey); }
    checks.push(item);
  }
  const anomalies = checks.filter((i) => i.error || i.localPresent === false || i.localMatchesLedger === false || i.remoteIdentityMatches === false || i.remoteMetadataMatchesLedger === false || i.remoteContentMatchesLocal === false || i.contentComparisonSkipped);
  if (anomalies.some((i) => !i.contentComparisonSkipped)) process.exitCode = 1;
  console.log(JSON.stringify({ checkedAt: new Date().toISOString(), readOnly: true, diskFileCount: disk.length, ledgerFileCount: ledger.length, diskFilesMissingFromLedger: disk.filter((p) => !Object.hasOwn(state.files || {}, nfc(p))).length, compared: checks.length, matchingContent: checks.filter((i) => i.remoteContentMatchesLocal).length, anomalies }, null, 2));
})().catch((e) => { console.error('Read-only integrity check failed:', e.name); process.exitCode = 1; });
