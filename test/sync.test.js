'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv, MemFS } = require('./helpers');
const { sha } = require('./mockServer');
const { decide } = require('../src/SyncEngine');
const { ManifestStore } = require('../src/ManifestStore');

const noWrites = (env) => env.server.opsLog().length === 0;

test('6 new local file is uploaded (folders created, remote bytes identical)', async () => {
  const env = await makeEnv();
  try {
    env.local.put('Notes/a.md', 'hello');
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok'); assert.equal(r.uploaded, 1);
    assert.equal(env.server.findLive('/Vault/Notes/', 'a.md').bytes.toString(), 'hello');
  } finally { await env.stop(); }
});

test('7 new remote file is downloaded', async () => {
  const env = await makeEnv();
  try {
    env.server.addFile('/Vault/', 'r.md', 'from server');
    const r = await env.engine.syncNow();
    assert.equal(r.downloaded, 1); assert.equal(env.local.text('r.md'), 'from server');
  } finally { await env.stop(); }
});

test('8 modifications on either side propagate (each direction), for all duplicate-name policies', async () => {
  for (const dup of ['allow', 'reject409', 'autorename']) {
    const env = await makeEnv({ server: { dup } });
    try {
      env.local.put('a.md', 'v1'); env.local.put('b.md', 'b1');
      await env.engine.syncNow();
      env.local.put('a.md', 'v2 local');                                   // local edit
      env.server.modifyFile(env.server.findLive('/Vault/', 'b.md').id, 'b2 remote'); // remote edit
      const r = await env.engine.syncNow();
      assert.equal(r.status, 'ok', dup); assert.equal(r.uploaded, 1, dup); assert.equal(r.downloaded, 1, dup);
      assert.equal(env.server.liveFiles().filter((f) => f.name === 'a.md').length, 1, dup + ': exactly one live a.md');
      assert.equal(env.server.findLive('/Vault/', 'a.md').bytes.toString(), 'v2 local');
      assert.equal(env.local.text('b.md'), 'b2 remote');
      env.server.log.length = 0;
      const r3 = await env.engine.syncNow();
      assert.equal(r3.uploaded + r3.downloaded + r3.conflicts, 0, dup + ': stable afterwards'); assert.ok(noWrites(env));
    } finally { await env.stop(); }
  }
});

test('9 edited on both sides -> conflict: nothing overwritten, copy kept, path frozen', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', 'base'); await env.engine.syncNow();
    env.local.put('a.md', 'LOCAL');
    const rec = env.server.findLive('/Vault/', 'a.md'); env.server.modifyFile(rec.id, 'REMOTE');
    const r = await env.engine.syncNow();
    assert.equal(r.conflicts, 1); assert.equal(r.uploaded + r.downloaded, 0);
    assert.equal(env.local.text('a.md'), 'LOCAL'); assert.equal(rec.bytes.toString(), 'REMOTE');
    const copies = [...env.local.files.keys()].filter((k) => k.includes('Tdrive conflict'));
    assert.equal(copies.length, 1); assert.equal(env.local.text(copies[0]), 'REMOTE');
    env.server.log.length = 0;
    const r2 = await env.engine.syncNow();                        // frozen: neither side overwritten
    assert.equal(env.local.text('a.md'), 'LOCAL'); assert.equal(rec.bytes.toString(), 'REMOTE');
    assert.equal(r2.downloaded, 0);
    assert.equal(r2.uploaded, 0);                                 // conflict copies are never uploaded
    assert.equal(env.server.liveFiles().filter((f) => f.name.includes('Tdrive conflict')).length, 0);
    assert.equal(env.engine.conflicts.list().length, 1);
  } finally { await env.stop(); }
});

test('9b resolve "both": local stays, remote copy kept locally, converges', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', 'base'); await env.engine.syncNow();
    env.local.put('a.md', 'LOCAL'); env.server.modifyFile(env.server.findLive('/Vault/', 'a.md').id, 'REMOTE');
    await env.engine.syncNow();
    await env.engine.conflicts.resolve('a.md', 'both', env.engine);
    assert.equal(env.server.findLive('/Vault/', 'a.md').bytes.toString(), 'LOCAL');
    const copy = [...env.local.files.keys()].find((k) => k.includes('Tdrive conflict'));
    assert.ok(copy, 'the remote version is kept as a local conflict copy');
    const r = await env.engine.syncNow();                         // copy stays LOCAL only; a.md stable
    assert.equal(r.conflicts, 0); assert.equal(r.status, 'ok');
    assert.equal(r.uploaded, 0, 'conflict copies are never uploaded');
    assert.equal(env.server.liveFiles().filter((f) => f.name.includes('Tdrive conflict')).length, 0);
    assert.ok(env.local.files.has(copy), 'local conflict copy is kept');
    const r2 = await env.engine.syncNow();
    assert.equal(r2.uploaded + r2.downloaded, 0);
  } finally { await env.stop(); }
});

test('9c resolve "remote": local version preserved as backup, then remote applied', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', 'base'); await env.engine.syncNow();
    env.local.put('a.md', 'LOCAL'); env.server.modifyFile(env.server.findLive('/Vault/', 'a.md').id, 'REMOTE');
    await env.engine.syncNow();
    await env.engine.conflicts.resolve('a.md', 'remote', env.engine);
    assert.equal(env.local.text('a.md'), 'REMOTE');
    const backup = [...env.local.files.keys()].find((k) => k.includes('local backup'));
    assert.equal(env.local.text(backup), 'LOCAL');
    const r = await env.engine.syncNow(); assert.equal(r.conflicts, 0);
  } finally { await env.stop(); }
});

test('10a local deletion is propagated to the Tdrive trash only (recoverable); not re-created', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', '1'); env.local.put('b.md', '2'); await env.engine.syncNow();
    env.local.files.delete('a.md');
    const r = await env.engine.syncNow();
    assert.equal(r.deletedRemote, 1); assert.equal(env.server.findLive('/Vault/', 'a.md'), undefined);
    assert.equal([...env.server.files.values()].filter((f) => f.trashed).length, 1);
    const r2 = await env.engine.syncNow(); assert.equal(r2.uploaded + r2.downloaded + r2.deletedRemote, 0);
  } finally { await env.stop(); }
});

test('10b remote deletion moves local file to trash; later local edit of a remotely-deleted file re-uploads', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', '1'); env.local.put('b.md', '2'); await env.engine.syncNow();
    env.server.removeFile(env.server.findLive('/Vault/', 'a.md').id);
    const r = await env.engine.syncNow();
    assert.equal(r.deletedLocal, 1); assert.equal(env.local.trashed[0].path, 'a.md');
    env.server.removeFile(env.server.findLive('/Vault/', 'b.md').id);
    env.local.put('b.md', '2 edited');
    const r2 = await env.engine.syncNow();
    assert.equal(r2.uploaded, 1); assert.equal(env.server.findLive('/Vault/', 'b.md').bytes.toString(), '2 edited');
  } finally { await env.stop(); }
});

test('10c mass deletion is blocked (limit) and empty-vault guard blocks deletions', async () => {
  const env = await makeEnv();
  try {
    for (let i = 0; i < 13; i++) env.local.put(`n${i}.md`, 'x' + i);
    await env.engine.syncNow();
    for (let i = 0; i < 12; i++) env.local.files.delete(`n${i}.md`);
    const r = await env.engine.syncNow();
    assert.equal(r.deletedRemote, 0); assert.ok(r.warnings.some((w) => /blocked/i.test(w)));
    assert.equal(env.server.liveFiles().length, 13);
    env.local.files.clear();                                     // e.g. vault failed to load
    const r2 = await env.engine.syncNow();
    assert.equal(r2.deletedRemote, 0); assert.equal(env.server.liveFiles().length, 13);
  } finally { await env.stop(); }
});

test('10d deletion mode "off" never deletes', async () => {
  const env = await makeEnv({ settings: { deletionPolicy: 'off' } });
  try {
    env.local.put('a.md', '1'); env.local.put('b.md', '2'); await env.engine.syncNow();
    env.local.files.delete('a.md'); await env.engine.syncNow();
    assert.ok(env.server.findLive('/Vault/', 'a.md'));
  } finally { await env.stop(); }
});

test('10e lost/old ledger never deletes healthy files and does not duplicate', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', '1'); env.local.put('b.md', '2'); await env.engine.syncNow();
    const dev2 = env.mkDevice({ local: env.local });              // same files, brand-new empty ledger
    env.server.log.length = 0;
    const r = await dev2.engine.syncNow();
    assert.equal(r.status, 'ok'); assert.equal(r.conflicts, 0); assert.equal(r.deletedLocal + r.deletedRemote, 0);
    assert.equal(r.skipped, 2); assert.ok(noWrites(env)); assert.equal(env.server.liveFiles().length, 2);
  } finally { await env.stop(); }
});

test('11 Arabic names, spaces, similar names: round-trip to a second device', async () => {
  const env = await makeEnv();
  try {
    const names = ['ملاحظاتي/مشروع جديد/الدرس الأول.md', 'note.md', 'Note.md', 'note 1.md', 'note-1.md', 'ملاحظاتي/صورة (1).png'];
    names.forEach((n, i) => env.local.put(n, 'content-' + i));
    const r = await env.engine.syncNow(); assert.equal(r.uploaded, names.length); assert.equal(r.status, 'ok');
    assert.ok(env.server.findLive('/Vault/ملاحظاتي/مشروع جديد/', 'الدرس الأول.md'));
    const dev2 = env.mkDevice();
    const r2 = await dev2.engine.syncNow(); assert.equal(r2.downloaded, names.length);
    names.forEach((n, i) => assert.equal(dev2.local.text(n), 'content-' + i));
  } finally { await env.stop(); }
});

test('12 empty and large (resumable) files sync both ways', async () => {
  const env = await makeEnv();
  try {
    const big = Buffer.alloc(5 * 1024 * 1024 + 123); for (let i = 0; i < big.length; i++) big[i] = (i * 13) & 255;
    env.local.put('empty.md', ''); env.local.put('Attachments/big.pdf', big);
    const r = await env.engine.syncNow(); assert.equal(r.status, 'ok'); assert.equal(r.uploaded, 2);
    assert.ok(env.server.log.some((l) => l.route === '/uploads'));   // took the resumable path
    const dev2 = env.mkDevice(); await dev2.engine.syncNow();
    assert.equal(dev2.local.text('empty.md'), '');
    assert.equal(sha(dev2.local.files.get('Attachments/big.pdf').data), sha(big));
  } finally { await env.stop(); }
});

test('5 partial failure: other files still sync, status is not "ok", failed file retried next run', async () => {
  const env = await makeEnv({ client: { maxRetries: 1 } });
  try {
    ['a.md', 'b.md', 'c.md'].forEach((n) => env.local.put(n, n));
    env.server.failRule((i) => i.method === 'POST' && i.route === '/files' && i.name === 'b.md', { status: 500 }, 99);
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'errors'); assert.equal(r.uploaded, 2); assert.equal(r.failures.length, 1);
    assert.equal(r.failures[0].path, 'b.md'); assert.equal(env.manifest.getFile('b.md'), undefined);
    assert.equal(env.manifest.data.meta.lastSuccessAt, null);       // never reports success
    env.server.rules.length = 0;
    const r2 = await env.engine.syncNow(); assert.equal(r2.status, 'ok'); assert.equal(r2.uploaded, 1);
  } finally { await env.stop(); }
});

test('2c repeated network failures abort the run as "offline" without touching data', async () => {
  const env = await makeEnv({ client: { maxRetries: 0 } });
  try {
    ['a', 'b', 'c', 'd'].forEach((n) => env.local.put(n + '.md', n));
    env.server.failRule((i) => i.method === 'POST' && i.route === '/files', 'drop', 99);
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'offline'); assert.equal(r.uploaded, 0);
    assert.equal(env.local.files.size, 4);
  } finally { await env.stop(); }
});

test('1e bad key aborts the whole run before any change', async () => {
  const env = await makeEnv();
  try {
    const dev = env.mkDevice({ client: env.mkClient('tdk_wrongwrongwrong') });
    dev.local.put('a.md', '1');
    const r = await dev.engine.syncNow();
    assert.equal(r.status, 'auth'); assert.equal(env.server.liveFiles().length, 0);
  } finally { await env.stop(); }
});

test('14 an existing local file is never overwritten without a ledger entry', async () => {
  const env = await makeEnv();
  try {
    env.local.put('x.md', 'LOCAL'); env.server.addFile('/Vault/', 'x.md', 'REMOTE');
    const r = await env.engine.syncNow();
    assert.equal(r.conflicts, 1); assert.equal(env.local.text('x.md'), 'LOCAL'); assert.equal(r.downloaded, 0);
  } finally { await env.stop(); }
});

test('14b local edit that lands during a download is not overwritten', async () => {
  const env = await makeEnv();
  try {
    env.local.put('x.md', 'v1'); await env.engine.syncNow();
    env.server.modifyFile(env.server.findLive('/Vault/', 'x.md').id, 'v2 remote');
    const { downloadFile } = require('../src/DownloadManager');
    const dev = env.mkDevice({ local: env.local, manifest: env.manifest, client: env.client, engine: {
      downloadFn: async (c, f, o) => { const d = await downloadFile(c, f, o); env.local.put('x.md', 'typed meanwhile'); return d; },
    } });
    const r = await dev.engine.syncNow();
    assert.equal(r.conflicts, 1); assert.equal(env.local.text('x.md'), 'typed meanwhile');
  } finally { await env.stop(); }
});

test('15 running sync twice with no changes performs no writes and no re-upload', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', '1'); env.local.put('d/b.md', '2'); env.server.addFile('/Vault/', 'r.md', 'r');
    await env.engine.syncNow();
    env.server.log.length = 0;
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok'); assert.equal(r.uploaded + r.downloaded, 0); assert.equal(r.skipped, 3);
    assert.ok(noWrites(env)); assert.ok(env.server.log.every((l) => !l.route.includes('/content'))); // no downloads either
    const r2 = await env.engine.syncNow(); assert.equal(r2.uploaded + r2.downloaded, 0);
  } finally { await env.stop(); }
});

test('exclusions: hidden folders and patterns are never uploaded', async () => {
  const env = await makeEnv({ settings: { excludePatterns: ['Private/**', '**/*.tmp'] } });
  try {
    ['.obsidian/plugins/x/data.json', 'Private/s.md', 'a.tmp', 'd/b.tmp', 'ok.md'].forEach((n) => env.local.put(n, 'x'));
    const r = await env.engine.syncNow(); assert.equal(r.uploaded, 1);
  } finally { await env.stop(); }
});

test('upload-only mode never downloads or deletes', async () => {
  const env = await makeEnv({ settings: { mode: 'upload-only' } });
  try {
    env.server.addFile('/Vault/', 'r.md', 'r'); env.local.put('a.md', '1');
    const r = await env.engine.syncNow(); assert.equal(r.uploaded, 1); assert.equal(r.downloaded, 0);
    assert.equal(env.local.text('r.md'), undefined);
  } finally { await env.stop(); }
});

test('pause stops the run between files and leaves the ledger consistent', async () => {
  const env = await makeEnv();
  try {
    for (let i = 0; i < 5; i++) env.local.put(`f${i}.md`, 'x' + i);
    let n = 0; env.engine.onProgress = (p) => { if (p.phase === 'sync' && ++n === 3) env.engine.pause(); };
    const r = await env.engine.syncNow(); assert.equal(r.status, 'paused');
    assert.ok(r.uploaded >= 1 && r.uploaded < 5);
    env.engine.onProgress = () => {};
    const r2 = await env.engine.syncNow(); assert.equal(r2.status, 'ok'); assert.equal(env.server.liveFiles().length, 5);
  } finally { await env.stop(); }
});

test('decide(): decision table', () => {
  const b = { hasLocal: true, hasRemote: true, hasEntry: true, localChanged: false, remoteChanged: false, conflictOpen: false };
  assert.equal(decide(b), 'skip');
  assert.equal(decide({ ...b, localChanged: true }), 'upload');
  assert.equal(decide({ ...b, remoteChanged: true }), 'download');
  assert.equal(decide({ ...b, localChanged: true, remoteChanged: true }), 'compare');
  assert.equal(decide({ ...b, hasRemote: false }), 'deleteLocal');
  assert.equal(decide({ ...b, hasRemote: false, localChanged: true }), 'upload');
  assert.equal(decide({ ...b, hasLocal: false }), 'deleteRemote');
  assert.equal(decide({ ...b, hasLocal: false, remoteChanged: true }), 'download');
  assert.equal(decide({ ...b, hasEntry: false }), 'compare');
  assert.equal(decide({ ...b, conflictOpen: true }), 'skip-conflict');
});

test('corrupt ledger resets safely', async () => {
  const m = new ManifestStore({ read: async () => '{not json', write: async () => {} });
  await m.load(); assert.equal(m.count(), 0); assert.ok(m.data.log.length === 1);
});
