'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv } = require('./helpers');
const { sha256Hex } = require('../src/util');
const { fingerprintOf } = require('../src/Chunker');
const { lcsMatch } = require('../src/MergeManager');

test('deleting one empty child never deletes its parent or sibling files', async () => {
  const env = await makeEnv({ server: { folderShape: 'strings' } });
  try {
    env.local.put('Parent/keep.md', 'keep'); await env.local.mkdir('Parent/Empty');
    await env.engine.syncNow(); env.local.dirs.delete('Parent/Empty');
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok'); assert.equal(r.deletedRemote, 1);
    assert.ok(env.server.folders.has('/Vault/Parent/'));
    assert.equal(env.server.findLive('/Vault/Parent/', 'keep.md').bytes.toString(), 'keep');
    assert.ok(!env.server.folders.has('/Vault/Parent/Empty/'));
    const req = env.server.log.find((r) => r.method === 'DELETE' && r.route === '/folders');
    assert.equal(req.query.path, '/Vault/Parent/'); assert.equal(req.query.name, 'Empty');
  } finally { await env.stop(); }
});

test('ALREADY_EXISTS reserved in trash is an error, not a successful folder creation', async () => {
  const env = await makeEnv();
  try {
    env.server.folders.add('/Vault/');
    env.client.createFolder = async () => { const e = new Error('Name is reserved in trash'); e.status = 409; e.code = 'ALREADY_EXISTS'; throw e; };
    await assert.rejects(env.engine._ensureFolders('Gone'), /reserved in trash/);
    assert.ok(!env.engine._folders.has('Vault/Gone'));
  } finally { await env.stop(); }
});

test('small encrypted replacement PATCH applies compression and encryption', async () => {
  const env = await makeEnv();
  try {
    env.engine.revisionFn = async () => null;
    env.engine.compressor = { maybeCompress: async (_, b) => Buffer.concat([Buffer.from('zip:'), Buffer.from(b)]) };
    env.engine.crypto = { encrypt: async (b) => Buffer.concat([Buffer.from('encrypted:'), Buffer.from(b)]) };
    let sent;
    env.client.patchContent = async (_, b) => { sent = Buffer.from(b); return { id: 'base', size: sent.length, checksum: await fingerprintOf([await sha256Hex(sent)]) }; };
    const r = await env.engine._replaceUpload({ name: 'secret.md', folderPath: '/', data: Buffer.from('SECRET'), mtime: 1, old: { id: 'base' } });
    assert.equal(r.id, 'base'); assert.equal(sent.toString(), 'encrypted:zip:SECRET');
    assert.equal(env.server.opsLog().length, 0);
  } finally { await env.stop(); }
});

test('revision backpressure never trashes the base or falls back to a fresh upload', async () => {
  const env = await makeEnv();
  try {
    env.engine.revisionFn = async () => { const e = new Error('RevisionDeferred'); e.status = 503; throw e; };
    await assert.rejects(env.engine._replaceUpload({ name: 'a.md', folderPath: '/', data: Buffer.from('new'), old: { id: 'base' } }), /RevisionDeferred/);
    assert.equal(env.server.opsLog().length, 0);
  } finally { await env.stop(); }
});

test('delta rebuilds moved and repeated chunks even when no new chunk hashes exist', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', 'AAAABBBB');
    const a = await sha256Hex('AAAA'); const b = await sha256Hex('BBBB');
    const E = { localHash: await sha256Hex('AAAABBBB'), chunkMap: { sizes: [4, 4], hashes: [a, b] } };
    env.engine.manifestFn = async () => ({ size: 12, chunks: [{ idx: 0, offset: 0, size: 4, checksum: b }, { idx: 1, offset: 4, size: 4, checksum: a }, { idx: 2, offset: 8, size: 4, checksum: b }] });
    env.engine.deltaFn = async (_, __, changed) => { assert.equal(changed.length, 0); return []; };
    const delta = await env.engine._deltaPull('a.md', { id: 'r', size: 12 }, E);
    assert.equal(Buffer.from(delta.buffer).toString(), 'BBBBAAAABBBB');
  } finally { await env.stop(); }
});

test('delta refuses a stale local baseline and encoded stored bytes', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', 'EDIT');
    const E = { localHash: await sha256Hex('BASE'), chunkMap: { sizes: [4], hashes: [await sha256Hex('BASE')] } };
    env.engine.manifestFn = async () => ({ size: 4, chunks: [{ idx: 0, offset: 0, size: 4, checksum: await sha256Hex('EDIT') }] });
    assert.equal(await env.engine._deltaPull('a.md', { id: 'r', size: 4 }, E), null);
    env.engine.crypto = {};
    assert.equal(await env.engine._deltaPull('a.md', { id: 'r', size: 4 }, E), null);
  } finally { await env.stop(); }
});

test('switching to a nonempty different root cannot apply the old deletion baseline', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', 'safe'); await env.engine.syncNow();
    env.settings.remoteFolder = '/Other/'; env.server.addFile('/Other/', 'sentinel.md', 'sentinel');
    const r = await env.engine.syncNow();
    assert.equal(r.deletedLocal + r.deletedRemote, 0); assert.equal(env.local.text('a.md'), 'safe');
    assert.ok(env.server.findLive('/Other/', 'a.md'));
    assert.ok(env.manifest.data.meta.previousScopeLedger.files['a.md']);
  } finally { await env.stop(); }
});

test('approval cannot delete newly added deletion candidates', async () => {
  const env = await makeEnv({ settings: { maxDeletesPerRun: 1 } });
  try {
    for (const name of ['a.md', 'b.md', 'c.md', 'keep.md']) env.local.put(name, name);
    await env.engine.syncNow(); env.local.files.delete('a.md'); env.local.files.delete('b.md');
    const first = await env.engine.syncNow(); const reviewed = first.blockedDeletes.items.map((p) => ({ ...p }));
    env.local.files.delete('c.md');
    const r = await env.engine.syncNow({ approveDeletes: true, approvedDeletes: reviewed });
    assert.equal(r.deletedRemote, 0); assert.equal(r.blockedDeletes.reason, 'changed');
    assert.ok(env.server.findLive('/Vault/', 'c.md'));
  } finally { await env.stop(); }
});

test('dry run performs no mutations including locks, ledger state and move hints', async () => {
  const env = await makeEnv({ settings: { remoteLock: true } });
  try {
    env.local.put('new.md', 'new'); env.manifest.data.meta.moveHints = { 'old.md': { to: 'new.md' } };
    const before = JSON.stringify(env.manifest.data);
    const r = await env.engine.syncNow({ dryRun: true });
    assert.equal(r.status, 'ok'); assert.deepEqual(r.plan.upload, ['new.md']);
    assert.equal(env.server.opsLog().length, 0); assert.equal(JSON.stringify(env.manifest.data), before);
  } finally { await env.stop(); }
});

test('root paths that list as empty still have their actual parent folders ensured', async () => {
  const env = await makeEnv();
  try {
    const list = env.client.listFolder.bind(env.client);
    env.client.listFolder = async (p) => { try { return await list(p); } catch (e) { if (e.status === 404) return { files: [], folders: [] }; throw e; } };
    env.local.put('a.md', 'a');
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok'); assert.equal(r.uploaded, 1); assert.ok(env.server.folders.has('/Vault/'));
  } finally { await env.stop(); }
});

test('selective sync and excluded directories do not create duplicates or unwanted empty folders', async () => {
  const env = await makeEnv({ server: { dupFolders: true }, settings: { syncFolders: ['A/B'], excludePatterns: ['A/B/Private/**'] } });
  try {
    env.local.put('A/B/a.md', 'a'); await env.local.mkdir('A/B/Private'); await env.local.mkdir('A/Other');
    const first = await env.engine.syncNow(); const second = await env.engine.syncNow();
    assert.equal(first.status, 'ok'); assert.equal(second.uploaded, 0);
    assert.ok(![...env.server.folders].some((p) => p.includes('(1)') || p.includes('/Private/') || p.includes('/Other/')));
  } finally { await env.stop(); }
});

test('a remotely moved file is not rescued under its old path', async () => {
  const env = await makeEnv();
  try {
    env.local.put('Old/a.md', 'a'); env.local.put('keep.md', 'keep'); await env.engine.syncNow();
    const f = env.server.findLive('/Vault/Old/', 'a.md');
    f.path = '/Vault/New/'; env.server.folders.add('/Vault/New/'); env.server.folders.delete('/Vault/Old/');
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok'); assert.equal(env.local.text('New/a.md'), 'a'); assert.equal(env.local.text('Old/a.md'), undefined);
    const again = await env.engine.syncNow(); assert.equal(again.uploaded + again.downloaded, 0);
  } finally { await env.stop(); }
});

test('Unicode-normalized remote names retain the actual local disk path', async () => {
  const env = await makeEnv();
  try {
    const raw = 'cafe\u0301.md'; env.local.put(raw, 'coffee');
    const r = await env.engine.syncNow(); assert.equal(r.status, 'ok'); assert.equal(r.uploaded, 1);
    assert.equal(env.server.findLive('/Vault/', 'caf\u00e9.md').bytes.toString(), 'coffee');
    env.server.modifyFile(env.server.findLive('/Vault/', 'caf\u00e9.md').id, 'new');
    const next = await env.engine.syncNow(); assert.equal(next.status, 'ok'); assert.equal(env.local.text(raw), 'new');
    assert.equal(env.local.files.size, 1);
  } finally { await env.stop(); }
});

test('disk errors during local rescue abort instead of deleting the remote file', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', 'a'); env.local.put('keep.md', 'k'); await env.engine.syncNow();
    env.local.list = async () => [{ path: 'keep.md', ...(await env.local.stat('keep.md')) }];
    const exists = env.local.exists.bind(env.local);
    env.local.exists = async (p) => { if (p === 'a.md') throw new Error('EACCES'); return exists(p); };
    const r = await env.engine.syncNow(); assert.equal(r.status, 'aborted'); assert.equal(r.deletedRemote, 0);
    assert.ok(env.server.findLive('/Vault/', 'a.md'));
  } finally { await env.stop(); }
});

test('a failed content update after a successful move stays dirty for the next run', async () => {
  const env = await makeEnv();
  try {
    env.local.put('Old/a.md', 'old'); env.local.put('keep.md', 'k'); await env.engine.syncNow();
    env.local.removeDir('Old'); env.local.put('New/a.md', 'edited');
    env.manifest.data.meta.moveHints = { 'Old/a.md': { to: 'New/a.md' } };
    const revision = env.engine.revisionFn;
    env.engine.revisionFn = async () => { const e = new Error('busy'); e.status = 503; throw e; };
    const first = await env.engine.syncNow(); assert.equal(first.status, 'errors'); assert.equal(first.moved, 1);
    assert.equal(env.server.findLive('/Vault/New/', 'a.md').bytes.toString(), 'old');
    env.engine.revisionFn = revision;
    const second = await env.engine.syncNow(); assert.equal(second.status, 'ok'); assert.equal(second.uploaded, 1);
    assert.equal(env.server.findLive('/Vault/New/', 'a.md').bytes.toString(), 'edited');
    assert.equal(env.server.liveFiles().filter((f) => f.name === 'a.md').length, 1);
  } finally { await env.stop(); }
});

test('text merge refuses quadratic allocations beyond its bounded work budget', () => {
  assert.throws(() => lcsMatch(Array(4000).fill('a'), Array(4000).fill('b')), /safe work limit/);
});

test('an editor folder rename expands hints for all descendants, including edited files', async () => {
  const env = await makeEnv({ settings: { maxDeletesPerRun: 2 } });
  try {
    for (let i = 0; i < 12; i++) env.local.put(`Old/f${i}.md`, 'old-' + i);
    env.local.put('keep.md', 'keep'); await env.engine.syncNow();
    const ids = new Map(env.server.liveFiles().map((f) => [f.name, f.id]));
    env.local.removeDir('Old');
    for (let i = 0; i < 12; i++) env.local.put(`New/f${i}.md`, 'edited-' + i);
    env.manifest.data.meta.moveHints = { Old: { to: 'New' } };
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok'); assert.equal(r.moved, 12); assert.equal(r.blockedDeletes, null);
    assert.equal(env.server.liveFiles().length, 13); assert.ok(!env.server.folders.has('/Vault/Old/'));
    for (let i = 0; i < 12; i++) { const f = env.server.findLive('/Vault/New/', `f${i}.md`); assert.equal(f.id, ids.get(f.name)); assert.equal(f.bytes.toString(), 'edited-' + i); }
  } finally { await env.stop(); }
});

test('metadata-only checksum fields do not falsely block a genuine local deletion', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', 'a'); env.local.put('keep.md', 'keep'); await env.engine.syncNow();
    const get = env.client.getFile.bind(env.client);
    env.client.getFile = async (id) => ({ ...(await get(id)), checksum: await sha256Hex('fingerprint') });
    env.local.files.delete('a.md');
    const r = await env.engine.syncNow(); assert.equal(r.status, 'ok'); assert.equal(r.deletedRemote, 1);
  } finally { await env.stop(); }
});
