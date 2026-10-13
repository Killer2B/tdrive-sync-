'use strict';
// v1.0.5: pending deletions are visible + approvable; empty folders sync; warnings are logged; ledger saved during a run.
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv } = require('./helpers');

const many = (env, dir, n) => { for (let i = 0; i < n; i++) env.local.put(`${dir}/f${i}.md`, 'x' + i); };

test('deleting a 12-file folder: blocked deletions are recorded and logged, then approval completes it (files + folder)', async () => {
  const env = await makeEnv();
  try {
    many(env, 'Big', 12); env.local.put('keep.md', 'k');
    await env.engine.syncNow();
    env.local.removeDir('Big');
    const r1 = await env.engine.syncNow();
    assert.equal(r1.deletedRemote, 0);
    assert.equal(r1.blockedDeletes.reason, 'limit');
    assert.equal(r1.blockedDeletes.items.length, 12);
    assert.ok(env.manifest.data.log.some((l) => /Deletions blocked/.test(l.msg)), 'warning reaches the persistent log');
    assert.equal(env.server.folders.has('/Vault/Big/'), true, 'nothing deleted without approval');
    const r2 = await env.engine.syncNow({ approveDeletes: true });
    assert.equal(r2.deletedRemote >= 12, true, JSON.stringify(r2));
    assert.equal(env.server.folders.has('/Vault/Big/'), false, 'folder itself removed after approval');
    assert.equal(env.server.liveFiles().length, 1);
    assert.equal(env.manifest.data.meta.blockedDeletes, null, 'pending list cleared');
    const r3 = await env.engine.syncNow();
    assert.equal(r3.uploaded + r3.downloaded + r3.deletedRemote + r3.deletedLocal, 0, JSON.stringify(r3));
  } finally { await env.stop(); }
});

test('renaming a 12-file folder leaves no duplicates once deletions are approved', async () => {
  const env = await makeEnv();
  try {
    many(env, 'Old', 12); env.local.put('keep.md', 'k');
    await env.engine.syncNow();
    many(env, 'New', 12); env.local.removeDir('Old');
    const r1 = await env.engine.syncNow();
    assert.equal(r1.blockedDeletes.items.length, 12);
    await env.engine.syncNow({ approveDeletes: true });
    assert.equal(env.server.liveFiles().length, 13);
    assert.equal(env.server.folders.has('/Vault/Old/'), false);
  } finally { await env.stop(); }
});

test('approval never bypasses the empty-vault guard', async () => {
  const env = await makeEnv();
  try {
    many(env, 'A', 3);
    await env.engine.syncNow();
    env.local.removeDir('A');                       // local vault now empty
    const r = await env.engine.syncNow({ approveDeletes: true });
    assert.equal(r.deletedRemote, 0);
    assert.equal(r.blockedDeletes.reason, 'empty-local');
    assert.equal(env.server.liveFiles().length, 3);
  } finally { await env.stop(); }
});

test('remote folder deleted with 12 files: approval trashes local files and folder', async () => {
  const env = await makeEnv();
  try {
    many(env, 'Big', 12); env.local.put('keep.md', 'k');
    await env.engine.syncNow();
    env.server.removeFolder('/Vault/Big/');
    const r1 = await env.engine.syncNow();
    assert.equal(r1.blockedDeletes.items.length, 12);
    await env.engine.syncNow({ approveDeletes: true });
    assert.equal([...env.local.files.keys()].filter((k) => k.startsWith('Big/')).length, 0);
    assert.ok(env.local.trashedDirs.includes('Big'));
  } finally { await env.stop(); }
});

test('empty folders sync in both directions and are then tracked (a later delete propagates)', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', 'a'); await env.engine.syncNow();
    env.local.dirs.add('EmptyLocal');
    await env.engine.syncNow();
    assert.ok(env.server.folders.has('/Vault/EmptyLocal/'), 'local empty folder created remotely');
    env.server.folders.add('/Vault/EmptyRemote/');
    await env.engine.syncNow();
    assert.ok(env.local.dirs.has('EmptyRemote'), 'remote empty folder created locally');
    env.local.dirs.delete('EmptyLocal');            // user deletes the (empty) folder locally
    const r = await env.engine.syncNow();
    assert.equal(env.server.folders.has('/Vault/EmptyLocal/'), false, JSON.stringify(r));
  } finally { await env.stop(); }
});

test('upload-only mode does not create local folders; download-only does not create remote folders', async () => {
  const env = await makeEnv({ settings: { mode: 'upload-only' } });
  try {
    env.local.put('a.md', 'a'); await env.engine.syncNow();
    env.local.dirs.add('E1');
    await env.engine.syncNow({ mode: 'download-only' });
    assert.equal(env.server.folders.has('/Vault/E1/'), false);
  } finally { await env.stop(); }
});

test('local folder kept when the adapter refuses to trash it (hidden items inside)', async () => {
  const env = await makeEnv();
  try {
    env.local.put('N/a.md', 'a'); env.local.put('keep.md', 'k');
    await env.engine.syncNow();
    env.server.removeFolder('/Vault/N/');
    env.local.trashFolder = async () => false;
    const r = await env.engine.syncNow();
    assert.ok(r.warnings.some((w) => /kept/.test(w)), JSON.stringify(r.warnings));
    assert.equal(r.deletedLocal >= 1, true);
  } finally { await env.stop(); }
});

test('ledger is saved during a long run, not only at the end', async () => {
  const env = await makeEnv();
  try {
    many(env, 'Up', 45);
    let saves = 0; const orig = env.manifest.save.bind(env.manifest);
    env.manifest.save = async () => { saves++; return orig(); };
    await env.engine.syncNow();
    assert.ok(saves >= 3, 'saves: ' + saves);
  } finally { await env.stop(); }
});
