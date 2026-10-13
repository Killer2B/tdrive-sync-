'use strict';
// Folder deletion in both directions (v1.0.4). Before this, folders were never deleted by the sync at all.
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv } = require('./helpers');

const seed = (env) => {
  env.local.put('root.md', 'r');
  env.local.put('Notes/a.md', 'a');
  env.local.put('Notes/Deep/b.md', 'b');
  env.local.put('Keep/k.md', 'k');
};
const hasFolder = (env, p) => env.server.folders.has(p);

for (const folderShape of ['default', 'pathOnly', 'strings']) {
  test(`delete a folder locally -> files AND folder go to Tdrive trash (${folderShape})`, async () => {
    const env = await makeEnv({ server: { folderShape } });
    try {
      seed(env);
      assert.equal((await env.engine.syncNow()).status, 'ok');
      assert.ok(hasFolder(env, '/Vault/Notes/Deep/'));
      env.local.removeDir('Notes');
      const r = await env.engine.syncNow();
      assert.equal(r.status, 'ok', JSON.stringify(r));
      assert.equal(hasFolder(env, '/Vault/Notes/'), false, 'remote folder removed');
      assert.equal(hasFolder(env, '/Vault/Notes/Deep/'), false, 'remote sub-folder removed');
      assert.equal(env.server.liveFiles().length, 2, 'only root.md and Keep/k.md remain live');
      assert.ok(hasFolder(env, '/Vault/Keep/'), 'untouched folder stays');
      // stable afterwards: nothing re-created, nothing else deleted
      env.server.log.length = 0;
      const r2 = await env.engine.syncNow();
      assert.equal(r2.uploaded + r2.downloaded + r2.deletedRemote + r2.deletedLocal, 0, JSON.stringify(r2));
      assert.equal(env.server.opsLog().length, 0);
    } finally { await env.stop(); }
  });
}

test('delete a folder on Tdrive -> local files AND local folder are trashed', async () => {
  const env = await makeEnv();
  try {
    seed(env); await env.engine.syncNow();
    env.server.removeFolder('/Vault/Notes/');
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok', JSON.stringify(r));
    assert.equal(env.local.files.has('Notes/a.md'), false);
    assert.equal(env.local.files.has('Notes/Deep/b.md'), false);
    assert.ok(env.local.trashedDirs.includes('Notes') && env.local.trashedDirs.includes('Notes/Deep'), 'folders trashed: ' + env.local.trashedDirs);
    assert.ok(env.local.files.has('Keep/k.md'));
    assert.ok(!env.local.dirs.has('Notes'));
  } finally { await env.stop(); }
});

test('empty folder (its files were already deleted by an older version) is still cleaned up: ledger without folder info', async () => {
  const env = await makeEnv();
  try {
    seed(env); await env.engine.syncNow();
    env.manifest.data.folders = {};                       // ledger written by v1.0.3: no folder records
    env.local.removeDir('Notes');
    const r = await env.engine.syncNow();
    assert.equal(hasFolder(env, '/Vault/Notes/'), false, JSON.stringify(r));
  } finally { await env.stop(); }
});

test('a folder that still contains something on the server is NOT deleted (file added by another device)', async () => {
  const env = await makeEnv();
  try {
    seed(env); await env.engine.syncNow();
    env.server.addFile('/Vault/Notes/', 'new-from-phone.md', 'x');
    env.local.removeDir('Notes');
    const r = await env.engine.syncNow();
    assert.ok(hasFolder(env, '/Vault/Notes/'), 'folder kept');
    assert.ok(env.server.findLive('/Vault/Notes/', 'new-from-phone.md'), 'new remote file kept');
    assert.equal(env.local.text('Notes/new-from-phone.md'), 'x', 'and it comes back locally: ' + JSON.stringify(r));
  } finally { await env.stop(); }
});

test('a local folder that still has an edited/new file is NOT trashed when the server folder vanished', async () => {
  const env = await makeEnv();
  try {
    seed(env); await env.engine.syncNow();
    env.server.removeFolder('/Vault/Notes/');
    env.local.put('Notes/a.md', 'edited after the last sync');       // unsynced work
    await env.engine.syncNow();
    assert.equal(env.local.text('Notes/a.md'), 'edited after the last sync');
    assert.equal(env.local.trashedDirs.includes('Notes'), false);
    assert.ok(env.server.findLive('/Vault/Notes/', 'a.md'), 're-uploaded, folder recreated');
  } finally { await env.stop(); }
});

test('server that only supports DELETE /folders?path= still works (id route fails first, then path)', async () => {
  const env = await makeEnv({ server: { folderDelete: 'byPath' } });
  try {
    seed(env); await env.engine.syncNow();
    env.local.removeDir('Notes');
    await env.engine.syncNow();
    assert.equal(hasFolder(env, '/Vault/Notes/'), false);
  } finally { await env.stop(); }
});

test('server that rejects folder deletion: reported as an error naming the attempts, nothing else is harmed', async () => {
  const env = await makeEnv({ server: { folderDelete: 'none' } });
  try {
    seed(env); await env.engine.syncNow();
    env.local.removeDir('Notes');
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'errors');
    const f = r.failures.find((x) => x.path === 'Notes/Deep/' || x.path === 'Notes/');
    assert.ok(f && /Folder delete failed/.test(f.error) && /405/.test(f.error), JSON.stringify(r.failures));
    assert.equal(env.server.liveFiles().length, 2);
  } finally { await env.stop(); }
});

test('deletion safety applies to folders: policy off / upload-only / empty vault delete nothing', async () => {
  for (const [label, settings, mutate] of [
    ['policy off', { deletionPolicy: 'off' }, (env) => env.local.removeDir('Notes')],
    ['upload-only', { mode: 'upload-only' }, (env) => env.local.removeDir('Notes')],
    ['vault looks empty', {}, (env) => { for (const d of ['Notes', 'Keep']) env.local.removeDir(d); env.local.files.clear(); env.local.dirs.clear(); }],
  ]) {
    const env = await makeEnv({ settings });
    try {
      seed(env);
      const first = await env.engine.syncNow({ mode: 'two-way' });
      assert.equal(first.status, 'ok', label + JSON.stringify(first));
      mutate(env);
      const live = env.server.liveFiles().length;
      await env.engine.syncNow();
      assert.ok(hasFolder(env, '/Vault/Notes/'), label + ': remote folder must survive');
      assert.equal(env.server.liveFiles().length, live, label + ': no file trashed');
    } finally { await env.stop(); }
  }
});

test('more folder deletions than the limit are blocked', async () => {
  const env = await makeEnv({ settings: { maxDeletesPerRun: 1 } });
  try {
    env.local.put('keep.md', 'k');
    for (const d of ['A', 'B', 'C']) env.local.put(`${d}/x.md`, d);
    await env.engine.syncNow();
    env.local.files.delete('A/x.md'); env.local.files.delete('B/x.md');       // files already gone (e.g. earlier run)
    env.manifest.data.files = Object.fromEntries(Object.entries(env.manifest.data.files).filter(([k]) => k === 'keep.md' || k === 'C/x.md'));
    env.local.dirs.delete('A'); env.local.dirs.delete('B');
    env.server.liveFiles().filter((f) => /\/(A|B)\/$/.test(f.path)).forEach((f) => { f.trashed = true; });
    const r = await env.engine.syncNow();
    assert.equal(r.deletedRemote, 1, JSON.stringify(r));
    assert.ok(r.warnings.some((w) => /Folder deletions blocked/.test(w)), JSON.stringify(r.warnings));
  } finally { await env.stop(); }
});

test('cleanEmptyRemoteFolders removes leftover EMPTY remote folders that do not exist locally (cleanup of old runs)', async () => {
  const env = await makeEnv();
  try {
    seed(env); await env.engine.syncNow();
    env.local.put('Local/only.md', 'l'); await env.engine.syncNow();
    // leftovers from v1.0.3: files gone everywhere, empty folders remain on the server only
    env.server.folders.add('/Vault/Old/'); env.server.folders.add('/Vault/Old/Sub/'); env.server.folders.add('/Vault/Old/Sub/Deeper/');
    env.server.folders.add('/Vault/EmptyButLocal/'); env.local.dirs.add('EmptyButLocal');
    const rep = await env.engine.cleanEmptyRemoteFolders();
    assert.equal(rep.deleted, 3, JSON.stringify(rep));
    assert.equal(hasFolder(env, '/Vault/Old/'), false);
    assert.ok(hasFolder(env, '/Vault/EmptyButLocal/'), 'exists locally -> kept');
    assert.ok(hasFolder(env, '/Vault/Notes/') && hasFolder(env, '/Vault/Keep/'), 'folders with files untouched');
    assert.equal(env.server.liveFiles().length, 5);
  } finally { await env.stop(); }
});
