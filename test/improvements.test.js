'use strict';
// Production improvements: rename/move in place, checksum verification, deletion audit/restore,
// conflict-copy isolation, selective sync, dry-run, adaptive concurrency.
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv } = require('./helpers');
const { sha } = require('./mockServer');

const buf = (s) => new TextEncoder().encode(s).buffer;
const txt = (b) => new TextDecoder().decode(b);

// ---------- 1. rename/move in place ----------

test('a local rename moves the remote file in place (no re-upload, same id)', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', 'hello rename');
    let r = await env.engine.syncNow();
    assert.equal(r.uploaded, 1, JSON.stringify(r));
    const id = env.server.findLive('/Vault/', 'a.md').id;

    env.local.removeDir('.');                       // stray: nothing
    env.local.files.delete('a.md');
    env.local.put('renamed.md', 'hello rename');    // same bytes, new name
    env.server.log.length = 0;
    r = await env.engine.syncNow();
    assert.equal(r.moved, 1, JSON.stringify(r));
    assert.equal(r.uploaded, 0, 'no re-upload');
    assert.equal(env.server.findLive('/Vault/', 'a.md'), undefined, 'old name gone');
    const live = env.server.findLive('/Vault/', 'renamed.md');
    assert.ok(live, 'new name present');
    assert.equal(live.id, id, 'same remote file (moved, not re-created)');
    assert.equal(sha(live.bytes), sha(Buffer.from('hello rename')));
    // A move is not an upload/trash pair.
    assert.equal(env.server.log.filter((l) => l.method === 'POST' && l.route === '/files').length, 0);
    assert.equal(env.server.log.filter((l) => l.method === 'DELETE').length, 0);
  } finally { await env.stop(); }
});

test('a local move into another folder PATCHes the path (no re-upload)', async () => {
  const env = await makeEnv();
  try {
    env.local.put('Notes/x.md', 'payload');
    await env.engine.syncNow();
    const id = env.server.findLive('/Vault/Notes/', 'x.md').id;
    env.local.removeDir('Notes');
    env.local.put('Archive/x.md', 'payload');
    env.server.log.length = 0;
    const r = await env.engine.syncNow();
    assert.equal(r.moved, 1, JSON.stringify(r));
    assert.ok(env.server.findLive('/Vault/Archive/', 'x.md'), 'moved to Archive');
    assert.equal(env.server.findLive('/Vault/Notes/', 'x.md'), undefined);
    assert.equal(env.server.findLive('/Vault/Archive/', 'x.md').id, id, 'same file id');
    assert.equal(env.server.log.filter((l) => l.method === 'POST' && l.route === '/files').length, 0);
  } finally { await env.stop(); }
});

test('a rename with a different body is still a normal re-upload (not a move)', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', 'v1');
    await env.engine.syncNow();
    env.local.files.delete('a.md');
    env.local.put('b.md', 'v2-different');
    const r = await env.engine.syncNow();
    assert.equal(r.moved, 0, JSON.stringify(r));
    assert.ok(env.server.findLive('/Vault/', 'b.md'));
  } finally { await env.stop(); }
});

// ---------- 10. dry-run preview ----------

test('dry-run reports the plan but changes nothing', async () => {
  const env = await makeEnv();
  try {
    env.local.put('new.md', 'x');
    env.local.put('Notes/y.md', 'y');
    const r = await env.engine.syncNow({ dryRun: true });
    assert.equal(r.plan.upload.length, 2, JSON.stringify(r.plan));
    assert.equal(env.server.liveFiles().length, 0, 'nothing uploaded');
    assert.equal(env.manifest.count(), 0, 'ledger untouched');
    // the real run then does it
    const r2 = await env.engine.syncNow();
    assert.equal(r2.uploaded, 2, JSON.stringify(r2));
  } finally { await env.stop(); }
});


// ---------- 5. conflict copies are never synced ----------

test('conflict copies are not uploaded on later syncs', async () => {
  const env = await makeEnv();
  try {
    env.local.put('note.md', 'base');
    await env.engine.syncNow();
    env.local.put('note.md', 'local edit');                       // local changed
    const rec = env.server.findLive('/Vault/', 'note.md');
    env.server.modifyFile(rec.id, 'remote edit');                 // remote changed too
    const r = await env.engine.syncNow();
    assert.equal(r.conflicts, 1, JSON.stringify(r));
    const copy = env.local.listFolders ? [...env.local.files.keys()].find((p) => /Tdrive conflict/.test(p)) : null;
    assert.ok(copy, 'a conflict copy exists locally: ' + [...env.local.files.keys()].join(','));
    env.server.log.length = 0;
    const r2 = await env.engine.syncNow();
    const uploaded = env.server.log.filter((l) => l.method === 'POST' && l.route === '/files').length;
    assert.equal(uploaded, 0, 'the conflict copy must not be uploaded');
    assert.equal(env.server.liveFiles().filter((f) => /Tdrive conflict/.test(f.name)).length, 0);
    assert.ok(env.local.files.has(copy), 'local conflict copy is kept');
  } finally { await env.stop(); }
});


const { merge3 } = require('../src/MergeManager');

// ---------- 6. three-way merge ----------

test('merge3: non-overlapping edits merge cleanly', () => {
  const base = 'a\nb\nc\nd';
  const local = 'A\nb\nc\nd';     // changed line 1
  const remote = 'a\nb\nc\nD';    // changed line 4
  const m = merge3(base, local, remote);
  assert.equal(m.ok, true);
  assert.equal(m.text, 'A\nb\nc\nD');
});

test('merge3: overlapping conflicting edits are reported with markers', () => {
  const base = 'a\nb\nc';
  const m = merge3(base, 'a\nX\nc', 'a\nY\nc');
  assert.equal(m.ok, false);
  assert.ok(m.conflicts >= 1);
  assert.match(m.text, /<<<<<<< local/);
  assert.match(m.text, />>>>>>> remote/);
});

test('merge3: an edit on one side and none on the other keeps the edit', () => {
  const m = merge3('x\ny', 'x\nY', 'x\ny');
  assert.equal(m.ok, true);
  assert.equal(m.text, 'x\nY');
});

test('a text conflict with non-overlapping edits auto-merges into one file on both sides', async () => {
  const env = await makeEnv();
  try {
    env.local.put('note.md', 'one\ntwo\nthree');
    await env.engine.syncNow();
    env.local.put('note.md', 'ONE\ntwo\nthree');           // local edits line 1
    const rec = env.server.findLive('/Vault/', 'note.md');
    env.server.modifyFile(rec.id, 'one\ntwo\nTHREE');      // remote edits line 3
    let r = await env.engine.syncNow();
    assert.equal(r.conflicts, 1, JSON.stringify(r));
    const res = await env.engine.conflicts.resolve('note.md', 'merge', env.engine);
    assert.equal(res.resolved, true, JSON.stringify(res));
    assert.equal(env.local.text('note.md'), 'ONE\ntwo\nTHREE');
    const live = env.server.findLive('/Vault/', 'note.md');
    assert.equal(Buffer.from(live.bytes).toString(), 'ONE\ntwo\nTHREE');
    assert.equal(env.engine.conflicts.has('note.md'), false, 'conflict closed');
  } finally { await env.stop(); }
});


// ---------- 7. remote sync lock ----------

test('a fresh lock from another device blocks the run', async () => {
  const env = await makeEnv({ settings: { remoteLock: true } });
  try {
    env.local.put('seed.md', 'x');
    const r1 = await env.engine.syncNow();
    assert.equal(r1.status, 'ok', JSON.stringify(r1));
    const lf = env.server.addFile('/Vault/', '.tdrive-sync.lock-other-device', '{}');
    lf.lastModified = Date.now();
    env.local.put('new.md', 'y');
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'locked', JSON.stringify(r));
    assert.equal(env.server.findLive('/Vault/', 'new.md'), undefined, 'nothing uploaded while locked');
  } finally { await env.stop(); }
});

test('a stale lock is taken over', async () => {
  const env = await makeEnv({ settings: { remoteLock: true } });
  try {
    env.local.put('seed.md', 'x');
    await env.engine.syncNow();
    const lf = env.server.addFile('/Vault/', '.tdrive-sync.lock-other-device', '{}');
    lf.lastModified = Date.now() - 3600_000;
    env.local.put('new.md', 'y');
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok', JSON.stringify(r));
    assert.ok(env.server.findLive('/Vault/', 'new.md'), 'stale lock does not block');
  } finally { await env.stop(); }
});

// ---------- 8. selective sync ----------

test('selective sync touches only the chosen subtrees', async () => {
  const env = await makeEnv({ settings: { syncFolders: ['Notes'] } });
  try {
    env.local.put('root.md', 'root');
    env.local.put('Notes/a.md', 'a');
    env.local.put('Other/b.md', 'b');
    env.server.addFile('/Vault/Notes/', 'remote.md', 'r');
    env.server.addFile('/Vault/Other/', 'keepme.md', 'k');
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok', JSON.stringify(r));
    assert.ok(env.server.findLive('/Vault/Notes/', 'a.md'), 'in-scope local uploaded');
    assert.ok(env.server.findLive('/Vault/Notes/', 'remote.md'), 'in-scope remote kept');
    assert.equal(env.local.text('Notes/remote.md'), 'r', 'in-scope remote downloaded');
    assert.equal(env.server.findLive('/Vault/', 'root.md'), undefined, 'out-of-scope local not uploaded');
    assert.equal(env.server.findLive('/Vault/Other/', 'b.md'), undefined, 'out-of-scope local not uploaded');
    assert.ok(env.server.findLive('/Vault/Other/', 'keepme.md'), 'out-of-scope remote NOT deleted');
    assert.equal(env.local.files.has('Other/keepme.md'), false, 'out-of-scope remote not downloaded');
  } finally { await env.stop(); }
});

// ---------- 9. adaptive concurrency ----------

test('a 429 lowers the concurrency hint used for parallel parts', async () => {
  const env = await makeEnv();
  try {
    const client = env.mkClient(null, { maxRetries: 3 });
    assert.equal(client._concurrencyHint == null, true, 'no hint initially');
    env.server.failRule((i) => i.route === '/me', { status: 429, retryAfter: 0 }, 1);
    await client.me();
    assert.ok(Number.isFinite(client._concurrencyHint) && client._concurrencyHint >= 1, 'hint lowered to a positive number');
    assert.ok(client._concurrencyHint <= 2);
  } finally { await env.stop(); }
});

// ---------- 2. incremental remote scan ----------

test('an unchanged remote reuses the cached listing (no tree walk)', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', 'a'); env.local.put('Notes/b.md', 'b');
    const r1 = await env.engine.syncNow();
    assert.equal(r1.uploaded, 2, JSON.stringify(r1));
    const r0 = await env.engine.syncNow();                     // establishes the snapshot (nothing changes)
    assert.equal(r0.uploaded + r0.downloaded, 0);
    env.server.log.length = 0;
    const r2 = await env.engine.syncNow();
    assert.equal(env.engine._snapshotUsed, true, 'a later run reused the snapshot');
    const walks = env.server.log.filter((l) => l.method === 'GET' && l.route === '/files').length;
    assert.ok(walks <= 1, 'no recursive folder walk, saw ' + walks);
    assert.equal(r2.uploaded + r2.downloaded, 0);
  } finally { await env.stop(); }
});

test('activity after the snapshot forces a full scan and picks up the change', async () => {
  const env = await makeEnv();
  try {
    env.local.put('a.md', 'a');
    await env.engine.syncNow();
    env.server.addFile('/Vault/', 'from-phone.md', 'hello');
    env.server.addActivity('upload', 'x', '{}');
    const r = await env.engine.syncNow();
    assert.equal(r.downloaded, 1, JSON.stringify(r));
    assert.equal(env.local.text('from-phone.md'), 'hello');
  } finally { await env.stop(); }
});

test('incrementalScan disabled always walks the tree', async () => {
  const env = await makeEnv({ settings: { incrementalScan: false } });
  try {
    env.local.put('a.md', 'a');
    await env.engine.syncNow();
    env.server.log.length = 0;
    await env.engine.syncNow();
    assert.equal(env.engine._snapshotUsed, false);
    const walks = env.server.log.filter((l) => l.method === 'GET' && l.route === '/files').length;
    assert.ok(walks >= 1, 'full scan happened');
  } finally { await env.stop(); }
});

// ---------- 15. stream large downloads to disk ----------

test('a large download streams to disk and stays consistent (no re-download next run)', async () => {
  const env = await makeEnv({ settings: { streamThresholdBytes: 1024 * 1024 } });
  try {
    const big = Buffer.alloc(3 * 1024 * 1024 + 5); for (let i = 0; i < big.length; i++) big[i] = (i * 7 + 3) & 255;
    env.server.addFile('/Vault/', 'movie.bin', big);
    const r = await env.engine.syncNow();
    assert.equal(r.downloaded, 1, JSON.stringify(r));
    assert.ok(env.local.files.has('movie.bin'), 'written to disk');
    assert.equal(env.local.files.get('movie.bin').data.length, big.length, 'full size on disk');
    const entry = env.manifest.getFile('movie.bin');
    assert.equal(entry.localHash, null, 'hash deferred for streamed download');
    // next run: no re-download, and the hash is now filled in without being treated as a change
    const r2 = await env.engine.syncNow();
    assert.equal(r2.uploaded + r2.downloaded, 0, JSON.stringify(r2));
    assert.ok(env.manifest.getFile('movie.bin').localHash, 'hash computed lazily');
  } finally { await env.stop(); }
});

// ---------- 4. deletion audit + restore ----------

test('deletions are audited and can be restored', async () => {
  const env = await makeEnv();
  try {
    env.local.put('keep.md', 'k');
    env.local.put('gone.md', 'to be deleted');
    await env.engine.syncNow();
    const id = env.server.findLive('/Vault/', 'gone.md').id;

    env.local.files.delete('gone.md');            // user deletes locally
    const r = await env.engine.syncNow();
    assert.equal(r.deletedRemote, 1, JSON.stringify(r));
    assert.equal(env.server.findLive('/Vault/', 'gone.md'), undefined);
    const audit = env.manifest.data.meta.deletions;
    assert.ok(Array.isArray(audit) && audit.length, 'deletion recorded');
    const entry = audit.find((x) => x.path === 'gone.md');
    assert.ok(entry && entry.remoteId === id && entry.side === 'remote');

    const res = await env.engine.restoreLastDeletions();
    assert.equal(res.restored, 1, JSON.stringify(res));
    assert.ok(env.server.findLive('/Vault/', 'gone.md'), 'remote file restored from trash');
    assert.equal(txt(Buffer.from(await env.engine.downloadFn(env.client, env.server.findLive('/Vault/', 'gone.md'), {}))), 'to be deleted');
  } finally { await env.stop(); }
});
