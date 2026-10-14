'use strict';
// "Move the note to another folder, then keep editing it" must be a single in-place move on the server
// (same file id, only the changed chunks uploaded) and must never leave a stale duplicate at the old path.
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv } = require('./helpers');

const body = (n, s0) => { const b = Buffer.alloc(n); let s = s0 >>> 0; for (let i = 0; i < n; i++) { s = (s * 1664525 + 1013904223) >>> 0; b[i] = (s >>> 24) & 255; } return b; };

test('move + edit with a rename hint: same id, no duplicate at the old path, changed bytes only', async () => {
  const env = await makeEnv();
  try {
    const before = body(2 * 1024 * 1024, 42);
    env.local.put('كيف تقلل استخدام الرامات.md', before);
    await env.engine.syncNow();
    const id = env.server.findLive('/Vault/', 'كيف تقلل استخدام الرامات.md').id;

    // The editor moves the note into a subfolder AND the user keeps editing it (so the content differs).
    const after = Buffer.from(before); for (let i = 500000; i < 500400; i++) after[i] ^= 0xff;
    env.engine._moveHints = null;
    env.manifest.data.meta.moveHints = { 'كيف تقلل استخدام الرامات.md': { to: 'Pc/كيف تقلل استخدام الرامات.md', at: Date.now() } };
    env.local.files.delete('كيف تقلل استخدام الرامات.md');
    env.local.put('Pc/كيف تقلل استخدام الرامات.md', after);

    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok', JSON.stringify(r));
    assert.equal(r.moved, 1, 'recorded as a move: ' + JSON.stringify(r));
    assert.equal(r.uploaded, 0, 'not counted as a new upload');
    assert.equal(r.deletedRemote, 0, 'the old path was moved, not deleted separately');

    // Exactly one file with that name, at the new path, with the SAME server id.
    const all = env.server.liveFiles().filter((f) => f.name === 'كيف تقلل استخدام الرامات.md');
    assert.equal(all.length, 1, 'no stale duplicate left behind: ' + JSON.stringify(all.map((f) => f.path)));
    assert.equal(all[0].path, '/Vault/Pc/');
    assert.equal(all[0].id, id, 'the server file id was preserved (in-place move)');
    assert.equal(Buffer.compare(all[0].bytes, after), 0, 'the edited content was uploaded');

    // Converged: a second run does nothing.
    const r2 = await env.engine.syncNow();
    assert.equal(r2.uploaded + r2.downloaded + r2.moved + r2.deletedRemote, 0, JSON.stringify(r2));
  } finally { await env.stop(); }
});

test('move + edit WITHOUT any hint still moves in place (same basename, free destination)', async () => {
  const env = await makeEnv();
  try {
    const before = body(1500, 7);                       // small file, single chunk
    env.local.put('Notes/idea.md', before);
    await env.engine.syncNow();
    const id = env.server.findLive('/Vault/Notes/', 'idea.md').id;

    const after = body(1700, 9);                        // edited (different size + bytes)
    env.local.files.delete('Notes/idea.md');
    env.local.put('Archive/idea.md', after);            // moved while the app was closed (no hint)

    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok', JSON.stringify(r));
    const all = env.server.liveFiles().filter((f) => f.name === 'idea.md');
    assert.equal(all.length, 1, 'exactly one idea.md, no duplicate');
    assert.equal(all[0].path, '/Vault/Archive/');
    assert.equal(all[0].id, id, 'same server id (moved in place)');
    assert.equal(Buffer.compare(all[0].bytes, after), 0, 'edited content applied');
  } finally { await env.stop(); }
});

test('a real delete is NOT turned into a move (no same-name file created elsewhere)', async () => {
  const env = await makeEnv();
  try {
    env.local.put('Notes/keep.md', 'stay');
    env.local.put('Notes/gone.md', 'bye');
    await env.engine.syncNow();
    env.local.files.delete('Notes/gone.md');            // plain deletion, nothing created
    const r = await env.engine.syncNow();
    assert.equal(r.deletedRemote, 1, JSON.stringify(r));
    assert.equal(r.moved, 0, 'no move inferred');
    assert.equal(env.server.findLive('/Vault/Notes/', 'gone.md'), undefined, 'the file really is gone');
  } finally { await env.stop(); }
});

test('two genuinely different same-named files in different folders are kept independently', async () => {
  const env = await makeEnv();
  try {
    env.local.put('A/index.md', 'alpha');
    env.local.put('B/index.md', 'beta');
    const r = await env.engine.syncNow();
    assert.equal(r.uploaded, 2, JSON.stringify(r));
    assert.equal(r.moved, 0, 'different content is not a move');
    assert.equal(env.server.findLive('/Vault/A/', 'index.md').bytes.toString(), 'alpha');
    assert.equal(env.server.findLive('/Vault/B/', 'index.md').bytes.toString(), 'beta');
  } finally { await env.stop(); }
});
