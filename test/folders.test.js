'use strict';
// Regression tests: files inside sub-folders must never be re-uploaded / duplicated on later runs,
// whatever shape the server uses to describe folders in a listing.
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv } = require('./helpers');

const noWrites = (env) => env.server.opsLog().length === 0;
const seed = (env) => {
  env.local.put('root.md', 'r');
  env.local.put('Notes/a.md', 'a');
  env.local.put('Notes/Deep/b.md', 'b');
  env.local.put('Attachments/img.png', 'png-bytes');
};

for (const folderShape of ['default', 'pathOnly', 'strings', 'kindDir', 'inFiles']) {
  test(`folders (${folderShape}): 3 syncs -> one live copy per file, no re-upload, no extra folders`, async () => {
    const env = await makeEnv({ server: { folderShape, dupFolders: true } });
    try {
      seed(env);
      const r1 = await env.engine.syncNow();
      assert.equal(r1.status, 'ok', JSON.stringify(r1));
      assert.equal(r1.uploaded, 4);
      const foldersAfterFirst = [...env.server.folders].sort();
      env.server.log.length = 0;
      for (let i = 0; i < 2; i++) {
        const r = await env.engine.syncNow();
        assert.equal(r.status, 'ok', JSON.stringify(r));
        assert.equal(r.uploaded + r.downloaded + r.conflicts, 0, `run ${i + 2}: ${JSON.stringify(r)}`);
      }
      assert.ok(noWrites(env), 'no write requests on unchanged vault: ' + JSON.stringify(env.server.opsLog()));
      assert.equal(env.server.liveFiles().length, 4, 'exactly one live copy of each file');
      assert.deepEqual([...env.server.folders].sort(), foldersAfterFirst, 'no duplicate folders');
    } finally { await env.stop(); }
  });
}

test('a second device downloads nested files and then stays stable', async () => {
  const env = await makeEnv({ server: { folderShape: 'pathOnly' } });
  try {
    seed(env); await env.engine.syncNow();
    const dev2 = env.mkDevice();
    const r = await dev2.engine.syncNow();
    assert.equal(r.downloaded, 4, JSON.stringify(r));
    assert.equal(dev2.local.text('Notes/Deep/b.md'), 'b');
    env.server.log.length = 0;
    const r2 = await dev2.engine.syncNow();
    assert.equal(r2.downloaded + r2.uploaded, 0); assert.ok(noWrites(env));
  } finally { await env.stop(); }
});

test('server whose listing never shows sub-folder contents: no duplicates, no local deletions', async () => {
  const env = await makeEnv({ server: { hideSubfolderFiles: true } });
  try {
    seed(env);
    await env.engine.syncNow();
    env.server.log.length = 0;
    for (let i = 0; i < 3; i++) {
      const r = await env.engine.syncNow();
      assert.equal(r.uploaded, 0, `run ${i + 2}: ${JSON.stringify(r)}`);
      assert.equal(r.deletedLocal, 0);
    }
    assert.equal(env.server.liveFiles().length, 4);
    assert.equal(env.local.files.size, 4, 'no local file deleted');
  } finally { await env.stop(); }
});
