'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv } = require('./helpers');
const { sha256hex, fingerprintOf } = require('../src/Chunker');

async function baseline(rich = null) {
  const env = await makeEnv();
  try {
    const transport = env.client.transport;
    env.client.transport = async (req) => {
      const path = new URL(req.url).pathname;
      const match = /^\/api\/v1\/files\/([^/]+)\/chunks$/.exec(path);
      if (match && env.server.files.has(match[1])) {
        const stored = env.server.files.get(match[1]).bytes;
        const checksum = await sha256hex(stored);
        return { status: 200, headers: {}, body: Buffer.from(JSON.stringify({ ok: true, data: {
          size: stored.length, chunkSize: env.server.chunk, totalChunks: 1,
          chunks: [{ idx: 0, offset: 0, size: stored.length, checksum }], checksum: await fingerprintOf([checksum]),
          etag: rich && rich.etag || undefined,
        } })) };
      }
      const response = await transport(req);
      if (rich && path.endsWith('/content') && response.status < 300) return { ...response, headers: { ...response.headers, etag: rich.etag } };
      return response;
    };
    if (rich) {
      const get = env.client.getFile.bind(env.client);
      const upload = env.engine.uploadFn;
      env.client.getFile = async (id) => ({ ...(await get(id)), ...rich });
      env.engine.uploadFn = async (...args) => ({ ...(await upload(...args)), ...rich });
    }
    env.local.put('a.md', 'BASE'); env.local.put('keep.md', 'keep');
    assert.equal((await env.engine.syncNow()).status, 'ok');
    env.remote = env.server.findLive('/Vault/', 'a.md');
    env.entry = { ...env.manifest.getFile('a.md') };
    env.server.log.length = 0;
    env.local.put('a.md', 'LOCL');
    return env;
  } catch (e) { await env.stop(); throw e; }
}

function noReplacement(env) {
  assert.ok(!env.server.opsLog().some((r) => r.route === '/files/' + env.remote.id || r.route === '/uploads' || r.route === '/files'), 'no PATCH, revision session, fresh upload or trash');
  assert.equal(env.remote.trashed, false);
  assert.equal(env.local.text('a.md'), 'LOCL');
  assert.equal(env.local.text('keep.md'), 'keep');
}

async function stableConflict(env, report, remoteText = 'REMO') {
  assert.equal(report.status, 'ok', JSON.stringify(report.failures)); assert.equal(report.uploaded, 0); assert.equal(report.conflicts, 1);
  assert.equal(env.remote.bytes.toString(), remoteText);
  const conflict = { ...env.manifest.getConflict('a.md') };
  assert.equal(conflict.remoteId, env.remote.id);
  assert.equal(env.local.text(conflict.copyPath), remoteText);
  assert.deepEqual(env.manifest.getFile('a.md'), env.entry, 'unreviewed remote bytes must not advance the content baseline');
  for (let i = 0; i < 2; i++) {
    const next = await env.engine.syncNow();
    assert.equal(next.status, 'ok'); assert.equal(next.uploaded + next.downloaded, 0);
    assert.deepEqual(env.manifest.getConflict('a.md'), conflict);
    assert.equal(env.local.text(conflict.copyPath), remoteText);
  }
  noReplacement(env);
  assert.equal(env.server.liveFiles().length, 2);
  assert.equal(env.local.files.size, 3, 'subsequent runs must not create more conflict copies');
}

test('unchanged planned remote accepts a local-only replacement and converges', async () => {
  const env = await baseline();
  try {
    const report = await env.engine.syncNow();
    assert.equal(report.status, 'ok'); assert.equal(report.uploaded, 1); assert.equal(report.conflicts, 0);
    assert.equal(env.remote.bytes.toString(), 'LOCL');
    assert.equal(env.server.findLive('/Vault/', 'a.md').id, env.remote.id);
    assert.equal(env.manifest.getConflict('a.md'), undefined);
    const next = await env.engine.syncNow();
    assert.equal(next.uploaded + next.downloaded + next.conflicts, 0);
  } finally { await env.stop(); }
});

test('remote edit in planned upload progress is preserved as one stable conflict', async () => {
  const env = await baseline();
  try {
    let raced = false;
    env.engine.onProgress = (p) => {
      if (!raced && p.phase === 'sync' && p.action === 'upload' && p.current === 'a.md' && !p.bytes) {
        raced = true; env.server.modifyFile(env.remote.id, 'REMO');
      }
    };
    const report = await env.engine.syncNow();
    assert.equal(raced, true);
    await stableConflict(env, report);
  } finally { await env.stop(); }
});

test('remote edit during a null revision result is checked again before inline PATCH', async () => {
  const env = await baseline();
  try {
    let revisions = 0;
    env.engine.revisionFn = async () => { revisions++; env.server.modifyFile(env.remote.id, 'REMO'); return null; };
    const report = await env.engine.syncNow();
    assert.equal(revisions, 1, 'the initial unchanged guard must permit revision planning');
    await stableConflict(env, report);
  } finally { await env.stop(); }
});

for (const phase of ['compression', 'encryption']) {
  test(`remote edit during ${phase} is checked after transforms before inline PATCH`, async () => {
    const env = await baseline();
    try {
      const phases = [];
      env.engine.revisionFn = async () => { phases.push('revision'); return null; };
      const transform = (name) => async (data) => { phases.push(name); if (name === phase) env.server.modifyFile(env.remote.id, 'REMO'); return data; };
      env.engine.compressor = { maybeCompress: async (_, data) => transform('compression')(data), decompress: async (data) => data };
      env.engine.crypto = { encrypt: transform('encryption'), decrypt: async (data) => data };
      const report = await env.engine.syncNow();
      assert.deepEqual(phases, ['revision', 'compression', 'encryption']);
      await stableConflict(env, report);
    } finally { await env.stop(); }
  });
}

for (const token of ['etag', 'version']) {
  test(`rich metadata ${token} change detects a same-size same-mtime race despite sparse listing`, async () => {
    const rich = { etag: '"v1"', version: 1 };
    const env = await baseline(rich);
    try {
      assert.equal(env.entry.remoteEtag, '"v1"'); assert.equal(env.entry.remoteVersion, 1);
      let raced = false; let revisions = 0;
      env.engine.revisionFn = async () => { revisions++; return null; };
      env.engine.onProgress = (p) => {
        if (!raced && p.phase === 'sync' && p.action === 'upload' && p.current === 'a.md' && !p.bytes) {
          raced = true;
          const mtime = env.remote.lastModified;
          env.server.modifyFile(env.remote.id, 'REMO'); env.remote.lastModified = mtime;
          rich[token] = token === 'etag' ? '"v2"' : 2;
        }
      };
      const report = await env.engine.syncNow();
      assert.equal(raced, true); assert.equal(revisions, 0);
      assert.equal(env.remote.bytes.length, env.entry.remoteSize);
      assert.equal(env.remote.lastModified, env.entry.remoteMtime);
      await stableConflict(env, report);
    } finally { await env.stop(); }
  });
}

for (const failAt of [1, 2]) {
  test(`metadata query failure at replacement guard ${failAt} never writes or acknowledges content`, async () => {
    const env = await baseline();
    try {
      const get = env.client.getFile.bind(env.client); let queries = 0; let revisions = 0;
      env.client.getFile = async (id) => {
        if (id === env.remote.id && ++queries === failAt) throw Object.assign(new Error('metadata unavailable'), { status: 503, code: 'METADATA_UNAVAILABLE' });
        return get(id);
      };
      env.engine.revisionFn = async () => { revisions++; return null; };
      const report = await env.engine.syncNow();
      assert.ok(['errors', 'aborted'].includes(report.status));
      assert.equal(report.uploaded, 0); assert.equal(revisions, failAt - 1);
      assert.equal(env.remote.bytes.toString(), 'BASE');
      assert.equal(env.manifest.getConflict('a.md'), undefined);
      assert.deepEqual(env.manifest.getFile('a.md'), env.entry);
      noReplacement(env);
    } finally { await env.stop(); }
  });
}

test('a raced remote head equal to local content is adopted without PATCH or conflict', async () => {
  const env = await baseline();
  try {
    let raced = false;
    env.engine.onProgress = (p) => {
      if (!raced && p.phase === 'sync' && p.action === 'upload' && p.current === 'a.md' && !p.bytes) {
        raced = true; env.server.modifyFile(env.remote.id, 'LOCL');
      }
    };
    const report = await env.engine.syncNow();
    assert.equal(raced, true); assert.equal(report.status, 'ok'); assert.equal(report.uploaded, 0); assert.equal(report.conflicts, 0);
    assert.equal(env.manifest.getFile('a.md').remoteMtime, env.remote.lastModified);
    assert.equal(env.manifest.getConflict('a.md'), undefined);
    const next = await env.engine.syncNow(); assert.equal(next.uploaded + next.downloaded + next.conflicts, 0);
    noReplacement(env);
  } finally { await env.stop(); }
});

test('an unchanged reviewed conflict can still be resolved by keeping local', async () => {
  const env = await baseline();
  try {
    env.server.modifyFile(env.remote.id, 'REMO');
    assert.equal((await env.engine.syncNow()).conflicts, 1);
    const reviewed = { ...env.manifest.getConflict('a.md') };
    const result = await env.engine.conflicts.resolve('a.md', 'local', env.engine, { reviewed });
    assert.equal(result.resolved, true); assert.equal(env.manifest.getConflict('a.md'), undefined);
    assert.equal(env.remote.bytes.toString(), 'LOCL');
    const next = await env.engine.syncNow(); assert.equal(next.uploaded + next.downloaded + next.conflicts, 0);
  } finally { await env.stop(); }
});

test('a remote edit during reviewed conflict replacement keeps the review and both copies open', async () => {
  const env = await baseline();
  try {
    env.server.modifyFile(env.remote.id, 'REMO');
    assert.equal((await env.engine.syncNow()).conflicts, 1);
    const reviewed = { ...env.manifest.getConflict('a.md') }; let revisions = 0;
    env.engine.revisionFn = async () => { revisions++; env.server.modifyFile(env.remote.id, 'NEXT'); return null; };
    await assert.rejects(env.engine.conflicts.resolve('a.md', 'local', env.engine, { reviewed }), (e) => e.code === 'remote_changed');
    assert.equal(revisions, 1); assert.equal(env.engine.running, false);
    assert.deepEqual(env.manifest.getConflict('a.md'), reviewed);
    assert.equal(env.local.text(reviewed.copyPath), 'REMO');
    assert.equal(env.remote.bytes.toString(), 'NEXT');
    for (let i = 0; i < 2; i++) {
      const report = await env.engine.syncNow(); assert.equal(report.uploaded + report.downloaded, 0);
      assert.deepEqual(env.manifest.getConflict('a.md'), reviewed);
    }
    noReplacement(env);
  } finally { await env.stop(); }
});

test('a later remote edit refreshes an old reviewed conflict and requires renewed approval', async () => {
  const env = await baseline();
  try {
    env.server.modifyFile(env.remote.id, 'REMO');
    assert.equal((await env.engine.syncNow()).conflicts, 1);
    const reviewed = { ...env.manifest.getConflict('a.md') };
    env.server.modifyFile(env.remote.id, 'NEXT');
    await assert.rejects(env.engine.conflicts.resolve('a.md', 'local', env.engine, { reviewed }), /Remote file changed since this conflict was reviewed/);
    const latest = { ...env.manifest.getConflict('a.md') };
    assert.notEqual(latest.copyPath, reviewed.copyPath);
    assert.equal(env.local.text(reviewed.copyPath), 'REMO'); assert.equal(env.local.text(latest.copyPath), 'NEXT');
    await assert.rejects(env.engine.conflicts.resolve('a.md', 'local', env.engine, { reviewed }), /Conflict changed/);
    assert.deepEqual(env.manifest.getConflict('a.md'), latest);
    assert.equal(env.remote.bytes.toString(), 'NEXT');
    noReplacement(env);
  } finally { await env.stop(); }
});

test('a concurrent remote rename in the committed PATCH result is never renamed back', async () => {
  const env = await baseline();
  try {
    const patch = env.client.patchContent.bind(env.client);
    const rename = env.client.rename.bind(env.client); let renames = 0;
    env.client.rename = async (...args) => { renames++; return rename(...args); };
    env.client.patchContent = async (id, data) => {
      const committed = await patch(id, data);
      env.remote.name = 'renamed.md';
      return { ...committed, name: env.remote.name };
    };
    const report = await env.engine.syncNow();
    assert.equal(report.status, 'ok', JSON.stringify(report.failures)); assert.equal(report.uploaded, 1);
    assert.equal(renames, 0); assert.equal(env.remote.name, 'renamed.md');
    assert.equal(env.remote.bytes.toString(), 'LOCL'); assert.equal(env.local.text('a.md'), 'LOCL');
    assert.equal(env.manifest.getFile('a.md').remoteName, 'a.md', 'keep the requested path baseline until the remote rename is applied locally');
    assert.equal(env.server.opsLog().filter((r) => r.route === '/files/' + env.remote.id && r.method === 'PATCH').length, 1);
    env.client.patchContent = patch;
    const next = await env.engine.syncNow();
    assert.equal(next.status, 'ok'); assert.equal(next.uploaded, 0);
    assert.equal(env.local.text('a.md'), undefined); assert.equal(env.local.text('renamed.md'), 'LOCL');
    assert.equal(env.manifest.getFile('renamed.md').remoteId, env.remote.id);
    const again = await env.engine.syncNow(); assert.equal(again.uploaded + again.downloaded + again.conflicts, 0);
    assert.equal(env.remote.name, 'renamed.md'); assert.equal(env.server.liveFiles().length, 2); assert.equal(renames, 0);
  } finally { await env.stop(); }
});
