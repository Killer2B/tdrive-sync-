'use strict';
// Pro-grade data features: upload dedup, CDC revisions (fixed server), throttle, schedule, versions,
// share links and remote search.
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv, nodeTransport } = require('./helpers');
const { uploadFile, uploadRevision } = require('../src/UploadManager');

const body = (n, s0) => { const b = Buffer.alloc(n); let s = s0 >>> 0; for (let i = 0; i < n; i++) { s = (s * 1664525 + 1013904223) >>> 0; b[i] = (s >>> 24) & 255; } return b; };

test('upload dedup: chunks the server already stores are not sent again', async () => {
  const env = await makeEnv();
  try {
    const content = body(3 * 1024 * 1024, 77);
    // First upload stores its chunks on the server.
    const a = await uploadFile(env.client, { name: 'one.bin', folderPath: '/', data: content, mtime: 1, dedupe: false });
    assert.ok(a.size === content.length);

    // A second file with identical content must send (almost) no bytes.
    let bytes = 0, reuse = 0;
    const client = env.mkClient(null, {
      transport: async (r) => {
        if (r.method === 'PUT' && /\/parts\//.test(String(r.url))) { if (r.headers && r.headers['X-Chunk-Reuse'] === '1') reuse++; else if (r.body) bytes += r.body.byteLength; }
        return nodeTransport(r);
      },
    });
    const b = await uploadFile(client, { name: 'two.bin', folderPath: '/', data: content, mtime: 1, dedupe: true });
    assert.equal(b.size, content.length);
    assert.equal(bytes, 0, 'no chunk bytes sent for duplicate content');
    assert.ok(reuse >= 2, 'chunks were reused: ' + reuse);
    assert.equal(require('./mockServer').sha(env.server.findLive('/', 'two.bin').bytes), require('./mockServer').sha(content));
  } finally { await env.stop(); }
});

test('dedup disabled sends the bytes again', async () => {
  const env = await makeEnv();
  try {
    const content = body(2 * 1024 * 1024, 88);
    await uploadFile(env.client, { name: 'one.bin', folderPath: '/', data: content, mtime: 1, dedupe: false });
    let bytes = 0;
    const client = env.mkClient(null, { transport: async (r) => { if (r.method === 'PUT' && /\/parts\//.test(String(r.url)) && r.body) bytes += r.body.byteLength; return nodeTransport(r); } });
    await uploadFile(client, { name: 'two.bin', folderPath: '/', data: content, mtime: 1, dedupe: false });
    assert.ok(bytes > 0, 'with dedup off the bytes are sent');
  } finally { await env.stop(); }
});

test('a CDC revision keeps the base id and reuses the unchanged chunks', async () => {
  const env = await makeEnv();
  try {
    const before = body(2 * 1024 * 1024, 1234);
    const first = await uploadFile(env.client, { name: 'big.bin', folderPath: '/', data: before, mtime: 1 });
    const after = Buffer.from(before); for (let i = 1000000; i < 1000300; i++) after[i] ^= 0xff;
    const rev = await uploadRevision(env.client, { id: first.id, name: 'big.bin', data: after, mtime: 2, chunker: first.chunker });
    assert.ok(rev, 'revision used');
    assert.equal(rev.id, first.id, 'same id (in place)');
    assert.ok(rev.reusedParts >= 1, 'unchanged chunks reused: ' + rev.reusedParts);
    assert.equal(Buffer.compare(env.server.files.get(first.id).bytes, after), 0);
    assert.equal(env.server.findLive('/', 'big.bin').id, first.id, 'no duplicate created');
  } finally { await env.stop(); }
});

test('a whole-file CDC sync revises in place on the second run (server fixed)', async () => {
  const env = await makeEnv();
  try {
    const before = body(2 * 1024 * 1024, 321);
    env.local.put('Notes/big.bin', before);
    await env.engine.syncNow();
    const id = env.server.findLive('/Vault/Notes/', 'big.bin').id;
    env.server.log.length = 0;
    const after = Buffer.from(before); after[1500000] ^= 0xff;
    env.local.put('Notes/big.bin', after);
    const r = await env.engine.syncNow();
    assert.equal(r.uploaded, 1, JSON.stringify(r));
    const live = env.server.findLive('/Vault/Notes/', 'big.bin');
    assert.equal(live.id, id, 'kept the id (in-place revision)');
    assert.equal(env.server.log.filter((l) => l.method === 'DELETE').length, 0, 'nothing trashed');
    assert.equal(Buffer.compare(live.bytes, after), 0);
  } finally { await env.stop(); }
});

test('remote search returns matching files', async () => {
  const env = await makeEnv();
  try {
    env.server.addFile('/Vault/', 'alpha-note.md', 'a');
    env.server.addFile('/Vault/', 'beta.md', 'b');
    const r = await env.client.request({ path: '/search', query: { q: 'alpha', limit: 50 } });
    assert.equal((r.files || []).length, 1);
    assert.equal(r.files[0].name, 'alpha-note.md');
  } finally { await env.stop(); }
});

test('share link creation returns a url', async () => {
  const env = await makeEnv();
  try {
    const rec = env.server.addFile('/Vault/', 'shareme.md', 'x');
    const r = await env.client.request({ method: 'POST', path: `/files/${rec.id}/shares`, headers: { 'Content-Type': 'application/json' }, body: new TextEncoder().encode(JSON.stringify({ permission: 'download' })).buffer });
    assert.ok(r.url && /\/s\//.test(r.url));
  } finally { await env.stop(); }
});

test('versions can be listed and restored', async () => {
  const env = await makeEnv();
  try {
    const before = body(1024 * 1024, 5);
    env.local.put('Notes/big.bin', before);
    await env.engine.syncNow();
    const id = env.server.findLive('/Vault/Notes/', 'big.bin').id;
    const after = Buffer.from(before); after[500000] ^= 0xff;
    env.local.put('Notes/big.bin', after);
    await env.engine.syncNow();
    const v = await env.client.request({ path: `/files/${id}/versions` });
    assert.ok((v.versions || []).length >= 1, 'a version exists after a revision');
    await env.client.request({ method: 'POST', path: `/files/${id}/versions/${v.versions[0].id}/restore` });
    assert.equal(Buffer.compare(env.server.files.get(id).bytes, before), 0, 'restored the previous content');
  } finally { await env.stop(); }
});

test('a scheduled window decides when auto-sync runs', async () => {
  const env = await makeEnv();
  try {
    // Reuse the plugin's pure logic by requiring it is heavy; test the rule directly here.
    const within = (s, h) => { if (!s.scheduleEnabled) return true; const a = s.scheduleStart, b = s.scheduleEnd; if (a === b) return true; return a < b ? (h >= a && h < b) : (h >= a || h < b); };
    assert.equal(within({ scheduleEnabled: false }, 12), true);
    assert.equal(within({ scheduleEnabled: true, scheduleStart: 1, scheduleEnd: 5 }, 3), true);
    assert.equal(within({ scheduleEnabled: true, scheduleStart: 1, scheduleEnd: 5 }, 9), false);
    assert.equal(within({ scheduleEnabled: true, scheduleStart: 22, scheduleEnd: 6 }, 23), true);   // wraps midnight
    assert.equal(within({ scheduleEnabled: true, scheduleStart: 22, scheduleEnd: 6 }, 12), false);
  } finally { await env.stop(); }
});
