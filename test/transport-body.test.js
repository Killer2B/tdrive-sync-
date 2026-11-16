'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const Module = require('node:module');
const { createHash } = require('node:crypto');
const { uploadFile, uploadRevision } = require('../src/UploadManager');

const CAP = 5242880;
const CHUNKER = { algo: 'buzhash', min: 1024, avg: 2048, max: 4096, window: 48 };
const sha = (body) => createHash('sha256').update(body).digest('hex');
const ok = (data) => ({ status: 200, headers: { 'X-Synthetic': 'native' }, arrayBuffer: new TextEncoder().encode(JSON.stringify({ ok: true, data })).buffer });

function nativeClient(handler) {
  const calls = [];
  const stub = {
    Plugin: class {
      constructor(app) { this.app = app; }
      loadData() { assert.fail('Plugin onload and persisted settings are forbidden in transport tests'); }
    },
    PluginSettingTab: class {}, Setting: class {}, Notice: class {}, Modal: class {}, ItemView: class {}, TFile: class {},
    requestUrl: async (req) => {
      assert.equal(new URL(req.url).origin, 'https://transport.invalid');
      // Obsidian's SDK accepts ArrayBuffer|string, unlike fetch's additional BufferSource types.
      assert.ok(req.body === undefined || typeof req.body === 'string' || req.body instanceof ArrayBuffer,
        'requestUrl body must be ArrayBuffer|string, never an ArrayBuffer view');
      assert.equal(req.throw, false);
      assert.ok(!Object.keys(req.headers).some((key) => key.toLowerCase() === 'content-type'));
      calls.push(req);
      return handler(req);
    },
  };
  const filename = require.resolve('../src/plugin');
  const cached = require.cache[filename];
  const load = Module._load;
  let SourcePlugin;
  try {
    delete require.cache[filename];
    Module._load = function (request, ...args) { return request === 'obsidian' ? stub : load.call(this, request, ...args); };
    SourcePlugin = require(filename);
  } finally {
    Module._load = load;
    if (cached) require.cache[filename] = cached; else delete require.cache[filename];
  }
  const plugin = new SourcePlugin(Object.freeze({}));
  plugin.settings = { baseUrl: 'https://transport.invalid', apiKey: 'synthetic-transport-key', maxUploadKBps: 0, maxDownloadKBps: 0 };
  const client = plugin.makeClient();
  client.maxRetries = 0; client.maxServerRetries = 0;
  client.sleep = async () => assert.fail('Unexpected retry or rate-limit wait');
  return { client, calls };
}

for (const View of [Uint8Array, Uint8ClampedArray, Int8Array, Uint16Array, Int16Array, Uint32Array, Int32Array, Float32Array, Float64Array, BigInt64Array, BigUint64Array, DataView, Buffer]) {
  test(`native transport copies only the offset ${View.name} byte window`, async () => {
    const backing = new ArrayBuffer(64);
    const all = new Uint8Array(backing); all.fill(0xe7);
    const body = View === Buffer ? Buffer.from(backing, 16, 7)
      : View === DataView ? new DataView(backing, 16, 7) : new View(backing, 16, 2);
    const window = new Uint8Array(backing, body.byteOffset, body.byteLength);
    window.set(Uint8Array.from({ length: window.length }, (_, i) => i + 1));
    const expected = Buffer.from(window);
    const original = Buffer.from(all);
    const headers = { 'cOnTeNt-TyPe': 'application/octet-stream', 'X-Window': 'exact' };
    const env = nativeClient((req) => {
      assert.notEqual(req.body, backing, 'views must receive an independent copied ArrayBuffer');
      assert.equal(req.body.byteLength, expected.length);
      assert.deepEqual(Buffer.from(req.body), expected);
      assert.equal(req.contentType, 'application/octet-stream');
      assert.equal(req.headers['X-Window'], 'exact');
      new Uint8Array(req.body).fill(0);
      assert.deepEqual(Buffer.from(all), original, 'native payload mutation must not change the caller buffer');
      return ok({ accepted: true });
    });
    assert.deepEqual(await env.client.request({ method: 'POST', path: '/body', headers, body }), { accepted: true });
    assert.equal(env.calls.length, 1);
    assert.equal(headers['cOnTeNt-TyPe'], 'application/octet-stream', 'caller headers must not be mutated');
  });
}

test('native transport preserves a zero-length offset view as an empty ArrayBuffer', async () => {
  const body = new Uint8Array(new ArrayBuffer(16), 8, 0);
  const env = nativeClient((req) => {
    assert.equal(req.body.byteLength, 0);
    assert.notEqual(req.body, body.buffer);
    return ok({ accepted: true });
  });
  await env.client.request({ method: 'POST', path: '/body', body });
  assert.equal(env.calls.length, 1);
});

for (const [label, body] of [['ArrayBuffer', new Uint8Array([3, 1, 4]).buffer], ['string', 'synthetic body'], ['empty string', ''], ['absent body', undefined]]) {
  test(`native transport passes ${label} through unchanged`, async () => {
    const env = nativeClient((req) => { assert.equal(req.body, body); return ok({ accepted: true }); });
    await env.client.request({ method: 'POST', path: '/body', body });
    assert.equal(env.calls.length, 1);
  });
}

function uploadServer(source, sizes, chunker) {
  const parts = new Map(); let opened; let completed = false;
  const offsets = []; let total = 0;
  for (const size of sizes) { offsets.push(total); total += size; }
  assert.equal(total, source.length);
  const hashes = sizes.map((size, i) => sha(source.subarray(offsets[i], offsets[i] + size)));
  const fingerprint = sha(hashes.join('\n'));
  const env = nativeClient(async (req) => {
    const path = new URL(req.url).pathname;
    if (path === '/api/v1' && req.method === 'GET') return ok({ upload: { chunkBytes: CAP, oneShotMaxBytes: CAP } });
    if (path === '/api/v1/uploads' && req.method === 'POST') {
      assert.equal(req.contentType, 'application/json');
      opened = JSON.parse(new TextDecoder().decode(req.body));
      assert.equal(opened.size, source.length);
      assert.equal(opened.totalChunks, sizes.length);
      assert.equal(opened.fingerprint, fingerprint);
      assert.deepEqual(opened.chunker, chunker);
      return ok({ id: 'synthetic-upload', status: 'pending', revision: !!opened.baseId,
        chunkSize: chunker ? chunker.avg : CAP, totalChunks: sizes.length, chunker,
        fingerprint, received: [], base: opened.baseId ? { parts: [] } : undefined });
    }
    const match = /^\/api\/v1\/uploads\/synthetic-upload\/parts\/(\d+)$/.exec(path);
    if (match && req.method === 'PUT') {
      assert.ok(opened);
      const idx = Number(match[1]);
      assert.ok(idx >= 0 && idx < sizes.length);
      assert.ok(!parts.has(idx));
      assert.equal(req.contentType, 'application/octet-stream');
      assert.ok(req.body.byteLength <= CAP);
      assert.equal(req.body.byteLength, sizes[idx]);
      assert.equal(req.headers['X-Chunk-Size'], String(sizes[idx]));
      assert.equal(req.headers['X-Chunk-Offset'], String(offsets[idx]));
      assert.equal(req.headers['X-Content-SHA256'], sha(Buffer.from(req.body)));
      assert.equal(req.headers['X-Content-SHA256'], hashes[idx]);
      assert.deepEqual(Buffer.from(req.body), source.subarray(offsets[idx], offsets[idx] + sizes[idx]));
      if (chunker) {
        assert.ok(sizes[idx] <= chunker.max);
        assert.ok(idx === sizes.length - 1 || sizes[idx] >= chunker.min);
      }
      parts.set(idx, Buffer.from(req.body));
      return ok({ idx });
    }
    if (path === '/api/v1/uploads/synthetic-upload/complete' && req.method === 'POST') {
      assert.equal(parts.size, sizes.length);
      assert.deepEqual(Buffer.concat(sizes.map((_, i) => parts.get(i))), source);
      assert.equal(sha(sizes.map((_, i) => sha(parts.get(i))).join('\n')), fingerprint);
      completed = true;
      const file = { id: opened.baseId || 'synthetic-file', name: opened.name || 'synthetic.bin', path: opened.path || '/', size: source.length, checksum: fingerprint };
      return ok({ fileId: file.id, file, checksum: fingerprint });
    }
    assert.fail(`Unexpected fake HTTP request: ${req.method} ${path}`);
  });
  return { ...env, parts, opened: () => opened, completed: () => completed };
}

test('actual UploadManager new upload sends two exact native ArrayBuffer parts at the 5 MiB cap', async () => {
  const source = Buffer.alloc(CAP + 17, 0x39); source.fill(0xa6, CAP);
  const env = uploadServer(source, [CAP, 17]);
  const result = await uploadFile(env.client, { name: 'synthetic.bin', folderPath: '/', data: source, mtime: 1, chunker: false, dedupe: false, concurrency: 2 });
  assert.equal(result.id, 'synthetic-file');
  assert.equal(result.size, source.length);
  assert.equal(env.parts.size, 2);
  assert.ok(env.completed());
  assert.equal(env.opened().baseId, undefined);
});

test('actual UploadManager CDC revision sends two native parts with valid nonfinal and final sizes', async () => {
  const source = Buffer.alloc(6000);
  const env = uploadServer(source, [4096, 1904], CHUNKER);
  const result = await uploadRevision(env.client, { id: 'synthetic-base', name: 'synthetic.bin', data: source, mtime: 1, chunker: CHUNKER, concurrency: 2 });
  assert.equal(result.id, 'synthetic-base');
  assert.equal(result.size, 6000);
  assert.equal(env.parts.size, 2);
  assert.ok(env.completed());
  assert.equal(env.opened().baseId, 'synthetic-base');
});

test('actual UploadManager one-shot upload also preserves the native ArrayBuffer body', async () => {
  const source = new Uint8Array([0x55, 0x01, 0x02, 0x03, 0xaa]).subarray(1, 4);
  const env = nativeClient((req) => {
    const path = new URL(req.url).pathname;
    if (path === '/api/v1') return ok({ upload: { chunkBytes: CAP, oneShotMaxBytes: CAP } });
    assert.equal(path, '/api/v1/files');
    assert.equal(req.method, 'POST');
    assert.equal(req.contentType, 'application/octet-stream');
    assert.deepEqual(Buffer.from(req.body), Buffer.from([1, 2, 3]));
    return ok({ id: 'synthetic-file', name: 'synthetic.bin', path: '/', size: 3, checksum: sha(sha(Buffer.from(req.body))) });
  });
  const result = await uploadFile(env.client, { name: 'synthetic.bin', folderPath: '/', data: source });
  assert.equal(result.id, 'synthetic-file');
  assert.equal(env.calls.length, 2);
});
