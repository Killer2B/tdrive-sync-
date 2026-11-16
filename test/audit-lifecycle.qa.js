'use strict';
// QA regressions included in npm test; node --test test/audit-lifecycle.qa.js runs them alone. No user/vault writes.
const test = require('node:test');
const assert = require('node:assert/strict');
const Module = require('node:module');
const { spawnSync } = require('node:child_process');
const { ManifestStore } = require('../src/ManifestStore');
const { ConflictManager } = require('../src/ConflictManager');
const { CryptoManager } = require('../src/CryptoManager');
const { Compressor, compressBytes, decompressBytes, MAGIC } = require('../src/Compressor');
const { sha256Hex } = require('../src/util');

const bytes = (s) => new TextEncoder().encode(s).buffer;
const copy = (b) => Uint8Array.from(new Uint8Array(b)).buffer;
const text = (b) => new TextDecoder().decode(b);
const flush = () => new Promise((resolve) => setImmediate(resolve));
const deferred = () => { let resolve; const promise = new Promise((r) => { resolve = r; }); return { promise, resolve }; };
class TFolder { constructor(path) { this.path = path; this.children = []; this.parent = null; } }
class TFile {
  constructor(path, stat, parent) {
    this.path = path; this.name = path.split('/').at(-1); this.parent = parent;
    this.basename = this.name.replace(/\.[^.]*$/, ''); this.extension = this.name.includes('.') ? this.name.split('.').at(-1) : '';
    this.stat = { ctime: stat.mtime, size: stat.size, mtime: stat.mtime };
  }
}
class Element {
  constructor(opts = {}) { this.text = opts.text; this.children = []; }
  createEl(tag, opts) { const e = new Element(opts); this.children.push(e); return e; }
  createDiv(opts) { return this.createEl('div', opts); }
  createSpan(opts) { return this.createEl('span', opts); }
  empty() { this.children = []; } setText(s) { this.text = s; } addClass() {}
}
const stub = {
  Plugin: class {
    constructor(app, manifest) { this.app = app; this.manifest = manifest; this.disposers = []; }
    async loadData() { return this.inlineSettings; } async saveData() {}
    addCommand() {} addSettingTab() {} registerView() {} addRibbonIcon() {} registerInterval() {} registerEvent() {}
    addStatusBarItem() { return { setText() {} }; } register(fn) { this.disposers.push(fn); }
  },
  PluginSettingTab: class {}, ItemView: class {}, TFile,
  Modal: class {
    constructor(app) { this.app = app; this.contentEl = new Element(); }
    open() { this.app.opened.push(this); this.opening = this.onOpen(); } close() { this.onClose(); }
  },
  Setting: class {}, Notice: class {},
  requestUrl: async () => { throw new Error('QA forbids actual HTTP'); },
};
const originalLoad = Module._load;
let Plugin;
try {
  Module._load = function (request, ...args) { return request === 'obsidian' ? stub : originalLoad.call(this, request, ...args); };
  Plugin = require('../src/plugin');
} finally { Module._load = originalLoad; }

async function fixture(t, settings = {}, inlineLedger = null) {
  const disk = new Map(), index = new Map([['/', new TFolder('/')]]), events = new Map(), timers = new Map();
  const calls = [], removed = [], opened = []; let stamp = 1000, nextTimer = 0, saved = inlineLedger;
  const clock = { now: 1000000 };
  const stat = (p) => {
    const d = disk.get(p);
    return d ? d.folder ? { type: 'folder', size: 0, mtime: d.mtime } : { type: 'file', size: d.data.byteLength, mtime: d.mtime } : null;
  };
  const putDisk = (p, b, mtime = ++stamp) => disk.set(p, { data: typeof b === 'string' ? bytes(b) : copy(b), mtime });
  const emit = (kind, f, oldPath) => events.get(kind)?.(f, oldPath);
  const registerFile = (p) => {
    const parent = index.get(p.split('/').slice(0, -1).join('/') || '/');
    assert.ok(parent instanceof TFolder, 'createBinary requires an indexed TFolder parent');
    const f = new TFile(p, stat(p), parent); index.set(p, f); parent.children.push(f); return f;
  };
  const adapter = {
    exists: async (p) => p === '.qa-plugin/sync-state.json' ? saved !== null : disk.has(p),
    read: async (p) => { assert.equal(p, '.qa-plugin/sync-state.json'); return saved; },
    write: async (p, s) => { assert.equal(p, '.qa-plugin/sync-state.json'); saved = s; },
    stat: async (p) => stat(p),
    readBinary: async (p) => { assert.ok(disk.has(p), 'ENOENT ' + p); return copy(disk.get(p).data); },
    writeBinary: async (p, b, opts) => { calls.push(['raw-write', p]); putDisk(p, b, opts?.mtime); },
    appendBinary: async (p, b) => {
      const old = new Uint8Array(disk.get(p).data), add = new Uint8Array(b), out = new Uint8Array(old.length + add.length);
      out.set(old); out.set(add, old.length); putDisk(p, out.buffer);
    },
    remove: async (p) => { removed.push(p); disk.delete(p); },
    list: async (p) => ({ files: [...disk].filter(([k, d]) => k.startsWith(p + '/') && !d.folder).map(([k]) => k), folders: [] }),
  };
  const vault = {
    adapter, getName: () => 'InlineQA', getFiles: () => [...index.values()].filter((f) => f instanceof TFile),
    getAllLoadedFiles: () => [...index.values()], getAbstractFileByPath: (p) => index.get(p) || null,
    on: (kind, fn) => { events.set(kind, fn); return fn; },
    createFolder: async (p) => {
      if (disk.has(p) || index.has(p)) throw new Error('EEXIST');
      const parent = index.get(p.split('/').slice(0, -1).join('/') || '/'); assert.ok(parent instanceof TFolder);
      disk.set(p, { folder: true, mtime: ++stamp }); const f = new TFolder(p); f.parent = parent;
      index.set(p, f); parent.children.push(f); emit('create', f); return f;
    },
    createBinary: async (p, b, opts) => {
      calls.push(['create', p]); if (disk.has(p) || index.has(p)) throw new Error('EEXIST');
      putDisk(p, b, opts?.mtime); const f = registerFile(p); emit('create', f); return f;
    },
    modifyBinary: async (f, b, opts) => {
      calls.push(['modify', f.path]); assert.equal(index.get(f.path), f); assert.ok(f instanceof TFile);
      putDisk(f.path, b, opts?.mtime); f.stat = { ...f.stat, size: stat(f.path).size, mtime: stat(f.path).mtime }; emit('modify', f);
    },
    process: async (f, transform, opts) => {
      const current = new TextDecoder('utf-8', { ignoreBOM: true }).decode(await adapter.readBinary(f.path));
      const next = transform(current);
      if (next !== current) {
        calls.push(['modify', f.path]); assert.equal(index.get(f.path), f);
        putDisk(f.path, bytes(next), opts?.mtime);
        f.stat = { ...f.stat, size: stat(f.path).size, mtime: stat(f.path).mtime }; emit('modify', f);
      }
      return next;
    },
  };
  const app = { vault, opened, workspace: { onLayoutReady() {}, getActiveFile: () => null }, fileManager: {
    trashFile: async (f) => { disk.delete(f.path); index.delete(f.path); emit('delete', f); },
  } };
  const oldWindow = global.window, oldNow = Date.now;
  Date.now = () => clock.now;
  global.window = {
    setTimeout: (fn, ms) => { const id = ++nextTimer; timers.set(id, { fn, at: clock.now + ms }); return id; },
    clearTimeout: (id) => timers.delete(id), setInterval: () => { throw new Error('QA has no interval configuration'); },
    clearInterval: (id) => timers.delete(id), addEventListener() {}, removeEventListener() {},
  };
  const plugin = new Plugin(app, { dir: '.qa-plugin', id: 'tdrive-sync', version: '1.8.1' });
  plugin.inlineSettings = { language: 'en', apiKey: 'inline-owner-key', baseUrl: 'https://qa.invalid', remoteFolder: '/QA/', remoteLock: false, ...settings };
  t.after(async () => {
    try { await plugin.onunload(); for (const dispose of plugin.disposers) dispose(); assert.equal(timers.size, 0); }
    finally { Date.now = oldNow; if (oldWindow === undefined) delete global.window; else global.window = oldWindow; }
  });
  await plugin.onload();
  const put = (p, b, mtime) => { putDisk(p, b, mtime); const f = index.get(p); if (f) f.stat = { ...f.stat, size: stat(p).size, mtime: stat(p).mtime }; else registerFile(p); };
  const advance = async (ms) => {
    clock.now += ms;
    for (let n = 0; n < 20; n++) {
      const due = [...timers].find(([, v]) => v.at <= clock.now); if (!due) return;
      timers.delete(due[0]); due[1].fn(); await flush();
    }
    throw new Error('Unexpected QA timer loop');
  };
  return { plugin, local: plugin.local, adapter, vault, disk, index, calls, removed, opened, clock, timers, emit, put, putDisk, advance, saved: () => saved };
}
const contents = (h) => [...h.disk.values()].filter((d) => d.data).map((d) => text(d.data));
const elements = (e) => [e, ...e.children.flatMap(elements)];

// Installed adapters serialize reads, writes, and reconciliation in one FIFO queue.
function queuedAdapter(h) {
  let tail = Promise.resolve(); const order = [];
  const read = h.adapter.readBinary, write = h.adapter.writeBinary;
  const reconcile = async (p) => {
    const f = h.index.get(p);
    if (f instanceof TFile) {
      const stat = await h.adapter.stat(p);
      f.stat = { ...f.stat, size: stat.size, mtime: stat.mtime }; h.emit('modify', f);
    }
  };
  const queue = (kind, fn) => {
    const result = tail.then(async () => { order.push(kind); return fn(); });
    tail = result.catch(() => {}); return result;
  };
  h.adapter.readBinary = (p) => queue('read:' + p, () => read(p));
  h.adapter.writeBinary = (p, data, opts) => queue('write:' + p, async () => {
    await write(p, data, opts);
    await reconcile(p);
  });
  h.vault.modifyBinary = (f, data, opts) => h.adapter.writeBinary(f.path, data, opts);
  h.vault.process = (f, transform, opts) => queue('process:' + f.path, async () => {
    const current = new TextDecoder('utf-8', { ignoreBOM: true }).decode(await read(f.path));
    const next = transform(current);
    if (next !== current) { await write(f.path, bytes(next), opts); await reconcile(f.path); }
    return next;
  });
  return { order, queue };
}

test('queued adapter control: event snapshot reads execute after already queued supported binary writes', async (t) => {
  const h = await fixture(t); h.put('note.bin', new Uint8Array([255, 0, 1, 2]).buffer, 1000);
  const q = queuedAdapter(h), incoming = new Uint8Array([255, 0, 3, 4]).buffer;
  const observed = { captures: [] }; h.local._writes.set('note.bin', observed);
  const user = h.vault.modifyBinary(h.index.get('note.bin'), new Uint8Array([255, 0, 9, 9]).buffer, { mtime: 1000 });
  const remote = h.vault.modifyBinary(h.index.get('note.bin'), incoming, { mtime: 1000 });
  await Promise.all([user, remote]);
  const snapshots = await Promise.all(observed.captures); h.local._writes.delete('note.bin');
  assert.equal(snapshots.length, 2);
  for (const snapshot of snapshots) assert.deepEqual(snapshot.data, incoming, 'the event read cannot recover the earlier committed user bytes');
  assert.deepEqual(q.order, ['write:note.bin', 'write:note.bin', 'read:note.bin', 'read:note.bin']);
});

test('queued supported binary edit must not disappear behind an incoming replacement that accepts success', async (t) => {
  const h = await fixture(t); const original = new Uint8Array([255, 0, 1, 2]).buffer;
  const userBytes = new Uint8Array([255, 0, 9, 9]).buffer, incoming = new Uint8Array([255, 0, 3, 4]).buffer;
  h.put('note.bin', original, 1000); const q = queuedAdapter(h), modify = h.vault.modifyBinary;
  let incomingAttempts = 0, userWrite;
  h.vault.modifyBinary = (f, data, opts) => {
    incomingAttempts++;
    // Both use the supported API. The user is ahead of the incoming write in the adapter queue,
    // but reconciliation's event snapshot is appended behind that incoming write.
    userWrite = modify(f, userBytes, { mtime: 1000 });
    return modify(f, data, opts);
  };
  const outcome = await h.local.writeBinaryAtomic('note.bin', incoming, 1000).then((result) => ({ result }), (error) => ({ error }));
  if (!userWrite) userWrite = modify(h.index.get('note.bin'), userBytes, { mtime: 1000 });
  await userWrite;
  assert.ok(outcome.error, 'unsafe replacement must reject, not accept success; queue=' + q.order.join(','));
  assert.equal(incomingAttempts, 0, 'arbitrary existing binary must be refused before invoking modifyBinary');
  assert.deepEqual(await h.adapter.readBinary('note.bin'), userBytes);
  assert.match(outcome.error.message, /cannot safely compare.*binary.*original.*(kept|retained)/i);
  assert.equal(h.local._ownChanges.has('note.bin'), false); assert.equal(h.local._writes.size, 0);
});

test('queued UTF-8 process preserves supported user binary edits queued before or after the incoming text commit', async (t) => {
  for (const position of ['before', 'after']) {
    await t.test(position, async (t) => {
      const h = await fixture(t); h.put('note.md', 'BASE', 1000);
      const userBytes = new Uint8Array([255, 0, 9, 9]).buffer;
      const q = queuedAdapter(h), process = h.vault.process; let userWrite, processed = 0;
      h.vault.process = (f, transform, opts) => {
        processed++;
        if (position === 'before') userWrite = h.vault.modifyBinary(f, userBytes, { mtime: 1000 });
        const incoming = process(f, transform, opts);
        if (position === 'after') userWrite = h.vault.modifyBinary(f, userBytes, { mtime: 1000 });
        return incoming;
      };
      const outcome = await h.local.writeBinaryAtomic('note.md', bytes('REMO'), 1000).then((result) => ({ result }), (error) => ({ error }));
      await userWrite;
      assert.equal(processed, 1, 'exact UTF-8 replacement must still use queued process');
      assert.ok(outcome.error, 'interleaving changed bytes must not be acknowledged as the remote baseline');
      assert.deepEqual(await h.adapter.readBinary('note.md'), userBytes);
      assert.ok(q.order.includes('process:note.md')); assert.equal(h.local._writes.size, 0);
    });
  }
});

test('missing queued text process rejects existing UTF-8 replacement but still permits new binary creation', async (t) => {
  const h = await fixture(t); h.put('note.md', 'ORIGINAL'); delete h.vault.process;
  await assert.rejects(h.local.writeBinaryAtomic('note.md', bytes('REMOTE')), (error) => {
    assert.equal(error.code, 'LOCAL_UNSAFE_REPLACEMENT');
    assert.match(error.message, /requires Obsidian's queued text process API.*original retained/);
    return true;
  });
  assert.equal(text(h.disk.get('note.md').data), 'ORIGINAL');
  assert.equal(h.calls.filter(([kind]) => kind === 'modify').length, 0);
  const incoming = new Uint8Array([255, 0, 3, 4]).buffer;
  await h.local.writeBinary('new.bin', incoming, 1000, { createOnly: true });
  assert.deepEqual(await h.adapter.readBinary('new.bin'), incoming); assert.ok(h.index.get('new.bin') instanceof TFile);
});

test('queued exact UTF-8 bytes remain replaceable regardless of extension, including BOM and NUL', async (t) => {
  const h = await fixture(t, { syncOnChange: true }); h.put('text.bin', bytes('\ufeffOLD\0'), 1000);
  const q = queuedAdapter(h), incoming = bytes('\ufeffREMOTE\0');
  h.vault.modifyBinary = async () => { assert.fail('the plugin must not use unchecked modifyBinary'); };
  assert.deepEqual(await h.local.writeBinaryAtomic('text.bin', incoming, 7777), { size: incoming.byteLength, mtime: 7777 });
  assert.deepEqual(await h.adapter.readBinary('text.bin'), incoming); assert.ok(q.order.includes('process:text.bin'));
  await flush(); assert.equal(h.plugin._pendingChange, false); assert.equal(h.local._writes.size, 0);
});

test('either non-UTF8 side rejects before process or modify without replacing the original bytes', async (t) => {
  const h = await fixture(t), binary = new Uint8Array([255, 0, 3, 4]).buffer;
  h.vault.modifyBinary = h.vault.process = async () => { assert.fail('unsafe replacement must not reach a mutation API'); };
  for (const [p, original, incoming] of [['old.bin', binary, bytes('TEXT')], ['new.bin', bytes('TEXT'), binary]]) {
    h.put(p, original, 1000);
    await assert.rejects(h.local.writeBinaryAtomic(p, incoming, 1000), (error) => {
      assert.equal(error.code, 'LOCAL_UNSAFE_REPLACEMENT');
      assert.match(error.message, /binary.*original retained.*new file/); return true;
    });
    assert.deepEqual(await h.adapter.readBinary(p), original);
  }
});

test('QA-LIFE-001 recovery/conflict-copy allocation must not overwrite a new user file at its selected name', async (t) => {
  const h = await fixture(t); h.put('note.md', 'LOCAL');
  const manager = new ConflictManager({ manifest: h.plugin.ledger, local: h.local, now: () => 1000 });
  const exists = h.local.exists.bind(h.local); let collided;
  h.local.exists = async (p) => {
    const result = await exists(p);
    if (!collided && p.includes('Tdrive conflict') && !result) { collided = p; h.put(p, 'USER-RECOVERY-NOTE'); }
    return result;
  };
  let failure;
  try { await manager.record('note.md', { id: 'r', contentVersion: 1 }, bytes('REMOTE')); } catch (e) { failure = e; }
  assert.ok(collided, 'collision is injected after the name check, before the supported vault commit');
  assert.equal(text(h.disk.get('note.md').data), 'LOCAL');
  assert.equal(text(h.disk.get(collided).data), 'USER-RECOVERY-NOTE', 'a recovery-copy name collision must retry allocation or stop, never replace an unrelated indexed user file');
  assert.ok(contents(h).includes('USER-RECOVERY-NOTE'), 'the only user bytes must not be deleted with the temporary backup');
  if (!failure) assert.notEqual(h.plugin.ledger.getConflict('note.md').copyPath, collided);
});

test('QA-LIFE-002 same-size same-mtime edit during awaited local replacement must survive a remote replacement', async (t) => {
  const h = await fixture(t); h.put('note.md', 'BASE', 1000);
  const modify = h.vault.modifyBinary, entered = deferred(), resume = deferred();
  const process = h.vault.process; let callbacks = 0;
  h.vault.modifyBinary = async (...args) => { entered.resolve(); await resume.promise; return modify(...args); };
  h.vault.process = async (f, transform, opts) => {
    entered.resolve(); await resume.promise;
    return process(f, (current) => { callbacks++; return transform(current); }, opts);
  };
  const writing = h.local.writeBinaryAtomic('note.md', bytes('REMO'), 1000).then((result) => ({ result }), (error) => ({ error }));
  await entered.promise;
  await modify(h.index.get('note.md'), bytes('USER'), { mtime: 1000 });
  assert.deepEqual(await h.local.stat('note.md'), { size: 4, mtime: 1000 });
  resume.resolve(); const outcome = await writing;
  assert.equal(callbacks, 1, 'the pending supported process must actually invoke its comparison callback');
  assert.ok(outcome.error, 'changed in-queue bytes must reject the incoming replacement');
  assert.ok(contents(h).includes('USER'), 'a write completed after the last hash guard; its bytes must not disappear from both target and recovery storage');
  if (outcome.result) assert.equal(text(h.disk.get('note.md').data), 'USER');
});

test('QA-LIFE-003 verified stream commit must not become a failed download solely because staging cleanup fails', async (t) => {
  const h = await fixture(t), sink = await h.local.beginStreamWrite('new.bin');
  await sink.write(bytes('VERIFIED-REMOTE'));
  const remove = h.adapter.remove;
  h.adapter.remove = async (p) => { if (p.includes('.tdrive-part-')) throw new Error('EACCES cleanup'); return remove(p); };
  let committed, failure;
  try { committed = await sink.close(); } catch (e) { failure = e; }
  assert.equal(text(h.disk.get('new.bin').data), 'VERIFIED-REMOTE');
  assert.ok(h.index.get('new.bin') instanceof TFile);
  assert.deepEqual(await h.local.stat('new.bin'), { size: 15, mtime: h.index.get('new.bin').stat.mtime });
  assert.equal(failure, undefined, 'creation, byte hash and index stats are already verified; redundant staging cleanup must not reject the irreversible commit');
  assert.equal(committed, 'new.bin');
});

test('QA-LIFE-004 malformed gzip must reject only its caller, without an uncaught writer-promise rejection', () => {
  const source = `
    const { decompressBytes, MAGIC, HEADER_LEN } = require(${JSON.stringify(require.resolve('../src/Compressor'))});
    const broken = new Uint8Array(HEADER_LEN + 3);
    broken.set(MAGIC); broken[MAGIC.length] = 1;
    new DataView(broken.buffer).setUint32(MAGIC.length + 1, 1, false);
    decompressBytes(broken.buffer).then(() => { process.exitCode = 7; }, () => { process.stdout.write('caught\\n'); });
  `;
  const child = spawnSync(process.execPath, ['--unhandled-rejections=strict', '-e', source], { encoding: 'utf8', timeout: 5000, windowsHide: true });
  assert.ifError(child.error);
  assert.equal(child.stdout, 'caught\n', 'the application catches the decompression error');
  assert.equal(child.status, 0, 'a caught malformed gzip must not also crash the host via unobserved write()/close() rejection: ' + child.stderr);
});

test('create-only copies retry collisions inside supported creation for conflict, local backup and merge draft names', async (t) => {
  const h = await fixture(t); h.put('note.md', 'LOCAL');
  const manager = new ConflictManager({ manifest: h.plugin.ledger, local: h.local, now: () => 1000 });
  const create = h.vault.createBinary;
  for (const label of ['Tdrive conflict', 'Tdrive local backup', 'Tdrive merged']) {
    const collided = []; let attempts = 0;
    h.vault.createBinary = async (p, b, opts) => {
      attempts++;
      if (attempts <= 2) { collided.push(p); h.put(p, 'INDEPENDENT-USER-' + attempts); }
      return create(p, b, opts);
    };
    const copyPath = await manager._writeCopy('note.md', label, bytes('REMOTE'));
    assert.equal(attempts, 3);
    assert.equal(text(h.disk.get(copyPath).data), 'REMOTE');
    for (const [i, p] of collided.entries()) assert.equal(text(h.disk.get(p).data), 'INDEPENDENT-USER-' + (i + 1));
  }
  assert.equal(h.calls.filter(([kind]) => kind === 'modify').length, 0);
  assert.equal(text(h.disk.get('note.md').data), 'LOCAL');
});

test('create-only copies never retry an I/O failure or publish a conflict before a verified create', async (t) => {
  const h = await fixture(t); h.put('note.md', 'LOCAL'); let attempts = 0;
  h.vault.createBinary = async () => { attempts++; throw new Error('EACCES'); };
  const manager = new ConflictManager({ manifest: h.plugin.ledger, local: h.local, now: () => 1000 });
  await assert.rejects(manager.record('note.md', { id: 'r' }, bytes('REMOTE')), /EACCES/);
  assert.equal(attempts, 1); assert.equal(h.plugin.ledger.getConflict('note.md'), undefined);
  assert.equal(text(h.disk.get('note.md').data), 'LOCAL');
});

test('queued text process compares at its in-queue read, not just before awaiting the write', async (t) => {
  const h = await fixture(t); h.put('note.md', 'BASE', 1000);
  const entered = deferred(), resume = deferred(), modify = h.vault.modifyBinary;
  let processed = 0;
  h.vault.process = async (f, transform, opts) => {
    processed++; entered.resolve(); await resume.promise;
    const current = new TextDecoder('utf-8', { ignoreBOM: true }).decode(await h.adapter.readBinary(f.path));
    const next = transform(current);
    if (next !== current) await modify(f, bytes(next), opts);
    return next;
  };
  const writing = h.local.writeBinaryAtomic('note.md', bytes('REMO'), 1000);
  const rejected = assert.rejects(writing, /changed before the queued text commit/);
  await entered.promise;
  // No event: the queue's own read must catch even a same-stat external edit before that read.
  h.put('note.md', 'USER', 1000); resume.resolve(); await rejected;
  assert.equal(processed, 1); assert.equal(text(h.disk.get('note.md').data), 'USER');
  assert.ok(contents(h).includes('BASE')); assert.equal(h.calls.filter(([kind]) => kind === 'modify').length, 0);
});

test('queued text process preserves exact UTF-8 BOM bytes and verified index/mtime on success', async (t) => {
  const h = await fixture(t); h.put('note.md', bytes('\ufeffBASE'), 1000); let processed = 0;
  const modify = h.vault.modifyBinary;
  h.vault.process = async (f, transform, opts) => {
    processed++;
    const current = new TextDecoder('utf-8', { ignoreBOM: true }).decode(await h.adapter.readBinary(f.path));
    const next = transform(current); await modify(f, bytes(next), opts); return next;
  };
  const target = bytes('\ufeffREMOTE');
  assert.deepEqual(await h.local.writeBinaryAtomic('note.md', target, 2000), { size: target.byteLength, mtime: 2000 });
  assert.equal(processed, 1); assert.deepEqual(await h.adapter.readBinary('note.md'), target);
  assert.equal(h.local._writes.size, 0); assert.ok(h.removed.every((p) => p.includes('.tdrive-backup-')));
});

test('queued text recovery snapshots every same-tick external edit with auto-sync disabled and keeps the baseline uncommitted', async (t) => {
  const h = await fixture(t); h.put('note.md', 'BASE', 1000);
  const process = h.vault.process, entered = deferred(), resume = deferred();
  h.vault.process = async (...args) => { entered.resolve(); await resume.promise; return process(...args); };
  const writing = h.local.writeBinaryAtomic('note.md', bytes('REMO'), 1000).then(() => null, (error) => error);
  await entered.promise;
  // Both events are dispatched without yielding, so deferring the snapshot read loses the first edit.
  h.put('note.md', 'EDIT', 1000); h.emit('modify', h.index.get('note.md'));
  h.put('note.md', 'USER', 1000); h.emit('modify', h.index.get('note.md'));
  resume.resolve(); const error = await writing;
  assert.match(error.message, /changed before the queued text commit/);
  assert.equal(error.recoveryPaths.length, 2);
  assert.deepEqual(new Set(error.recoveryPaths.map((p) => text(h.disk.get(p).data))), new Set(['EDIT', 'USER']));
  assert.equal(text(h.disk.get('note.md').data), 'USER');
  assert.ok(h.disk.has(error.backupPath)); assert.equal(h.local._writes.size, 0);
  assert.equal(h.local._ownChanges.has('note.md'), false); assert.equal(h.plugin._pendingChange, false);
});

test('binary replacement refuses safely even when change-event capture is installed', async (t) => {
  const h = await fixture(t); const original = new Uint8Array([255, 0, 1, 2]).buffer;
  h.put('note.bin', original);
  await assert.rejects(h.local.writeBinaryAtomic('note.bin', new Uint8Array([255, 0, 3, 4]).buffer), /cannot safely compare and replace arbitrary binary/);
  assert.deepEqual(await h.adapter.readBinary('note.bin'), original);
  assert.equal(h.calls.filter(([kind]) => kind === 'modify').length, 0); assert.equal(h.local._writes.size, 0);
});

test('a verified staging commit leaves only staging behind when cleanup fails, never removes the target on abort', async (t) => {
  const h = await fixture(t), sink = await h.local.beginStreamWrite('new.bin'); await sink.write(bytes('REMOTE'));
  const remove = h.adapter.remove;
  h.adapter.remove = async (p) => { if (p.includes('.tdrive-part-')) throw new Error('EACCES'); return remove(p); };
  assert.equal(await sink.close(), 'new.bin');
  assert.equal([...h.disk.keys()].filter((p) => p.includes('.tdrive-part-')).length, 1);
  await assert.rejects(sink.abort(), /EACCES/);
  assert.equal(text(h.disk.get('new.bin').data), 'REMOTE'); assert.ok(!h.removed.includes('new.bin'));
});

test('gzip read and writer promises drain large incompressible buffers concurrently without backpressure deadlock', () => {
  const source = `
    const assert = require('node:assert/strict');
    const { compressBytes, decompressBytes } = require(${JSON.stringify(require.resolve('../src/Compressor'))});
    const plain = new Uint8Array(4 * 1024 * 1024); let seed = 1;
    for (let i = 0; i < plain.length; i++) { seed ^= seed << 13; seed ^= seed >>> 17; seed ^= seed << 5; plain[i] = seed & 255; }
    (async () => {
      const packed = await compressBytes(plain.buffer);
      assert.deepEqual(new Uint8Array(await decompressBytes(packed)), plain);
      process.stdout.write('roundtrip\\n');
    })().catch((error) => { console.error(error); process.exitCode = 1; });
  `;
  const child = spawnSync(process.execPath, ['--unhandled-rejections=strict', '-e', source], { encoding: 'utf8', timeout: 10000, windowsHide: true });
  assert.ifError(child.error); assert.equal(child.status, 0, child.stderr); assert.equal(child.stdout, 'roundtrip\n');
});

test('gzip still rejects an incorrect declared plaintext length after all stream promises succeed', async () => {
  const packed = await compressBytes(bytes('SYNTHETIC-PLAINTEXT'));
  new DataView(packed).setUint32(MAGIC.length + 1, 1, false);
  await assert.rejects(decompressBytes(packed), /Decompression size mismatch/);
});

test('gzip writer failures reject compression and decompression even when readable consumption succeeds', () => {
  const source = `
    const assert = require('node:assert/strict');
    const { compressBytes, decompressBytes, MAGIC, HEADER_LEN } = require(${JSON.stringify(require.resolve('../src/Compressor'))});
    class FailingWriterStream {
      constructor() {
        this.writable = { getWriter: () => ({ write: () => Promise.reject(new Error('writer failed')), close: () => Promise.reject(new Error('close failed')) }) };
        this.readable = new ReadableStream({ start(controller) { controller.enqueue(new Uint8Array([65])); controller.close(); } });
      }
    }
    globalThis.CompressionStream = globalThis.DecompressionStream = FailingWriterStream;
    (async () => {
      await assert.rejects(compressBytes(new Uint8Array([65]).buffer), /writer failed/);
      const packed = new Uint8Array(HEADER_LEN); packed.set(MAGIC); packed[MAGIC.length] = 1;
      new DataView(packed.buffer).setUint32(MAGIC.length + 1, 1, false);
      await assert.rejects(decompressBytes(packed.buffer), /writer failed/);
      process.stdout.write('caught-both\\n');
    })().catch((error) => { console.error(error); process.exitCode = 1; });
  `;
  const child = spawnSync(process.execPath, ['--unhandled-rejections=strict', '-e', source], { encoding: 'utf8', timeout: 5000, windowsHide: true });
  assert.ifError(child.error); assert.equal(child.status, 0, child.stderr); assert.equal(child.stdout, 'caught-both\n');
});

test('QA-LIFE-101 control: disk adapter writes never fabricate a TFile or permit an unindexed overwrite', async (t) => {
  const h = await fixture(t); await h.adapter.writeBinary('unindexed.md', bytes('LOCAL'));
  assert.equal(h.index.has('unindexed.md'), false); assert.deepEqual(await h.local.list(), []);
  await assert.rejects(h.local.writeBinary('unindexed.md', bytes('REMOTE')), /not registered/);
  assert.equal(text(h.disk.get('unindexed.md').data), 'LOCAL');
  await h.local.writeBinary('Folder/new.md', bytes('REGISTERED'), 7777);
  assert.ok(h.index.get('Folder') instanceof TFolder); assert.ok(h.index.get('Folder/new.md') instanceof TFile);
  assert.deepEqual(h.index.get('Folder/new.md').stat, { size: 10, mtime: 7777, ctime: 7777 });
});

test('QA-LIFE-102 control: a silently corrupt backup cannot reach modifyBinary', async (t) => {
  const h = await fixture(t); h.put('note.md', 'ORIGINAL');
  const raw = h.adapter.writeBinary;
  h.adapter.writeBinary = async (p, b) => raw(p, p.includes('.tdrive-backup-') ? bytes('CORRUPTED') : b);
  await assert.rejects(h.local.writeBinaryAtomic('note.md', bytes('REMOTE')), (e) => {
    assert.match(e.message, /backup verification failed/); assert.ok(e.backupPath); return true;
  });
  assert.equal(text(h.disk.get('note.md').data), 'ORIGINAL'); assert.equal(h.calls.filter(([k]) => k === 'modify').length, 0);
});

test('QA-LIFE-103 control: backup cleanup failure after an indexed verified replacement is nonfatal', async (t) => {
  const h = await fixture(t); h.put('note.md', 'ORIGINAL');
  h.adapter.remove = async () => { throw new Error('EACCES cleanup'); };
  const result = await h.local.writeBinaryAtomic('note.md', bytes('REMOTE'), 8000);
  assert.deepEqual(result, { size: 6, mtime: 8000 }); assert.equal(text(h.disk.get('note.md').data), 'REMOTE');
  assert.ok(contents(h).includes('ORIGINAL')); assert.ok(!h.removed.includes('note.md'));
});

test('QA-LIFE-104 control: same-size same-mtime edits during backup preparation fail closed by hash', async (t) => {
  const h = await fixture(t); h.put('note.md', 'BASE', 1000); const raw = h.adapter.writeBinary;
  h.adapter.writeBinary = async (p, b) => { await raw(p, b); if (p.includes('.tdrive-backup-')) h.put('note.md', 'USER', 1000); };
  await assert.rejects(h.local.writeBinaryAtomic('note.md', bytes('REMO'), 1000), /changed while preparing/);
  assert.equal(text(h.disk.get('note.md').data), 'USER'); assert.ok(contents(h).includes('BASE'));
  assert.equal(h.calls.filter(([k]) => k === 'modify').length, 0);
});

test('QA-LIFE-105 control: quiet/busy events coalesce, own writes do not loop, delayed read completion cannot outlive unload', async (t) => {
  const h = await fixture(t, { syncOnChange: true }); let runs = 0;
  h.plugin.runSync = async () => { runs++; };
  await h.local.writeBinary('note.md', bytes('SYNC')); await flush();
  assert.equal(h.plugin._pendingChange, false);
  const work = deferred(), operation = h.plugin._withOperation(() => work.promise);
  h.put('note.md', 'USER'); await h.emit('modify', h.index.get('note.md'));
  h.plugin._quietUntil = h.clock.now + 3000;
  assert.equal(h.plugin._pendingChange, true); assert.equal(h.timers.size, 0);
  work.resolve(); await operation; await h.advance(2999); assert.equal(runs, 0);
  await h.advance(1); assert.equal(runs, 1); await h.advance(10000); assert.equal(runs, 1);
  h.local._trackOwnChange('note.md', await sha256Hex(bytes('SYNC')));
  const reading = deferred(), started = deferred(), read = h.adapter.readBinary;
  h.adapter.readBinary = async (p) => { started.resolve(); await reading.promise; return read(p); };
  const event = h.emit('modify', h.index.get('note.md')); await started.promise;
  await h.plugin.onunload(); reading.resolve(); await event; await h.advance(10000);
  assert.equal(runs, 1); assert.equal(h.plugin._pendingChange, false); assert.equal(h.timers.size, 0);
});

test('QA-LIFE-106 control: read EIO blocks saves, corrupt JSON drops destructive baselines, saved owner/journal round-trip unchanged', async () => {
  const inline = { version: 1, files: { 'note.md': { remoteId: 'r' } }, conflicts: { 'note.md': { remoteId: 'r', copyPath: 'copy.md' } },
    meta: { deviceId: '11111111-1111-4111-a111-111111111111', uploadSessionsScope: 'scope', uploadSessions: { target: { owner: '11111111-1111-4111-a111-111111111111', phase: 'uploading', sessionId: 'owned-session' } } } };
  let raw = JSON.stringify(inline), fail = false, writes = 0;
  const ledger = new ManifestStore({ read: async () => { if (fail) throw new Error('EIO'); return raw; }, write: async (s) => { writes++; raw = s; } });
  await ledger.load(); await ledger.save();
  assert.deepEqual(JSON.parse(raw).meta.uploadSessions, inline.meta.uploadSessions); assert.equal(JSON.parse(raw).meta.deviceId, inline.meta.deviceId);
  fail = true; await assert.rejects(ledger.load(), /EIO/); await assert.rejects(ledger.save(), /recovered before saving/);
  assert.equal(writes, 1); assert.ok(ledger.getConflict('note.md'));
  fail = false; raw = '{invalid-json'; await ledger.load();
  assert.deepEqual(ledger.fileKeys(), []); assert.deepEqual(ledger.conflictKeys(), []); assert.deepEqual(ledger.data.meta.deletions, []);
  assert.match(ledger.data.log[0].msg, /reset/); assert.equal(raw, '{invalid-json', 'load itself must not write the corrupt storage');
});

test('QA-LIFE-107 control: encryption settings rotate keys, old plaintext passthrough is documented, wrong-key ciphertext fails closed', async (t) => {
  const h = await fixture(t, { e2eeEnabled: true, e2eePassphrase: 'original-key', e2eeSalt: 'salt-a' });
  const original = h.plugin.makeCrypto(), plain = bytes('OLD-UNENCRYPTED-NOTE');
  const encrypted = await original.encrypt(plain); assert.equal(text(await original.decrypt(encrypted)), text(plain));
  h.plugin.settings.e2eePassphrase = 'changed-key'; h.plugin.settings.e2eeSalt = 'salt-b';
  const changed = h.plugin.makeCrypto(); assert.notEqual(changed, original);
  assert.equal(await changed.decrypt(plain), plain, 'CryptoManager56-59 and settings description explicitly preserve old plaintext; no retroactive migration is promised');
  await assert.rejects(changed.decrypt(encrypted), /wrong passphrase|corrupted/);
  h.plugin.settings.e2eePassphrase = ''; assert.throws(() => h.plugin.makeCrypto(), /passphrase is empty/);
  assert.equal(await new CryptoManager('').decrypt(plain), plain);
  const compressor = new Compressor({ minBytes: 1 });
  const long = bytes('plain repeated text\n'.repeat(400)); const packed = await compressor.maybeCompress('note.md', long);
  assert.ok(packed.byteLength < long.byteLength); assert.equal(text(await compressor.decompress(packed)), text(long));
});

test('QA-LIFE-108 control: manual guard holds through release, stale modal carries only reviewed deletion scope/identities', async (t) => {
  const h = await fixture(t), releasing = deferred(), released = deferred(); let leaseCalls = 0;
  const operation = h.plugin._withOperation(async () => {
    const engine = { running: true, releaseOperationLease: async () => { leaseCalls++; released.resolve(); await releasing.promise; } };
    h.plugin.engine = engine; h.plugin._operation.engine = engine; return 1;
  });
  await released.promise; await assert.rejects(h.plugin._withOperation(async () => 2), /already running/i);
  releasing.resolve(); assert.equal(await operation, 1); assert.equal(leaseCalls, 1); assert.equal(h.plugin._operation, null); assert.equal(h.plugin.engine.running, false);
  const pending = { reason: 'limit', items: [{ action: 'deleteRemote', path: 'reviewed.md', remoteId: 'r', contentVersion: 1 }] };
  h.plugin.ledger.data.meta.syncScope = 'reviewed-scope'; h.plugin.ledger.data.meta.blockedDeletes = pending; h.plugin.approveDeletes();
  const button = elements(h.opened.at(-1).contentEl).find((e) => typeof e.onclick === 'function'); let approved;
  h.plugin.runSync = (opts) => { approved = opts; };
  pending.items[0].remoteId = 'unreviewed'; h.plugin.ledger.data.meta.syncScope = 'new-scope'; button.onclick();
  assert.deepEqual(approved.approvedDeletes, [{ action: 'deleteRemote', path: 'reviewed.md', remoteId: 'r', contentVersion: 1 }]);
  assert.equal(approved.approvedScope, 'reviewed-scope');
});
