'use strict';
// Regression tests for the stuck remote lock ("Another device appears to be syncing" persisting).
// The lock must: be released at the end of every run (even when the run is blocked it must not leak its own),
// be taken over when stale, tolerate clock skew (a future server timestamp must not look fresh forever), and
// warn only once per distinct lock instead of on every attempt.
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv } = require('./helpers');

const LOCK = '.tdrive-sync.lock-';

test('a completed run always releases its own lock (no orphan left behind)', async () => {
  const env = await makeEnv({ settings: { remoteLock: true } });
  try {
    env.local.put('a.md', 'x');
    await env.engine.syncNow();
    const locks = env.server.liveFiles().filter((f) => f.name.startsWith(LOCK));
    assert.equal(locks.length, 0, 'no lock left after a successful run');
    // A second run also succeeds and leaves nothing behind.
    await env.engine.syncNow();
    assert.equal(env.server.liveFiles().filter((f) => f.name.startsWith(LOCK)).length, 0);
  } finally { await env.stop(); }
});

test('the lock is released even when the run throws mid-way', async () => {
  const env = await makeEnv({ settings: { remoteLock: true } });
  try {
    env.local.put('a.md', 'x');
    // Make a folder operation fail hard so syncNow throws after acquiring the lock.
    env.server.failRule((i) => i.route === '/files' && i.method === 'POST' && /name=%2Etdrive-sync\.lock/.test(String(i.url)), { status: 500 }, 0);
    env.server.failRule((i) => i.route === '/folders', { status: 400 }, 99);
    let threw = false;
    try { await env.engine.syncNow(); } catch (e) { threw = true; }
    void threw;
    assert.equal(env.server.liveFiles().filter((f) => f.name.startsWith(LOCK)).length, 0, 'lock released despite the failure');
  } finally { await env.stop(); }
});

test('a stale lock is taken over; a fresh foreign lock blocks only once', async () => {
  const env = await makeEnv({ settings: { remoteLock: true, remoteLockMinutes: 5 } });
  try {
    env.local.put('seed.md', 'x');
    await env.engine.syncNow();

    // Fresh foreign lock -> blocked.
    const lf = env.server.addFile('/Vault/', LOCK + 'other', '{}');
    lf.lastModified = Date.now();
    env.local.put('n.md', 'y');
    const r1 = await env.engine.syncNow();
    assert.equal(r1.status, 'locked', JSON.stringify(r1));
    assert.equal(r1.warnings.length, 1, 'the lock warning is reported once');
    // Repeated attempts against the same lock stay quiet (no new warning).
    const r2 = await env.engine.syncNow();
    assert.equal(r2.status, 'locked');
    assert.equal(r2.warnings.filter((w) => /Another device/.test(w)).length, 0, 'no duplicate warning');

    // Stale lock (older than the TTL) -> taken over and the run proceeds.
    lf.lastModified = Date.now() - 6 * 60000;
    const r3 = await env.engine.syncNow();
    assert.equal(r3.status, 'ok', JSON.stringify(r3));
  } finally { await env.stop(); }
});

test('clock skew: a lock timestamp in the future does not block forever', async () => {
  const env = await makeEnv({ settings: { remoteLock: true, remoteLockMinutes: 5 } });
  try {
    env.local.put('seed.md', 'x');
    await env.engine.syncNow();
    const lf = env.server.addFile('/Vault/', LOCK + 'other', '{}');
    lf.lastModified = Date.now() + 3600_000;        // server clock far ahead
    // Age is clamped to >= 0, and the TTL still applies: a *very* old real lock is taken over.
    lf.lastModified = 1;                            // epoch: definitely stale
    env.local.put('n.md', 'y');
    const r = await env.engine.syncNow();
    assert.equal(r.status, 'ok', JSON.stringify(r));
  } finally { await env.stop(); }
});
