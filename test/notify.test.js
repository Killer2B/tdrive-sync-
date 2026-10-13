'use strict';
// Sync notification styles: shared icons, four visually distinct presets, accurate numbers, Arabic + English.
const test = require('node:test');
const assert = require('node:assert/strict');
const NOTIFY = require('../src/notify');
const { makeT } = require('../src/i18n');

const ar = makeT('ar'); const en = makeT('en');
const okRep = { status: 'ok', uploaded: 2, downloaded: 1, moved: 0, skipped: 3, conflicts: 0, errors: 0, durationMs: 1300 };
const errRep = { status: 'errors', uploaded: 1, downloaded: 2, moved: 0, skipped: 0, conflicts: 0, errors: 2, failures: [{ path: 'x.md', error: 'HTTP 500' }], durationMs: 3200 };

test('four styles exist and are unique', () => {
  assert.deepEqual(NOTIFY.STYLES, ['technical', 'concise', 'elegant', 'detailed']);
  assert.ok(NOTIFY.isStyle('elegant') && !NOTIFY.isStyle('fancy'));
});

test('technical style is byte-for-byte the original message', () => {
  assert.equal(NOTIFY.formatResult('technical', ar, okRep).text, 'Tdrive: اكتملت المزامنة ↑2 ↓1 ⇄0 =3 ⚠0 ✗0');
  assert.equal(NOTIFY.formatResult('technical', en, okRep).text, 'Tdrive: sync complete ↑2 ↓1 ⇄0 =3 ⚠0 ✗0');
  const fail = NOTIFY.formatResult('technical', ar, errRep).text;
  assert.equal(fail, 'Tdrive: لم تكتمل المزامنة (errors) ↑1 ↓2 ⇄0 =0 ⚠0 ✗2\nHTTP 500');
});

test('every style keeps the numbers accurate', () => {
  for (const style of NOTIFY.STYLES) {
    const { text } = NOTIFY.formatResult(style, ar, okRep);
    assert.match(text, /2/, style + ' should mention 2 uploads');
    assert.match(text, /1/, style + ' should mention the download');
    assert.ok(!/5|9/.test(text), style + ' must not invent numbers');
  }
  // The full counters appear in every style except the deliberately terse "concise" one.
  for (const style of ['technical', 'elegant', 'detailed']) {
    assert.match(NOTIFY.formatResult(style, ar, okRep).text, /3/, style + ' should mention 3 skipped');
  }
});

test('styles are visually distinct (no two identical)', () => {
  const seen = new Set(NOTIFY.STYLES.map((s) => NOTIFY.formatResult(s, ar, okRep).text));
  assert.equal(seen.size, NOTIFY.STYLES.length);
});

test('each style is recognisable: line count and icons', () => {
  const tech = NOTIFY.formatResult('technical', ar, okRep).text;
  const conc = NOTIFY.formatResult('concise', ar, okRep).text;
  const eleg = NOTIFY.formatResult('elegant', ar, okRep).text;
  const det = NOTIFY.formatResult('detailed', ar, okRep).text;

  assert.equal(tech.split('\n').length, 1);
  assert.equal(conc.split('\n').length, 1);
  assert.match(conc, /✅/);
  assert.equal(eleg.split('\n').length, 2);
  assert.match(eleg, /·/);
  assert.ok(det.split('\n').length >= 7, 'detailed lists every counter on its own line');
  assert.match(det, /🕒/);
});

test('concise says "no changes" when nothing happened', () => {
  const none = { status: 'ok', uploaded: 0, downloaded: 0, moved: 0, skipped: 0, conflicts: 0, errors: 0 };
  assert.match(NOTIFY.formatResult('concise', ar, none).text, /لا تغييرات/);
});

test('success/error icons are consistent across styles', () => {
  assert.match(NOTIFY.formatResult('concise', ar, okRep).text, /✅/);
  assert.match(NOTIFY.formatResult('elegant', ar, okRep).text, /✅/);
  assert.match(NOTIFY.formatResult('detailed', ar, okRep).text, /✅/);
  for (const s of ['concise', 'elegant', 'detailed']) assert.match(NOTIFY.formatResult(s, ar, errRep).text, /❌/, s);
});

test('warning / retry / progress / state use the shared icons', () => {
  const warn = { blockedDeletes: { items: [{}, {}, {}], reason: 'limit' } };
  for (const s of NOTIFY.STYLES) {
    const w = NOTIFY.formatWarning(s, ar, warn);
    assert.ok(w && w.text, s);
    assert.equal(NOTIFY.formatWarning(s, ar, { blockedDeletes: null }), null);
  }
  assert.match(NOTIFY.formatWarning('concise', ar, warn).text, /⚠️/);
  assert.match(NOTIFY.formatWarning('concise', ar, warn).text, /3/);
  assert.match(NOTIFY.formatRetry('concise', ar, { attempt: 2, status: 429 }).text, /🔁/);
  assert.match(NOTIFY.formatRetry('technical', ar, { attempt: 2, status: 429 }).text, /Tdrive: إعادة المحاولة \(429\)/);
  assert.equal(NOTIFY.formatProgress('technical', ar, { action: 'upload', bytes: { done: 5e6, total: 1e7 } }), '↑50%');
  assert.equal(NOTIFY.formatProgress('concise', ar, { action: 'upload', bytes: { done: 5e6, total: 1e7 } }), '⏳50%');
  assert.equal(NOTIFY.formatProgress('technical', ar, { done: 2, total: 5 }), '2/5');
  assert.match(NOTIFY.formatState('concise', ar, 'locked').text, /🔒/);
  assert.match(NOTIFY.formatState('technical', ar, 'paused').text, /متوقفة/);
});

test('preview samples cover every requested state', () => {
  const keys = NOTIFY.samples('elegant', ar).map((s) => s.key);
  for (const k of ['success', 'upload', 'download', 'progress', 'warning', 'error', 'retry']) assert.ok(keys.includes(k), k);
  for (const s of NOTIFY.samples('detailed', en)) assert.ok(s.text && s.title, s.key);
});
