'use strict';
// Sync notification styles: shared icon set (real icon names, never emoji), four visually distinct presets,
// accurate numbers, Arabic + English.
const test = require('node:test');
const assert = require('node:assert/strict');
const NOTIFY = require('../src/notify');
const { makeT } = require('../src/i18n');

const ar = makeT('ar'); const en = makeT('en');
// Emoji proper (pictographs/keycap/variation selectors) — NOT the plain typographic symbols the technical style uses.
const EMOJI = /[\u{1F000}-\u{1FAFF}\u{20E3}\u{FE0F}]/u;
// The exact emoji that the rejected first version used, which must never come back.
const BANNED = ['✅', '⬆️', '⬇️', '🔄', '➖', '⚔️', '❌', '🔁', '⏳', '⏸️', '🔒', '🔑', '🕒'];
const noEmoji = (s, where) => { assert.ok(!EMOJI.test(s), where + ' must not contain emoji: ' + s); for (const b of BANNED) assert.ok(!String(s).includes(b), where + ' must not use ' + b); };
const okRep = { status: 'ok', uploaded: 2, downloaded: 1, moved: 0, skipped: 3, conflicts: 0, errors: 0, durationMs: 1300 };
const errRep = { status: 'errors', uploaded: 1, downloaded: 2, moved: 0, skipped: 0, conflicts: 0, errors: 2, failures: [{ path: 'x.md', error: 'HTTP 500' }], durationMs: 3200 };

const iconsOf = (fmt) => (fmt.rows || []).flat().map((s) => s.icon).filter(Boolean);

test('four styles exist and are unique', () => {
  assert.deepEqual(NOTIFY.STYLES, ['technical', 'concise', 'elegant', 'detailed']);
  assert.ok(NOTIFY.isStyle('elegant') && !NOTIFY.isStyle('fancy'));
});

test('icon set uses real icon names, never emoji', () => {
  for (const [state, names] of Object.entries(NOTIFY.ICONS)) {
    assert.ok(Array.isArray(names) && names.length, state);
    for (const n of names) assert.match(n, /^[a-z0-9]+(-[a-z0-9]+)*$/, `${state}: ${n} must be a Lucide-style name`);
  }
  for (const [state, g] of Object.entries(NOTIFY.GLYPH)) noEmoji(g, 'glyph for ' + state);
  assert.ok(!EMOJI.test('↑↓⇄=⚠✗'), 'technical glyphs stay typographic');
});

test('technical style is byte-for-byte the original message and text-only', () => {
  const f = NOTIFY.formatResult('technical', ar, okRep);
  assert.equal(f.text, 'Tdrive: اكتملت المزامنة ↑2 ↓1 ⇄0 =3 ⚠0 ✗0');
  assert.equal(f.rows, null, 'technical shows no icons');
  assert.equal(NOTIFY.formatResult('technical', en, okRep).text, 'Tdrive: sync complete ↑2 ↓1 ⇄0 =3 ⚠0 ✗0');
  assert.equal(NOTIFY.formatResult('technical', ar, errRep).text, 'Tdrive: لم تكتمل المزامنة (errors) ↑1 ↓2 ⇄0 =0 ⚠0 ✗2\nHTTP 500');
});

test('every style keeps the numbers accurate and shows no emoji', () => {
  for (const style of NOTIFY.STYLES) {
    const { text } = NOTIFY.formatResult(style, ar, okRep);
    assert.match(text, /2/, style + ' should mention 2 uploads');
    assert.match(text, /1/, style + ' should mention the download');
    assert.ok(!/5|9/.test(text), style + ' must not invent numbers');
  }
  for (const style of ['technical', 'elegant', 'detailed']) {
    assert.match(NOTIFY.formatResult(style, ar, okRep).text, /3/, style + ' should mention 3 skipped');
  }
});

test('styles are visually distinct (no two identical)', () => {
  const seen = new Set(NOTIFY.STYLES.map((s) => NOTIFY.formatResult(s, ar, okRep).text));
  assert.equal(seen.size, NOTIFY.STYLES.length);
});

test('each style is recognisable by layout and icons', () => {
  const conc = NOTIFY.formatResult('concise', ar, okRep);
  const eleg = NOTIFY.formatResult('elegant', ar, okRep);
  const det = NOTIFY.formatResult('detailed', ar, okRep);

  assert.equal(conc.rows.length, 1, 'concise is a single line');
  assert.deepEqual(iconsOf(conc)[0], 'success');
  assert.ok(iconsOf(conc).includes('upload') && iconsOf(conc).includes('download'));

  assert.equal(eleg.rows.length, 2, 'elegant is two lines');
  assert.deepEqual(iconsOf(eleg)[0], 'success');
  assert.ok(iconsOf(eleg).includes('upload'), 'elegant carries state icons in the detail line');

  assert.ok(det.rows.length >= 7, 'detailed lists every counter on its own line');
  for (const need of ['success', 'upload', 'download', 'move', 'skip', 'conflict', 'error', 'clock']) {
    assert.ok(iconsOf(det).includes(need), 'detailed missing ' + need);
  }
});

test('concise says "no changes" when nothing happened', () => {
  const none = { status: 'ok', uploaded: 0, downloaded: 0, moved: 0, skipped: 0, conflicts: 0, errors: 0 };
  assert.match(NOTIFY.formatResult('concise', ar, none).text, /لا تغييرات/);
  assert.deepEqual(iconsOf(NOTIFY.formatResult('concise', ar, none)), ['success']);
});

test('success and error use the same icon in every styled preset', () => {
  for (const s of ['concise', 'elegant', 'detailed']) {
    assert.equal(iconsOf(NOTIFY.formatResult(s, ar, okRep))[0], 'success', s);
    assert.equal(iconsOf(NOTIFY.formatResult(s, ar, errRep))[0], 'error', s);
  }
});

test('warning / retry / progress / state use the shared icons', () => {
  const warn = { blockedDeletes: { items: [{}, {}, {}], reason: 'limit' } };
  for (const s of NOTIFY.STYLES) {
    const w = NOTIFY.formatWarning(s, ar, warn);
    assert.ok(w && w.text, s);
    assert.equal(NOTIFY.formatWarning(s, ar, { blockedDeletes: null }), null);
  }
  assert.deepEqual(iconsOf(NOTIFY.formatWarning('concise', ar, warn)), ['warning']);
  assert.match(NOTIFY.formatWarning('concise', ar, warn).text, /3/);

  assert.deepEqual(iconsOf(NOTIFY.formatRetry('concise', ar, { attempt: 2, status: 429 })), ['retry']);
  assert.equal(NOTIFY.formatRetry('technical', ar, { attempt: 2, status: 429 }).text, 'Tdrive: إعادة المحاولة (429)');

  assert.deepEqual(NOTIFY.formatProgress('technical', ar, { action: 'upload', bytes: { done: 5e6, total: 1e7 } }), { icon: null, text: '↑50%' });
  assert.deepEqual(NOTIFY.formatProgress('concise', ar, { action: 'upload', bytes: { done: 5e6, total: 1e7 } }), { icon: 'progress', text: '50%' });
  assert.equal(NOTIFY.formatProgress('technical', ar, { done: 2, total: 5 }).text, '2/5');

  assert.deepEqual(iconsOf(NOTIFY.formatState('concise', ar, 'locked')), ['locked']);
  assert.deepEqual(iconsOf(NOTIFY.formatState('elegant', ar, 'noApiKey')), ['key']);
  const pausedTech = NOTIFY.formatState('technical', ar, 'paused');
  assert.equal(pausedTech.rows, null, 'technical state notice has no icons');
  assert.equal(pausedTech.text, ar('notice.paused'));
});

test('upload and download use cloud-with-arrow icons, and other real cloud icons are used where meaningful', () => {
  assert.match(NOTIFY.ICONS.upload[0], /^cloud-upload/, 'upload must use a cloud with an up arrow');
  assert.match(NOTIFY.ICONS.download[0], /^cloud-download/, 'download must use a cloud with a down arrow');
  assert.equal(NOTIFY.ICONS.success[0], 'cloud-check', 'a completed cloud sync shows a verified cloud');
  assert.equal(NOTIFY.ICONS.offline[0], 'cloud-off', 'offline shows a crossed-out cloud');
  assert.equal(NOTIFY.ICONS.retry[0], 'cloud-sync', 'a retry shows syncing with the cloud');
  assert.equal(NOTIFY.ICONS.warning[0], 'cloud-alert', 'a cloud warning shows the cloud + alert icon');
  // No state may fall back to a bare arrow as its primary icon any more.
  for (const [state, chain] of Object.entries(NOTIFY.ICONS)) {
    if (state === 'compare' || state === 'move') continue;
    assert.notEqual(chain[0], 'arrow-up', state + ' primary must not be a bare arrow');
    assert.notEqual(chain[0], 'arrow-down', state + ' primary must not be a bare arrow');
  }
});

test('a network/offline failure uses the crossed-out cloud icon in every styled preset', () => {
  const off = { status: 'offline', uploaded: 0, downloaded: 0, moved: 0, skipped: 0, conflicts: 0, errors: 1, failures: [{ error: 'network' }] };
  for (const s of ['concise', 'elegant', 'detailed']) assert.equal(iconsOf(NOTIFY.formatResult(s, ar, off))[0], 'offline', s);
  const auth = { status: 'auth', uploaded: 0, downloaded: 0, moved: 0, skipped: 0, conflicts: 0, errors: 1, failures: [{ error: 'HTTP 401' }] };
  for (const s of ['concise', 'elegant', 'detailed']) assert.equal(iconsOf(NOTIFY.formatResult(s, ar, auth))[0], 'key', s);
});

test('preview samples cover every requested state, in both languages', () => {
  const keys = NOTIFY.samples('elegant', ar).map((s) => s.key);
  for (const k of ['success', 'upload', 'download', 'progress', 'warning', 'error', 'retry']) assert.ok(keys.includes(k), k);
  for (const s of NOTIFY.samples('detailed', en)) assert.ok(s.text && s.title, s.key);
  for (const s of NOTIFY.samples('concise', ar)) noEmoji(s.text, s.key);
});
