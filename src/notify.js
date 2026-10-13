'use strict';
// Sync notification styles.
//
// Real icons, not emoji: each state maps to a Lucide icon name (Obsidian ships the Lucide set and renders it
// via `setIcon`). The lists below are fallback chains — the first name the running Obsidian knows wins.
// Verified against Lucide: upload/download use the cloud-with-arrow icons, and cloud state icons are used
// wherever the meaning is genuinely cloud-related (verified sync, syncing, offline, cloud warning).
// GLYPH is a plain typographic fallback used only where no DOM/icons are available (tests, CLI).

const ICONS = {
  success: ['cloud-check', 'circle-check', 'check'],       // cloud + check: the sync landed on the cloud
  upload: ['cloud-upload', 'upload', 'arrow-up-from-line'], // cloud with an up arrow
  download: ['cloud-download', 'download', 'arrow-down-to-line'], // cloud with a down arrow
  compare: ['git-compare', 'refresh-cw'],
  move: ['move', 'corner-up-right', 'arrow-right-left'],
  skip: ['circle-minus', 'minus'],
  conflict: ['git-merge', 'circle-alert'],
  warning: ['cloud-alert', 'triangle-alert', 'circle-alert'],   // cloud + ! : a problem reported by the cloud
  error: ['circle-x'],
  retry: ['cloud-sync', 'refresh-cw', 'rotate-cw'],        // syncing with the cloud again
  progress: ['loader', 'loader-circle'],
  paused: ['pause'],
  locked: ['lock'],
  key: ['key', 'key-round'],
  clock: ['clock', 'timer'],
  offline: ['cloud-off', 'wifi-off'],                      // cloud crossed out: no connection
};

// Text-only fallback (typographic symbols, never emoji).
const GLYPH = {
  success: '✓', upload: '↑', download: '↓', compare: '⇅', move: '→', skip: '−', conflict: '≠',
  warning: '!', error: '✕', retry: '↻', progress: '…', paused: '‖', locked: '⊘', key: '*', clock: 't',
  offline: '∅',
};

const STYLES = ['technical', 'concise', 'elegant', 'detailed'];
const isStyle = (s) => STYLES.includes(s);

const counts = (rep) => ({
  up: rep.uploaded || 0,
  down: rep.downloaded || 0,
  moved: rep.moved || 0,
  skip: rep.skipped || 0,
  conf: rep.conflicts || 0,
  err: rep.errors || 0,
});

const firstError = (rep) => (rep.failures && rep.failures[0] && rep.failures[0].error) ? String(rep.failures[0].error) : '';

const durationText = (ms) => {
  if (!Number.isFinite(ms) || ms < 0) return '';
  const s = ms / 1000;
  return s < 10 ? s.toFixed(1) + 's' : Math.round(s) + 's';
};

const pctOf = (pr) => {
  if (pr && pr.bytes && pr.bytes.total) return Math.min(100, Math.floor((pr.bytes.done / pr.bytes.total) * 100));
  if (pr && pr.total) return Math.min(100, Math.floor(((pr.done || 0) / pr.total) * 100));
  return 0;
};

function reasonLabel(t, reason) {
  const map = { limit: 'status.reasonLimit', 'empty-local': 'status.reasonEmptyLocal', 'empty-remote': 'status.reasonEmptyRemote' };
  return map[reason] ? t(map[reason]) : String(reason || '');
}

function statusLabel(t, status) {
  const map = { offline: 'notify.state.offline', auth: 'notify.state.auth', paused: 'notify.state.paused', locked: 'notify.state.locked', busy: 'notify.state.busy', aborted: 'notify.state.aborted' };
  return map[status] ? t(map[status]) : String(status || '');
}

function actionLabel(t, action) {
  const map = { upload: 'notify.action.upload', download: 'notify.action.download', compare: 'notify.action.compare' };
  return map[action] ? t(map[action]) : t('notify.action.sync');
}

/** Icon for a failed run: offline/network shows a crossed-out cloud, auth shows a key, otherwise the error icon. */
function statusIcon(status) {
  if (status === 'offline') return 'offline';
  if (status === 'auth') return 'key';
  if (status === 'aborted' || status === 'paused') return 'warning';
  return 'error';
}

/** Renders the structured rows to plain text (fallback / log / tests): "<glyph> text" per segment. */
function rowsToPlain(rows) {
  if (!rows) return '';
  return rows.map((row) => row.map((seg) => (seg.icon ? (GLYPH[seg.icon] || '') + ' ' : '') + seg.text).join('  ')).join('\n');
}

const seg = (icon, text) => ({ icon, text });

/** The completion/failure notice. Returns { style, text, rows, timeout }.
 *  `rows` is null only for the technical style (text-only, byte-identical to the original). */
function formatResult(style, t, rep) {
  const k = counts(rep);
  const ok = rep.status === 'ok';

  if (style === 'technical') {
    const msg = `↑${k.up} ↓${k.down} ⇄${k.moved} =${k.skip} ⚠${k.conf} ✗${k.err}`;
    const text = ok ? t('notice.synced', { msg }) : t('notice.failed', { status: rep.status, msg }) + (firstError(rep) ? '\n' + firstError(rep) : '');
    return { style, text, rows: null, timeout: ok ? 0 : 10000 };
  }

  if (style === 'concise') {
    if (ok) {
      const row = [seg('success', t('notify.done'))];
      if (k.up) row.push(seg('upload', String(k.up)));
      if (k.down) row.push(seg('download', String(k.down)));
      if (k.moved) row.push(seg('move', String(k.moved)));
      if (!k.up && !k.down && !k.moved) row.push(seg(null, t('notify.noChanges')));
      if (k.conf) row.push(seg('conflict', String(k.conf)));
      if (k.err) row.push(seg('error', String(k.err)));
      const rows = [row];
      return { style, rows, text: rowsToPlain(rows), timeout: 6000 };
    }
    const rows = [[seg(statusIcon(rep.status), statusLabel(t, rep.status)), seg('upload', String(k.up)), seg('download', String(k.down))]];
    const fe = firstError(rep);
    if (fe) rows.push([seg(null, fe)]);
    return { style, rows, text: rowsToPlain(rows), timeout: 10000 };
  }

  if (style === 'elegant') {
    const details = [seg('upload', `${t('notify.up')} ${k.up}`), seg('download', `${t('notify.down')} ${k.down}`)];
    if (k.moved) details.push(seg('move', `${t('notify.moved')} ${k.moved}`));
    if (k.skip) details.push(seg('skip', `${t('notify.skipped')} ${k.skip}`));
    if (k.conf) details.push(seg('conflict', `${t('notify.conflicts')} ${k.conf}`));
    if (k.err) details.push(seg('error', `${t('notify.errors')} ${k.err}`));
    const head = ok ? seg('success', t('notify.done')) : seg(statusIcon(rep.status), t('notify.failed', { status: statusLabel(t, rep.status) }));
    const rows = [head ? [head] : [], details];
    const fe = firstError(rep);
    if (fe) rows.push([seg(null, fe)]);
    return { style, rows, text: rowsToPlain(rows), timeout: ok ? 7000 : 10000 };
  }

  // detailed
  const rows = [[ok ? seg('success', t('notify.done')) : seg(statusIcon(rep.status), t('notify.failed', { status: statusLabel(t, rep.status) }))]];
  rows.push([seg('upload', `${t('notify.up')}: ${k.up}`)]);
  rows.push([seg('download', `${t('notify.down')}: ${k.down}`)]);
  rows.push([seg('move', `${t('notify.moved')}: ${k.moved}`)]);
  rows.push([seg('skip', `${t('notify.skipped')}: ${k.skip}`)]);
  rows.push([seg('conflict', `${t('notify.conflicts')}: ${k.conf}`)]);
  rows.push([seg('error', `${t('notify.errors')}: ${k.err}`)]);
  const d = durationText(rep.durationMs);
  if (d) rows.push([seg('clock', d)]);
  const fe = firstError(rep);
  if (fe) rows.push([seg(null, fe)]);
  return { style, rows, text: rowsToPlain(rows), timeout: ok ? 9000 : 14000 };
}

/** The blocked-deletions warning notice. */
function formatWarning(style, t, rep) {
  const bd = rep.blockedDeletes;
  if (!bd || !bd.items || !bd.items.length) return null;
  const n = bd.items.length;
  const reason = reasonLabel(t, bd.reason);
  if (style === 'technical') return { style, text: t('notice.blockedDeletes', { n, reason: bd.reason }), rows: null, timeout: 10000 };
  const count = { style, rows: [[seg('warning', t('notify.blocked', { n }))]], text: '', timeout: 9000 };
  if (style === 'concise') { count.text = rowsToPlain(count.rows); return count; }
  count.rows.push([seg(null, `${t('notify.reason')}: ${reason}`)]);
  count.text = rowsToPlain(count.rows);
  if (style === 'detailed') { count.timeout = 12000; count.rows[0] = [seg('warning', t('notify.blockedTitle', { n }))]; count.text = rowsToPlain(count.rows); }
  return count;
}

/** A single retry attempt (emitted at most once per run by the caller, to stay concise). */
function formatRetry(style, t, info) {
  const st = info && info.status ? String(info.status) : t('notify.network');
  const attempt = (info && info.attempt) || 0;
  if (style === 'technical') return { style, text: `Tdrive: ${t('notify.retry')} (${st})`, rows: null, timeout: 5000 };
  let row;
  if (style === 'concise') row = [seg('retry', t('notify.retryShort'))];
  else if (style === 'elegant') row = [seg('retry', t('notify.retryShort')), seg(null, st)];
  else row = [seg('retry', t('notify.retryShort')), seg(null, st), seg(null, t('notify.attempt', { n: attempt }))];
  const rows = [row];
  return { style, rows, text: rowsToPlain(rows), timeout: style === 'detailed' ? 6000 : (style === 'elegant' ? 5000 : 4000) };
}

/** Short progress indicator ({ icon, text }) used by the status bar. `technical` keeps the original glyphs. */
function formatProgress(style, t, pr) {
  const pct = pctOf(pr);
  if (style === 'technical') {
    if (pr && pr.bytes && pr.bytes.total) {
      const glyph = { upload: '↑', download: '↓', compare: '⇅' }[pr.action] || '';
      return { icon: null, text: `${glyph}${pct}%` };
    }
    return { icon: null, text: `${(pr && pr.done) || 0}/${(pr && pr.total) || 0}` };
  }
  if (style === 'concise') return { icon: 'progress', text: `${pct}%` };
  if (style === 'elegant') return { icon: 'progress', text: `${pct}%` };
  return { icon: 'progress', text: `${actionLabel(t, pr && pr.action)} ${pct}%` };
}

/** Simple state notices (paused / busy / no API key / locked), styled with the matching state icon. */
function formatState(style, t, kind) {
  const spec = {
    paused: { icon: 'paused', key: 'notice.paused' },
    busy: { icon: 'progress', key: 'notice.busy' },
    noApiKey: { icon: 'key', key: 'notice.noApiKey' },
    locked: { icon: 'locked', key: 'notice.paused' },
  }[kind];
  if (!spec) return { style, text: '', rows: null, timeout: 0 };
  if (kind === 'locked') {
    const plain = style === 'technical' ? `Tdrive: ${t('notify.state.locked')}` : t('notify.state.locked');
    if (style === 'technical') return { style, text: plain, rows: null, timeout: 9000 };
    const rows = [[seg('locked', t('notify.state.locked'))]];
    return { style, rows, text: rowsToPlain(rows), timeout: 9000 };
  }
  const label = t(spec.key);
  if (style === 'technical') return { style, text: label, rows: null, timeout: 0 };
  const rows = [[seg(spec.icon, label)]];
  return { style, rows, text: rowsToPlain(rows), timeout: 0 };
}

/** Sample items for every state, used by the in-settings preview. */
function samples(style, t) {
  const okRep = { status: 'ok', uploaded: 2, downloaded: 1, moved: 0, skipped: 3, conflicts: 0, errors: 0, durationMs: 1300 };
  const upRep = { status: 'ok', uploaded: 5, downloaded: 0, moved: 0, skipped: 0, conflicts: 0, errors: 0, durationMs: 800 };
  const downRep = { status: 'ok', uploaded: 0, downloaded: 4, moved: 0, skipped: 0, conflicts: 0, errors: 0, durationMs: 2100 };
  const mixedRep = { status: 'ok', uploaded: 3, downloaded: 2, moved: 1, skipped: 4, conflicts: 1, errors: 0, durationMs: 5400 };
  const errRep = { status: 'errors', uploaded: 1, downloaded: 0, moved: 0, skipped: 0, conflicts: 0, errors: 2, failures: [{ path: 'note.md', error: 'HTTP 500' }], durationMs: 3200 };
  const prog = { phase: 'sync', action: 'upload', done: 6, total: 10, bytes: { done: 6 * 1024 * 1024, total: 10 * 1024 * 1024 } };
  const warn = { blockedDeletes: { items: [{ path: 'a' }, { path: 'b' }, { path: 'c' }], reason: 'limit' } };
  const pr = formatProgress(style, t, prog);
  const warnN = formatWarning(style, t, warn);
  return [
    { key: 'success', title: t('notify.preview.success'), ...formatResult(style, t, okRep) },
    { key: 'upload', title: t('notify.preview.upload'), ...formatResult(style, t, upRep) },
    { key: 'download', title: t('notify.preview.download'), ...formatResult(style, t, downRep) },
    { key: 'mixed', title: t('notify.preview.mixed'), ...formatResult(style, t, mixedRep) },
    { key: 'progress', title: t('notify.preview.progress'), text: pr.text, rows: [[{ icon: pr.icon, text: pr.text }]], timeout: 0 },
    { key: 'warning', title: t('notify.preview.warning'), ...(warnN || { text: '', rows: [] }) },
    { key: 'error', title: t('notify.preview.error'), ...formatResult(style, t, errRep) },
    { key: 'retry', title: t('notify.preview.retry'), ...formatRetry(style, t, { attempt: 2, status: 429 }) },
    { key: 'locked', title: t('notify.preview.locked'), ...formatState(style, t, 'locked') },
  ];
}

module.exports = { ICONS, GLYPH, STYLES, isStyle, rowsToPlain, formatResult, formatWarning, formatRetry, formatProgress, formatState, samples };
