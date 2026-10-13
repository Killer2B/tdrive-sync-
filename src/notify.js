'use strict';
// Sync notification styles.
//
// Design goals:
//  - ONE shared icon set per state (consistent and easy to read everywhere), so the same state always
//    shows the same glyph (success, upload, download, progress, warning, error, retry, ...).
//  - Four visually distinct presets that differ only by layout / separators / verbosity:
//      technical (current, unchanged) · concise (one line) · elegant (two lines) · detailed (multi-line).
//  - Accurate numbers: every counter derives from the sync report; nothing is invented or dropped silently.
//
// This module is pure formatting: it never talks to the network and never changes the sync mechanism.

const ICONS = {
  success: '✅',
  upload: '⬆️',
  download: '⬇️',
  compare: '⇅',
  move: '🔄',
  skip: '➖',
  conflict: '⚔️',
  warning: '⚠️',
  error: '❌',
  retry: '🔁',
  progress: '⏳',
  paused: '⏸️',
  locked: '🔒',
  key: '🔑',
  clock: '🕒',
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

/** The completion/failure notice. Returns { text, timeout } (timeout 0 = Obsidian default). */
function formatResult(style, t, rep) {
  const k = counts(rep);
  const ok = rep.status === 'ok';

  if (style === 'concise') {
    if (ok) {
      const transfers = [];
      if (k.up) transfers.push(`${ICONS.upload}${k.up}`);
      if (k.down) transfers.push(`${ICONS.download}${k.down}`);
      if (k.moved) transfers.push(`${ICONS.move}${k.moved}`);
      let txt = `${ICONS.success} ${t('notify.done')}`;
      txt += transfers.length ? ` · ${transfers.join(' ')}` : ` · ${t('notify.noChanges')}`;
      if (k.conf) txt += ` · ${ICONS.conflict}${k.conf}`;
      if (k.err) txt += ` · ${ICONS.error}${k.err}`;
      return { text: txt, timeout: 6000 };
    }
    let txt = `${ICONS.error} ${statusLabel(t, rep.status)} · ${ICONS.upload}${k.up} ${ICONS.download}${k.down}`;
    const fe = firstError(rep);
    if (fe) txt += `\n${fe}`;
    return { text: txt, timeout: 10000 };
  }

  if (style === 'elegant') {
    const parts = [`${ICONS.upload} ${t('notify.up')} ${k.up}`, `${ICONS.download} ${t('notify.down')} ${k.down}`];
    if (k.moved) parts.push(`${ICONS.move} ${t('notify.moved')} ${k.moved}`);
    if (k.skip) parts.push(`${ICONS.skip} ${t('notify.skipped')} ${k.skip}`);
    if (k.conf) parts.push(`${ICONS.conflict} ${t('notify.conflicts')} ${k.conf}`);
    if (k.err) parts.push(`${ICONS.error} ${t('notify.errors')} ${k.err}`);
    if (ok) return { text: `${ICONS.success} ${t('notify.done')}\n${parts.join('  ·  ')}`, timeout: 7000 };
    let txt = `${ICONS.warning} ${t('notify.failed', { status: statusLabel(t, rep.status) })}\n${parts.join('  ·  ')}`;
    const fe = firstError(rep);
    if (fe) txt += `\n${ICONS.error} ${fe}`;
    return { text: txt, timeout: 10000 };
  }

  if (style === 'detailed') {
    const head = ok ? `${ICONS.success} ${t('notify.done')}` : `${ICONS.error} ${t('notify.failed', { status: statusLabel(t, rep.status) })}`;
    const lines = [
      head,
      `${ICONS.upload} ${t('notify.up')}: ${k.up}`,
      `${ICONS.download} ${t('notify.down')}: ${k.down}`,
      `${ICONS.move} ${t('notify.moved')}: ${k.moved}`,
      `${ICONS.skip} ${t('notify.skipped')}: ${k.skip}`,
      `${ICONS.conflict} ${t('notify.conflicts')}: ${k.conf}`,
      `${ICONS.error} ${t('notify.errors')}: ${k.err}`,
    ];
    const d = durationText(rep.durationMs);
    if (d) lines.push(`${ICONS.clock} ${d}`);
    const fe = firstError(rep);
    if (fe) lines.push(`${ICONS.error} ${fe}`);
    return { text: lines.join('\n'), timeout: ok ? 9000 : 14000 };
  }

  // technical — kept byte-for-byte identical to the original format.
  const msg = `↑${k.up} ↓${k.down} ⇄${k.moved} =${k.skip} ⚠${k.conf} ✗${k.err}`;
  if (ok) return { text: t('notice.synced', { msg }), timeout: 0 };
  return { text: t('notice.failed', { status: rep.status, msg }) + (firstError(rep) ? '\n' + firstError(rep) : ''), timeout: 10000 };
}

/** The blocked-deletions warning notice. */
function formatWarning(style, t, rep) {
  const bd = rep.blockedDeletes;
  if (!bd || !bd.items || !bd.items.length) return null;
  const n = bd.items.length;
  const reason = reasonLabel(t, bd.reason);
  if (style === 'concise') return { text: `${ICONS.warning} ${t('notify.blocked', { n })}`, timeout: 9000 };
  if (style === 'elegant') return { text: `${ICONS.warning} ${t('notify.blocked', { n })}\n${t('notify.reason')}: ${reason}`, timeout: 9000 };
  if (style === 'detailed') return { text: `${ICONS.warning} ${t('notify.blockedTitle', { n })}\n${t('notify.reason')}: ${reason}`, timeout: 12000 };
  return { text: t('notice.blockedDeletes', { n, reason: bd.reason }), timeout: 10000 };
}

/** A single retry attempt (emitted at most once per run by the caller, to stay concise). */
function formatRetry(style, t, info) {
  const st = info && info.status ? String(info.status) : t('notify.network');
  const attempt = info && info.attempt ? info.attempt : 0;
  if (style === 'concise') return { text: `${ICONS.retry} ${t('notify.retryShort')}`, timeout: 4000 };
  if (style === 'elegant') return { text: `${ICONS.retry} ${t('notify.retryShort')}  ·  ${st}`, timeout: 5000 };
  if (style === 'detailed') return { text: `${ICONS.retry} ${t('notify.retryShort')}  ·  ${st}  ·  ${t('notify.attempt', { n: attempt })}`, timeout: 6000 };
  return { text: `Tdrive: ${t('notify.retry')} (${st})`, timeout: 5000 };
}

/** Short progress text (used by the status bar). `technical` keeps the original glyphs. */
function formatProgress(style, t, pr) {
  const pct = pctOf(pr);
  if (style === 'technical') {
    if (pr && pr.bytes && pr.bytes.total) {
      const glyph = { upload: '↑', download: '↓', compare: '⇅' }[pr.action] || '';
      return `${glyph}${pct}%`;
    }
    return `${(pr && pr.done) || 0}/${(pr && pr.total) || 0}`;
  }
  if (style === 'concise') return `${ICONS.progress}${pct}%`;
  if (style === 'elegant') return `${ICONS.progress} ${pct}%`;
  return `${ICONS.progress} ${actionLabel(t, pr && pr.action)} ${pct}%`;
}

/** Simple state notices (paused / busy / no API key / locked), styled. */
function formatState(style, t, kind) {
  if (kind === 'paused') return { text: style === 'technical' ? t('notice.paused') : `${ICONS.paused} ${t('notify.state.paused')}`, timeout: 0 };
  if (kind === 'busy') return { text: style === 'technical' ? t('notice.busy') : `${ICONS.progress} ${t('notify.state.busy')}`, timeout: 0 };
  if (kind === 'noApiKey') return { text: style === 'technical' ? t('notice.noApiKey') : `${ICONS.key} ${t('notice.noApiKey')}`, timeout: 0 };
  if (kind === 'locked') return { text: style === 'technical' ? `Tdrive: ${t('notify.state.locked')}` : `${ICONS.locked} ${t('notify.state.locked')}`, timeout: 9000 };
  return { text: '', timeout: 0 };
}

/** Sample strings for every state, used by the in-settings preview. */
function samples(style, t) {
  const okRep = { status: 'ok', uploaded: 2, downloaded: 1, moved: 0, skipped: 3, conflicts: 0, errors: 0, durationMs: 1300 };
  const upRep = { status: 'ok', uploaded: 5, downloaded: 0, moved: 0, skipped: 0, conflicts: 0, errors: 0, durationMs: 800 };
  const downRep = { status: 'ok', uploaded: 0, downloaded: 4, moved: 0, skipped: 0, conflicts: 0, errors: 0, durationMs: 2100 };
  const mixedRep = { status: 'ok', uploaded: 3, downloaded: 2, moved: 1, skipped: 4, conflicts: 1, errors: 0, durationMs: 5400 };
  const errRep = { status: 'errors', uploaded: 1, downloaded: 0, moved: 0, skipped: 0, conflicts: 0, errors: 2, failures: [{ path: 'note.md', error: 'HTTP 500' }], durationMs: 3200 };
  const prog = { phase: 'sync', action: 'upload', done: 6, total: 10, bytes: { done: 6 * 1024 * 1024, total: 10 * 1024 * 1024 } };
  const warn = { blockedDeletes: { items: [{ path: 'a' }, { path: 'b' }, { path: 'c' }], reason: 'limit' } };
  return [
    { key: 'success', title: t('notify.preview.success'), ...formatResult(style, t, okRep) },
    { key: 'upload', title: t('notify.preview.upload'), ...formatResult(style, t, upRep) },
    { key: 'download', title: t('notify.preview.download'), ...formatResult(style, t, downRep) },
    { key: 'mixed', title: t('notify.preview.mixed'), ...formatResult(style, t, mixedRep) },
    { key: 'progress', title: t('notify.preview.progress'), text: formatProgress(style, t, prog), timeout: 0 },
    { key: 'warning', title: t('notify.preview.warning'), ...formatWarning(style, t, warn) },
    { key: 'error', title: t('notify.preview.error'), ...formatResult(style, t, errRep) },
    { key: 'retry', title: t('notify.preview.retry'), ...formatRetry(style, t, { attempt: 2, status: 429 }) },
    { key: 'locked', title: t('notify.preview.locked'), ...formatState(style, t, 'locked') },
  ];
}

module.exports = { ICONS, STYLES, isStyle, formatResult, formatWarning, formatRetry, formatProgress, formatState, samples };
