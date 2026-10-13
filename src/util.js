'use strict';
// Pure helpers. No Node-only APIs: works in Obsidian Android/Windows webviews and Node 18+.

function isArrayBuffer(x) { return Object.prototype.toString.call(x) === '[object ArrayBuffer]'; }

function asArrayBuffer(data) {
  if (isArrayBuffer(data)) return data;
  if (ArrayBuffer.isView(data)) return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
  if (typeof data === 'string') return new TextEncoder().encode(data).buffer;
  throw new TypeError('Unsupported binary type');
}

function toHex(buf) {
  const u = new Uint8Array(buf);
  let s = '';
  for (let i = 0; i < u.length; i++) s += u[i].toString(16).padStart(2, '0');
  return s;
}

async function sha256Hex(data) {
  return toHex(await crypto.subtle.digest('SHA-256', asArrayBuffer(data)));
}

function uuidFromHex(h) {
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-a${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

function randomUuid() {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) return crypto.randomUUID();
  const b = new Uint8Array(16);
  crypto.getRandomValues(b);
  return uuidFromHex(toHex(b.buffer));
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nfc = (s) => s.normalize('NFC');

function splitPath(p) { return p.split('/').filter((x) => x.length > 0); }

function splitRel(rel) {
  const i = rel.lastIndexOf('/');
  return i < 0 ? { dir: '', name: rel } : { dir: rel.slice(0, i), name: rel.slice(i + 1) };
}

// Remote folder path in the form "/a/b/" (documented example: path=/docs/).
function folderPathFor(root, relDir) {
  const parts = [...splitPath(root || ''), ...splitPath(relDir || '')];
  return parts.length ? '/' + parts.join('/') + '/' : '/';
}

function globToRegExp(glob) {
  let re = '';
  for (let i = 0; i < glob.length; i++) {
    const c = glob[i];
    if (c === '*') {
      if (glob[i + 1] === '*') {
        if (glob[i + 2] === '/') { re += '(?:.*/)?'; i += 2; } else { re += '.*'; i += 1; }
      } else re += '[^/]*';
    } else if (c === '?') re += '[^/]';
    else re += c.replace(/[.+^${}()|[\]\\]/g, '\\$&');
  }
  return new RegExp('^' + re + '$');
}

// Hidden segments (".obsidian", ".git", ".trash"...) are always excluded.
function makeExcluder(patterns) {
  const res = (patterns || []).map((p) => p.trim()).filter(Boolean).map(globToRegExp);
  return (rel) => {
    if (splitPath(rel).some((seg) => seg.startsWith('.'))) return true;
    return res.some((r) => r.test(rel));
  };
}

const KEY_RE = /tdk_[A-Za-z0-9_\-]+/g;
function redact(text, secret) {
  let s = String(text == null ? '' : text);
  if (secret && secret.length >= 6) s = s.split(secret).join('***');
  return s.replace(KEY_RE, 'tdk_***');
}

function pad(n) { return String(n).padStart(2, '0'); }
function stamp(ms) {
  const d = new Date(ms);
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}-${pad(d.getMinutes())}-${pad(d.getSeconds())}`;
}

function suffixName(rel, label, ms) {
  const { dir, name } = splitRel(rel);
  const dot = name.lastIndexOf('.');
  const base = dot > 0 ? name.slice(0, dot) : name;
  const ext = dot > 0 ? name.slice(dot) : '';
  return (dir ? dir + '/' : '') + `${base} (${label} ${stamp(ms)})${ext}`;
}

function concatBuffers(chunks) {
  let total = 0;
  for (const c of chunks) total += c.byteLength;
  const out = new Uint8Array(total);
  let o = 0;
  for (const c of chunks) { out.set(new Uint8Array(c), o); o += c.byteLength; }
  return out.buffer;
}

function hexToBytes(hex) {
  const s = String(hex == null ? '' : hex);
  const n = s.length >> 1; const out = new Uint8Array(n);
  for (let i = 0; i < n; i++) out[i] = parseInt(s.substr(i * 2, 2), 16) || 0;
  return out;
}

/** SHA-256 of a region of an ArrayBuffer WITHOUT copying it (crypto.subtle accepts a typed-array view). */
async function sha256HexView(buffer, offset, length) {
  const view = new Uint8Array(buffer, offset, length);
  return toHex(await crypto.subtle.digest('SHA-256', view));
}

const clamp = (n, lo, hi) => Math.min(hi, Math.max(lo, n));

/** Runs `worker(item, index)` over `items` with at most `limit` in flight. Result order matches input.
 *  Stops pulling new work after the first rejection and rethrows it (already-started work settles). */
async function pool(items, limit, worker) {
  const results = new Array(items.length);
  const size = Math.max(1, Math.min(Math.floor(limit) || 1, items.length || 1));
  let next = 0; let aborted = false;
  const runner = async () => {
    while (!aborted) {
      const i = next++;
      if (i >= items.length) return;
      try { results[i] = await worker(items[i], i); }
      catch (e) { aborted = true; throw e; }
    }
  };
  const settled = await Promise.allSettled(Array.from({ length: size }, runner));
  const failed = settled.find((r) => r.status === 'rejected');
  if (failed) throw failed.reason;
  return results;
}

const UNITS = ['B', 'KB', 'MB', 'GB', 'TB'];
function bytesToHuman(n) {
  if (!Number.isFinite(n) || n < 0) return '—';
  let v = n; let u = 0;
  while (v >= 1024 && u < UNITS.length - 1) { v /= 1024; u++; }
  return (u === 0 ? String(v) : v.toFixed(v < 10 ? 1 : 0)) + ' ' + UNITS[u];
}

module.exports = {
  asArrayBuffer, toHex, sha256Hex, hexToBytes, sha256HexView, uuidFromHex, randomUuid, sleep, nfc,
  splitPath, splitRel, folderPathFor, globToRegExp, makeExcluder, redact, stamp, suffixName, concatBuffers,
  clamp, pool, bytesToHuman,
};
