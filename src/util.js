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

module.exports = {
  asArrayBuffer, toHex, sha256Hex, uuidFromHex, randomUuid, sleep, nfc, splitPath, splitRel,
  folderPathFor, globToRegExp, makeExcluder, redact, stamp, suffixName, concatBuffers,
};
