'use strict';
// Content-defined chunking (CDC) so an edit keeps almost every chunk boundary — the key to "send/download only
// the changes". The buzhash implementation matches the Tdrive reference chunker exactly, so both sides agree.
//
// A "plan" is [{ offset, size }]. Chunk hashes are SHA-256 (hex) of each chunk's bytes.

const sha256hex = async (buf) => {
  const d = await crypto.subtle.digest('SHA-256', buf);
  return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, '0')).join('');
};

// ---- buzhash table (fixed seed, same as the API reference) ----
const T = new Uint32Array(256);
{ let s = 0x9e3779b9; for (let i = 0; i < 256; i++) { s = (s * 1664525 + 1013904223) >>> 0; T[i] = s; } }
const rol = (x, n) => ((x << n) | (x >>> (32 - n))) >>> 0;

/** Bounds allowed by the API: 1024 <= min <= avg <= max <= chunkBytes; window 16..256. */
const CDC_BOUNDS = { minMin: 1024, windowMin: 16, windowMax: 256 };
const MAX_CDC_MAX = 5242880;   // a chunk may never exceed the server's part cap

/** Chooses CDC parameters suited to a file. Small text files get small chunks (cheap edits); large binaries get
 *  bigger chunks (fewer requests). Both sides read the same params from the manifest, so any choice is consistent.
 *  Chunk counts are kept well under MAX_MAP_CHUNKS so the map stays small in the ledger. */
function chooseChunker(size, maxChunk) {
  if (size <= 256 * 1024) return { algo: 'buzhash', min: 1024, avg: 2048, max: 4096, window: 48 };
  if (size <= 16 * 1024 * 1024) return { algo: 'buzhash', min: 16 * 1024, avg: 64 * 1024, max: 256 * 1024, window: 48 };
  if (size <= 256 * 1024 * 1024) return { algo: 'buzhash', min: 64 * 1024, avg: 512 * 1024, max: 2 * 1024 * 1024, window: 64 };
  const max = Math.min(Number.isFinite(maxChunk) && maxChunk > 0 ? maxChunk : MAX_CDC_MAX, MAX_CDC_MAX);
  return { algo: 'buzhash', min: 256 * 1024, avg: Math.min(2 * 1024 * 1024, Math.floor(max / 2)), max, window: 96 };
}

/** Plausible CDC params (clamped to the API bounds) or null if the object is not a usable chunker. */
function normalizeChunker(ch) {
  if (!ch || ch.algo !== 'buzhash') return null;
  const min = Math.max(CDC_BOUNDS.minMin, ch.min | 0);
  const avg = Math.max(min, ch.avg | 0);
  const max = Math.max(avg, ch.max | 0);
  const window = Math.min(CDC_BOUNDS.windowMax, Math.max(CDC_BOUNDS.windowMin, ch.window | 0));
  return { algo: 'buzhash', min, avg, max, window };
}

/** Splits `bytes` at content-defined boundaries. Returns [{ offset, size }]. */
function cdc(bytes, ch) {
  const { min, avg, max, window } = ch;
  const mask = (avg & (avg - 1)) === 0 ? avg - 1 : (1 << Math.ceil(Math.log2(avg))) - 1;
  const ring = new Uint8Array(window); const out = [];
  let h = 0; let start = 0;
  for (let i = 0; i < bytes.length; i++) {
    const b = bytes[i]; const outb = ring[i % window];
    ring[i % window] = b;
    h = rol(h, 1) ^ (i >= window ? rol(T[outb], window % 32) : 0) ^ T[b];
    const len = i - start + 1;
    if (len >= max || (len >= min && (h & mask) === 0)) { out.push({ offset: start, size: len }); start = i + 1; }
  }
  if (start < bytes.length) out.push({ offset: start, size: bytes.length - start });
  return out;
}

/** Fixed-size plan (used when the server chunked an object that way, e.g. a one-shot upload). */
function fixed(size, chunkSize) {
  const out = [];
  for (let o = 0; o < size; o += chunkSize) out.push({ offset: o, size: Math.min(chunkSize, size - o) });
  if (out.length === 0) out.push({ offset: 0, size: 0 });
  return out;
}

/** SHA-256 of every chunk in `plan` (in order). */
async function hashPlan(bytes, plan) {
  const hashes = new Array(plan.length);
  for (let i = 0; i < plan.length; i++) {
    const p = plan[i];
    hashes[i] = await sha256hex(bytes.subarray(p.offset, p.offset + p.size));
  }
  return hashes;
}

/** A plan (with hashes) for `data` using CDC. Returns { chunker, chunks: [{offset,size,hash}] }. */
async function cdcPlan(data, chunker) {
  const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
  const plan = cdc(bytes, chunker);
  const hashes = await hashPlan(bytes, plan);
  return { chunker, chunks: plan.map((p, i) => ({ offset: p.offset, size: p.size, hash: hashes[i] })) };
}

/** The API fingerprint: sha256 of the newline-joined chunk hashes. */
async function fingerprintOf(hashes) {
  return sha256hex(new TextEncoder().encode(hashes.join('\n')));
}

module.exports = { cdc, fixed, hashPlan, cdcPlan, fingerprintOf, chooseChunker, normalizeChunker, sha256hex, CDC_BOUNDS };
