'use strict';
// Bandwidth reduction for note-like files.
//
// CONTEXT: the API DOES support sending/receiving only the changes (chunk manifest + delta + CDC + revisions) —
// see src/Chunker.js and the delta/revision paths. Compression is an ADDITIONAL saving that stacks with that:
// it sends FEWER BYTES for the content that has to move. Obsidian vaults are mostly text
// (Markdown/JSON/CSV/…), which typically compresses 5–20×, so a one-character edit to a 200 KB note
// transfers ~15–30 KB instead of 200 KB.
//
// This is applied ONLY when it is clearly worth it and clearly safe:
//   - the file looks like text (by extension + a binary sniff),
//   - it is at least `minBytes` (small files stay readable and the framing overhead would dominate),
//   - gzip actually shrinks it by at least `minSaving` (otherwise the original bytes are stored unchanged).
//
// The payload is self-describing, so a file uploaded as plaintext (before enabling this, or because it did
// not shrink) still downloads unchanged:
//   [ 7-byte magic "TDRVZIP" ][ 1-byte version ][ 4-byte big-endian uncompressed length ][ gzip stream ]
//
// This module is pure byte transformation; it never talks to the network and never changes the sync engine.

const MAGIC = new Uint8Array([0x54, 0x44, 0x52, 0x56, 0x5a, 0x49, 0x50]); // "TDRVZIP"
const VERSION = 1;
const HEADER_LEN = MAGIC.length + 1 + 4;   // 12
const GZIP = /^(md|markdown|txt|text|json|jsonc|json5|csv|tsv|log|xml|html|htm|css|scss|less|js|mjs|cjs|ts|tsx|jsx|yml|yaml|toml|ini|conf|cfg|svg|tex|bib|rtf|org|rst|adoc|srt|vtt)$/i;

const gzipSupported = () => typeof CompressionStream === 'function' && typeof DecompressionStream === 'function';

/** True if the buffer is one of our gzip containers (not plain text). */
function isCompressed(buf) {
  if (!buf || buf.byteLength < HEADER_LEN) return false;
  const u = new Uint8Array(buf, 0, MAGIC.length);
  for (let i = 0; i < MAGIC.length; i++) if (u[i] !== MAGIC[i]) return false;
  return true;
}

/** A quick binary sniff: NUL byte in the first 1 KB means "not text", whatever the extension says. */
function looksBinary(bytes) {
  const n = Math.min(bytes.byteLength, 1024);
  for (let i = 0; i < n; i++) if (bytes[i] === 0) return true;
  return false;
}

function extOf(name) {
  const s = String(name || '');
  const dot = s.lastIndexOf('.');
  return dot >= 0 ? s.slice(dot + 1) : '';
}

// ---- gzip helpers (CompressionStream based; async and dependency-free) ----

async function gunzipBytes(buf) {
  const ds = new DecompressionStream('gzip');
  const w = ds.writable.getWriter();
  w.write(new Uint8Array(buf));
  w.close();
  return new Response(ds.readable).arrayBuffer();
}

async function gzipBytes(buf) {
  const cs = new CompressionStream('gzip');
  const w = cs.writable.getWriter();
  w.write(new Uint8Array(buf));
  w.close();
  return new Response(cs.readable).arrayBuffer();
}

const be32 = (n) => new Uint8Array([(n >>> 24) & 255, (n >>> 16) & 255, (n >>> 8) & 255, n & 255]);
const readBe32 = (buf) => new DataView(buf, MAGIC.length + 1, 4).getUint32(0, false);

/** Wraps `plain` in the container. Returns `plain` unchanged when compression is not supported. */
async function compressBytes(plain) {
  if (!gzipSupported()) return plain;
  const gz = await gzipBytes(plain);
  const out = new Uint8Array(HEADER_LEN + gz.byteLength);
  out.set(MAGIC, 0);
  out[MAGIC.length] = VERSION;
  out.set(be32(plain.byteLength), MAGIC.length + 1);
  out.set(new Uint8Array(gz), HEADER_LEN);
  return out.buffer;
}

/** Unwraps a container. A buffer without the magic header is returned unchanged (plaintext passthrough). */
async function decompressBytes(buf) {
  if (!isCompressed(buf)) return buf;
  const ver = new Uint8Array(buf, MAGIC.length, 1)[0];
  if (ver !== VERSION) throw new Error('Unsupported compressed file version ' + ver + ' — update the plugin');
  const expected = readBe32(buf);
  const gz = buf.slice(HEADER_LEN);
  const plain = await gunzipBytes(gz);
  if (plain.byteLength !== expected) throw new Error(`Decompression size mismatch: ${plain.byteLength} vs ${expected}`);
  return plain;
}

class Compressor {
  constructor({ minBytes = 64 * 1024, minSaving = 0.9 } = {}) {
    this.minBytes = minBytes;
    this.minSaving = minSaving;   // keep compressed only if (compressed / original) <= minSaving
  }

  /** Compresses `data` only when it is text-like, large enough and actually shrinks. Else returns it as-is. */
  async maybeCompress(name, data) {
    const bytes = data instanceof ArrayBuffer ? new Uint8Array(data) : (data.buffer ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength) : new Uint8Array(data));
    if (!gzipSupported()) return data;
    if (bytes.byteLength < this.minBytes) return data;
    if (!GZIP.test(extOf(name))) return data;
    if (looksBinary(bytes)) return data;
    const packed = await compressBytes(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength));
    if (packed.byteLength >= bytes.byteLength * this.minSaving) return data;   // not worth it: store plaintext
    return packed;
  }

  /** Undoes maybeCompress. Harmless on plaintext (returns it unchanged). */
  async decompress(buf) { return decompressBytes(buf); }
}

module.exports = { Compressor, isCompressed, compressBytes, decompressBytes, gzipSupported, MAGIC, VERSION, HEADER_LEN };
