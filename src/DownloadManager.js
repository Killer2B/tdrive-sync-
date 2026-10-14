'use strict';
const { concatBuffers, pool, clamp, sleep } = require('./util');
const { sha256hex } = require('./Chunker');

const MAX_CONCURRENCY = 6;
const MAX_STALLS = 6;          // consecutive no-progress responses before we stop and reconcile
const MAX_REQUESTS = 100000;   // absolute safety bound (a normal multi-part download is never capped)

/** One HTTP Range request for a byte slice of a file. */
function rangeRequest(client, id, from, to) {
  return client.request({
    path: '/files/' + encodeURIComponent(id) + '/content',
    headers: { Range: `bytes=${from}-${to}` }, binary: true,
  });
}

/** The authoritative current size from the file metadata (used when a listing/size looks stale). */
async function serverSize(client, id) {
  try { const f = await client.getFile(id); return f && Number.isFinite(f.size) ? f.size : null; }
  catch (e) { return null; }
}

const totalFromHeaders = (headers) => {
  const m = /bytes\s+(\d+)-(\d+)\/(\d+|\*)/i.exec((headers && headers['content-range']) || '');
  if (m && m[3] !== '*') return Number(m[3]);
  const cl = Number(headers && headers['content-length']);
  return Number.isFinite(cl) && cl > 0 ? cl : null;
};

/**
 * Downloads [from, total) as a sequence of ranged requests, starting each request where the previous body
 * actually ended (so a truncated response is continued, not restarted). Returns { buf, total, got }.
 * A 200 answer (server ignored Range) is accepted as the whole body. Byte counting uses the *received*
 * length plus the server's own Content-Range/Content-Length total — never a guess.
 */
async function readFrom(client, id, from, expectedTotal, chunkBytes, onProgress) {
  const chunks = [];
  let offset = from;
  let total = Number.isFinite(expectedTotal) ? expectedTotal : null;
  let stalls = 0;                    // consecutive responses that made no progress
  let guard = 0;                     // absolute safety bound on requests
  while (total === null || offset < total) {
    if (++guard > MAX_REQUESTS) break;
    const before = offset;
    const res = await rangeRequest(client, id, offset, offset + chunkBytes - 1);
    if (res.status === 416) {
      // RFC 7233: a range past EOF answers 416 with "Content-Range: bytes *\/<total>" and no body.
      // That total is the AUTHORITATIVE size of what the server stores — we already hold all of it.
      const n = /bytes\s+\*\/(\d+)/i.exec((res.headers && res.headers['content-range']) || '');
      if (n) total = Number(n[1]);
      break;
    }
    if (res.status === 200) {
      // Server ignored Range and returned a whole body (possibly with a Content-Length we can trust).
      const cl = Number(res.headers && res.headers['content-length']);
      const whole = offset === 0 ? res.body : res.body.slice(offset);
      if (whole.byteLength > 0) { chunks.push(whole); offset += whole.byteLength; }
      const t = Number.isFinite(cl) && cl > 0 ? cl : offset;
      total = t;
    } else if (res.status === 206) {
      const m = /bytes\s+(\d+)-(\d+)\/(\d+|\*)/i.exec((res.headers && res.headers['content-range']) || '');
      if (m) {
        if (Number(m[1]) !== offset) throw new Error(`Unexpected Content-Range start (asked ${offset}, got ${m[1]})`);
        if (m[3] !== '*') total = Number(m[3]);
      }
      if (res.body.byteLength > 0) {
        let body = res.body;
        if (total !== null && offset + body.byteLength > total) body = body.slice(0, total - offset);   // never over-read
        chunks.push(body); offset += body.byteLength;
      }
    } else {
      throw new TdriveError('Unexpected download status ' + res.status, { status: res.status, retryable: res.status >= 500 || res.status === 429 });
    }
    if (onProgress) onProgress(offset, total || 0);
    if (offset === before) { if (++stalls > MAX_STALLS) break; } else stalls = 0;
  }
  let buf = concatBuffers(chunks);
  if (total !== null && from + buf.byteLength > total) buf = buf.slice(0, total - from);   // never over-read
  return { buf, total, got: from + buf.byteLength };
}

/**
 * Sequential download that is resilient to short/truncated responses, a stale advertised size AND a stale
 * metadata size (the "941/5348" and "HTTP 416" families):
 *  1. read the whole object; a 416 past EOF is treated as authoritative EOF, not an error;
 *  2. if fewer bytes arrived than the advertised size, fetch ONLY the missing tail. The tail read itself
 *     discovers the true total (206 Content-Range, or 416 "bytes *\/N") — so a metadata size that is larger
 *     than the stored object can never produce a request past EOF, and the shorter object is accepted;
 *  3. the trusted total is written back onto the remote entry so the ledger stays consistent.
 */
async function downloadSequential(client, file, chunkBytes, onProgress, opts = {}) {
  const advertised = Number.isFinite(file.size) ? file.size : null;
  let r = await readFrom(client, file.id, 0, advertised, chunkBytes, onProgress);
  let buf = r.buf;
  // Whenever we appear short of the advertised size, fetch the tail from the exact offset. The tail read is the
  // arbiter: if more bytes exist we get them (recovering a truncated/under-reported stream); if the object really
  // is shorter, the server answers 416 with "bytes */<realTotal>" and readFrom reports that authoritative total.
  if (advertised != null && buf.byteLength < advertised) {
    const tail = await readFrom(client, file.id, buf.byteLength, advertised, chunkBytes, onProgress);
    if (tail.buf.byteLength > 0) buf = concatBuffers([buf, tail.buf]);
    const total = tail.total != null ? tail.total : advertised;
    if (buf.byteLength !== total) throw new TdriveError(`Incomplete download: ${buf.byteLength}/${total} bytes`, { retryable: true });
    if (file && file.size !== total) { try { file.size = total; } catch (e) { /* frozen entry */ } }
    return buf;
  }
  const total = r.total != null ? r.total : advertised;
  if (total != null && buf.byteLength !== total) {
    throw new TdriveError(`Incomplete download: ${buf.byteLength}/${total} bytes`, { retryable: true });
  }
  if (total != null && file && file.size !== total) { try { file.size = total; } catch (e) { /* frozen entry */ } }
  return buf;
}

async function verifySha(buf, expectSha, label) {
  const got = await sha256hex(new Uint8Array(buf));
  if (String(got).toLowerCase() !== String(expectSha).toLowerCase()) {
    throw new Error(`Download checksum mismatch${label ? ' for ' + label : ''}: ${String(got).slice(0, 16)}… != expected ${String(expectSha).slice(0, 16)}…`);
  }
}

/** TdriveError is imported lazily to avoid a circular dependency at module load. */
class TdriveError extends Error {
  constructor(message, o = {}) { super(message); this.name = 'TdriveError'; Object.assign(this, o); }
}

/**
 * Reads the exact byte range [from, to] and returns it. Some servers answer a Range request with a SHORTER
 * body than asked (they may even echo the full Content-Range while truncating), so the range is re-requested
 * from where the body actually ended until it is complete. This is what makes parallel downloads safe.
 */
async function readRange(client, id, from, to) {
  const chunks = []; let cur = from; let stalls = 0;
  while (cur <= to) {
    const before = cur;
    const res = await rangeRequest(client, id, cur, to);
    if (res.status === 416) break;                 // past EOF: authoritative end of the object
    if (res.status === 200) {
      // Server ignored Range and returned the whole body.
      const whole = cur === 0 ? res.body : res.body.slice(cur);
      if (whole.byteLength > 0) { chunks.push(whole); cur += whole.byteLength; }
    } else if (res.status === 206) {
      const m = /bytes\s+(\d+)-(\d+)\/(\d+|\*)/i.exec(res.headers['content-range'] || '');
      if (m && Number(m[1]) !== cur) throw new Error('Unexpected Content-Range start');
      if (res.body.byteLength > 0) { chunks.push(res.body); cur += res.body.byteLength; }
    } else {
      throw new TdriveError('Unexpected download status ' + res.status, { status: res.status, retryable: res.status >= 500 || res.status === 429 });
    }
    if (cur === before) { if (++stalls > MAX_STALLS) throw new TdriveError(`Incomplete range: ${cur - from}/${to - from + 1} bytes`, { retryable: true }); } else stalls = 0;
  }
  return concatBuffers(chunks);
}


/**
 * Parallel ranged download. The first chunk is read alone; the rest are fanned out. Every part is read with
 * `readRange`, so a truncated/errored response is completed instead of silently corrupting the file.
 */
async function downloadParallel(client, file, chunkBytes, concurrency, onProgress) {
  const known = file.size;
  const first = await readRange(client, file.id, 0, Math.min(chunkBytes, known) - 1);
  let done = first.byteLength;
  if (onProgress) onProgress(Math.min(done, known), known);
  const ranges = [];
  for (let off = first.byteLength; off < known; off += chunkBytes) ranges.push([off, Math.min(off + chunkBytes, known) - 1]);
  const parts = await pool(ranges, clamp(concurrency, 1, MAX_CONCURRENCY), async ([a, b]) => {
    const buf = await readRange(client, file.id, a, b);
    done += buf.byteLength;
    if (onProgress) onProgress(Math.min(done, known), known);
    return buf;
  });
  const out = concatBuffers([first, ...parts]);
  if (out.byteLength !== known) throw new Error(`Incomplete download: ${out.byteLength}/${known} bytes`);
  if (onProgress) onProgress(known, known);
  return out;
}

/** Sequential ranged download written straight to `sink` (disk), so a huge file never sits in memory.
 *  Used only for plaintext downloads of a known size; the caller handles hashing/recording.
 *  Resilient the same way as downloadSequential: continues across truncated responses and reconciles the
 *  authoritative size before giving up, and never leaves a partial file silently accepted. */
async function downloadToSink(client, file, chunkBytes, onProgress, sink) {
  const advertised = Number.isFinite(file.size) ? file.size : null;
  const target = advertised != null ? advertised : (await serverSize(client, file.id));
  if (!Number.isFinite(target)) throw new TdriveError('Cannot stream an unknown-size download', { retryable: true });
  let offset = 0; let stalls = 0; let guard = 0;
  while (offset < target) {
    if (++guard > MAX_REQUESTS) break;
    const before = offset;
    const res = await rangeRequest(client, file.id, offset, offset + chunkBytes - 1);
    if (res.status === 416) break;                       // past EOF: authoritative end
    if (res.status === 200) {
      const whole = offset === 0 ? res.body : res.body.slice(offset);
      if (whole.byteLength > 0) { await sink.write(whole); offset += whole.byteLength; }
    } else if (res.status === 206) {
      const m = /bytes\s+(\d+)-(\d+)\//i.exec(res.headers['content-range'] || '');
      if (m && Number(m[1]) !== offset) throw new Error('Unexpected Content-Range start');
      if (res.body.byteLength > 0) { await sink.write(res.body); offset += res.body.byteLength; }
    } else {
      throw new TdriveError('Unexpected download status ' + res.status, { status: res.status, retryable: res.status >= 500 || res.status === 429 });
    }
    if (onProgress) onProgress(offset, target);
    if (offset === before) { if (++stalls > MAX_STALLS) break; } else stalls = 0;
  }
  if (offset !== target) throw new TdriveError(`Incomplete download: ${offset}/${target} bytes`, { retryable: true });
  if (onProgress) onProgress(target, target);
  return { streamed: true, size: offset };
}

async function downloadFile(client, file, opts = {}) {
  const chunkBytes = opts.chunkBytes || 4 * 1024 * 1024;
  const hint = Number.isFinite(client._concurrencyHint) ? client._concurrencyHint : Infinity;
  const concurrency = clamp(Math.min(opts.concurrency || 1, hint), 1, MAX_CONCURRENCY);
  const known = Number.isFinite(file.size) ? file.size : null;
  // Streaming to disk is only safe for a plaintext, un-compressed download of a known size.
  if (opts.sink && known !== null && known > 0 && !opts.crypto && !opts.compressor) return downloadToSink(client, file, chunkBytes, opts.onProgress, opts.sink);
  let buf;
  if (known === 0) buf = new ArrayBuffer(0);
  else if (known !== null && known > chunkBytes && concurrency > 1) {
    try { buf = await downloadParallel(client, file, chunkBytes, concurrency, opts.onProgress); }
    catch (e) {
      // A stale size can make a parallel part hit 416; fall back to the resilient sequential reader.
      if (e && e.status === 416 || /Incomplete download|Unexpected download status/.test(String(e && e.message))) buf = await downloadSequential(client, file, chunkBytes, opts.onProgress, opts);
      else throw e;
    }
  } else buf = await downloadSequential(client, file, chunkBytes, opts.onProgress, opts);
  // Verify integrity of the RAW stored bytes: the manifest checksum describes exactly what is on the server,
  // i.e. the bytes BEFORE decryption/decompression (our container/encryption wraps the stored object).
  if (opts.expectSha) await verifySha(buf, opts.expectSha, opts.verifyLabel);
  if (opts.crypto) buf = await opts.crypto.decrypt(buf);       // decrypt first, then inflate
  if (opts.compressor) buf = await opts.compressor.decompress(buf);
  return buf;
}

/** The server's chunk manifest: { version, etag, chunkSize, chunker, totalChunks, chunks:[{idx,offset,size,checksum}] }.
 *  Returns null when the object has no manifest. Pass `etag` to let the server answer 304 (unchanged). */
async function fetchManifest(client, id, etag) {
  try {
    const res = await client.request({ path: `/files/${encodeURIComponent(id)}/chunks`, headers: etag ? { 'If-None-Match': etag } : undefined, allow304: true });
    if (res && res.notModified) return { notModified: true };
    return res || null;
  } catch (e) {
    if (e.status === 404) return null;
    throw e;
  }
}

/**
 * Fetches only the byte ranges that changed, using the server's chunk manifest, and appends them as
 * [{offset, size, checksum}] verified parts. The caller splices them into the local copy.
 */
async function downloadChangedRanges(client, id, changedChunks, onProgress) {
  const parts = [];
  let done = 0; let total = 0;
  for (const c of changedChunks) total += c.size;
  const conc = clamp(Number.isFinite(client._concurrencyHint) ? client._concurrencyHint : 3, 1, MAX_CONCURRENCY);
  await pool(changedChunks, conc, async (c) => {
    const buf = await readRange(client, id, c.offset, c.offset + c.size - 1);
    if (buf.byteLength !== c.size) throw new Error(`Delta part ${c.idx} size mismatch: ${buf.byteLength} vs ${c.size}`);
    if (c.checksum) {
      const got = await sha256hex(new Uint8Array(buf));
      if (got !== c.checksum) throw new Error(`Delta part ${c.idx} failed checksum verification`);
    }
    done += buf.byteLength;
    if (onProgress) onProgress(done, total);
    parts.push({ offset: c.offset, size: c.size, data: buf });
  });
  return parts;
}

module.exports = { downloadFile, downloadSequential, downloadParallel, readRange, readFrom, downloadToSink, fetchManifest, downloadChangedRanges, verifySha };
