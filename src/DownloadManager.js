'use strict';
const { concatBuffers, pool, clamp } = require('./util');
const { sha256hex, fingerprintOf } = require('./Chunker');
const { normalizeReceipt } = require('./normalize');

const MAX_CONCURRENCY = 6;
const MAX_STALLS = 6;          // consecutive no-progress responses before we stop and reconcile
const MAX_REQUESTS = 100000;   // absolute safety bound (a normal multi-part download is never capped)

/** One HTTP Range request for a byte slice of a file. */
async function rangeRequest(client, id, from, to, state = {}) {
  if (!Number.isSafeInteger(from) || from < 0 || !Number.isSafeInteger(to) || to < from) throw new Error('Invalid download range');
  const res = await client.request({
    path: '/files/' + encodeURIComponent(id) + '/content',
    headers: { Range: `bytes=${from}-${to}`, ...(state.etag ? { 'If-Range': state.etag } : {}) }, binary: true,
  });
  const etag = res.headers && res.headers.etag;
  if (state.etag && etag !== state.etag && !(res.status === 416 && !etag)) throw new TdriveError('Remote content changed during download', { code: 'content_changed', retryable: true });
  if (etag) state.etag = etag;
  if (res.status === 206) {
    const m = /^bytes\s+(\d+)-(\d+)\/(\d+|\*)$/i.exec((res.headers && res.headers['content-range']) || '');
    if (!m || Number(m[1]) !== from || Number(m[2]) < from || Number(m[2]) > to || res.body.byteLength > Number(m[2]) - from + 1) throw new TdriveError('Invalid Content-Range response', { retryable: true });
    res.total = m[3] === '*' ? null : Number(m[3]);
    if (![Number(m[1]), Number(m[2])].every(Number.isSafeInteger) || (res.total !== null && (!Number.isSafeInteger(res.total) || res.total <= Number(m[2])))) throw new TdriveError('Invalid Content-Range total', { retryable: true });
  } else if (res.status === 200) {
    const cl = res.headers && res.headers['content-length'];
    if (cl != null && (!/^\d+$/.test(String(cl)) || Number(cl) !== res.body.byteLength)) throw new TdriveError('Incomplete download response body', { retryable: true });
    res.total = res.body.byteLength;
  } else if (res.status === 416) {
    const m = /^bytes\s+\*\/(\d+)$/i.exec((res.headers && res.headers['content-range']) || '');
    if (!m || !Number.isSafeInteger(Number(m[1])) || from < Number(m[1])) throw new TdriveError('Invalid unsatisfied Content-Range', { retryable: true });
    res.total = Number(m[1]);
  }
  if (res.total != null) {
    if (state.total != null && state.total !== res.total) throw new TdriveError('Remote content size changed during download', { code: 'content_changed', retryable: true });
    state.total = res.total;
  }
  return res;
}

/** The authoritative current size from the file metadata (used when a listing/size looks stale). */
async function serverSize(client, id) {
  try { const f = await client.getFile(id); return f && Number.isFinite(f.size) ? f.size : null; }
  catch (e) { return null; }
}

/**
 * Downloads [from, total) as a sequence of ranged requests, starting each request where the previous body
 * actually ended (so a truncated response is continued, not restarted). Returns { buf, total, got }.
 * A 200 answer (server ignored Range) is accepted as the whole body. Byte counting uses the *received*
 * length plus the server's own Content-Range/Content-Length total — never a guess.
 */
async function readFrom(client, id, from, expectedTotal, chunkBytes, onProgress, state = {}) {
  const chunks = [];
  let offset = from;
  let total = Number.isFinite(expectedTotal) ? expectedTotal : null;
  let stalls = 0;                    // consecutive responses that made no progress
  let guard = 0;                     // absolute safety bound on requests
  while (total === null || offset < total) {
    if (++guard > MAX_REQUESTS) break;
    const before = offset;
    const res = await rangeRequest(client, id, offset, offset + chunkBytes - 1, state);
    if (res.total != null) total = res.total;
    if (res.status === 416) {
      // RFC 7233: a range past EOF answers 416 with "Content-Range: bytes *\/<total>" and no body.
      // That total is the AUTHORITATIVE size of what the server stores — we already hold all of it.
      break;
    }
    if (res.status === 200) {
      // Server ignored Range and returned a whole body (possibly with a Content-Length we can trust).
      const whole = offset === 0 ? res.body : res.body.slice(offset);
      if (whole.byteLength > 0) { chunks.push(whole); offset += whole.byteLength; }
    } else if (res.status === 206) {
      if (res.body.byteLength > 0) {
        chunks.push(res.body); offset += res.body.byteLength;
      }
    } else {
      throw new TdriveError('Unexpected download status ' + res.status, { status: res.status, retryable: res.status >= 500 || res.status === 429 });
    }
    if (onProgress) onProgress(offset, total || 0);
    if (offset === before) { if (++stalls > MAX_STALLS) break; } else stalls = 0;
  }
  const buf = concatBuffers(chunks);
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
  const state = opts.state || {};
  const advertised = Number.isFinite(file.size) && (file.size !== 0 || opts.manifest) ? file.size : null;
  let r = await readFrom(client, file.id, 0, advertised, chunkBytes, onProgress, state);
  let buf = r.buf;
  // Whenever we appear short of the advertised size, fetch the tail from the exact offset. The tail read is the
  // arbiter: if more bytes exist we get them (recovering a truncated/under-reported stream); if the object really
  // is shorter, the server answers 416 with "bytes */<realTotal>" and readFrom reports that authoritative total.
  let total = r.total != null ? r.total : advertised;
  if (advertised != null && buf.byteLength < advertised) {
    const tail = await readFrom(client, file.id, buf.byteLength, advertised, chunkBytes, onProgress, state);
    if (tail.buf.byteLength > 0) buf = concatBuffers([buf, tail.buf]);
    if (tail.total != null) total = tail.total;
  }
  if (total != null && buf.byteLength !== total) {
    throw new TdriveError(`Incomplete download: ${buf.byteLength}/${total} bytes`, { retryable: true });
  } else if (total != null && file && file.size !== total) {
    try { file.size = total; } catch (e) { /* frozen entry */ }     // converge a stale advertised size
  }
  return buf;
}

async function verifySha(buf, expectSha, label) {
  const got = await sha256hex(new Uint8Array(buf));
  if (String(got).toLowerCase() !== String(expectSha).toLowerCase()) {
    throw new Error(`Download checksum mismatch${label ? ' for ' + label : ''}: got ${got} (${buf.byteLength} bytes) != expected ${expectSha}`);
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
async function readRange(client, id, from, to, state = {}) {
  const chunks = []; let cur = from; let stalls = 0;
  while (cur <= to) {
    const before = cur;
    const res = await rangeRequest(client, id, cur, to, state);
    if (res.status === 416) break;                 // past EOF: authoritative end of the object
    if (res.status === 200) {
      // Server ignored Range and returned the whole body.
      const whole = res.body.slice(cur, to + 1);
      if (whole.byteLength > 0) { chunks.push(whole); cur += whole.byteLength; }
    } else if (res.status === 206) {
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
async function downloadParallel(client, file, chunkBytes, concurrency, onProgress, state = {}) {
  const known = file.size;
  const first = await readRange(client, file.id, 0, Math.min(chunkBytes, known) - 1, state);
  let done = first.byteLength;
  if (onProgress) onProgress(Math.min(done, known), known);
  const ranges = [];
  for (let off = first.byteLength; off < known; off += chunkBytes) ranges.push([off, Math.min(off + chunkBytes, known) - 1]);
  const parts = await pool(ranges, clamp(concurrency, 1, MAX_CONCURRENCY), async ([a, b]) => {
    const buf = await readRange(client, file.id, a, b, state);
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
async function downloadToSink(client, file, chunkBytes, onProgress, sink, opts = {}) {
  const state = opts.state || {};
  if (opts.manifest) {
    let done = 0;
    for (const c of opts.manifest.chunks) {
      const buf = c.size === 0 ? new ArrayBuffer(0) : await readRange(client, file.id, c.offset, c.offset + c.size - 1, state);
      await verifySha(buf, c.checksum, 'stream chunk ' + c.idx);
      await sink.write(buf); done += buf.byteLength;
      if (onProgress) onProgress(done, opts.manifest.size);
    }
    return { streamed: true, size: done };
  }
  if (opts.expectSha) throw new Error('A chunk manifest is required for verified streaming');
  const advertised = Number.isFinite(file.size) ? file.size : null;
  const target = advertised != null ? advertised : (await serverSize(client, file.id));
  if (!Number.isFinite(target)) throw new TdriveError('Cannot stream an unknown-size download', { retryable: true });
  let offset = 0; let stalls = 0; let guard = 0;
  while (offset < target) {
    if (++guard > MAX_REQUESTS) break;
    const before = offset;
    const res = await rangeRequest(client, file.id, offset, offset + chunkBytes - 1, state);
    if (res.status === 416) break;                       // past EOF: authoritative end
    if (res.status === 200) {
      const whole = offset === 0 ? res.body : res.body.slice(offset);
      if (whole.byteLength > 0) { await sink.write(whole); offset += whole.byteLength; }
    } else if (res.status === 206) {
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

async function downloadWhole(client, file, opts) {
  const res = await client.request({
    path: '/files/' + encodeURIComponent(file.id) + '/content',
    binary: true,
  });
  if (res.status !== 200) {
    throw new TdriveError('Unexpected full download status ' + res.status, { status: res.status, retryable: res.status >= 500 || res.status === 429 });
  }
  let buf = res.body;
  if (opts.state.etag && (!res.headers || res.headers.etag !== opts.state.etag)) throw new TdriveError('Remote content changed during download', { code: 'content_changed', retryable: true });
  const cl = res.headers && res.headers['content-length'];
  if (cl != null && Number(cl) !== buf.byteLength) throw new TdriveError('Incomplete download response body', { retryable: true });
  if (opts.manifest) await verifyManifest(buf, opts.manifest);
  if (opts.expectSha) await verifySha(buf, opts.expectSha, `${opts.verifyLabel || ''} (full response)`);
  if (file && file.size !== buf.byteLength) { try { file.size = buf.byteLength; } catch (e) { /* frozen entry */ } }
  if (opts.crypto) buf = await opts.crypto.decrypt(buf);
  if (opts.compressor) buf = await opts.compressor.decompress(buf);
  return buf;
}

async function validateManifestEvidence(file, manifest) {
  if (!manifest) return;
  const pinned = normalizeReceipt(file); const current = normalizeReceipt(manifest);
  const chunks = manifest.chunks || manifest.parts;
  if (!manifest.notModified && Array.isArray(chunks) && chunks.every((c) => c && /^[0-9a-f]{64}$/.test(c.checksum))) {
    current.checksum = manifest.checksum || await fingerprintOf(chunks.map((c) => c.checksum));
  }
  if (['etag', 'contentVersion', 'checksum'].some((field) => pinned[field] != null && current[field] != null && pinned[field] !== current[field])) {
    throw new TdriveError('Remote content changed before download', { code: 'content_changed', retryable: true });
  }
}

async function downloadFile(client, file, opts = {}) {
  const chunkBytes = opts.chunkBytes || 4 * 1024 * 1024;
  if (!Number.isSafeInteger(chunkBytes) || chunkBytes <= 0) throw new Error('Invalid download chunk size');
  const manifest = await fetchManifest(client, file.id);
  await validateManifestEvidence(file, manifest);
  const state = { etag: manifest && manifest.etag || file.etag || undefined, total: manifest ? manifest.size : undefined };
  opts = { ...opts, manifest, state };
  if (manifest) { try { file.size = manifest.size; } catch (_) { file = { ...file, size: manifest.size }; } }
  if (opts.forceFull || (!manifest && file.size === 0)) return downloadWhole(client, file, opts);
  const hint = Number.isFinite(client._concurrencyHint) ? client._concurrencyHint : Infinity;
  const concurrency = clamp(Math.min(opts.concurrency || 1, hint), 1, MAX_CONCURRENCY);
  const known = Number.isFinite(file.size) ? file.size : null;
  // Streaming to disk is only safe for a plaintext, un-compressed download of a known size.
  if (opts.sink && known !== null && known > 0 && !opts.crypto && !opts.compressor) return downloadToSink(client, file, chunkBytes, opts.onProgress, opts.sink, opts);
  let buf;
  if (known === 0 && manifest) buf = new ArrayBuffer(0);
  else if (known !== null && known > chunkBytes && concurrency > 1) {
    try { buf = await downloadParallel(client, file, chunkBytes, concurrency, opts.onProgress, state); }
    catch (e) {
      // A stale size can make a parallel part hit 416; fall back to the resilient sequential reader.
      if (e && e.status === 416 || /Incomplete download|Unexpected download status/.test(String(e && e.message))) buf = await downloadSequential(client, file, chunkBytes, opts.onProgress, opts);
      else throw e;
    }
  } else buf = await downloadSequential(client, file, chunkBytes, opts.onProgress, opts);
  if (manifest) await verifyManifest(buf, manifest);
  // Verify integrity of the RAW stored bytes: the manifest checksum describes exactly what is on the server,
  // i.e. the bytes BEFORE decryption/decompression (our container/encryption wraps the stored object).
  if (opts.expectSha) {
    await verifySha(buf, opts.expectSha, `${opts.verifyLabel || ''} (ranged response)`);
    // The checksum is authoritative: if the real length differs from the advertised size, adopt the real one.
    if (file && file.size !== buf.byteLength) { try { file.size = buf.byteLength; } catch (e) { /* frozen entry */ } }
  }
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
    if (!res || !Number.isSafeInteger(res.size) || res.size < 0 || !Array.isArray(res.chunks) || res.chunks.length > 200000 || !Number.isSafeInteger(res.totalChunks) || res.totalChunks !== res.chunks.length || (res.etag != null && (typeof res.etag !== 'string' || !res.etag))) throw new TdriveError('Invalid chunk manifest', { code: 'bad_response' });
    let offset = 0;
    for (let i = 0; i < res.chunks.length; i++) {
      const c = res.chunks[i];
      if (!c || c.idx !== i || c.offset !== offset || !Number.isSafeInteger(c.size) || c.size < 0 || (c.size === 0 && res.size !== 0) || !/^[0-9a-f]{64}$/.test(c.checksum)) throw new TdriveError('Invalid chunk manifest entry', { code: 'bad_response' });
      offset += c.size;
    }
    if (offset !== res.size || (!res.chunks.length && res.size !== 0)) throw new TdriveError('Chunk manifest does not cover the file', { code: 'bad_response' });
    if (res.checksum && res.checksum !== await fingerprintOf(res.chunks.map((c) => c.checksum))) throw new TdriveError('Chunk manifest checksum mismatch', { code: 'bad_response' });
    return res;
  } catch (e) {
    if (e.status === 404) return null;
    throw e;
  }
}

async function verifyManifest(buf, manifest) {
  if (buf.byteLength !== manifest.size) throw new TdriveError('Download size does not match chunk manifest', { retryable: true });
  for (const c of manifest.chunks) await verifySha(buf.slice(c.offset, c.offset + c.size), c.checksum, 'chunk ' + c.idx);
}

/**
 * Fetches only the byte ranges that changed, using the server's chunk manifest, and appends them as
 * [{offset, size, checksum}] verified parts. The caller splices them into the local copy.
 */
async function downloadChangedRanges(client, id, changedChunks, onProgress) {
  const state = {};
  if (!Array.isArray(changedChunks) || changedChunks.some((c) => !c || !Number.isSafeInteger(c.offset) || c.offset < 0 || !Number.isSafeInteger(c.size) || c.size <= 0 || !/^[0-9a-f]{64}$/.test(c.checksum))) throw new TdriveError('Invalid delta chunk manifest', { code: 'bad_response' });
  const parts = [];
  let done = 0; let total = 0;
  for (const c of changedChunks) total += c.size;
  const conc = clamp(Number.isFinite(client._concurrencyHint) ? client._concurrencyHint : 3, 1, MAX_CONCURRENCY);
  await pool(changedChunks, conc, async (c) => {
    const buf = await readRange(client, id, c.offset, c.offset + c.size - 1, state);
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

module.exports = { downloadFile, downloadSequential, downloadParallel, readRange, readFrom, downloadToSink, fetchManifest, downloadChangedRanges, verifySha, validateManifestEvidence };
