'use strict';
const { concatBuffers, pool, clamp } = require('./util');

const MAX_CONCURRENCY = 6;

/** One HTTP Range request for a byte slice of a file. */
function rangeRequest(client, id, from, to) {
  return client.request({
    path: '/files/' + encodeURIComponent(id) + '/content',
    headers: { Range: `bytes=${from}-${to}` }, binary: true,
  });
}

/**
 * Reads the exact byte range [from, to] and returns it. Some servers answer a Range request with a SHORTER
 * body than asked (they may even echo the full Content-Range while truncating), so the range is re-requested
 * from where the body actually ended until it is complete. This is what makes parallel downloads safe.
 */
async function readRange(client, id, from, to) {
  const chunks = []; let cur = from;
  while (cur <= to) {
    const res = await rangeRequest(client, id, cur, to);
    if (res.status === 200) {
      // Server ignored Range and returned the whole body.
      const whole = cur === 0 ? res.body : res.body.slice(cur);
      if (whole.byteLength === 0) throw new Error('Empty download response');
      chunks.push(whole); cur += whole.byteLength;
      continue;
    }
    if (res.status !== 206) throw new Error('Unexpected download status ' + res.status);
    const m = /bytes\s+(\d+)-(\d+)\/(\d+|\*)/i.exec(res.headers['content-range'] || '');
    if (m && Number(m[1]) !== cur) throw new Error('Unexpected Content-Range start');
    if (res.body.byteLength === 0) throw new Error('Empty download part');
    chunks.push(res.body); cur += res.body.byteLength;
  }
  return concatBuffers(chunks);
}

/**
 * Sequential ranged download. Each chunk is its own request that resumes from the last received offset, so an
 * interrupted (or truncated) transfer recovers instead of restarting. Works even when the server ignores Range.
 */
async function downloadSequential(client, file, chunkBytes, onProgress) {
  const known = Number.isFinite(file.size) ? file.size : null;
  const chunks = []; let offset = 0; let total = known;
  while (total === null || offset < total) {
    const res = await rangeRequest(client, file.id, offset, offset + chunkBytes - 1);
    if (res.status === 206) {
      const m = /bytes\s+(\d+)-(\d+)\/(\d+|\*)/i.exec(res.headers['content-range'] || '');
      if (m) {
        if (Number(m[1]) !== offset) throw new Error('Unexpected Content-Range start');
        if (m[3] !== '*') total = Number(m[3]);
      }
      if (res.body.byteLength === 0) break;
      chunks.push(res.body); offset += res.body.byteLength;
      if (total === null && res.body.byteLength < chunkBytes) total = offset;
    } else {
      // Server ignored Range and returned the whole body.
      const whole = offset === 0 ? res.body : res.body.slice(offset);
      chunks.push(whole); offset += whole.byteLength; total = offset;
    }
    if (onProgress) onProgress(offset, total);
  }
  if (known !== null && offset !== known) throw new Error(`Incomplete download: ${offset}/${known} bytes`);
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
 *  Used only for plaintext downloads of a known size; the caller handles hashing/recording. */
async function downloadToSink(client, file, chunkBytes, onProgress, sink) {
  const known = file.size;
  let offset = 0;
  while (offset < known) {
    const res = await rangeRequest(client, file.id, offset, offset + chunkBytes - 1);
    if (res.status === 206) {
      const m = /bytes\s+(\d+)-(\d+)\//i.exec(res.headers['content-range'] || '');
      if (m && Number(m[1]) !== offset) throw new Error('Unexpected Content-Range start');
      if (res.body.byteLength === 0) break;
      await sink.write(res.body); offset += res.body.byteLength;
    } else {
      const whole = offset === 0 ? res.body : res.body.slice(offset);
      await sink.write(whole); offset += whole.byteLength;
    }
    if (onProgress) onProgress(offset, known);
  }
  if (offset !== known) throw new Error(`Incomplete download: ${offset}/${known} bytes`);
  if (onProgress) onProgress(known, known);
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
  else if (known !== null && known > chunkBytes && concurrency > 1) buf = await downloadParallel(client, file, chunkBytes, concurrency, opts.onProgress);
  else buf = await downloadSequential(client, file, chunkBytes, opts.onProgress);
  if (opts.crypto) buf = await opts.crypto.decrypt(buf);       // decrypt first, then inflate
  if (opts.compressor) buf = await opts.compressor.decompress(buf);
  return buf;
}

module.exports = { downloadFile, downloadSequential, downloadParallel, readRange, downloadToSink };
