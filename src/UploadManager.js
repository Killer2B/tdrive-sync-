'use strict';
const { asArrayBuffer, sha256Hex, randomUuid, uuidFromHex, pool, clamp, sleep } = require('./util');
const { normalizeFileResponse } = require('./normalize');
const { cdcPlan, chooseChunker, normalizeChunker, fingerprintOf, fixed, hashPlan } = require('./Chunker');

const json = (o) => new TextEncoder().encode(JSON.stringify(o)).buffer;

const MAX_PART_BYTES = 5242880;   // the API hard cap on a single part
const MAX_CONCURRENCY = 6;

async function uploadOneShot(client, { name, folderPath, data }) {
  const key = randomUuid(); // reused across internal retries -> server de-duplicates
  const res = await client.request({
    method: 'POST', path: '/files', query: { name, path: folderPath },
    headers: { 'Content-Type': 'application/octet-stream', 'Idempotency-Key': key }, body: data,
  });
  const f = normalizeFileResponse(res);
  if (!f || !f.id) throw new Error('Upload response did not include a file id');
  if (f.size != null && f.size !== data.byteLength) throw new Error(`Upload size mismatch: server ${f.size} vs ${data.byteLength}`);
  return f;
}

const partIdx = (x) => (typeof x === 'number' ? x : x && (x.idx ?? x.index ?? x.part ?? x.partIndex));
const maxPart = (client, fallback) => {
  const remembered = Number.isFinite(client._uploadChunk) && client._uploadChunk > 0 ? client._uploadChunk : 0;
  return clamp(remembered || fallback || MAX_PART_BYTES, 4096, MAX_PART_BYTES);
};

/** Asks the server which of these chunk hashes it already stores (≤500 per call). Returns a Set. */
async function lookupStoredChunks(client, hashes) {
  const out = new Set();
  if (!hashes || !hashes.length || !client.chunksLookup) return out;
  for (let i = 0; i < hashes.length; i += 500) {
    const batch = hashes.slice(i, i + 500);
    try {
      const r = await client.chunksLookup(batch);
      for (const h of (r && (r.available || r.chunks)) || []) out.add(h);
    } catch (e) { /* lookup is an optimisation: on any error just send the bytes */ }
  }
  return out;
}

/**
 * Resumable upload with content-defined chunking (CDC): the boundaries follow the content, so a later edit only
 * changes a few chunks — which is what makes both incremental upload (revision reuse) and incremental download
 * (delta) effective. Falls back to a fixed one-chunk plan when the server does not accept a chunker.
 */
async function uploadResumable(client, { name, folderPath, data, mtime, onProgress, concurrency, chunker, resolveChunk, dedupe }) {
  const bytes = new Uint8Array(data);
  const total = bytes.byteLength;
  const enc = encodeURIComponent;
  const cap = maxPart(client, resolveChunk);

  const send = async (ch) => {
    // ch === false means "no CDC": plain fixed-size parts at the server's chunk size (a base the server CAN
    // revise in place, which is useful when in-place edits matter more than fine-grained deltas).
    let planRes;
    if (ch === false) {
      const plan = fixed(total, cap);
      const hashes = await hashPlan(bytes, plan);
      planRes = { chunker: null, chunks: plan.map((p, i) => ({ offset: p.offset, size: p.size, hash: hashes[i] })) };
    } else {
      planRes = await cdcPlan(bytes, ch);
    }
    const hashes = planRes.chunks.map((c) => c.hash);
    const fingerprint = await fingerprintOf(hashes);
    const clientKey = uuidFromHex(await sha256Hex(`${folderPath}|${name}|${fingerprint}|${total}|${mtime || 0}`));
    const body = { name, path: folderPath, size: total, fingerprint, clientKey, contentType: 'application/octet-stream', lastModified: mtime || Date.now(), totalChunks: planRes.chunks.length };
    if (planRes.chunker) body.chunker = planRes.chunker;
    const sess = await client.request({ method: 'POST', path: '/uploads', headers: { 'Content-Type': 'application/json' }, body: json(body) });
    const id = sess && (sess.id || sess.uploadId);
    if (!id) throw new Error('Upload session response did not include an id');
    if (sess.chunker) ch = normalizeChunker(sess.chunker) || ch;
    return { id, chunks: planRes.chunks, hashes, fingerprint, chunker: planRes.chunker, received: new Set(((sess.received) || []).map(partIdx).filter((x) => Number.isInteger(x))) };
  };

  let s = await send(chunker === false ? false : (normalizeChunker(chunker) || chooseChunker(total, cap)));
  client._uploadChunk = MAX_PART_BYTES;   // remember the cap for later uploads

  const missing = []; let done = 0;
  for (let i = 0; i < s.chunks.length; i++) {
    if (s.received.has(i)) done += s.chunks[i].size; else missing.push(i);
  }
  if (onProgress) onProgress(Math.min(done, total), total);

  const conc = clamp(Math.min(concurrency || 1, Number.isFinite(client._concurrencyHint) ? client._concurrencyHint : Infinity), 1, MAX_CONCURRENCY);
  // Deduplicated upload: chunks that the server already stores (from any earlier upload) are claimed by hash
  // with no bytes on the wire. This is the biggest saving when the same content exists in several files.
  const stored = dedupe === false ? new Set() : await lookupStoredChunks(client, missing.map((i) => s.chunks[i].hash));
  // Recently stored chunks may not be queryable yet (still draining to storage). If the first lookup found
  // little, look the rest up once more after a short pause: one cheap JSON round-trip can save megabytes.
  if (dedupe !== false && missing.length > 2) {
    const stillMissing = missing.filter((i) => !stored.has(s.chunks[i].hash));
    if (stillMissing.length > 2 && stored.size < missing.length * 0.9) {
      await sleep(2500);
      const again = await lookupStoredChunks(client, stillMissing.map((i) => s.chunks[i].hash));
      for (const h of again) stored.add(h);
    }
  }
  let reuseBroken = false;
  await pool(missing, conc, async (i) => {
    const c = s.chunks[i];
    if (!reuseBroken && stored.has(c.hash)) {
      try {
        await client.request({ method: 'PUT', path: `/uploads/${enc(s.id)}/parts/${i}`, headers: { 'X-Content-SHA256': c.hash, 'X-Chunk-Reuse': '1', 'X-Chunk-Offset': String(c.offset), 'X-Chunk-Size': String(c.size) } });
        done += c.size;
        if (onProgress) onProgress(Math.min(done, total), total);
        return;
      } catch (e) { reuseBroken = true; /* fall through and send the bytes */ }
    }
    await client.request({
      method: 'PUT', path: `/uploads/${enc(s.id)}/parts/${i}`,
      headers: { 'Content-Type': 'application/octet-stream', 'X-Content-SHA256': c.hash, 'X-Chunk-Offset': String(c.offset), 'X-Chunk-Size': String(c.size) },
      body: bytes.slice(c.offset, c.offset + c.size),
    });
    done += c.size;
    if (onProgress) onProgress(Math.min(done, total), total);
  });

  const fin = await client.request({ method: 'POST', path: `/uploads/${enc(s.id)}/complete` });
  const srvCk = fin && (fin.checksum || (fin.file && fin.file.checksum));
  if (srvCk && srvCk !== s.fingerprint) throw new Error('Upload checksum mismatch reported by the server — the file was not stored correctly');
  let f = normalizeFileResponse(fin);
  if (!f || !f.id) throw new Error('Completed upload did not return a file');
  if (f.size == null) f = await client.getFile(f.id);
  if (f.size !== total) throw new Error(`Upload size mismatch: server ${f.size} vs ${total}`);
  f.chunker = s.chunker;
  f.chunks = s.chunks;
  return f;
}

/**
 * In-place content update ("revision"): writes a new version of an EXISTING file, transferring only the chunks
 * that changed. Unchanged chunks are reused server-side by hash (no bytes on the wire). Atomic; the file keeps
 * its id/name/path and the previous content is archived. Returns null when a revision does not apply (single
 * chunk / no revision support), so the caller can fall back. Auth/network errors still throw.
 */
async function uploadRevision(client, opts) {
  const { id, mtime, onProgress, concurrency } = opts;
  let data = asArrayBuffer(opts.data);
  if (opts.compressor) data = await opts.compressor.maybeCompress(opts.name, data);
  if (opts.crypto) data = await opts.crypto.encrypt(data);
  const bytes = new Uint8Array(data);
  const total = bytes.byteLength;
  const enc = encodeURIComponent;
  const cap = maxPart(client, opts.chunkBytes);

  const chunker = normalizeChunker(opts.chunker) || chooseChunker(total, cap);
  const planRes = await cdcPlan(bytes, chunker);
  const chunks = planRes.chunks;
  const hashes = chunks.map((c) => c.hash);
  if (chunks.length <= 1) return null;   // nothing to reuse
  const fingerprint = await fingerprintOf(hashes);

  const open = async () => client.request({
    method: 'POST', path: '/uploads', headers: { 'Content-Type': 'application/json' },
    body: json({ baseId: id, mode: 'replace', size: total, fingerprint, clientKey: randomUuid(), contentType: 'application/octet-stream', lastModified: mtime || Date.now(), chunker, totalChunks: chunks.length }),
  });

  let sess = await open();
  if (!sess || !(sess.id || sess.uploadId)) return null;
  const sid = sess.id || sess.uploadId;
  const isRevision = sess.revision === true || (sess.base && Array.isArray(sess.base.parts));
  if (!isRevision) { try { await client.request({ method: 'DELETE', path: '/uploads/' + enc(sid) }); } catch (e) { /* abort */ } return null; }
  client._uploadChunk = MAX_PART_BYTES;

  // Which new chunks the server already stores (base manifest, or a dedup lookup as a fallback).
  const reusable = new Set();
  const baseParts = (sess.base && sess.base.parts) || sess.parts || [];
  if (Array.isArray(baseParts)) for (const p of baseParts) if (p && p.checksum) reusable.add(p.checksum);

  const received = new Set(((sess.received) || []).map(partIdx).filter((x) => Number.isInteger(x)));
  const missing = []; let done = 0; let reused = 0;
  for (let i = 0; i < chunks.length; i++) {
    if (received.has(i)) done += chunks[i].size; else missing.push(i);
  }
  if (onProgress) onProgress(Math.min(done, total), total);

  const conc = clamp(Math.min(concurrency || 1, Number.isFinite(client._concurrencyHint) ? client._concurrencyHint : Infinity), 1, MAX_CONCURRENCY);
  let reuseBroken = false;
  await pool(missing, conc, async (i) => {
    const c = chunks[i];
    if (!reuseBroken && reusable.has(c.hash)) {
      try {
        await client.request({ method: 'PUT', path: `/uploads/${enc(sid)}/parts/${i}`, headers: { 'X-Content-SHA256': c.hash, 'X-Chunk-Reuse': '1', 'X-Chunk-Offset': String(c.offset), 'X-Chunk-Size': String(c.size) } });
        reused++; done += c.size;
        if (onProgress) onProgress(Math.min(done, total), total);
        return;
      } catch (e) { reuseBroken = true; /* send the bytes instead */ }
    }
    await client.request({
      method: 'PUT', path: `/uploads/${enc(sid)}/parts/${i}`,
      headers: { 'Content-Type': 'application/octet-stream', 'X-Content-SHA256': c.hash, 'X-Chunk-Offset': String(c.offset), 'X-Chunk-Size': String(c.size) },
      body: bytes.slice(c.offset, c.offset + c.size),
    });
    done += c.size;
    if (onProgress) onProgress(Math.min(done, total), total);
  });

  // The base's current version may still be finalising: the server answers a retryable 503 RevisionDeferred and
  // the session stays pending. Retrying `complete` is safe (idempotent) and never creates a duplicate.
  let fin; let deferred = 0;
  for (;;) {
    try { fin = await client.request({ method: 'POST', path: `/uploads/${enc(sid)}/complete` }); break; }
    catch (e) {
      if (e.status === 503 && deferred < 8) { deferred++; await sleep(Math.min(15000, (e.retryAfter ? e.retryAfter * 1000 : 2000) + deferred * 750)); continue; }
      throw e;
    }
  }
  const srvCk = fin && (fin.checksum || (fin.file && fin.file.checksum));
  if (srvCk && srvCk !== fingerprint) throw new Error('Upload checksum mismatch reported by the server — the file was not stored correctly');
  const fid = (fin && (fin.id || fin.fileId || (fin.file && fin.file.id))) || id;
  // Safety net: a committed revision MUST keep the base id. If the server answered `already:true` with a
  // different id it quietly created a duplicate instead of updating in place — undo it and report "not usable"
  // so the caller falls back to a normal upload.
  if (fid && fid !== id) {
    try { await client.trash(fid); } catch (e) { /* best effort cleanup of the stray duplicate */ }
    throw new Error('Revision did not apply in place (server returned a different file id)');
  }
  let f = normalizeFileResponse(fin);
  if (!f || !f.id || f.size == null) f = await client.getFile(fid);
  if (f.size != null && f.size !== total) throw new Error(`Upload size mismatch: server ${f.size} vs ${total}`);
  f.reusedParts = reused;
  f.chunker = chunker;
  f.chunks = chunks;
  return f;
}

async function uploadFile(client, opts) {
  let data = asArrayBuffer(opts.data);
  // Shrink first (text-like files only), then encrypt: compressing ciphertext would do nothing. Both steps
  // are transparent on download, and both are self-describing so older plaintext files keep working.
  if (opts.compressor) data = await opts.compressor.maybeCompress(opts.name, data);
  if (opts.crypto) data = await opts.crypto.encrypt(data);
  const lim = await client.limits();
  if (data.byteLength <= lim.oneShotMaxBytes) return uploadOneShot(client, { ...opts, data });
  return uploadResumable(client, { ...opts, data, resolveChunk: lim.chunkBytes });
}

module.exports = { uploadFile, uploadRevision, uploadOneShot, uploadResumable, MAX_PART_BYTES };
