'use strict';
const { asArrayBuffer, sha256Hex, sha256HexView, uuidFromHex, randomUuid, pool, clamp } = require('./util');
const { normalizeFileResponse } = require('./normalize');

const json = (o) => new TextEncoder().encode(JSON.stringify(o)).buffer;

// Conservative guesses used only to build the FIRST session; the server's own advertised part size wins.
const MAX_PART_BYTES = 5000000;
const MIN_PART_BYTES = 256 * 1024;
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

async function uploadResumable(client, { name, folderPath, data, mtime, onProgress, concurrency }, provisionalChunk) {
  const total = data.byteLength;
  const enc = encodeURIComponent;

  // Opens a session for a given part size. The fingerprint must cover the exact parts, so it is computed
  // here from `chunk`. Hashing uses views (no copy of the data).
  const openSession = async (chunk) => {
    const n = Math.max(1, Math.ceil(total / chunk));
    const hashes = new Array(n);
    for (let i = 0; i < n; i++) {
      const start = i * chunk;
      hashes[i] = await sha256HexView(data, start, Math.min(start + chunk, total) - start);
    }
    const fingerprint = await sha256Hex(hashes.join('\n'));
    const clientKey = uuidFromHex(await sha256Hex(`${folderPath}|${name}|${fingerprint}|${total}|${mtime || 0}`));
    const sess = await client.request({
      method: 'POST', path: '/uploads', headers: { 'Content-Type': 'application/json' },
      body: json({ name, path: folderPath, size: total, fingerprint, clientKey, contentType: 'application/octet-stream', lastModified: mtime || Date.now() }),
    });
    const id = (sess && (sess.id || sess.uploadId || (sess.upload && sess.upload.id)));
    if (!id) throw new Error('Upload session response did not include an id');
    const srv = Number(sess && sess.chunkSize);
    return { id, chunk, n, hashes, fingerprint, srvChunk: Number.isFinite(srv) && srv > 0 ? srv : null };
  };

  let s = await openSession(provisionalChunk);
  // Some servers (Tdrive among them) enforce their own part size, reported only in the session. The part
  // size decides the fingerprint, so discard that session and re-open with the server's size.
  if (s.srvChunk && s.srvChunk !== provisionalChunk) {
    try { await client.request({ method: 'DELETE', path: '/uploads/' + enc(s.id) }); } catch (e) { /* best effort */ }
    s = await openSession(s.srvChunk);
  }
  const chunk = s.srvChunk || s.chunk;
  const n = s.n;
  const hashes = s.hashes;
  const id = s.id;
  client._uploadChunk = chunk;   // reuse the authoritative size for later uploads on this client

  const st = await client.request({ path: '/uploads/' + enc(id) });
  const received = new Set(((st && (st.received || st.parts)) || []).map(partIdx).filter((x) => Number.isInteger(x)));
  const missing = [];
  let done = 0;
  for (let i = 0; i < n; i++) {
    const len = Math.min(chunk, total - i * chunk);
    if (received.has(i)) done += len; else missing.push(i);
  }
  if (onProgress) onProgress(Math.min(done, total), total);

  const conc = clamp(Math.min(concurrency || 1, Number.isFinite(client._concurrencyHint) ? client._concurrencyHint : Infinity), 1, MAX_CONCURRENCY);
  await pool(missing, conc, async (i) => {
    const start = i * chunk;
    const len = Math.min(chunk, total - start);
    await client.request({
      method: 'PUT', path: `/uploads/${enc(id)}/parts/${i}`,
      headers: { 'Content-Type': 'application/octet-stream', 'X-Content-SHA256': hashes[i] },
      body: data.slice(start, start + len),   // one transient copy per in-flight part, freed right after
    });
    done += len;
    if (onProgress) onProgress(Math.min(done, total), total);
  });

  const fin = await client.request({ method: 'POST', path: `/uploads/${enc(id)}/complete` });
  // The server returns its own checksum (sha256 of the part-hash list) which must equal our fingerprint.
  const srvCk = fin && (fin.checksum || (fin.file && fin.file.checksum));
  if (srvCk && srvCk !== s.fingerprint) throw new Error('Upload checksum mismatch reported by the server — the file was not stored correctly');
  let f = normalizeFileResponse(fin);
  if (!f || !f.id) throw new Error('Completed upload did not return a file');
  if (f.size == null) f = await client.getFile(f.id);
  if (f.size !== total) throw new Error(`Upload size mismatch: server ${f.size} vs ${total}`);
  return f;
}

/**
 * In-place content update ("revision"): writes a new version of an EXISTING file, transferring only the parts
 * that changed. Unchanged parts are reused server-side by SHA-256 (X-Chunk-Reuse: 1, no bytes on the wire).
 * The commit is atomic and the file keeps its id, name, path and share links.
 *
 * Returns the updated file on success, or null when a revision does not apply (single-part file, server without
 * revision support) so the caller can fall back to the normal create path. Auth/network errors still throw.
 */
async function uploadRevision(client, opts) {
  const { id, mtime, onProgress, concurrency } = opts;
  let data = asArrayBuffer(opts.data);        // accept a Buffer or ArrayBuffer transparently
  // Apply the SAME preparation as a normal upload (shrink then encrypt), so reused chunks match the base's
  // stored chunks and nothing is ever stored in the clear when E2EE is on. (Encryption randomises the payload,
  // so with E2EE on nothing is reused — expected; the file is simply re-sent.)
  if (opts.compressor) data = await opts.compressor.maybeCompress(opts.name, data);
  if (opts.crypto) data = await opts.crypto.encrypt(data);
  const total = data.byteLength;
  const enc = encodeURIComponent;
  const hint = Number.isFinite(client._concurrencyHint) ? client._concurrencyHint : Infinity;

  const guess = Number.isFinite(client._uploadChunk) && client._uploadChunk > 0 ? client._uploadChunk
    : (Number.isFinite(opts.chunkBytes) && opts.chunkBytes > 0 ? opts.chunkBytes : MAX_PART_BYTES);

  // Open a revision session for a given part size. The fingerprint covers the exact parts, so it is computed here.
  const open = async (chunk) => {
    const n = Math.max(1, Math.ceil(total / chunk));
    const hashes = new Array(n);
    for (let i = 0; i < n; i++) {
      const start = i * chunk;
      hashes[i] = await sha256HexView(data, start, Math.min(start + chunk, total) - start);
    }
    const fingerprint = await sha256Hex(hashes.join('\n'));
    const sess = await client.request({
      method: 'POST', path: '/uploads', headers: { 'Content-Type': 'application/json' },
      body: json({ baseId: id, mode: 'replace', size: total, fingerprint, clientKey: randomUuid(), contentType: 'application/octet-stream', lastModified: mtime || Date.now() }),
    });
    const sid = sess && (sess.id || sess.uploadId);
    if (!sid) return null;
    const srv = Number(sess && sess.chunkSize);
    return { sid, chunk, n, hashes, fingerprint, sess, srvChunk: Number.isFinite(srv) && srv > 0 ? srv : null };
  };

  let s = await open(clamp(Math.min(guess, MAX_PART_BYTES), MIN_PART_BYTES, MAX_PART_BYTES));
  if (!s) return null;
  if (s.n <= 1) { try { await client.request({ method: 'DELETE', path: '/uploads/' + enc(s.sid) }); } catch (e) { /* abort */ } return null; }
  // A revision always uses the base file's part size; if it differs from the guess, reopen with the right one.
  if (s.srvChunk && s.srvChunk !== s.chunk) {
    try { await client.request({ method: 'DELETE', path: '/uploads/' + enc(s.sid) }); } catch (e) { /* abort */ }
    s = await open(s.srvChunk);
    if (!s) return null;
  }
  client._uploadChunk = s.chunk;
  const isRevision = s.sess && (s.sess.revision === true || (s.sess.base && Array.isArray(s.sess.base.parts)));
  if (!isRevision) { try { await client.request({ method: 'DELETE', path: '/uploads/' + enc(s.sid) }); } catch (e) { /* abort */ } return null; }

  const chunk = s.chunk; const n = s.n; const hashes = s.hashes; const sid = s.sid;
  // Which parts the base already stores, by hash -> we can claim them without sending bytes.
  const reusable = new Set();
  const baseParts = (s.sess.base && s.sess.base.parts) || s.sess.parts || [];
  if (Array.isArray(baseParts)) for (const p of baseParts) if (p && p.checksum) reusable.add(p.checksum);

  const received = new Set(((s.sess.received) || []).map(partIdx).filter((x) => Number.isInteger(x)));
  const missing = []; let done = 0; let reused = 0;
  for (let i = 0; i < n; i++) {
    const len = Math.min(chunk, total - i * chunk);
    if (received.has(i)) done += len; else missing.push(i);
  }
  if (onProgress) onProgress(Math.min(done, total), total);

  const conc = clamp(Math.min(concurrency || 1, hint), 1, MAX_CONCURRENCY);
  let reuseBroken = false;   // once a reuse is refused, stop trying (send bytes for the rest)
  await pool(missing, conc, async (i) => {
    const start = i * chunk;
    const len = Math.min(chunk, total - start);
    if (!reuseBroken && reusable.has(hashes[i])) {
      try {
        await client.request({ method: 'PUT', path: `/uploads/${enc(sid)}/parts/${i}`, headers: { 'X-Content-SHA256': hashes[i], 'X-Chunk-Reuse': '1' } });
        reused++; done += len;
        if (onProgress) onProgress(Math.min(done, total), total);
        return;
      } catch (e) { reuseBroken = true; /* fall through: send the bytes */ }
    }
    await client.request({
      method: 'PUT', path: `/uploads/${enc(sid)}/parts/${i}`,
      headers: { 'Content-Type': 'application/octet-stream', 'X-Content-SHA256': hashes[i] },
      body: data.slice(start, start + len),
    });
    done += len;
    if (onProgress) onProgress(Math.min(done, total), total);
  });

  const fin = await client.request({ method: 'POST', path: `/uploads/${enc(sid)}/complete` });
  const srvCk = fin && (fin.checksum || (fin.file && fin.file.checksum));
  if (srvCk && srvCk !== s.fingerprint) throw new Error('Upload checksum mismatch reported by the server — the file was not stored correctly');
  const fid = (fin && (fin.id || fin.fileId || (fin.file && fin.file.id))) || id;
  let f = normalizeFileResponse(fin);
  if (!f || !f.id || f.size == null) f = await client.getFile(fid);
  if (f.size != null && f.size !== total) throw new Error(`Upload size mismatch: server ${f.size} vs ${total}`);
  f.reusedParts = reused;
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
  const remembered = Number.isFinite(client._uploadChunk) && client._uploadChunk > 0 ? client._uploadChunk : 0;
  const configured = Number.isFinite(opts.chunkBytes) && opts.chunkBytes > 0 ? opts.chunkBytes : lim.chunkBytes;
  const provisional = remembered || clamp(Math.min(configured, MAX_PART_BYTES), MIN_PART_BYTES, MAX_PART_BYTES);
  return uploadResumable(client, { ...opts, data }, provisional);
}

module.exports = { uploadFile, uploadRevision, uploadOneShot, uploadResumable, MAX_PART_BYTES };

