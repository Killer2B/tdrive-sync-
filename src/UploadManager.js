'use strict';
const { asArrayBuffer, sha256Hex, randomUuid, uuidFromHex, pool, clamp, sleep } = require('./util');
const { normalizeFileResponse, normalizeReceipt, normalizeName, normalizePath } = require('./normalize');
const { cdcPlan, chooseChunker, normalizeChunker, fingerprintOf, fixed, hashPlan } = require('./Chunker');

const json = (o) => new TextEncoder().encode(JSON.stringify(o)).buffer;

const MAX_PART_BYTES = 5242880;   // the API hard cap on a single part
const MAX_CONCURRENCY = 6;

async function uploadOneShot(client, { name, folderPath, data }) {
  data = asArrayBuffer(data);
  const chunks = [{ offset: 0, size: data.byteLength, hash: await sha256Hex(data) }];
  const fingerprint = await fingerprintOf(chunks.map((c) => c.hash));
  const key = randomUuid(); // reused across internal retries -> server de-duplicates
  const res = await client.request({
    method: 'POST', path: '/files', query: { name: normalizeName(name), path: normalizePath(folderPath) },
    headers: { 'Content-Type': 'application/octet-stream', 'Idempotency-Key': key }, body: data,
  });
  const ack = normalizeFileResponse(res);
  if (!ack || !ack.id) throw protocolError('Upload response did not include a file id');
  try { return await completedFile(client, res, ack.id, { size: data.byteLength, fingerprint, chunks, data }); }
  catch (e) { e.fileId = ack.id; throw e; }
}

const partIdx = (x) => (typeof x === 'number' ? x : x && (x.idx ?? x.index ?? x.part ?? x.partIndex));
const protocolError = (message) => Object.assign(new Error(message), { code: 'bad_response' });

function checkSession(sess, chunks, cap, chunker, fingerprint) {
  if (!sess || !['pending', 'uploading'].includes(sess.status)) throw protocolError('Upload session is not in an active transfer state');
  if (!sess || !Number.isSafeInteger(sess.chunkSize) || sess.chunkSize <= 0 || sess.chunkSize > cap || (!chunker && sess.chunkSize !== cap)) throw protocolError('Upload session chunkSize disagrees with the upload plan');
  if (!Number.isSafeInteger(sess.totalChunks) || sess.totalChunks !== chunks.length || (sess.fingerprint && sess.fingerprint !== fingerprint)) throw protocolError('Upload session manifest disagrees with the upload plan');
  if (sess.chunker && (!chunker || ['algo', 'min', 'avg', 'max', 'window'].some((k) => sess.chunker[k] !== chunker[k]))) throw protocolError('Upload session changed the chunker');
  if (!Array.isArray(sess.received) || sess.received.some((x) => !Number.isInteger(partIdx(x)) || partIdx(x) < 0 || partIdx(x) >= chunks.length)) throw protocolError('Invalid received upload parts');
}

function checkFingerprint(receipt, fingerprint) {
  if (receipt.checksum != null && receipt.checksum !== fingerprint) throw protocolError('Upload checksum mismatch reported by the server');
}

/** Proves weak receipts without treating a newer GET's content tokens as the committed receipt. */
async function validateUploadReceipt(client, { file, receipt, size, fingerprint, chunks, data }) {
  receipt = normalizeReceipt(receipt || {});
  checkFingerprint(receipt, fingerprint);
  if (!file || !file.id || file.trashed) throw protocolError('Completed upload metadata did not include the file');
  if (receipt.contentVersion == null && receipt.checksum == null) {
    const { fetchManifest } = require('./DownloadManager');
    const manifest = await fetchManifest(client, file.id);
    const changed = () => Object.assign(new Error('Upload content does not match the committed bytes'), { code: 'content_changed' });
    if (manifest) {
      if (manifest.size !== size || (receipt.etag != null && manifest.etag !== receipt.etag)) throw changed();
      if (data != null) {
        const bytes = new Uint8Array(asArrayBuffer(data));
        for (const c of manifest.chunks) if (await sha256Hex(bytes.subarray(c.offset, c.offset + c.size)) !== c.checksum) throw changed();
      } else if (await fingerprintOf(manifest.chunks.map((c) => c.checksum)) !== fingerprint) throw changed();
    } else {
      const res = await client.request({ path: '/files/' + encodeURIComponent(file.id) + '/content', binary: true });
      if (res.status !== 200 || !res.body || res.body.byteLength !== size ||
        (receipt.etag != null && (!res.headers || res.headers.etag !== receipt.etag))) throw changed();
      if (data != null) {
        if (await sha256Hex(res.body) !== await sha256Hex(data)) throw changed();
      } else {
        if (!chunks) throw protocolError('Cannot verify recovered upload without a chunk manifest');
        if (await fingerprintOf(await hashPlan(new Uint8Array(res.body), chunks)) !== fingerprint) throw changed();
      }
    }
  }
  const tokens = Object.fromEntries(['contentVersion', 'checksum', 'etag', 'version'].map((field) => [field, receipt[field] == null ? null : receipt[field]]));
  return { ...file, ...tokens, size, raw: { ...file.raw, ...tokens, size } };
}

async function completedFile(client, response, id, proof) {
  const receipt = normalizeReceipt(response);
  checkFingerprint(receipt, proof.fingerprint);
  const terminal = response && ['ready', 'committed'].includes(response.status);
  const original = terminal && !response.file && !response.item ? null : normalizeFileResponse(response);
  if (original && original.id !== String(id)) throw protocolError('Completed upload returned a different file id');
  const raw = response && (response.file || response.item) || response;
  const ack = normalizeFileResponse({ ...raw, id: String(id) });
  if (ack && ack.size != null && ack.size !== proof.size) throw protocolError('Upload receipt size mismatch');
  let file = original;
  if (!file || !file.name || file.size == null) file = await client.getFile(id);
  if (!file || file.id !== String(id) || !file.name) throw protocolError('Completed upload metadata did not include the file');
  // Name/path may be hydrated, but content-associated metadata must come from the acknowledgment only.
  file = { ...file, mtime: ack ? ack.mtime : null, sha256: ack ? ack.sha256 : null,
    raw: { ...(ack && ack.raw), id: file.id, name: file.name, path: file.path } };
  return validateUploadReceipt(client, { file, receipt, ...proof });
}

async function checkJournal(record, { key, owner, scope, target }) {
  if (record && (record.owner !== owner || record.scope !== scope || record.target !== JSON.stringify(target) || !record.body || record.body.clientKey !== record.clientKey ||
    !Number.isSafeInteger(record.generation) || record.generation < 0 || !/^[0-9a-f]{64}$/.test(record.sourceHash) || !/^[0-9a-f]{64}$/.test(record.source) || !/^[0-9a-f]{64}$/.test(record.fingerprint) ||
    !Number.isSafeInteger(record.size) || record.size < 0 || record.body.fingerprint !== record.fingerprint || record.body.size !== record.size ||
    !['opening', 'uploading', 'completing', 'completed', 'aborting', 'aborted'].includes(record.phase) ||
    record.clientKey !== uuidFromHex(await sha256Hex(JSON.stringify([key, record.source, record.fingerprint, record.generation || 0]))))) throw protocolError('Upload journal ownership mismatch');
}

/** Read-only remote recovery before sync planning; never reopens, completes or cancels a session. */
async function recoverCommittedUpload(client, { key, uploadJournal }) {
  if (!uploadJournal || !uploadJournal.owner || typeof uploadJournal.get !== 'function' || typeof uploadJournal.set !== 'function') throw protocolError('Invalid upload journal');
  let record = await uploadJournal.get(key);
  const scope = await sha256Hex(JSON.stringify([client.origin, client.apiKey]));
  if (!record || record.owner !== uploadJournal.owner || record.scope !== scope || !['opening', 'uploading', 'completing', 'completed'].includes(record.phase)) return null;
  const body = record.body;
  try {
    if (!body) return null;
    const target = body.baseId ? ['replace', String(body.baseId)] : ['create', normalizePath(body.path), normalizeName(body.name)];
    if (key !== await sha256Hex(JSON.stringify([scope, 'owner', uploadJournal.owner, target]))) return null;
    await checkJournal(record, { key, owner: uploadJournal.owner, scope, target });
  } catch (e) { if (e.code === 'bad_response') return null; throw e; }
  if (record.phase !== 'completed') {
    if (!record.sessionId) return null;
    let state;
    try { state = await client.getUpload(record.sessionId); }
    catch (e) { if (e.status === 404) return null; throw e; }
    if (!state || state.clientKey !== record.clientKey) throw protocolError('Upload session ownership changed');
    if (['uploadId', 'id'].some((field) => state[field] != null && String(state[field]) !== String(record.sessionId))) throw protocolError('Upload session response changed the owned session id');
    if (!['ready', 'committed'].includes(state.status)) return null;
    const ids = [state.fileId, state.file_id, body.baseId, record.fileId,
      ...['file', 'item'].flatMap((field) => ['id', 'fileId', 'file_id', '_id'].map((alias) => state[field] && state[field][alias]))].filter((id) => id != null);
    const id = ids[0];
    if (!id || ids.some((other) => String(other) !== String(id))) throw protocolError('Completed upload returned a different file id');
    record = { ...record, fileId: String(id), phase: 'completed', receipt: normalizeReceipt(state) };
    await uploadJournal.set(key, record);
  }
  if (!record.fileId || (body.baseId && record.fileId !== String(body.baseId))) throw protocolError('Completed upload journal file id mismatch');
  const file = await completedFile(client, { fileId: record.fileId, ...record.receipt }, record.fileId,
    { size: record.size, fingerprint: record.fingerprint });
  return { key, record, file };
}

/** Optional durable hook: { owner, get(key), set(key, record) }. Records contain hashes, never payloads. */
async function runSession(client, opts, plan, transfer) {
  let journal = opts.uploadJournal;
  const canAbort = !!journal || (typeof opts.uploadOwner === 'string' && !!opts.uploadOwner);
  if (journal) {
    if (typeof journal.owner !== 'string' || !journal.owner || typeof journal.get !== 'function' || typeof journal.set !== 'function') throw protocolError('Invalid upload journal');
  } else {
    if (!client._uploadSessions) client._uploadSessions = new Map();
    // Preserve unjournaled cross-client resume, but shared legacy identities cannot authorize cancellation.
    journal = { owner: opts.uploadOwner || 'unjournaled', get: (key) => client._uploadSessions.get(key), set: (key, value) => client._uploadSessions.set(key, value) };
  }
  const scope = await sha256Hex(JSON.stringify([client.origin, client.apiKey]));
  const target = plan.body.baseId ? ['replace', String(plan.body.baseId)] : ['create', plan.body.path, plan.body.name];
  const key = await sha256Hex(JSON.stringify([scope, canAbort ? 'owner' : 'shared', journal.owner, target]));
  const sourceHash = opts._sourceHash || await sha256Hex(opts.data);
  const identity = [sourceHash, opts.mtime || 0, !!opts.crypto, !!opts.compressor, plan.chunker];
  if (Object.prototype.hasOwnProperty.call(plan.body, 'expectedVersion')) identity.push(plan.body.expectedVersion);
  const source = await sha256Hex(JSON.stringify(identity));
  let record = await journal.get(key);
  await checkJournal(record, { key, owner: journal.owner, scope, target });
  if (record && record.source === source && record.body.expectedVersion !== plan.body.expectedVersion) throw protocolError('Upload journal changed expectedVersion');
  if (record && record.receipt) record.receipt = normalizeReceipt(record.receipt);
  const save = async () => journal.set(key, { ...record, body: { ...record.body }, ...(record.receipt ? { receipt: { ...record.receipt } } : {}) });
  const statusOf = (s) => s && s.status;
  const abortPending = async () => {
    if (!canAbort) throw Object.assign(protocolError('An owned upload journal is required for cancellation'), { retryable: true });
    if (!record.sessionId) {
      const opened = await client.request({ method: 'POST', path: '/uploads', headers: { 'Content-Type': 'application/json' }, body: json(record.body) });
      record.sessionId = opened && (opened.uploadId || opened.id);
      if (!record.sessionId) throw protocolError('Cannot recover the owned upload session');
      await save();
    }
    let state;
    try { state = await client.getUpload(record.sessionId); }
    catch (e) { if (e.status !== 404) throw e; record.phase = 'aborted'; await save(); return; }
    if (state && state.clientKey && state.clientKey !== record.clientKey) throw protocolError('Upload session ownership changed');
    if (statusOf(state) === 'aborted') { record.phase = 'aborted'; await save(); return; }
    if (['ready', 'committed'].includes(statusOf(state))) { record.phase = 'completed'; record.fileId = state.fileId || record.body.baseId || state.id; record.receipt = normalizeReceipt(state); await save(); return; }
    if (!['pending', 'uploading'].includes(statusOf(state))) throw Object.assign(protocolError('Owned upload is not safe to abort'), { retryable: true });
    record.phase = 'aborting'; await save();
    const ack = await client.abortUpload(record.sessionId);
    // A queued abort is not proof that the pending session has already disappeared.
    record.phase = ack && ack.queued ? 'aborting' : 'aborted';
    await save();
    if (record.phase === 'aborting') throw Object.assign(protocolError('Owned upload abort is queued'), { code: 'upload_abort_pending', retryable: true });
  };
  if (record && record.phase === 'aborting') await abortPending();
  if (record && record.phase !== 'aborted' && (record.source !== source || record.fingerprint !== plan.body.fingerprint || record.size !== plan.body.size)) {
    if (record.phase !== 'completed') await abortPending();
  }
  const same = record && record.phase !== 'aborted' && record.source === source && record.fingerprint === plan.body.fingerprint && record.size === plan.body.size;
  if (plan.inline) return null;
  if (!same) {
    const generation = record ? (record.generation || 0) + 1 : 0;
    const clientKey = uuidFromHex(await sha256Hex(JSON.stringify([key, source, plan.body.fingerprint, generation])));
    record = { owner: journal.owner, scope, target: JSON.stringify(target), sourceHash, source, fingerprint: plan.body.fingerprint, size: plan.body.size, generation, clientKey,
      sessionId: null, phase: 'opening', body: { ...plan.body, clientKey } };
    await save();
  }
  let reused = 0;
  try {
    let fin;
    if (record.phase === 'completed') {
      fin = { fileId: record.fileId, ...record.receipt };
    } else {
      const sess = await client.request({ method: 'POST', path: '/uploads', headers: { 'Content-Type': 'application/json' }, body: json(record.body) });
      const sid = sess && (sess.uploadId || sess.id);
      if (!sid || (record.sessionId && sid !== record.sessionId)) throw protocolError('Upload session response changed the owned session id');
      record.sessionId = sid;
      await save();
      if (!['ready', 'committed'].includes(statusOf(sess))) {
        if (plan.body.baseId && !(sess.revision === true || (sess.base && Array.isArray(sess.base.parts)))) throw protocolError('Server did not open an in-place revision');
        checkSession(sess, plan.chunks, plan.cap, plan.chunker, plan.body.fingerprint);
        record.phase = 'uploading'; await save();
        reused = await transfer(sess, sid) || 0;
      }
      record.phase = 'completing'; await save();
      fin = ['ready', 'committed'].includes(statusOf(sess)) ? sess : await client.request({ method: 'POST', path: `/uploads/${encodeURIComponent(sid)}/complete`,
        ...(Object.prototype.hasOwnProperty.call(record.body, 'expectedVersion') ? { headers: { 'Content-Type': 'application/json' }, body: json({ expectedVersion: record.body.expectedVersion }) } : {}) });
      const f = normalizeFileResponse(fin);
      let fid = fin && ((fin.file && fin.file.id) || fin.fileId) ||
        (['ready', 'committed'].includes(statusOf(sess)) ? plan.body.baseId || (statusOf(sess) === 'ready' && sess.id) : (f && f.id) || plan.body.baseId);
      if (!fid) {
        const state = await client.getUpload(sid);
        fid = state && (state.fileId || (state.file && state.file.id) || (state.status === 'ready' && state.id));
      }
      if (!fid) throw protocolError('Completed upload did not return a file id');
      if (plan.body.baseId && String(fid) !== String(plan.body.baseId)) throw protocolError('Revision did not apply in place (server returned a different file id)');
      record.receipt = normalizeReceipt(fin);
      record.fileId = String(fid); record.phase = 'completed'; await save();
    }
    if (plan.body.baseId && record.fileId !== String(plan.body.baseId)) throw protocolError('Revision did not apply in place (server returned a different file id)');
    const f = await completedFile(client, fin, record.fileId,
      { size: plan.body.size, fingerprint: plan.body.fingerprint, chunks: plan.chunks, data: plan.data });
    f.chunker = plan.chunker; f.chunks = plan.chunks;
    if (plan.body.baseId) f.reusedParts = reused;
    return f;
  } catch (e) {
    e.uploadId = record.sessionId;
    // Never abort committed revisions, auth failures, outages, or another device's session.
    if (record.phase !== 'completed' && ![412, 423].includes(e.status) && !['VERSION_CONFLICT', 'PRECONDITION_FAILED', 'LEASE_CONFLICT'].includes(e.code) && !e.retryable && !e.isNetwork && (e.code === 'bad_response' || (e.status >= 400 && e.status < 500 && ![401, 403, 404, 429].includes(e.status)))) {
      try { await abortPending(); }
      catch (cleanup) { e.uploadCleanupError = cleanup.message; }
    }
    throw e;
  }
}

/** Asks the server which of these chunk hashes it already stores (≤500 per call). Returns a Set. */
async function lookupStoredChunks(client, hashes) {
  const out = new Set();
  if (!hashes || !hashes.length || !client.chunksLookup) return out;
  for (let i = 0; i < hashes.length; i += 500) {
    const batch = hashes.slice(i, i + 500);
    try {
      const r = await client.chunksLookup(batch);
      for (const h of (r && (r.available || r.chunks)) || []) out.add(h);
    } catch (e) { if (e.status !== 404 && e.status !== 405) throw e; }
  }
  return out;
}

/**
 * Resumable upload with content-defined chunking (CDC): the boundaries follow the content, so a later edit only
 * changes a few chunks — which is what makes both incremental upload (revision reuse) and incremental download
 * (delta) effective. Falls back to a fixed one-chunk plan when the server does not accept a chunker.
 */
async function uploadResumable(client, opts) {
  let { name, folderPath } = opts;
  const { data, mtime, onProgress, concurrency, chunker, dedupe } = opts;
  name = normalizeName(name); folderPath = normalizePath(folderPath);
  const bytes = new Uint8Array(data);
  const total = bytes.byteLength;
  const enc = encodeURIComponent;
  const cap = (await client.limits()).chunkBytes;

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
    if (!hashes.length || hashes.length > 200000) throw new Error('Upload chunk count exceeds API limits');
    const fingerprint = await fingerprintOf(hashes);
    const body = { name, path: folderPath, size: total, fingerprint, contentType: 'application/octet-stream', lastModified: mtime || Date.now(), totalChunks: planRes.chunks.length };
    if (planRes.chunker) body.chunker = planRes.chunker;
    return { body, chunks: planRes.chunks, chunker: planRes.chunker, cap, data };
  };

  if (chunker && !normalizeChunker(chunker, cap)) throw new Error('Invalid upload chunker');
  const s = await send(chunker === false ? false : (normalizeChunker(chunker, cap) || chooseChunker(total, cap)));
  client._uploadChunk = cap;

  return runSession(client, opts, s, async (sess, id) => {
    const received = new Set(sess.received.map(partIdx));
    const missing = []; let done = 0;
    for (let i = 0; i < s.chunks.length; i++) {
      if (received.has(i)) done += s.chunks[i].size; else missing.push(i);
    }
    if (onProgress) onProgress(Math.min(done, total), total);

    const conc = clamp(Math.min(concurrency || 1, Number.isFinite(client._concurrencyHint) ? client._concurrencyHint : Infinity), 1, MAX_CONCURRENCY);
    // Deduplicated upload: chunks already stored can be claimed by hash without sending bytes.
    const stored = dedupe === false ? new Set() : await lookupStoredChunks(client, missing.map((i) => s.chunks[i].hash));
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
          await client.request({ method: 'PUT', path: `/uploads/${enc(id)}/parts/${i}`, headers: { 'X-Content-SHA256': c.hash, 'X-Chunk-Reuse': '1', 'X-Chunk-Offset': String(c.offset), 'X-Chunk-Size': String(c.size) } });
          done += c.size;
          if (onProgress) onProgress(Math.min(done, total), total);
          return;
        } catch (e) { if (e.status !== 409 || e.code !== 'ChunkNotAvailable') throw e; reuseBroken = true; }
      }
      await client.request({
        method: 'PUT', path: `/uploads/${enc(id)}/parts/${i}`,
        headers: { 'Content-Type': 'application/octet-stream', 'X-Content-SHA256': c.hash, 'X-Chunk-Offset': String(c.offset), 'X-Chunk-Size': String(c.size) },
        body: bytes.slice(c.offset, c.offset + c.size),
      });
      done += c.size;
      if (onProgress) onProgress(Math.min(done, total), total);
    });
  });
}

/**
 * In-place content update ("revision"): writes a new version of an EXISTING file, transferring only the chunks
 * that changed. Unchanged chunks are reused server-side by hash (no bytes on the wire). Atomic; the file keeps
 * its id/name/path and the previous content is archived. Returns null when a revision does not apply (single
 * chunk / no revision support), so the caller can fall back. Auth/network errors still throw.
 */
async function uploadRevision(client, opts) {
  const { id, mtime, onProgress, concurrency, expectedVersion } = opts;
  if (expectedVersion !== undefined && (!Number.isSafeInteger(expectedVersion) || expectedVersion <= 0)) throw Object.assign(new Error('Invalid expectedVersion'), { code: 'bad_request' });
  let data = asArrayBuffer(opts.data);
  const sourceHash = await sha256Hex(data);
  if (opts.compressor) data = await opts.compressor.maybeCompress(opts.name, data);
  if (opts.crypto) data = await opts.crypto.encrypt(data);
  const bytes = new Uint8Array(data);
  const total = bytes.byteLength;
  const enc = encodeURIComponent;
  let cap = (await client.limits()).chunkBytes;
  if (opts.chunker === false) {
    try {
      const base = await client.request({ path: '/files/' + enc(id) + '/chunks' });
      if (!base || !Number.isSafeInteger(base.chunkSize) || base.chunkSize <= 0) throw protocolError('Invalid base file chunkSize');
      cap = base.chunkSize;
    } catch (e) { if (e.status !== 404) throw e; }
  }
  if (opts.chunker && !normalizeChunker(opts.chunker, cap)) throw new Error('Invalid revision chunker');
  const chunker = opts.chunker === false ? null : (normalizeChunker(opts.chunker, cap) || chooseChunker(total, cap));
  const planRes = chunker ? await cdcPlan(bytes, chunker) : { chunks: [] };
  if (!chunker) {
    const plan = fixed(total, cap); const hs = await hashPlan(bytes, plan);
    planRes.chunks = plan.map((p, i) => ({ ...p, hash: hs[i] }));
  }
  const chunks = planRes.chunks;
  const hashes = chunks.map((c) => c.hash);
  if (chunks.length > 200000) throw new Error('Revision chunk count exceeds API limits');
  const fingerprint = await fingerprintOf(hashes);

  const body = { baseId: id, mode: 'replace', ...(expectedVersion === undefined ? {} : { expectedVersion }), size: total, fingerprint, contentType: 'application/octet-stream', lastModified: mtime || Date.now(), ...(chunker ? { chunker } : {}), totalChunks: chunks.length };
  return runSession(client, { ...opts, _sourceHash: sourceHash }, { body, chunks, chunker, cap, data, inline: chunks.length <= 1 }, async (sess, sid) => {
    const reusable = new Set();
    const baseParts = (sess.base && sess.base.parts) || sess.parts || [];
    if (Array.isArray(baseParts)) for (const p of baseParts) if (p && p.checksum) reusable.add(p.checksum);

    const received = new Set(sess.received.map(partIdx));
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
        } catch (e) { if (e.status !== 409 || e.code !== 'ChunkNotAvailable') throw e; reuseBroken = true; }
      }
      await client.request({
        method: 'PUT', path: `/uploads/${enc(sid)}/parts/${i}`,
        headers: { 'Content-Type': 'application/octet-stream', 'X-Content-SHA256': c.hash, 'X-Chunk-Offset': String(c.offset), 'X-Chunk-Size': String(c.size) },
        body: bytes.slice(c.offset, c.offset + c.size),
      });
      done += c.size;
      if (onProgress) onProgress(Math.min(done, total), total);
    });
    return reused;
  });
}

async function uploadFile(client, opts) {
  let data = asArrayBuffer(opts.data);
  const sourceHash = await sha256Hex(data);
  // Shrink first (text-like files only), then encrypt: compressing ciphertext would do nothing. Both steps
  // are transparent on download, and both are self-describing so older plaintext files keep working.
  if (opts.compressor) data = await opts.compressor.maybeCompress(opts.name, data);
  if (opts.crypto) data = await opts.crypto.encrypt(data);
  const lim = await client.limits();
  if (data.byteLength <= lim.oneShotMaxBytes) {
    if (opts.uploadJournal || client._uploadSessions) {
      const body = { name: normalizeName(opts.name), path: normalizePath(opts.folderPath), size: data.byteLength, fingerprint: await fingerprintOf([await sha256Hex(data)]) };
      await runSession(client, { ...opts, data, _sourceHash: sourceHash }, { body, chunker: null, inline: true }, null);
    }
    return uploadOneShot(client, { ...opts, data });
  }
  return uploadResumable(client, { ...opts, data, _sourceHash: sourceHash });
}

module.exports = { uploadFile, uploadRevision, uploadOneShot, uploadResumable, validateUploadReceipt, recoverCommittedUpload, MAX_PART_BYTES };
