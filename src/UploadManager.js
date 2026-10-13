'use strict';
const { asArrayBuffer, sha256Hex, uuidFromHex, randomUuid } = require('./util');
const { normalizeFileResponse } = require('./normalize');

const json = (o) => new TextEncoder().encode(JSON.stringify(o)).buffer;

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

async function uploadResumable(client, { name, folderPath, data, mtime, onProgress }, chunkBytes) {
  const n = Math.ceil(data.byteLength / chunkBytes);
  const parts = []; const hashes = [];
  for (let i = 0; i < n; i++) {
    const p = data.slice(i * chunkBytes, (i + 1) * chunkBytes);
    parts.push(p); hashes.push(await sha256Hex(p));
  }
  const fingerprint = await sha256Hex(hashes.join('\n'));
  const clientKey = uuidFromHex(await sha256Hex(`${folderPath}|${name}|${fingerprint}|${data.byteLength}|${mtime || 0}`));
  const sess = await client.request({
    method: 'POST', path: '/uploads', headers: { 'Content-Type': 'application/json' },
    body: json({ name, path: folderPath, size: data.byteLength, fingerprint, clientKey, contentType: 'application/octet-stream', lastModified: mtime || Date.now() }),
  });
  const id = (sess && (sess.id || sess.uploadId || (sess.upload && sess.upload.id)));
  if (!id) throw new Error('Upload session response did not include an id');

  const st = await client.request({ path: '/uploads/' + encodeURIComponent(id) });
  const received = new Set(((st && st.received) || []).map(partIdx));
  let done = received.size * chunkBytes;
  for (let i = 0; i < n; i++) {
    if (received.has(i)) continue;
    await client.request({
      method: 'PUT', path: `/uploads/${encodeURIComponent(id)}/parts/${i}`,
      headers: { 'Content-Type': 'application/octet-stream', 'X-Content-SHA256': hashes[i] }, body: parts[i],
    });
    done += parts[i].byteLength;
    if (onProgress) onProgress(Math.min(done, data.byteLength), data.byteLength);
  }
  const fin = await client.request({ method: 'POST', path: `/uploads/${encodeURIComponent(id)}/complete` });
  let f = normalizeFileResponse(fin);
  if (!f || !f.id) throw new Error('Completed upload did not return a file');
  if (f.size == null) f = await client.getFile(f.id);
  if (f.size !== data.byteLength) throw new Error(`Upload size mismatch: server ${f.size} vs ${data.byteLength}`);
  return f;
}

async function uploadFile(client, opts) {
  const data = asArrayBuffer(opts.data);
  const lim = await client.limits();
  if (data.byteLength <= lim.oneShotMaxBytes) return uploadOneShot(client, { ...opts, data });
  return uploadResumable(client, { ...opts, data }, lim.chunkBytes);
}

module.exports = { uploadFile, uploadOneShot, uploadResumable };
