'use strict';
const { concatBuffers } = require('./util');

/**
 * Ranged download (HTTP Range is documented). Each chunk is its own request, so an interrupted
 * transfer resumes from the last received offset instead of restarting. Verified by size.
 */
async function downloadFile(client, file, opts = {}) {
  const chunkBytes = opts.chunkBytes || 4 * 1024 * 1024;
  const known = Number.isFinite(file.size) ? file.size : null;
  if (known === 0) return new ArrayBuffer(0);
  const chunks = []; let offset = 0; let total = known;
  while (total === null || offset < total) {
    const res = await client.request({
      path: '/files/' + encodeURIComponent(file.id) + '/content',
      headers: { Range: `bytes=${offset}-${offset + chunkBytes - 1}` }, binary: true,
    });
    if (res.status === 206) {
      const m = /bytes\s+(\d+)-(\d+)\/(\d+|\*)/i.exec(res.headers['content-range'] || '');
      if (m) {
        if (Number(m[1]) !== offset) throw new Error('Unexpected Content-Range start');
        if (m[3] !== '*') total = Number(m[3]);
      }
      if (res.body.byteLength === 0) break;
      chunks.push(res.body); offset += res.body.byteLength;
      if (total === null && res.body.byteLength < chunkBytes) { total = offset; }
    } else {
      // Server ignored Range and returned the whole body.
      const whole = offset === 0 ? res.body : res.body.slice(offset);
      chunks.push(whole); offset += whole.byteLength; total = offset;
    }
    if (opts.onProgress) opts.onProgress(offset, total);
  }
  if (known !== null && offset !== known) throw new Error(`Incomplete download: ${offset}/${known} bytes`);
  return concatBuffers(chunks);
}

module.exports = { downloadFile };
