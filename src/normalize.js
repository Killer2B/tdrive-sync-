'use strict';
// Reject incomplete listings rather than interpreting an unknown response as an empty directory.

function badResponse(message) { return Object.assign(new Error(message), { code: 'bad_response' }); }

function normalizeName(name) {
  if (typeof name !== 'string' || !name || /[\/\\\x00-\x1f\x7f]/.test(name) || name === '.' || name === '..') throw badResponse('Invalid remote name');
  return name.normalize('NFC');
}

function normalizePath(path) {
  if (typeof path !== 'string' || !path.startsWith('/')) throw badResponse('Invalid remote path');
  const p = path.normalize('NFC');
  const full = p.endsWith('/') ? p : p + '/';
  const parts = full === '/' ? [] : full.slice(1, -1).split('/');
  if (full.length > 1024 || parts.length > 32) throw badResponse('Remote path exceeds API limits');
  parts.forEach(normalizeName);
  return full;
}

function pick(o, keys) {
  for (const k of keys) if (o && o[k] != null) return o[k];
  return undefined;
}

// Presence matters for receipts: an explicit null must not fall through to a newer GET head.
function normalizeReceipt(data) {
  const out = {};
  const objects = [data, data && data.file, data && data.item, data && data.content];
  const fields = { contentVersion: ['contentVersion', 'content_version'], checksum: ['checksum', 'contentChecksum', 'content_checksum'], etag: ['etag', 'contentEtag', 'content_etag'], version: ['version'] };
  for (const o of objects) if (o && typeof o === 'object') {
    for (const k of fields.contentVersion) if (Object.prototype.hasOwnProperty.call(o, k) && o[k] != null && (!Number.isSafeInteger(o[k]) || o[k] <= 0)) throw badResponse('Invalid remote contentVersion');
  }
  for (const [field, aliases] of Object.entries(fields)) {
    for (const o of objects) {
      const key = aliases.find((k) => o && Object.prototype.hasOwnProperty.call(o, k) && o[k] !== undefined);
      if (key !== undefined) { out[field] = o[key]; break; }
    }
  }
  return out;
}

function parseTime(v) {
  if (v == null || v === '') return null;
  if (typeof v === 'number') return v < 1e11 ? Math.round(v * 1000) : v;
  if (typeof v === 'string') {
    if (/^\d+(\.\d+)?$/.test(v)) return parseTime(Number(v));
    const t = Date.parse(v);
    return Number.isNaN(t) ? null : t;
  }
  return null;
}

const FOLDER_WORDS = ['folder', 'directory', 'dir'];
const lastSeg = (p) => { const a = String(p || '').split('/').filter(Boolean); return a.length ? a[a.length - 1] : undefined; };

function isFolderRaw(r) {
  const t = (v) => (typeof v === 'string' ? v.toLowerCase() : '');
  return FOLDER_WORDS.includes(t(r.type)) || FOLDER_WORDS.includes(t(r.kind)) || r.isFolder === true || r.is_folder === true ||
    r.isDir === true || r.isDirectory === true || /(^|[/.+-])(x-)?(folder|directory)$/.test(t(r.mimeType));
}

function normalizeEntry(raw, asFolder) {
  if (typeof raw === 'string') return asFolder ? { id: null, name: normalizeName(raw), size: null, mtime: null, sha256: null, isFolder: true, raw } : null;
  if (!raw || typeof raw !== 'object') return null;
  const folder = !!asFolder || isFolderRaw(raw);
  const size = pick(raw, ['size', 'bytes', 'sizeBytes', 'size_bytes', 'length']);
  if (size != null && ((!Number.isSafeInteger(Number(size)) || Number(size) < 0) || (typeof size !== 'number' && !(typeof size === 'string' && /^\d+$/.test(size))))) throw badResponse('Invalid remote file size');
  const id = pick(raw, ['id', 'fileId', 'file_id', 'folderId', 'folder_id', '_id']);
  let name = pick(raw, ['name', 'filename', 'fileName', 'title']);
  // Some servers describe a folder only by its path ("/a/b/"): the name is the last segment.
  if (name == null && folder) name = lastSeg(pick(raw, ['path', 'fullPath', 'full_path']));
  const receipt = normalizeReceipt(raw);
  return {
    id: id != null ? String(id) : null,
    name: typeof name === 'string' ? normalizeName(name) : name,
    path: raw.path == null ? null : normalizePath(raw.path),
    size: size == null ? null : Number(size),
    mtime: parseTime(pick(raw, ['lastModified', 'modifiedAt', 'modified_at', 'updatedAt', 'updated_at', 'mtime', 'modified'])),
    sha256: pick(raw, ['sha256', 'contentSha256']) || null,
    checksum: receipt.checksum == null ? null : receipt.checksum,
    etag: receipt.etag == null ? null : receipt.etag,
    contentVersion: receipt.contentVersion == null ? null : receipt.contentVersion,
    version: raw.version == null ? null : raw.version,
    isFolder: folder,
    trashed: !!(raw.trashed || raw.isTrashed || raw.deleted || raw.deletedAt || raw.deleted_at || raw.inTrash),
    raw,
  };
}

function normalizeFileResponse(data) {
  const raw = (data && (data.file || data.item)) || data;
  const f = normalizeEntry(raw);
  const receipt = normalizeReceipt(data);
  return f && f.id && !f.isFolder ? Object.assign(f, receipt) : null;
}

function normalizeList(data) {
  const files = [];
  const folders = [];
  const push = (r, folder = false) => {
    const e = normalizeEntry(r, folder);
    if (!e || !e.name || (!e.isFolder && (!e.id || !Number.isSafeInteger(e.size) || e.size < 0))) throw badResponse('Malformed remote listing entry');
    (e.isFolder ? folders : files).push(e);
  };
  let recognized = false;
  if (Array.isArray(data)) { recognized = true; data.forEach((r) => push(r)); }
  else if (data && typeof data === 'object') {
    for (const k of ['files', 'items', 'entries', 'results']) if (Object.prototype.hasOwnProperty.call(data, k)) {
      if (!Array.isArray(data[k])) throw badResponse('Malformed remote file listing');
      recognized = true; data[k].forEach((r) => push(r));
    }
    for (const k of ['folders', 'directories', 'dirs']) {
      if (Object.prototype.hasOwnProperty.call(data, k)) {
        if (!Array.isArray(data[k])) throw badResponse('Malformed remote folder listing');
        data[k].forEach((r) => push(r, true));
      }
    }
  }
  if (!recognized) throw badResponse('Unrecognized remote listing');
  return { files, folders };
}

function remoteSig(r) {
  const version = r.contentVersion == null ? r.version : r.contentVersion;
  return `${r.id}|${r.size == null ? '' : r.size}|${r.mtime == null ? '' : r.mtime}|${r.checksum || ''}|${version == null ? '' : version}`;
}

module.exports = { normalizeEntry, normalizeFileResponse, normalizeList, normalizeReceipt, remoteSig, parseTime, normalizeName, normalizePath };
