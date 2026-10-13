'use strict';
// The public docs do not specify the JSON shape of file objects. These normalizers read the
// most plausible field names defensively. Every assumption is listed in docs/API-CONTRACT.md.

function pick(o, keys) {
  for (const k of keys) if (o && o[k] != null) return o[k];
  return undefined;
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
  if (typeof raw === 'string') return asFolder && raw ? { id: null, name: raw.replace(/\/+$/, ''), size: null, mtime: null, sha256: null, isFolder: true, raw } : null;
  if (!raw || typeof raw !== 'object') return null;
  const folder = !!asFolder || isFolderRaw(raw);
  const size = pick(raw, ['size', 'bytes', 'sizeBytes', 'size_bytes', 'length']);
  const id = pick(raw, ['id', 'fileId', 'file_id', 'folderId', 'folder_id', '_id']);
  let name = pick(raw, ['name', 'filename', 'fileName', 'title']);
  // Some servers describe a folder only by its path ("/a/b/"): the name is the last segment.
  if (name == null && folder) name = lastSeg(pick(raw, ['path', 'fullPath', 'full_path']));
  if (typeof name === 'string' && folder) name = name.replace(/\/+$/, '').split('/').pop();
  return {
    id: id != null ? String(id) : null,
    name,
    size: size == null ? null : Number(size),
    mtime: parseTime(pick(raw, ['lastModified', 'modifiedAt', 'modified_at', 'updatedAt', 'updated_at', 'mtime', 'modified'])),
    sha256: pick(raw, ['sha256', 'contentSha256']) || null,
    isFolder: folder,
    trashed: !!(raw.trashed || raw.isTrashed || raw.deleted || raw.deletedAt || raw.deleted_at || raw.inTrash),
    raw,
  };
}

function normalizeFileResponse(data) {
  const raw = (data && (data.file || data.item)) || data;
  return normalizeEntry(raw);
}

function normalizeList(data) {
  const files = [];
  const folders = [];
  const push = (r) => {
    const e = normalizeEntry(r);
    if (!e || !e.name) return;
    (e.isFolder ? folders : files).push(e);
  };
  if (Array.isArray(data)) data.forEach(push);
  else if (data && typeof data === 'object') {
    for (const k of ['files', 'items', 'entries', 'results']) if (Array.isArray(data[k])) data[k].forEach(push);
    for (const k of ['folders', 'directories', 'dirs']) {
      if (Array.isArray(data[k])) data[k].forEach((r) => { const e = normalizeEntry(r, true); if (e && e.name) folders.push(e); });
    }
  }
  return { files, folders };
}

function remoteSig(r) {
  return `${r.id}|${r.size == null ? '' : r.size}|${r.mtime == null ? '' : r.mtime}`;
}

module.exports = { normalizeEntry, normalizeFileResponse, normalizeList, remoteSig, parseTime };
