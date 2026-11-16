'use strict';
// Release ZIPs contain installable plugin code only, never account settings or sync history.
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const root = path.join(__dirname, '..');
function crc32(bytes) {
  let crc = 0xffffffff;
  for (const byte of bytes) { crc ^= byte; for (let i = 0; i < 8; i++) crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1)); }
  return (crc ^ 0xffffffff) >>> 0;
}
function archive(files) {
  const local = []; const central = []; let offset = 0; let centralSize = 0;
  for (const { name, data } of files) {
    const filename = Buffer.from(name); const packed = zlib.deflateRawSync(data); const crc = crc32(data);
    const h = Buffer.alloc(30); h.writeUInt32LE(0x04034b50); h.writeUInt16LE(20, 4); h.writeUInt16LE(0x800, 6); h.writeUInt16LE(8, 8); h.writeUInt16LE(33, 12);
    h.writeUInt32LE(crc, 14); h.writeUInt32LE(packed.length, 18); h.writeUInt32LE(data.length, 22); h.writeUInt16LE(filename.length, 26);
    local.push(h, filename, packed);
    const c = Buffer.alloc(46); c.writeUInt32LE(0x02014b50); c.writeUInt16LE(20, 4); c.writeUInt16LE(20, 6); c.writeUInt16LE(0x800, 8); c.writeUInt16LE(8, 10); c.writeUInt16LE(33, 14);
    c.writeUInt32LE(crc, 16); c.writeUInt32LE(packed.length, 20); c.writeUInt32LE(data.length, 24); c.writeUInt16LE(filename.length, 28); c.writeUInt32LE(offset, 42);
    central.push(c, filename); centralSize += c.length + filename.length; offset += h.length + filename.length + packed.length;
  }
  const end = Buffer.alloc(22); end.writeUInt32LE(0x06054b50); end.writeUInt16LE(files.length, 8); end.writeUInt16LE(files.length, 10); end.writeUInt32LE(centralSize, 12); end.writeUInt32LE(offset, 16);
  return Buffer.concat([...local, ...central, end]);
}
function entries(bytes) {
  const check = (valid, message = 'Invalid release archive') => { if (!valid) throw new Error(message); };
  const end = bytes.lastIndexOf(Buffer.from([0x50, 0x4b, 0x05, 0x06]));
  check(end >= 0 && end + 22 <= bytes.length);
  check(end + 22 + bytes.readUInt16LE(end + 20) === bytes.length);
  const count = bytes.readUInt16LE(end + 10); const centralStart = bytes.readUInt32LE(end + 16);
  check(bytes.readUInt16LE(end + 4) === 0 && bytes.readUInt16LE(end + 6) === 0 && bytes.readUInt16LE(end + 8) === count);
  check(count !== 0xffff && centralStart + bytes.readUInt32LE(end + 12) === end, 'Invalid ZIP central directory');
  let at = centralStart; const files = []; const ranges = []; const names = new Set();
  for (let i = 0; i < count; i++) {
    check(at + 46 <= end && bytes.readUInt32LE(at) === 0x02014b50, 'Invalid ZIP central directory');
    const flags = bytes.readUInt16LE(at + 8); const method = bytes.readUInt16LE(at + 10);
    const length = bytes.readUInt16LE(at + 28); const offset = bytes.readUInt32LE(at + 42);
    const packedSize = bytes.readUInt32LE(at + 20); const size = bytes.readUInt32LE(at + 24); const crc = bytes.readUInt32LE(at + 16);
    const next = at + 46 + length + bytes.readUInt16LE(at + 30) + bytes.readUInt16LE(at + 32);
    check(next <= end && bytes.readUInt16LE(at + 34) === 0, 'Invalid ZIP central entry');
    check((flags & ~0x800) === 0 && (method === 0 || method === 8), 'Unsupported ZIP method or flags');
    check(offset + 30 <= centralStart && bytes.readUInt32LE(offset) === 0x04034b50, 'Invalid ZIP local header');
    check(bytes.readUInt16LE(offset + 6) === flags && bytes.readUInt16LE(offset + 8) === method, 'ZIP header mismatch');
    check(bytes.readUInt32LE(offset + 14) === crc, 'ZIP CRC header mismatch');
    check(bytes.readUInt32LE(offset + 18) === packedSize && bytes.readUInt32LE(offset + 22) === size, 'ZIP size header mismatch');
    const localLength = bytes.readUInt16LE(offset + 26);
    const start = offset + 30 + localLength + bytes.readUInt16LE(offset + 28);
    check(start + packedSize <= centralStart, 'Invalid ZIP content bounds');
    const filename = bytes.subarray(at + 46, at + 46 + length);
    check(localLength === length && bytes.subarray(offset + 30, offset + 30 + localLength).equals(filename), 'ZIP filename mismatch');
    const name = filename.toString();
    check(!names.has(name), 'Duplicate ZIP filename'); names.add(name);
    ranges.push([offset, start + packedSize]);
    const packed = bytes.subarray(start, start + packedSize);
    const decoded = method === 8 ? zlib.inflateRawSync(packed, { maxOutputLength: size + 1, info: true }) : null;
    if (decoded) check(decoded.engine.bytesWritten === packedSize, 'ZIP compressed size mismatch');
    const data = decoded ? decoded.buffer : packed;
    check(data.length === size, 'ZIP uncompressed size mismatch');
    check(crc32(data) === crc, 'ZIP CRC checksum mismatch');
    files.push({ name, data }); at = next;
  }
  check(at === end, 'Invalid ZIP central directory length');
  ranges.sort((a, b) => a[0] - b[0]);
  for (let i = 1; i < ranges.length; i++) check(ranges[i - 1][1] <= ranges[i][0], 'Overlapping ZIP entries');
  return files;
}
if (require.main === module) {
  const target = path.join(root, 'tdrive-sync.zip');
  if (process.argv.includes('--inspect')) {
    const files = entries(fs.readFileSync(target)); const manifest = files.find((f) => /(^|\/)manifest\.json$/.test(f.name));
    const settings = files.find((f) => /(^|\/)data\.json$/.test(f.name));
    const history = files.find((f) => /(^|\/)sync-state\.json$/.test(f.name));
    const account = settings ? JSON.parse(settings.data) : {};
    const state = history ? JSON.parse(history.data) : {};
    console.log(JSON.stringify({ entries: files.map((f) => f.name), version: manifest ? JSON.parse(manifest.data).version : null, containsUserState: !!settings || !!history, containsApiKey: !!account.apiKey, containsCopiedDeviceIdentity: !!(state.meta && state.meta.deviceId) }, null, 2));
  } else {
    fs.writeFileSync(target, archive(['main.js', 'manifest.json', 'styles.css'].map((name) => ({ name: 'tdrive-sync/' + name, data: fs.readFileSync(path.join(root, name)) }))));
    console.log('Packaged tdrive-sync.zip with code, manifest and styles only');
  }
}
module.exports = { archive, entries, crc32 };
