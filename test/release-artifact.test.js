'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const { archive, entries, crc32 } = require('../scripts/package');
const root = path.join(__dirname, '..');
test('installable ZIP matches running code and excludes credentials, history and device identity', () => {
  const files = entries(fs.readFileSync(path.join(root, 'tdrive-sync.zip')));
  assert.deepEqual(files.map((f) => f.name).sort(), ['tdrive-sync/main.js', 'tdrive-sync/manifest.json', 'tdrive-sync/styles.css']);
  for (const file of files) assert.deepEqual(file.data, fs.readFileSync(path.join(root, path.basename(file.name))));
});
test('release packager preserves bytes and validates the standard ZIP checksum', () => {
  assert.equal(crc32(Buffer.from('123456789')), 0xcbf43926);
  const data = Buffer.from('Generated release fixture');
  assert.deepEqual(entries(archive([{ name: 'tdrive-sync/test.txt', data }])), [{ name: 'tdrive-sync/test.txt', data }]);
});
test('QA-TOOLS-001: release reader rejects a corrupted CRC in both headers', () => {
  const zip = archive([{ name: 'tdrive-sync/test.txt', data: Buffer.from('Generated checksum fixture') }]);
  const central = zip.readUInt32LE(zip.length - 6);
  zip.writeUInt32LE(0, 14); zip.writeUInt32LE(0, central + 16);
  assert.throws(() => entries(zip), /CRC|checksum/i);
});
test('release reader rejects truncated, mismatched and out-of-bounds ZIP headers', () => {
  const zip = archive([{ name: 'tdrive-sync/test.txt', data: Buffer.from('Generated header fixture') }]);
  const central = zip.readUInt32LE(zip.length - 6);
  assert.throws(() => entries(zip.subarray(0, zip.length - 1)), /Invalid/i);
  for (const mutate of [
    (copy) => copy.writeUInt32LE(0, 0),
    (copy) => copy.writeUInt32LE(0xffffffff, central + 42),
    (copy) => copy.writeUInt32LE(0xffffffff, central + 20),
    (copy) => copy.writeUInt16LE(0xffff, 26),
    (copy) => copy.writeUInt16LE(1, central + 10),
    (copy) => copy.writeUInt16LE(1, central + 8),
    (copy) => copy.writeUInt32LE(0, copy.length - 10),
    (copy) => { copy.writeUInt32LE(0, 22); copy.writeUInt32LE(0, central + 24); },
    (copy) => { copy[30] ^= 1; }
  ]) {
    const copy = Buffer.from(zip); mutate(copy);
    assert.throws(() => entries(copy));
  }
});
test('release reader supports empty archives and empty file contents', () => {
  assert.deepEqual(entries(archive([])), []);
  const files = [{ name: 'tdrive-sync/empty.txt', data: Buffer.alloc(0) }];
  assert.deepEqual(entries(archive(files)), files);
});
test('release reader rejects unconsumed bytes after the DEFLATE stream', () => {
  const original = archive([{ name: 'tdrive-sync/test.txt', data: Buffer.from('Generated length fixture') }]);
  const central = original.readUInt32LE(original.length - 6); const extra = Buffer.alloc(32, 0x42);
  const zip = Buffer.concat([original.subarray(0, central), extra, original.subarray(central)]);
  const packedSize = original.readUInt32LE(18) + extra.length;
  zip.writeUInt32LE(packedSize, 18); zip.writeUInt32LE(packedSize, central + extra.length + 20);
  zip.writeUInt32LE(central + extra.length, zip.length - 6);
  assert.throws(() => entries(zip), /compressed size/i);
});
