'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { makeEnv } = require('./helpers');
const { emptyReport } = require('../src/SyncEngine');

test('expired unknown sessions are reported without cancellation or bypassing folder safety', async () => {
  let now = Date.UTC(2026, 10, 10); const e = await makeEnv(); e.engine.now = () => now;
  try {
    e.manifest.data.meta.syncScope = 'scope'; e.engine._capabilities = { uploadSessionListing: true };
    let deletes = 0; let listings = 0; let aborts = 0;
    e.client.deleteFolder = async () => { deletes++; throw Object.assign(new Error('Pending upload'), { status: 409, code: 'UPLOAD_IN_PROGRESS' }); };
    e.client.abortUpload = async () => { aborts++; };
    e.client.listUploads = async (opts) => {
      listings++; assert.equal(opts.path, '/Vault/Old/'); assert.equal(opts.active, true);
      return [
        { id: 'unknown-old', status: 'pending', expiresAt: '2026-11-07T00:00:00Z', token: 'must-not-persist' },
        { id: 'unknown-current', status: 'pending', expiresAt: '2026-11-11T00:00:00Z' },
      ];
    };
    const rep = emptyReport(); rep.status = 'ok';
    assert.equal(await e.engine._deleteRemoteFolder('Old', '/Vault/Old/', null, rep), false);
    const block = e.manifest.data.meta.blockedFolders.Old;
    assert.equal(block.activeUploadCount, 2); assert.equal(block.expiredUploadCount, 1);
    assert.deepEqual(block.blockingUploads.map((s) => s.id), ['unknown-old', 'unknown-current']);
    assert.ok(!JSON.stringify(block).includes('must-not-persist'));
    await e.engine._deleteRemoteFolder('Old', '/Vault/Old/', null, rep);
    assert.equal(deletes, 1); assert.equal(listings, 1);
    now += 6 * 60000;
    await e.engine._deleteRemoteFolder('Old', '/Vault/Old/', null, rep);
    assert.equal(deletes, 1); assert.equal(listings, 2); assert.equal(aborts, 0);
    e.engine._reportBlockedFolders(rep); assert.equal(rep.status, 'blocked'); assert.equal(rep.deferredFolders, 1);
  } finally { await e.stop(); }
});

test('session inspection failure preserves the deletion blocker and never enables deletion', async () => {
  const e = await makeEnv();
  try {
    e.manifest.data.meta.syncScope = 'scope'; e.engine._capabilities = { uploadSessionListing: true };
    e.client.deleteFolder = async () => { throw Object.assign(new Error('Pending upload'), { status: 409, code: 'UPLOAD_IN_PROGRESS' }); };
    e.client.listUploads = async () => { throw new Error('Inspection unavailable'); };
    assert.equal(await e.engine._deleteRemoteFolder('Old', '/Vault/Old/', null, emptyReport()), false);
    assert.equal(e.manifest.data.meta.blockedFolders.Old.code, 'UPLOAD_IN_PROGRESS');
    assert.match(e.manifest.data.meta.blockedFolders.Old.sessionInspectionError, /unavailable/);
  } finally { await e.stop(); }
});
