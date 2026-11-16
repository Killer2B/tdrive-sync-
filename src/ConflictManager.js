'use strict';
const { suffixName, sha256Hex } = require('./util');
const { merge3 } = require('./MergeManager');
const { remoteSig } = require('./normalize');

/**
 * A conflict = changed locally AND remotely since the last successful sync (or both exist with
 * different content and no ledger entry). Nothing is overwritten: the remote content is saved as a
 * local conflict copy, the local file is untouched, and the path is frozen until the user resolves it.
 */
class ConflictManager {
  constructor({ manifest, local, now = () => Date.now() }) {
    this.manifest = manifest; this.local = local; this.now = now;
  }
  has(rel) { return !!this.manifest.getConflict(rel); }
  list() { return this.manifest.conflictKeys().map((p) => ({ path: p, ...this.manifest.getConflict(p) })); }

  async _uniqueName(rel, label) {
    let name = suffixName(rel, label, this.now());
    let n = 2;
    while (await this.local.exists(name)) { name = suffixName(rel, label + ' ' + n, this.now()); n++; }
    return name;
  }

  async _writeCopy(rel, label, data, allocated) {
    const hash = await sha256Hex(data);
    for (let attempt = 0; attempt < 100; attempt++) {
      const name = await this._uniqueName(rel, label);
      if (allocated) await allocated(name);
      try {
        await this.local.writeBinary(name, data, undefined, { createOnly: true });
        if (await sha256Hex(await this.local.readBinary(name)) !== hash) throw new Error('Conflict copy verification failed; original content was not replaced.');
        return name;
      } catch (e) {
        if (e.code !== 'LOCAL_FILE_EXISTS') throw e;
      }
    }
    throw new Error('Could not allocate a new conflict copy without a name collision');
  }

  async record(rel, remote, remoteData) {
    const copyPath = await this._writeCopy(rel, 'Tdrive conflict', remoteData);
    this.manifest.setConflict(rel, { copyPath, remoteId: remote.id, contentVersion: remote.contentVersion == null ? null : remote.contentVersion, remoteSig: remote.sig || remoteSig(remote), remoteHash: await sha256Hex(remoteData), at: this.now() });
    this.manifest.addLog('warn', `Conflict: ${rel} (remote version saved as ${copyPath})`);
    return copyPath;
  }

  /** choice: 'merge' | 'local' | 'remote' | 'both' | 'cancel' */
  async resolve(rel, choice, engine, options = {}) {
    if (choice === 'cancel') return { resolved: false };
    if (!['merge', 'local', 'remote', 'both'].includes(choice)) throw new Error('Unknown choice ' + choice);
    if (engine.running) throw new Error('Sync is already running');
    const original = this.manifest.getConflict(rel);
    if (!original) throw new Error('No open conflict for ' + rel);
    // Retry intent is internal progress, not a change to the versions the user reviewed.
    const reviewKey = (conflict) => { const { resolution, ...review } = conflict; return JSON.stringify(review); };
    const reviewed = reviewKey(options.reviewed || original);
    if (reviewed !== reviewKey(original)) throw new Error('Conflict changed; reopen it before resolving.');
    const checkActive = () => {
      if (engine.stopRequested || (engine.getSettings && engine.getSettings().paused)) throw new Error('Sync is paused');
      if (options.checkActive) options.checkActive();
    };
    checkActive();
    engine.running = true;
    try {
      await engine.prepare({ acquireLease: false });
      checkActive();
      const c = this.manifest.getConflict(rel);
      if (c !== original) return { resolved: false };
      if (reviewKey(c) !== reviewed) throw new Error('Conflict changed; reopen it before resolving.');
      const localExists = await this.local.exists(rel);
      if (!localExists && choice !== 'remote') throw new Error('Local version no longer exists; reopen the conflict before resolving.');
      const stat = localExists ? await this.local.stat(rel) : null;
      if (localExists && !stat) throw new Error('Local file changed while taking the conflict snapshot; conflict remains open.');
      const localStat = stat ? { ...stat, path: rel, exists: true } : null;
      const existingLedger = { ...(this.manifest.getFile(rel) || {}) };
      const localData = localExists ? await this.local.readBinary(rel) : null;
      const localHash = localData !== null ? await sha256Hex(localData) : null;
      const meta = this.manifest.data.meta || {};
      const previousProgress = c.resolution;
      let progress = c.resolution;
      if (!progress || progress.choice !== choice || progress.reviewed !== reviewed || progress.scope !== meta.syncScope || progress.owner !== meta.deviceId) progress = null;
      const assertReview = () => {
        checkActive();
        if (this.manifest.getConflict(rel) !== c || reviewKey(c) !== reviewed) throw new Error('Conflict changed; reopen it before resolving.');
      };
      const saveProgress = async (next) => {
        assertReview();
        const previous = c.resolution;
        c.resolution = next;
        try { await this.manifest.save(); }
        catch (e) { if (c.resolution === next) { if (previous) c.resolution = previous; else delete c.resolution; } throw e; }
        progress = next;
        assertReview();
      };
      const assertLocal = async (hash) => {
        assertReview();
        if (!(await this.local.exists(rel)) || await sha256Hex(await this.local.readBinary(rel)) !== hash) throw new Error('Local file changed during conflict resolution; conflict remains open.');
      };
      const assertCopy = async (path, hash) => {
        assertReview();
        if (!path || !(await this.local.exists(path)) || await sha256Hex(await this.local.readBinary(path)) !== hash) throw new Error('Preserved copy changed; conflict remains open. The edited copy was not overwritten.');
      };
      const requireBaseline = (path, hash, id, result, isPush = false) => {
        const entry = this.manifest.getFile(path);
        if (!result || result.conflict || result.resolved === false || isPush && !result.id || !entry || !entry.remoteId || entry.localHash !== hash || id && entry.remoteId !== id || result.id && entry.remoteId !== result.id) {
          throw new Error('Conflict publication did not establish a committed baseline; conflict remains open.');
        }
        return entry;
      };
      let remote = null;
      try { remote = await engine.client.getFile(c.remoteId); } catch (e) { if (e.status !== 404) throw e; }
      checkActive();

      let remoteData = null;
      const refreshConflict = async (latest, data) => {
        checkActive();
        assertReview();
        await this.record(rel, latest, data);
        const refreshed = this.manifest.getConflict(rel);
        try { await this.manifest.save(); }
        catch (e) { if (this.manifest.getConflict(rel) === refreshed) this.manifest.setConflict(rel, c); throw e; }
        throw new Error('Remote file changed since this conflict was reviewed; its latest version was preserved in a new conflict copy. Reopen the conflict before resolving.');
      };
      let downloadedHash = null;
      let reviewedRemoteHash = c.remoteHash;
      if (remote) {
        remoteData = await engine.downloadFn(engine.client, remote, engine._downOpts());
        checkActive();
        let expectedHash = c.remoteHash;
        if (!expectedHash && await this.local.exists(c.copyPath)) expectedHash = await sha256Hex(await this.local.readBinary(c.copyPath));
        reviewedRemoteHash = expectedHash;
        const generationChanged = c.contentVersion != null
          ? remote.contentVersion == null || String(c.contentVersion) !== String(remote.contentVersion)
          : c.remoteSig && c.remoteSig !== (remote.sig || remoteSig(remote));
        downloadedHash = await sha256Hex(remoteData);
        // A durably recorded publication may have committed despite a lost response/save. Only the
        // exact chosen bytes on the same remote identity can complete that attempt without a new write.
        const recovered = progress && progress.publishing && progress.targetHash === downloadedHash && progress.targetHash === localHash;
        if ((!expectedHash || downloadedHash !== expectedHash || generationChanged) && !recovered) await refreshConflict(remote, remoteData);
      }
      const checkReviewed = async () => {
        checkActive();
        assertReview();
        let latest = null;
        try { latest = await engine.client.getFile(c.remoteId); } catch (e) { if (e.status !== 404) throw e; }
        checkActive();
        const generationChanged = latest && remote && (remote.contentVersion != null
          ? latest.contentVersion == null || String(latest.contentVersion) !== String(remote.contentVersion)
          : (latest.sig || remoteSig(latest)) !== (remote.sig || remoteSig(remote)));
        if ((latest && !remote) || (!latest && remote) || generationChanged) {
          if (latest) await refreshConflict(latest, await engine.downloadFn(engine.client, latest, engine._downOpts()));
          throw new Error('Remote file no longer exists; reopen the conflict before resolving.');
        }
        // Same-generation metadata changes do not alter the bytes the user reviewed.
        remote = latest;
      };
      let chosenData = localData;
      let chosenHash = localHash;
      if (choice === 'merge' && localHash !== downloadedHash && !(progress && progress.targetHash === localHash)) {
        // Auto-merge needs the last synced base text (stored for small text files) plus both new versions.
        const E = this.manifest.getFile(rel);
        if (!remote) throw new Error('Remote version no longer exists; keep local or both instead.');
        if (!E || typeof E.baseText !== 'string') throw new Error('Auto-merge unavailable: no base version (the file was first synced before 1.1.0 or is binary/large).');
        const dec = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
        let m;
        try {
          if (localData.byteLength > 65536 || remoteData.byteLength > 65536 || new TextEncoder().encode(E.baseText).byteLength > 65536) throw new Error('Text merge exceeds the safe size limit; resolve manually');
          const localText = dec.decode(localData); const remoteText = dec.decode(remoteData);
          if ([E.baseText, localText, remoteText].some((s) => /[\u0000-\u0008\u000b\u000c\u000e-\u001f]/.test(s))) throw new Error('Binary content is not safe to merge as text');
          if (await sha256Hex(new TextEncoder().encode(localText)) !== localHash || await sha256Hex(new TextEncoder().encode(remoteText)) !== downloadedHash) throw new Error('Content is not lossless UTF-8 text');
          m = merge3(E.baseText, localText, remoteText);
        }
        catch (e) { throw new Error('Auto-merge failed: ' + e.message); }
        if (!m.ok) {
          await assertLocal(localHash);
          await checkReviewed();
          const draft = await this._writeCopy(rel, 'Tdrive merged', new TextEncoder().encode(m.text).buffer);
          this.manifest.addLog('warn', `Conflict on ${rel} has ${m.conflicts} overlapping region(s); a merged draft with markers was written to ${draft}. The conflict stays open.`);
          await this.manifest.save();
          return { resolved: false, merged: false, conflicts: m.conflicts, draft, text: m.text, localHash, remoteHash: downloadedHash };
        }
        chosenData = new TextEncoder().encode(m.text).buffer;
        chosenHash = await sha256Hex(chosenData);
      }
      const targetHash = choice === 'remote' ? downloadedHash : chosenHash;
      if (!progress || progress.targetHash !== targetHash) {
        progress = { choice, reviewed, scope: meta.syncScope, owner: meta.deviceId, localHash, targetHash };
      }

      if (choice === 'remote') {
        if (!remote) throw new Error('Remote version no longer exists; keep local or both instead.');
        // Preserve the local version first, then write the remote one.
        if (localExists && localHash !== downloadedHash) {
          if (!progress.backupPath || !(await this.local.exists(progress.backupPath)) || await sha256Hex(await this.local.readBinary(progress.backupPath)) !== localHash) {
            progress = { ...progress, backupPath: await this._writeCopy(rel, 'Tdrive local backup', localData) };
          }
        }
        await checkReviewed();
        await saveProgress(progress);
        if (localExists && localHash !== downloadedHash) await assertCopy(progress.backupPath, localHash);
        if (!localExists && await this.local.exists(rel)) throw new Error('Local file appeared during conflict resolution; conflict remains open.');
        // Commit the already reviewed bytes; a second download could silently apply a newer revision.
        const result = await engine._writeDownloaded(rel, remote, remoteData, localStat, localStat ? { ...existingLedger, localHash, localSize: localStat.size, localMtime: localStat.mtime } : null);
        if (result && result.conflict) throw new Error('Local file changed during conflict resolution; conflict remains open.');
        requireBaseline(rel, downloadedHash, remote.id, result);
        await assertLocal(downloadedHash);
      } else {
        const lock = await engine.acquireOperationLease();
        if (lock && lock.blocked) throw new Error('Another device is syncing; conflict remains open.');
        await checkReviewed();
        await assertLocal(localHash);
        if (choice === 'both' && (localHash !== downloadedHash || progress.copyPath)) {
          let preservedData = remoteData;
          if (!remote && c.copyPath && await this.local.exists(c.copyPath)) {
            preservedData = await this.local.readBinary(c.copyPath);
            const hash = await sha256Hex(preservedData);
            if (!reviewedRemoteHash || hash !== reviewedRemoteHash) throw new Error('Preserved remote copy changed; conflict remains open.');
          } else if (downloadedHash !== reviewedRemoteHash) {
             // The source already contains the chosen local version. Recover the original remote
             // bytes from the preserved copy, never mistake the new source for the old remote.
            preservedData = null;
            for (const path of new Set([progress.copyPath, c.copyPath])) {
              if (!path || !(await this.local.exists(path))) continue;
              const snapshot = new Uint8Array(await this.local.readBinary(path)).slice().buffer;
              if (await sha256Hex(snapshot) === reviewedRemoteHash) { preservedData = snapshot; break; }
            }
            if (!preservedData) throw new Error('No exact reviewed remote snapshot remains; conflict remains open. Edited copies were not overwritten.');
          }
          if (!preservedData) throw new Error('Remote version no longer exists; cannot preserve both versions.');
          const preservedHash = await sha256Hex(preservedData);
          if (!progress.copyPath || !(await this.local.exists(progress.copyPath)) || await sha256Hex(await this.local.readBinary(progress.copyPath)) !== preservedHash) {
            // Persist the allocated name before creation too: a failed save must not leave an
            // untracked ordinary-sync copy that a later run could upload as a duplicate.
            const copyPath = await this._writeCopy(rel, 'Tdrive kept remote', preservedData, (path) => saveProgress({ ...progress, copyPath: path }));
            progress = { ...progress, copyPath };
          }
          await saveProgress(progress);
          const copy = await engine.pushLocal(progress.copyPath, null, undefined, progress.copyPath, { expectedLocalHash: preservedHash, createOnly: true });
          requireBaseline(progress.copyPath, preservedHash, null, copy, true);
          await saveProgress({ ...progress, copyRemoteId: copy.id, preservedHash });
          await checkReviewed();
          await assertLocal(localHash);
          await assertCopy(progress.copyPath, preservedHash);
        }
        if (choice === 'merge' && chosenHash !== localHash) {
          if (!progress.backupPath || !(await this.local.exists(progress.backupPath)) || await sha256Hex(await this.local.readBinary(progress.backupPath)) !== localHash) {
            progress = { ...progress, backupPath: await this._writeCopy(rel, 'Tdrive local backup', localData) };
          }
          await saveProgress(progress);
          await checkReviewed();
          await assertLocal(localHash);
          await assertCopy(progress.backupPath, localHash);
          if (typeof this.local.writeBinaryAtomic === 'function') await this.local.writeBinaryAtomic(rel, chosenData, undefined, { expectedHash: localHash });
          else await this.local.writeBinary(rel, chosenData, undefined, { expectedHash: localHash });
        }
        await assertLocal(chosenHash);
        if (choice === 'both' && progress.copyPath) await assertCopy(progress.copyPath, progress.preservedHash);
        await checkReviewed();
        if (remote && chosenHash === downloadedHash) {
          const snapshot = await this.local.stat(rel);
          const result = await engine._writeDownloaded(rel, remote, chosenData, { ...snapshot, path: rel }, { ...existingLedger, localHash: chosenHash, localSize: snapshot.size, localMtime: snapshot.mtime });
          requireBaseline(rel, chosenHash, remote.id, result);
        } else {
          await saveProgress({ ...progress, publishing: true });
          let result;
          try { result = await engine.pushLocal(rel, remote, undefined, rel, { expectedContentVersion: remote && remote.contentVersion, expectedLocalHash: chosenHash }); }
          catch (e) {
            // An explicit precondition rejection committed nothing. A plain lost response is
            // different: retain its durable intent so a retry can recognize the exact result.
            if (choice === 'local' && e.code === 'remote_changed' && this.manifest.getConflict(rel) === c) {
              if (previousProgress) c.resolution = previousProgress; else delete c.resolution;
              await this.manifest.save();
            } else if (this.manifest.getConflict(rel) !== c) {
              // The engine may have preserved a newly rejected generation as a fresh review.
              await this.manifest.save();
            }
            throw e;
          }
          requireBaseline(rel, chosenHash, remote && remote.id, result, true);
        }
        await assertLocal(chosenHash);
        if (choice === 'both' && progress.copyPath) await assertCopy(progress.copyPath, progress.preservedHash);
      }
      // The transfer's proven baseline is sufficient. Best-effort listing refresh is not commit
      // evidence, and must not manufacture success or replace the receipt with a later head.
      assertReview();
      this.manifest.deleteConflict(rel);
      try { await this.manifest.save(); }
      catch (e) { if (!this.manifest.getConflict(rel)) this.manifest.setConflict(rel, c); throw e; }
      // Keep recovery/review copies: a user may have edited them, and trash has no queued hash guard.
      this.manifest.addLog('info', `Conflict resolved (${choice === 'merge' ? 'auto-merged' : 'kept ' + choice}): ${rel}`);
      return { resolved: true, ...(progress.copyPath ? { copyPath: progress.copyPath } : {}) };
    } finally {
      try { await engine.releaseOperationLease(); }
      finally { engine.running = false; }
    }
  }
}

module.exports = { ConflictManager, sha256Hex };
