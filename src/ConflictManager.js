'use strict';
const { suffixName, sha256Hex } = require('./util');
const { merge3 } = require('./MergeManager');

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

  async record(rel, remote, remoteData) {
    const copyPath = await this._uniqueName(rel, 'Tdrive conflict');
    await this.local.writeBinary(rel === copyPath ? copyPath + '.copy' : copyPath, remoteData);
    this.manifest.setConflict(rel, { copyPath, remoteId: remote.id, remoteSig: remote.sig, at: this.now() });
    this.manifest.addLog('warn', `Conflict: ${rel} (remote version saved as ${copyPath})`);
    return copyPath;
  }

  /** choice: 'local' | 'remote' | 'both' | 'cancel' */
  async resolve(rel, choice, engine) {
    const c = this.manifest.getConflict(rel);
    if (!c) throw new Error('No open conflict for ' + rel);
    if (choice === 'cancel') return { resolved: false };
    await engine.prepare();
    let remote = null;
    try { remote = await engine.client.getFile(c.remoteId); } catch (e) { if (e.status !== 404) throw e; }

    if (choice === 'merge') {
      // Auto-merge needs the last synced base text (stored for small text files) plus both new versions.
      const E = this.manifest.getFile(rel);
      if (!remote) throw new Error('Remote version no longer exists; keep local or both instead.');
      if (!E || typeof E.baseText !== 'string') throw new Error('Auto-merge unavailable: no base version (the file was first synced before 1.1.0 or is binary/large).');
      const localData = await this.local.readBinary(rel);
      const remoteData = await engine.downloadFn(engine.client, remote, engine._downOpts());
      const dec = new TextDecoder();
      let m;
      try { m = merge3(E.baseText, dec.decode(localData), dec.decode(remoteData)); }
      catch (e) { throw new Error('Auto-merge failed: ' + e.message); }
      if (m.ok) {
        await this.local.writeBinary(rel, new TextEncoder().encode(m.text).buffer);
        await engine.pushLocal(rel, remote);
        if (await this.local.exists(c.copyPath)) await this.local.trash(c.copyPath);
        this.manifest.addLog('info', `Conflict auto-merged (no overlapping edits): ${rel}`);
      } else {
        const draft = await this._uniqueName(rel, 'Tdrive merged');
        await this.local.writeBinary(draft, new TextEncoder().encode(m.text).buffer);
        this.manifest.addLog('warn', `Conflict on ${rel} has ${m.conflicts} overlapping region(s); a merged draft with markers was written to ${draft}. The conflict stays open.`);
        await this.manifest.save();
        return { resolved: false, merged: false, conflicts: m.conflicts, draft };
      }
    } else if (choice === 'remote') {
      if (!remote) throw new Error('Remote version no longer exists; keep local or both instead.');
      // Preserve the local version first, then write the remote one.
      const backup = await this._uniqueName(rel, 'Tdrive local backup');
      await this.local.writeBinary(backup, await this.local.readBinary(rel));
      await engine.pullRemote(rel, remote);
      this.manifest.addLog('info', `Conflict resolved (kept remote): ${rel}; local version kept as ${backup}`);
    } else if (choice === 'local' || choice === 'both') {
      await engine.pushLocal(rel, remote);
      if (choice === 'local' && await this.local.exists(c.copyPath)) await this.local.trash(c.copyPath);
      this.manifest.addLog('info', `Conflict resolved (${choice === 'local' ? 'kept local' : 'kept both'}): ${rel}`);
    } else throw new Error('Unknown choice ' + choice);
    const E = this.manifest.getFile(rel);
    if (E) await engine.refreshSigs(new Map([[rel, E.remoteId]]));
    this.manifest.deleteConflict(rel);
    await this.manifest.save();
    return { resolved: true };
  }
}

module.exports = { ConflictManager, sha256Hex };
