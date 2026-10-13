'use strict';
const { suffixName, sha256Hex } = require('./util');

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

    if (choice === 'remote') {
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
