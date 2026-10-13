'use strict';
const { createMockServer } = require('./mockServer');
const { TdriveClient } = require('../src/TdriveClient');
const { ManifestStore } = require('../src/ManifestStore');
const { SyncEngine } = require('../src/SyncEngine');

async function nodeTransport({ url, method, headers, body }) {
  const r = await fetch(url, { method, headers, body });
  return { status: r.status, headers: Object.fromEntries([...r.headers].map(([k, v]) => [k.toLowerCase(), v])), body: await r.arrayBuffer() };
}

class MemFS {
  constructor() { this.files = new Map(); this.dirs = new Set(); this.trashed = []; this.trashedDirs = []; this.clock = 5000; }
  _parents(p) { const a = p.split('/'); a.pop(); const out = []; for (let i = 1; i <= a.length; i++) out.push(a.slice(0, i).join('/')); return out; }
  put(path, data) { this.files.set(path, { data: Buffer.from(data), mtime: ++this.clock }); this._parents(path).forEach((d) => this.dirs.add(d)); }
  text(path) { const f = this.files.get(path); return f ? f.data.toString() : undefined; }
  /** The user deletes a whole folder in the file explorer (files + sub-folders go away). */
  removeDir(d) { for (const k of [...this.files.keys()]) if (k.startsWith(d + '/')) this.files.delete(k); for (const x of [...this.dirs]) if (x === d || x.startsWith(d + '/')) this.dirs.delete(x); }
  async list() { return [...this.files].map(([path, f]) => ({ path, size: f.data.length, mtime: f.mtime })); }
  async listFolders() { return [...this.dirs]; }
  async readBinary(p) { const f = this.files.get(p); if (!f) throw new Error('ENOENT ' + p); return f.data.buffer.slice(f.data.byteOffset, f.data.byteOffset + f.data.length); }
  async writeBinary(p, buf) { this.put(p, buf); }
  async appendBinary(p, buf) {
    const prev = this.files.get(p);
    const add = Buffer.from(buf);
    const merged = prev ? Buffer.concat([prev.data, add]) : add;
    this.files.set(p, { data: merged, mtime: ++this.clock });
  }
  get canStream() { return true; }
  async beginStreamWrite(p) {
    let n = 0;
    return {
      write: async (chunk) => {
        const b = chunk instanceof ArrayBuffer ? Buffer.from(chunk) : Buffer.from(chunk.buffer ? chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.byteLength) : chunk);
        if (n++ === 0) this.put(p, b); else await this.appendBinary(p, b);
      },
      close: async () => p,
    };
  }
  async stat(p) { const f = this.files.get(p); return f ? { size: f.data.length, mtime: f.mtime } : null; }
  async exists(p) { return this.files.has(p) || this.dirs.has(p); }
  async trash(p) { this.trashed.push({ path: p, data: this.files.get(p).data }); this.files.delete(p); }   // empty parent folders stay
  async mkdir(p) { this.dirs.add(p); this._parents(p + '/_').forEach((d) => this.dirs.add(d)); }
  async trashFolder(p) { this.trashedDirs.push(p); this.dirs.delete(p); }
}

async function makeEnv(o = {}) {
  const server = await createMockServer(o.server).start();
  const sleeps = [];
  const settings = {
    baseUrl: server.url, remoteFolder: '/Vault/', mode: 'two-way', deletionPolicy: 'trash', maxDeletesPerRun: 10,
    excludePatterns: [], downloadChunkBytes: 64 * 1024, remoteLock: false, ...(o.settings || {}),
  };
  const mkClient = (key, extra) => new TdriveClient({ baseUrl: server.url, apiKey: key || server.key, transport: nodeTransport, allowInsecure: true, sleep: async (ms) => { sleeps.push(ms); }, maxRetries: 4, ...(o.client || {}), ...(extra || {}) });
  const mkDevice = (device = {}) => {
    const local = device.local || new MemFS();
    let blob = null;
    const manifest = device.manifest || new ManifestStore({ read: async () => blob, write: async (s) => { blob = s; } });
    const client = device.client || mkClient();
    const engine = new SyncEngine({ client, local, manifest, getSettings: () => settings, ...(device.engine || {}) });
    return { local, manifest, client, engine };
  };
  return { server, settings, sleeps, mkClient, mkDevice, ...mkDevice(), stop: () => server.stop() };
}

module.exports = { makeEnv, MemFS, nodeTransport };
