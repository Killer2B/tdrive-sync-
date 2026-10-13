'use strict';
// Optional client-side encryption (end-to-end): file CONTENT is encrypted with AES-256-GCM before it
// leaves the device, using a key derived from a passphrase with PBKDF2. The server (and anyone with the
// API key) only ever sees ciphertext. File/folder NAMES stay visible (the folder tree is not encrypted).
//
// Encrypted payload format (self-describing, so a file uploaded before E2EE was enabled still downloads
// unchanged):  [ 8-byte magic "TDRVSYNC" ][ 1-byte version ][ 12-byte IV ][ AES-GCM ciphertext + tag ]
//
// The PBKDF2 salt is a CONSTANT (not random per device) so every device that shares the same passphrase
// derives the same key and can decrypt each other's files. Use a long, unique passphrase (16+ characters).

const MAGIC = new Uint8Array([0x54, 0x44, 0x52, 0x56, 0x53, 0x59, 0x4e, 0x43]); // "TDRVSYNC"
const VERSION = 1;
const IV_LEN = 12;
const HEADER_LEN = MAGIC.length + 1 + IV_LEN;   // 21
const PBKDF2_ITERATIONS = 250000;
const DEFAULT_SALT = 'tdrive-sync-e2ee-v1';

/** True if the buffer starts with our magic header (i.e. it is one of our ciphertexts, not a plain file). */
function isEncrypted(buf) {
  if (!buf || buf.byteLength < HEADER_LEN) return false;
  const u = new Uint8Array(buf, 0, MAGIC.length);
  for (let i = 0; i < MAGIC.length; i++) if (u[i] !== MAGIC[i]) return false;
  return true;
}

class CryptoManager {
  constructor(passphrase, salt) { this.passphrase = passphrase; this.salt = salt || DEFAULT_SALT; this._key = null; }

  async key() {
    if (this._key) return this._key;
    if (!this.passphrase) throw new Error('Encryption is enabled but the passphrase is empty');
    const enc = new TextEncoder();
    const base = await crypto.subtle.importKey('raw', enc.encode(this.passphrase), 'PBKDF2', false, ['deriveKey']);
    this._key = await crypto.subtle.deriveKey(
      { name: 'PBKDF2', salt: enc.encode(this.salt), iterations: PBKDF2_ITERATIONS, hash: 'SHA-256' },
      base, { name: 'AES-GCM', length: 256 }, false, ['encrypt', 'decrypt'],
    );
    return this._key;
  }

  /** Encrypts plaintext bytes into a self-describing ciphertext (fresh random IV each call). */
  async encrypt(buf) {
    const key = await this.key();
    const iv = new Uint8Array(IV_LEN);
    crypto.getRandomValues(iv);
    const ct = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, buf));
    const out = new Uint8Array(HEADER_LEN + ct.byteLength);
    out.set(MAGIC, 0);
    out[MAGIC.length] = VERSION;
    out.set(iv, MAGIC.length + 1);
    out.set(ct, HEADER_LEN);
    return out.buffer;
  }

  /** Decrypts our own ciphertext. A buffer without our magic header (e.g. uploaded before E2EE) is
   *  returned unchanged, so enabling E2EE never breaks files that were already on the server. */
  async decrypt(buf) {
    if (!isEncrypted(buf)) return buf;
    const key = await this.key();
    const ver = new Uint8Array(buf, MAGIC.length, 1)[0];
    if (ver !== VERSION) throw new Error('Unsupported encrypted file version ' + ver + ' — update the plugin');
    const iv = new Uint8Array(buf, MAGIC.length + 1, IV_LEN);
    const ct = new Uint8Array(buf, HEADER_LEN);
    try {
      return await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, key, ct);
    } catch (e) {
      throw new Error('Decryption failed: wrong passphrase or the file is corrupted');
    }
  }
}

module.exports = { CryptoManager, isEncrypted, MAGIC, VERSION, HEADER_LEN, DEFAULT_SALT };
