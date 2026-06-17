import { createCipheriv, createDecipheriv, randomBytes } from "crypto";

/**
 * App-layer encryption for direct identifiers (e.g. clients.full_name).
 *
 * GDPR Art. 9 special-category data: identifiers are encrypted in the
 * application before they ever reach the database, and decrypted only when
 * rendering for the authenticated therapist. The DB stores ciphertext only.
 *
 * Algorithm: AES-256-GCM. Key is a 32-byte secret provided as base64 via the
 * CLIENT_ENCRYPTION_KEY env var (server-only — never exposed to the browser).
 *
 * Stored format: base64(iv).base64(authTag).base64(ciphertext)
 */

const ALGO = "aes-256-gcm";
const IV_LEN = 12;

function getKey(): Buffer {
  const raw = process.env.CLIENT_ENCRYPTION_KEY;
  if (!raw) {
    throw new Error(
      "CLIENT_ENCRYPTION_KEY is not set. Generate one with: " +
        "node -e \"console.log(require('crypto').randomBytes(32).toString('base64'))\"",
    );
  }
  const key = Buffer.from(raw, "base64");
  if (key.length !== 32) {
    throw new Error("CLIENT_ENCRYPTION_KEY must decode to exactly 32 bytes (base64).");
  }
  return key;
}

/** Encrypt a plaintext identifier. Returns the stored ciphertext string. */
export function encryptField(plaintext: string): string {
  const iv = randomBytes(IV_LEN);
  const cipher = createCipheriv(ALGO, getKey(), iv);
  const enc = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
  const tag = cipher.getAuthTag();
  return [iv.toString("base64"), tag.toString("base64"), enc.toString("base64")].join(".");
}

/** Decrypt a stored ciphertext string back to plaintext. */
export function decryptField(stored: string): string {
  const [ivB64, tagB64, dataB64] = stored.split(".");
  if (!ivB64 || !tagB64 || !dataB64) {
    throw new Error("Malformed ciphertext.");
  }
  const decipher = createDecipheriv(ALGO, getKey(), Buffer.from(ivB64, "base64"));
  decipher.setAuthTag(Buffer.from(tagB64, "base64"));
  const dec = Buffer.concat([
    decipher.update(Buffer.from(dataB64, "base64")),
    decipher.final(),
  ]);
  return dec.toString("utf8");
}
