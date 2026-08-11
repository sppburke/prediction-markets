// Pure credential-rotation helpers (#508 Phase C, Decision 9).
// Ciphertext is display-safe only through its short SHA-256 fingerprint; plaintext never enters
// RPC arguments, metadata queries, API responses, or the account_events audit payload.
import { createHash } from "node:crypto";

export const CREDENTIAL_FINGERPRINT_HEX_LENGTH = 16;

export function fingerprintCiphertext(ciphertext: string): string {
  return createHash("sha256")
    .update(ciphertext, "utf8")
    .digest("hex")
    .slice(0, CREDENTIAL_FINGERPRINT_HEX_LENGTH);
}

export function nextBundleVersion(previous: number | null | undefined): number {
  return previous === null || previous === undefined ? 1 : previous + 1;
}

export function buildCredentialPlaintext({
  accountId,
  bundleVersion,
  keyId,
  credentials,
}: {
  accountId: string;
  bundleVersion: number;
  keyId: string;
  credentials: Record<string, unknown>;
}): string {
  return JSON.stringify({
    ...credentials,
    account_id: accountId,
    bundle_version: bundleVersion,
    key_id: keyId,
  });
}

export function buildCredentialRotationArgs({
  accountId,
  bundleVersion,
  keyId,
  sealedBundle,
  actor,
}: {
  accountId: string;
  bundleVersion: number;
  keyId: string;
  sealedBundle: string;
  actor: string;
}) {
  return {
    p_account_id: accountId,
    p_bundle_version: bundleVersion,
    p_key_id: keyId,
    p_sealed_bundle: sealedBundle,
    p_fingerprint: fingerprintCiphertext(sealedBundle),
    p_actor: actor,
  };
}
