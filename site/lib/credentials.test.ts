import { describe, expect, it } from "vitest";

import {
  buildCredentialPlaintext,
  buildCredentialRotationArgs,
  fingerprintCiphertext,
  nextBundleVersion,
} from "./credentials";

describe("sealed credential helpers (#508 Phase C)", () => {
  it("produces a stable short SHA-256 ciphertext fingerprint", () => {
    expect(fingerprintCiphertext("sealed-bundle")).toBe("b84118f4daffed36");
    expect(fingerprintCiphertext("sealed-bundle")).toHaveLength(16);
  });

  it("starts at version one and increments the prior version", () => {
    expect(nextBundleVersion(null)).toBe(1);
    expect(nextBundleVersion(undefined)).toBe(1);
    expect(nextBundleVersion(7)).toBe(8);
  });

  it("embeds authoritative bundle metadata in plaintext", () => {
    expect(
      JSON.parse(
        buildCredentialPlaintext({
          accountId: "primary",
          bundleVersion: 4,
          keyId: "service-key-2",
          credentials: { api_key: "plain", account_id: "forged" },
        }),
      ),
    ).toEqual({
      api_key: "plain",
      account_id: "primary",
      bundle_version: 4,
      key_id: "service-key-2",
    });
  });

  it("places ciphertext in RPC args only as p_sealed_bundle", () => {
    const sealed = "-----BEGIN AGE ENCRYPTED FILE-----\nciphertext";
    const args = buildCredentialRotationArgs({
      accountId: "primary",
      bundleVersion: 2,
      keyId: "service-key-1",
      sealedBundle: sealed,
      actor: "admin@example.com",
    });
    expect(
      Object.entries(args)
        .filter(([, value]) => value === sealed)
        .map(([key]) => key),
    ).toEqual(["p_sealed_bundle"]);
    expect(args.p_fingerprint).toBe(fingerprintCiphertext(sealed));
    expect(JSON.stringify({ ...args, p_sealed_bundle: undefined })).not.toContain(sealed);
  });
});
