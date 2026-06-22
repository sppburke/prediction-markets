import { afterEach, describe, expect, it } from "vitest";

import { ALLOWED_EMAIL, isAllowedEmail, passwordMatches } from "./auth";

describe("isAllowedEmail (#398 WS3 single-email gate)", () => {
  it("admits exactly the allowlisted email", () => {
    expect(isAllowedEmail(ALLOWED_EMAIL)).toBe(true);
  });

  it("is case-insensitive", () => {
    expect(isAllowedEmail(ALLOWED_EMAIL.toUpperCase())).toBe(true);
    expect(isAllowedEmail("SppBurke@Gmail.com")).toBe(true);
  });

  it("rejects any other account", () => {
    expect(isAllowedEmail("attacker@gmail.com")).toBe(false);
    expect(isAllowedEmail("sppburke@example.com")).toBe(false);
    expect(isAllowedEmail("")).toBe(false);
  });

  it("rejects missing/null/undefined", () => {
    expect(isAllowedEmail(null)).toBe(false);
    expect(isAllowedEmail(undefined)).toBe(false);
  });
});

describe("passwordMatches (#398 WS3 bare-IP password login)", () => {
  afterEach(() => {
    delete process.env.SITE_PASSWORD;
  });

  it("admits the exact configured password", () => {
    process.env.SITE_PASSWORD = "s3cret-pw";
    expect(passwordMatches("s3cret-pw")).toBe(true);
  });

  it("rejects a wrong password (and is case-sensitive)", () => {
    process.env.SITE_PASSWORD = "s3cret-pw";
    expect(passwordMatches("wrong")).toBe(false);
    expect(passwordMatches("S3CRET-PW")).toBe(false);
  });

  it("rejects everything when SITE_PASSWORD is unset (no accidental open access)", () => {
    expect(passwordMatches("anything")).toBe(false);
    expect(passwordMatches("")).toBe(false);
  });

  it("rejects an empty configured password (treated as unset)", () => {
    process.env.SITE_PASSWORD = "";
    expect(passwordMatches("")).toBe(false);
  });

  it("rejects non-string input", () => {
    process.env.SITE_PASSWORD = "s3cret-pw";
    expect(passwordMatches(undefined)).toBe(false);
    expect(passwordMatches(null)).toBe(false);
    expect(passwordMatches(12345)).toBe(false);
  });
});
