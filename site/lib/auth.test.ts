import { describe, expect, it } from "vitest";

import { ALLOWED_EMAIL, isAllowedEmail } from "./auth";

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
