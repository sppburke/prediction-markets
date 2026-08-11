import { describe, expect, it } from "vitest";

import {
  ALLOWED_EMAIL,
  buildAuthCallbacks,
  canSignInWithEmail,
  isAllowedEmail,
  normalizeLoginEmail,
  type LoginEmailLookup,
} from "./auth";

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

describe("viewer login admission (#508 Phase C)", () => {
  it("normalizes with lower(btrim()) semantics", () => {
    expect(normalizeLoginEmail("  Viewer@Example.COM  ")).toBe("viewer@example.com");
    expect(normalizeLoginEmail("   ")).toBeNull();
    expect(normalizeLoginEmail(null)).toBeNull();
  });

  it("short-circuits the case-insensitive admin without a lookup", async () => {
    let lookups = 0;
    const lookup: LoginEmailLookup = async () => {
      lookups += 1;
      return { rows: [], error: null };
    };
    expect(await canSignInWithEmail(ALLOWED_EMAIL.toUpperCase(), lookup)).toBe(true);
    expect(lookups).toBe(0);
  });

  it("admits exactly one normalized viewer match", async () => {
    const lookup: LoginEmailLookup = async (email) => ({
      rows: [{ account_id: "viewer", login_email: email }],
      error: null,
    });
    expect(await canSignInWithEmail(" Viewer@Example.COM ", lookup)).toBe(true);
  });

  it("denies cleared/null email and lookup errors", async () => {
    const cleared: LoginEmailLookup = async () => ({
      rows: [{ account_id: "viewer", login_email: null }],
      error: null,
    });
    const failed: LoginEmailLookup = async () => ({ rows: [], error: new Error("DB down") });
    expect(await canSignInWithEmail(null, cleared)).toBe(false);
    expect(await canSignInWithEmail("viewer@example.com", cleared)).toBe(false);
    expect(await canSignInWithEmail("viewer@example.com", failed)).toBe(false);
  });
});

describe("Auth.js session contract (#508 Phase C)", () => {
  it("keeps Edge authorization DB-free and admits any session", () => {
    let lookups = 0;
    const callbacks = buildAuthCallbacks(async () => {
      lookups += 1;
      return { rows: [], error: null };
    });
    expect(callbacks.authorized({ auth: { user: { email: "anyone@example.com" } } })).toBe(
      true,
    );
    expect(callbacks.authorized({ auth: null })).toBe(false);
    expect(lookups).toBe(0);
  });

  it("adds no JWT, role, or account claims and returns email-only user data", () => {
    const callbacks = buildAuthCallbacks(async () => ({ rows: [], error: null }));
    expect("jwt" in callbacks).toBe(false);
    const session = callbacks.session({
      session: {
        expires: "2099-01-01T00:00:00.000Z",
        user: {
          email: "viewer@example.com",
          name: "must be stripped",
          image: "must be stripped",
          role: "admin",
          account_id: "forged",
        },
      },
    });
    expect(session).toEqual({
      expires: "2099-01-01T00:00:00.000Z",
      user: { email: "viewer@example.com" },
    });
    expect(session.user).not.toHaveProperty("role");
    expect(session.user).not.toHaveProperty("account_id");
  });
});
