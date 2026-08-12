import { beforeEach, describe, expect, it, vi } from "vitest";

// React cache() gets its cache from the active Server Component request dispatcher. Vitest has no
// RSC dispatcher, so this mock models one request cache; constructing a new resolver models the
// next request and proves the memo does not become a process-global authorization cache.
vi.mock("react", async () => {
  const actual = await vi.importActual<typeof import("react")>("react");
  return {
    ...actual,
    cache: <Args extends unknown[], Result>(fn: (...args: Args) => Result) => {
      const memo = new Map<string, Result>();
      return (...args: Args): Result => {
        const key = JSON.stringify(args);
        if (!memo.has(key)) memo.set(key, fn(...args));
        return memo.get(key) as Result;
      };
    },
  };
});
vi.mock("server-only", () => ({}));
vi.mock("./supabase-server", () => ({ getServiceRoleSupabase: vi.fn() }));

import { ALLOWED_EMAIL, type LoginEmailLookup } from "./auth";
import { createCachedAccessResolver, resolveAccessUncached } from "./authz";

describe("resolveAccess (#508 Phase C)", () => {
  let lookup: ReturnType<typeof vi.fn<LoginEmailLookup>>;

  beforeEach(() => {
    lookup = vi.fn<LoginEmailLookup>();
  });

  it("fails closed on lookup error, no rows, and ambiguous rows", async () => {
    lookup.mockResolvedValueOnce({ rows: [], error: new Error("DB down") });
    expect(await resolveAccessUncached("viewer@example.com", lookup)).toBeNull();

    lookup.mockResolvedValueOnce({ rows: [], error: null });
    expect(await resolveAccessUncached("viewer@example.com", lookup)).toBeNull();

    lookup.mockResolvedValueOnce({
      rows: [
        { account_id: "one", login_email: "viewer@example.com" },
        { account_id: "two", login_email: "viewer@example.com" },
      ],
      error: null,
    });
    expect(await resolveAccessUncached("viewer@example.com", lookup)).toBeNull();
  });

  it("binds one normalized viewer to the matching account id", async () => {
    lookup.mockResolvedValue({
      rows: [{ account_id: "viewer-account", login_email: "viewer@example.com" }],
      error: null,
    });
    await expect(resolveAccessUncached(" Viewer@Example.COM ", lookup)).resolves.toEqual({
      role: "viewer",
      accountId: "viewer-account",
    });
    expect(lookup).toHaveBeenCalledWith("viewer@example.com");
  });

  it("never derives admin from account data", async () => {
    lookup.mockResolvedValue({
      rows: [
        {
          account_id: "forged-admin",
          login_email: ALLOWED_EMAIL,
          role: "admin",
        } as { account_id: string; login_email: string; role: string },
      ],
      error: null,
    });
    await expect(resolveAccessUncached(ALLOWED_EMAIL.toUpperCase(), lookup)).resolves.toEqual({
      role: "admin",
    });
    expect(lookup).not.toHaveBeenCalled();

    lookup.mockResolvedValue({
      rows: [
        {
          account_id: "viewer-only",
          login_email: "viewer@example.com",
          role: "admin",
        } as { account_id: string; login_email: string; role: string },
      ],
      error: null,
    });
    await expect(resolveAccessUncached("viewer@example.com", lookup)).resolves.toEqual({
      role: "viewer",
      accountId: "viewer-only",
    });
  });

  it("per-request cache performs one lookup and a new request resolves again", async () => {
    lookup.mockResolvedValue({
      rows: [{ account_id: "viewer", login_email: "viewer@example.com" }],
      error: null,
    });
    const thisRequest = createCachedAccessResolver(lookup);
    await Promise.all([
      thisRequest("viewer@example.com"),
      thisRequest("viewer@example.com"),
    ]);
    expect(lookup).toHaveBeenCalledTimes(1);

    const nextRequest = createCachedAccessResolver(lookup);
    await nextRequest("viewer@example.com");
    expect(lookup).toHaveBeenCalledTimes(2);
  });
});
