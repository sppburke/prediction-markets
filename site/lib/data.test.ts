import { describe, expect, it } from "vitest";

import { guardWatchedSnapshot, readConsistentWatchedSet } from "./data";

const runtime = (token: string, count: number) => ({
  updated_at: token,
  watchlist_size: count,
});

describe("watchlist projection token guard (#544)", () => {
  it("accepts matching tokens and a matching row count, including valid empty", () => {
    const populated = guardWatchedSnapshot(
      runtime("t1", 2),
      [{ wallet_hex: "0xAA" }, { wallet_hex: "0xBB" }],
      runtime("t1", 2),
    );
    expect(populated.status).toBe("available");
    if (populated.status === "available") {
      expect([...populated.wallets]).toEqual(["0xaa", "0xbb"]);
    }
    expect(guardWatchedSnapshot(runtime("t2", 0), [], runtime("t2", 0))).toMatchObject({
      status: "available",
      count: 0,
    });
  });

  it("rejects a token change", () => {
    expect(
      guardWatchedSnapshot(runtime("old", 1), [{ wallet_hex: "0xAA" }], runtime("new", 1)),
    ).toMatchObject({ status: "unavailable", reason: "token_changed" });
  });

  it("rejects a row-count mismatch without turning it into empty", () => {
    expect(guardWatchedSnapshot(runtime("t1", 1), [], runtime("t1", 1))).toMatchObject({
      status: "unavailable",
      reason: "count_mismatch",
    });
    expect(
      guardWatchedSnapshot(runtime("t1", 1), [{ wallet_hex: "0xAA" }], runtime("t1", 2)),
    ).toMatchObject({ status: "unavailable", reason: "count_mismatch" });
  });

  it("retries one token race and accepts only the second complete generation", async () => {
    const runtimes = [runtime("old", 1), runtime("new", 2), runtime("new", 2), runtime("new", 2)];
    const watchlists = [
      [{ wallet_hex: "0xOLD" }],
      [{ wallet_hex: "0xAA" }, { wallet_hex: "0xBB" }],
    ];
    let runtimeReads = 0;
    let watchlistReads = 0;
    const result = await readConsistentWatchedSet(
      async () => ({ data: runtimes[runtimeReads++], error: null }),
      async () => ({ data: watchlists[watchlistReads++], error: null }),
    );
    expect(result.status).toBe("available");
    expect(runtimeReads).toBe(4);
    expect(watchlistReads).toBe(2);
    if (result.status === "available") {
      expect([...result.wallets]).toEqual(["0xaa", "0xbb"]);
    }
  });

  it("returns typed unavailable for read errors and a missing runtime row", async () => {
    await expect(
      readConsistentWatchedSet(
        async () => ({ data: null, error: "offline" }),
        async () => ({ data: [], error: null }),
      ),
    ).resolves.toMatchObject({ status: "unavailable", reason: "read_error" });
    await expect(
      readConsistentWatchedSet(
        async () => ({ data: null, error: null }),
        async () => ({ data: [], error: null }),
      ),
    ).resolves.toMatchObject({ status: "unavailable", reason: "runtime_missing" });
  });
});
