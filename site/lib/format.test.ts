import { describe, expect, it } from "vitest";
import {
  PRECISION,
  formatAge,
  formatDate,
  formatEdge,
  formatInt,
  formatPct,
  formatPrice,
  formatQty,
  formatTstat,
  formatUsd,
  shortWallet,
  signClass,
  toNum,
} from "./format";

// Issue #343 PR3 hard AC: no raw full-precision NUMERIC string reaches the DOM.
// These are the exact full-precision strings @supabase/supabase-js returns for
// NUMERIC columns — the formatter must round them to the declared precision.
const RAW_USD = "1579.97989245199679481500";
const RAW_EDGE = "0.02342341998877665544";
const RAW_FRAC = "0.62347891234567890000";
const RAW_PRICE = "0.51839201928374650000";
const RAW_TSTAT = "3.18273645019283746500";

/** Decimal places actually present in a rendered string (0 if none). */
function decimals(s: string): number {
  const cleaned = s.replace(/[^0-9.]/g, "");
  const dot = cleaned.indexOf(".");
  return dot === -1 ? 0 : cleaned.length - dot - 1;
}

describe("formatter precision contract (the PR3 AC)", () => {
  it("currency rounds full-precision strings to exactly 2 dp", () => {
    expect(formatUsd(RAW_USD)).toBe("$1,579.98");
    expect(decimals(formatUsd(RAW_USD))).toBe(PRECISION.usd);
  });

  it("percentage rounds fractions to 1 dp", () => {
    expect(formatPct(RAW_FRAC)).toBe("62.3%");
    expect(decimals(formatPct(RAW_FRAC))).toBe(PRECISION.pct);
  });

  it("ranker edge rounds to 4 dp", () => {
    expect(formatEdge(RAW_EDGE)).toBe("0.0234");
    expect(decimals(formatEdge(RAW_EDGE))).toBe(PRECISION.edge);
  });

  it("t-stat rounds to 2 dp", () => {
    expect(formatTstat(RAW_TSTAT)).toBe("3.18");
    expect(decimals(formatTstat(RAW_TSTAT))).toBe(PRECISION.tstat);
  });

  it("price rounds to 3 dp", () => {
    expect(formatPrice(RAW_PRICE)).toBe("0.518");
    expect(decimals(formatPrice(RAW_PRICE))).toBe(PRECISION.price);
  });

  it("never emits more decimals than declared for any raw input", () => {
    const cases: Array<[(v: string) => string, number]> = [
      [formatUsd, PRECISION.usd],
      [formatPct, PRECISION.pct],
      [formatEdge, PRECISION.edge],
      [formatTstat, PRECISION.tstat],
      [formatPrice, PRECISION.price],
    ];
    const raws = [RAW_USD, RAW_EDGE, RAW_FRAC, RAW_PRICE, RAW_TSTAT];
    for (const [fn, p] of cases) {
      for (const raw of raws) {
        expect(decimals(fn(raw))).toBeLessThanOrEqual(p);
      }
    }
  });

  it("share quantity keeps 2 dp and never rounds fractional contracts to int", () => {
    // Polymarket fills are fractional ($25 / 0.525 ≈ 47.62 shares).
    expect(formatQty("47.61904761904762")).toBe("47.62");
    expect(formatQty("0.4")).toBe("0.40"); // a small fill must NOT collapse to "0"
    expect(decimals(formatQty("47.61904761904762"))).toBe(PRECISION.qty);
  });

  it("integers carry no decimals and group thousands", () => {
    expect(formatInt("449")).toBe("449");
    expect(formatInt(12345)).toBe("12,345");
    expect(decimals(formatInt("449"))).toBe(0);
  });
});

describe("null / empty handling", () => {
  it("renders an em dash for null / undefined / empty", () => {
    for (const fn of [formatUsd, formatPct, formatEdge, formatTstat, formatPrice, formatQty, formatInt]) {
      expect(fn(null)).toBe("—");
      expect(fn(undefined)).toBe("—");
      expect(fn("")).toBe("—");
    }
  });

  it("toNum parses strings and rejects garbage", () => {
    expect(toNum("12.5")).toBe(12.5);
    expect(toNum(12.5)).toBe(12.5);
    expect(toNum("not-a-number")).toBeNull();
    expect(toNum(null)).toBeNull();
  });
});

describe("signed currency and sign class", () => {
  it("prefixes a + only for strictly positive when sign requested", () => {
    expect(formatUsd("12.5", { sign: true })).toBe("+$12.50");
    expect(formatUsd("-12.5", { sign: true })).toBe("-$12.50");
    expect(formatUsd("0", { sign: true })).toBe("$0.00");
  });

  it("classifies sign with zero neutral", () => {
    expect(signClass("1")).toBe("pos");
    expect(signClass("-1")).toBe("neg");
    expect(signClass("0")).toBe("zero");
    expect(signClass(null)).toBe("zero");
  });
});

describe("wallet abbreviation", () => {
  it("abbreviates a 0x address keeping head and tail", () => {
    expect(shortWallet("0x1234567890abcdef1234567890abcdef12345678")).toBe("0x1234…5678");
  });
  it("leaves short strings intact", () => {
    expect(shortWallet("0xabcd")).toBe("0xabcd");
  });
});

describe("last-trade date / age formatting (#357 PR4)", () => {
  // Fixed UTC anchor. Date.UTC is a pure function of its arguments (no wall
  // clock), so every assertion below is deterministic.
  const T = Date.UTC(2026, 5, 17, 0, 0, 0) / 1000; // 2026-06-17T00:00:00Z, in seconds

  it("formats epoch seconds as a UTC calendar date", () => {
    expect(formatDate(T)).toBe("2026-06-17");
    expect(formatDate(String(T))).toBe("2026-06-17"); // supabase may return bigint as a string
  });

  it("renders coarse relative-age buckets from an injected clock", () => {
    expect(formatAge(T, T + 30)).toBe("just now");
    expect(formatAge(T, T + 5 * 60)).toBe("5m ago");
    expect(formatAge(T, T + 3 * 3600)).toBe("3h ago");
    expect(formatAge(T, T + 8 * 86400)).toBe("8d ago");
  });

  it("clamps a future timestamp (clock skew) to just now", () => {
    expect(formatAge(T, T - 100)).toBe("just now");
  });

  it("renders an em dash for null / undefined / empty", () => {
    expect(formatDate(null)).toBe("—");
    expect(formatDate(undefined)).toBe("—");
    expect(formatDate("")).toBe("—");
    expect(formatAge(null, T)).toBe("—");
    expect(formatAge(undefined, T)).toBe("—");
    expect(formatAge("", T)).toBe("—");
  });
});
