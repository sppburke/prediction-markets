import { describe, expect, it } from "vitest";
import { MAX_MARK_KEYS, parseMarkKeys, unrealizedPnl } from "./marks";

const KEY = "0xed8673f80fe4fa7c282ca57d3eed6ccfcb9eb39db04adb4b4f82ffebf36e1205:1";

describe("parseMarkKeys", () => {
  it("returns trimmed, de-duplicated keys", () => {
    expect(parseMarkKeys({ keys: [KEY, ` ${KEY} `, "", "0xabc:0"] })).toEqual([KEY, "0xabc:0"]);
  });

  it("accepts a key list far longer than a URL can carry", () => {
    // 183 open keys made a 13 KB request line on 10/7, which nginx refused with 414.
    const keys = Array.from({ length: 1000 }, (_, i) => `${KEY.slice(0, -1)}${i}`);
    expect(parseMarkKeys({ keys })).toHaveLength(1000);
  });

  it("rejects malformed bodies", () => {
    expect(parseMarkKeys(null)).toBeNull();
    expect(parseMarkKeys("keys")).toBeNull();
    expect(parseMarkKeys({})).toBeNull();
    expect(parseMarkKeys({ keys: KEY })).toBeNull();
    expect(parseMarkKeys({ keys: [KEY, 7] })).toBeNull();
  });

  it("rejects more keys than the page can load", () => {
    expect(parseMarkKeys({ keys: Array(MAX_MARK_KEYS).fill(KEY) })).toEqual([KEY]);
    expect(parseMarkKeys({ keys: Array(MAX_MARK_KEYS + 1).fill(KEY) })).toBeNull();
  });
});

describe("unrealizedPnl", () => {
  it("nets a BUY's recorded principal and entry fee", () => {
    // 100 contracts bought for 40.5 (not 0.40 × 100) plus a 1.68 fee, marked at 0.55.
    const fill = { side: "buy", contracts: "100", fill_price: "0.40", principal: "40.5", fee: "1.68" };
    expect(unrealizedPnl(fill, 0.55)).toBeCloseTo(55 - 40.5 - 1.68, 9);
  });

  it("nets a SELL the other way", () => {
    const fill = { side: "sell", contracts: "50", fill_price: "0.30", principal: "15", fee: "0.5" };
    expect(unrealizedPnl(fill, 0.2)).toBeCloseTo(15 - 10 - 0.5, 9);
  });

  it("keeps the notional formula for a fill without principal and fee", () => {
    const fill = { side: "buy", contracts: "20", fill_price: "0.25", principal: null, fee: null };
    expect(unrealizedPnl(fill, 0.4)).toBeCloseTo((0.4 - 0.25) * 20, 9);
  });

  it("returns null when an input does not parse", () => {
    expect(unrealizedPnl({ side: "buy", contracts: "x", fill_price: "0.4", principal: "1", fee: "0" }, 0.5)).toBeNull();
    expect(unrealizedPnl({ side: "buy", contracts: "1", fill_price: "y", principal: null, fee: null }, 0.5)).toBeNull();
    expect(unrealizedPnl({ side: "buy", contracts: "1", fill_price: "0.4", principal: "1", fee: "0" }, NaN)).toBeNull();
  });
});
