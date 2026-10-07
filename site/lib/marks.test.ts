import { describe, expect, it } from "vitest";
import { MAX_MARK_KEYS, parseMarkKeys } from "./marks";

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
