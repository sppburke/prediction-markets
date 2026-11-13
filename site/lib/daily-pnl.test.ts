import { describe, expect, it } from "vitest";
import { centralDay, dailyPnl, monthCells, shiftMonth } from "./daily-pnl";

const unix = (iso: string) => Date.parse(iso) / 1000;
const buy = { side: "buy", contracts: "100", fill_price: "0.40", principal: "40", fee: "1" };

describe("centralDay", () => {
  it("splits days at Central Time midnight, across the DST change", () => {
    expect(centralDay(unix("2026-11-09T05:59:59Z"))).toBe("2026-11-08"); // CST, UTC-6
    expect(centralDay(unix("2026-11-09T06:00:00Z"))).toBe("2026-11-09");
    expect(centralDay(unix("2026-11-01T04:59:59Z"))).toBe("2026-10-31"); // CDT, UTC-5
    expect(centralDay(unix("2026-11-01T05:00:00Z"))).toBe("2026-11-01");
  });
});

describe("dailyPnl", () => {
  const settled = [
    { market_id: "won", outcome_prices: ["0", "1"], settled_at_unix: unix("2026-11-09T05:59:59Z") },
    { market_id: "lost", outcome_prices: ["1", "0"], settled_at_unix: unix("2026-11-09T06:00:00Z") },
    { market_id: "late", outcome_prices: ["0", "1"], settled_at_unix: unix("2026-11-09T20:00:00Z") },
    { market_id: "half", outcome_prices: ["0.5", "0.5"], settled_at_unix: unix("2026-11-10T12:00:00Z") },
    { market_id: "null", outcome_prices: [null, "1"], settled_at_unix: unix("2026-11-10T12:00:00Z") },
  ];

  it("sums each fill's realized P&L into its market's Central Time settlement day", () => {
    const fills = [
      { ...buy, market_id: "won", outcome_id: 1 }, // 100 − 40 − 1 = 59
      { ...buy, market_id: "lost", outcome_id: 1 }, // 0 − 40 − 1 = −41
      { ...buy, market_id: "late", outcome_id: 1 }, // +59
      { ...buy, market_id: "half", outcome_id: 0 }, // 50 − 40 − 1 = 9
      { ...buy, market_id: "open", outcome_id: 1 }, // unsettled: skipped
    ];
    expect(dailyPnl(fills, settled)).toEqual([
      { day: "2026-11-08", pnl: 59 },
      { day: "2026-11-09", pnl: 18 },
      { day: "2026-11-10", pnl: 9 },
    ]);
  });

  it("counts an absent or JSON-null payout as 0, like the view's coalesce", () => {
    const fills = [
      { ...buy, market_id: "won", outcome_id: 5 },
      { ...buy, market_id: "null", outcome_id: 0 },
    ];
    expect(dailyPnl(fills, settled)).toEqual([
      { day: "2026-11-08", pnl: -41 },
      { day: "2026-11-10", pnl: -41 },
    ]);
  });

  it("sums before rounding: two +$0.004 fills make one +$0.008 day", () => {
    const tiny = { side: "buy", contracts: "1", fill_price: "0.5", principal: "0.996", fee: "0" };
    const fills = [{ ...tiny, market_id: "won", outcome_id: 1 }, { ...tiny, market_id: "won", outcome_id: 1 }];
    const [row] = dailyPnl(fills, settled);
    expect(row.pnl).toBeCloseTo(0.008, 9);
  });

  it("fails on an unreadable payout or fill instead of showing a wrong total", () => {
    const bad = [{ market_id: "bad", outcome_prices: ["x", "1"], settled_at_unix: unix("2026-11-10T12:00:00Z") }];
    expect(() => dailyPnl([{ ...buy, market_id: "bad", outcome_id: 0 }], bad)).toThrow("unreadable P&L");
    expect(() => dailyPnl([{ ...buy, contracts: "x", market_id: "won", outcome_id: 1 }], settled)).toThrow(
      "unreadable P&L",
    );
  });
});

describe("monthCells", () => {
  it("pads the month to whole Sunday-first weeks", () => {
    const october = monthCells("2026-10"); // starts Thursday
    expect(october.slice(0, 5)).toEqual([null, null, null, null, "2026-10-01"]);
    expect(october).toHaveLength(35);
    expect(october.filter(Boolean)).toHaveLength(31);
    expect(monthCells("2026-11")[0]).toBe("2026-11-01"); // starts Sunday
    expect(monthCells("2026-02")).toHaveLength(28); // 28 days from a Sunday
    expect(monthCells("2026-08")).toHaveLength(42); // 31 days from a Saturday
  });
});

describe("shiftMonth", () => {
  it("crosses year boundaries", () => {
    expect(shiftMonth("2026-01", -1)).toBe("2025-12");
    expect(shiftMonth("2026-12", 1)).toBe("2027-01");
  });
});
