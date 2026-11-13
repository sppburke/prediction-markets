// Realized paper P&L per Central Time day, for the overview calendar. A fill's P&L belongs to the day its market
// settled (`settled_markets.settled_at_unix`: when pe-service received the resolution), with the economics of
// `wallet_live_stats`: `fillPnl` at the resolved outcome price, a missing price counting as 0.
import { type Numeric, toNum } from "./format";
import { fillPnl } from "./marks";
import type { PaperFill } from "./types";

export interface DailyPnl {
  day: string; // "YYYY-MM-DD", Central Time
  pnl: number; // unrounded USD
}

export interface SettledMarket {
  market_id: string;
  outcome_prices: Numeric[];
  settled_at_unix: number;
}

export type FillEconomics = Pick<
  PaperFill,
  "market_id" | "outcome_id" | "side" | "contracts" | "fill_price" | "principal" | "fee"
>;

const CENTRAL_DATE = new Intl.DateTimeFormat("en-CA", {
  timeZone: "America/Chicago",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});

/** Central Time calendar day ("YYYY-MM-DD") of an epoch-seconds instant. */
export function centralDay(unixSeconds: number): string {
  return CENTRAL_DATE.format(new Date(unixSeconds * 1000));
}

/** One row per Central Time day with settled fills, oldest first; fills of unsettled markets are skipped. Like the
 * view's cast, a payout that does not parse fails the whole read instead of becoming a wrong total. */
export function dailyPnl(fills: FillEconomics[], settled: SettledMarket[]): DailyPnl[] {
  const markets = new Map(settled.map((market) => [market.market_id, market]));
  const days = new Map<string, number>();
  for (const fill of fills) {
    const market = markets.get(fill.market_id);
    if (!market) continue;
    const payout = market.outcome_prices[fill.outcome_id];
    const pnl = fillPnl(fill, payout === undefined || payout === null ? 0 : (toNum(payout) ?? NaN));
    if (pnl === null) throw new Error(`unreadable P&L for market ${fill.market_id}`);
    const day = centralDay(market.settled_at_unix);
    days.set(day, (days.get(day) ?? 0) + pnl);
  }
  return [...days].map(([day, pnl]) => ({ day, pnl })).sort((a, b) => a.day.localeCompare(b.day));
}

/** The days ("YYYY-MM-DD") of `month` ("YYYY-MM"), padded with nulls to whole Sunday-first weeks. */
export function monthCells(month: string): (string | null)[] {
  const [year, m] = month.split("-").map(Number);
  const cells: (string | null)[] = Array(new Date(Date.UTC(year, m - 1, 1)).getUTCDay()).fill(null);
  const length = new Date(Date.UTC(year, m, 0)).getUTCDate();
  for (let day = 1; day <= length; day++) cells.push(`${month}-${String(day).padStart(2, "0")}`);
  while (cells.length % 7) cells.push(null);
  return cells;
}

/** `month` ("YYYY-MM") moved by `delta` months. */
export function shiftMonth(month: string, delta: number): string {
  const [year, m] = month.split("-").map(Number);
  return new Date(Date.UTC(year, m - 1 + delta, 1)).toISOString().slice(0, 7);
}
