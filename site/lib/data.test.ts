import { createClient } from "@supabase/supabase-js";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { KpiCards } from "../components/KpiCards";
import { PnlCalendar } from "../components/PnlCalendar";
import { WalletTabs } from "../components/WalletTabs";
import {
  fetchPaperResults,
  fetchWalletStats,
  fetchWatchedSet,
  guardWatchedSnapshot,
  readConsistentWatchedSet,
} from "./data";
import { getSupabase } from "./supabase";
import type { WalletLiveStats } from "./types";

vi.mock("./supabase", () => ({ getSupabase: vi.fn() }));
vi.mock("@/lib/daily-pnl", async () => import("./daily-pnl"));
vi.mock("@/lib/format", async () => import("./format"));
vi.mock("@/lib/marks", async () => import("./marks"));

const runtime = (token: string, count: number) => ({
  updated_at: token,
  watchlist_size: count,
});

const wallet = (index: number) => `0x${index.toString(16).padStart(40, "0")}`;

const statsRow = (index: number): WalletLiveStats => ({
  wallet: wallet(index),
  live_total_fills: 3,
  live_settled_count: 2,
  live_open_fills: 1,
  live_wins: 1,
  live_win_rate: "0.5",
  live_realized_pnl: "1",
  live_edge: "0.5",
  rank: index,
  ls_edge: null,
  ls_tstat: null,
  fill_rate: null,
  n_trades: null,
  hit_rate: null,
  avg_price: null,
  last_trade_unix: null,
});

type Reply = { data?: unknown; error?: string };

function stubSupabase(read: (table: string, params: URLSearchParams) => Reply | Promise<Reply>) {
  const request = vi.fn<typeof fetch>(async (input) => {
    const url = new URL(typeof input === "string" ? input : input instanceof URL ? input : input.url);
    const result = await read(url.pathname.split("/").at(-1) ?? "", url.searchParams);
    return new Response(JSON.stringify(result.error ? { message: result.error } : result.data), {
      status: result.error ? 400 : 200,
      headers: { "content-type": "application/json" },
    });
  });
  vi.mocked(getSupabase).mockReturnValue(createClient("https://supabase.test", "test-anon-key", {
    auth: { persistSession: false },
    global: { fetch: request },
  }));
}

/** The one order the fake API filters and sorts by. */
const byText = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

function page<Row>(rows: Row[], key: keyof Row & string, params: URLSearchParams): Row[] {
  expect(params.get("order")).toBe(`${key}.asc`);
  expect(params.get("limit")).toBe("1000");
  expect(params.has("offset")).toBe(false);
  const filters = params.getAll(key).map((filter) => {
    const [op, value] = [filter.slice(0, filter.indexOf(".")), filter.slice(filter.indexOf(".") + 1)];
    expect(["gt", "gte", "lt"]).toContain(op);
    return (text: string) => {
      const order = byText(text, value);
      return op === "gt" ? order > 0 : op === "gte" ? order >= 0 : order < 0;
    };
  });
  return rows
    .filter((row) => filters.every((passes) => passes(String(row[key]))))
    .sort((a, b) => byText(String(a[key]), String(b[key])))
    .slice(0, Number(params.get("limit")));
}

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

describe("dashboard pagination (#757)", () => {
  it("renders all 1,001 watched wallets and their KPIs exactly once across a stats refresh", async () => {
    const initialStats = Array.from({ length: 1001 }, (_, index) => ({
      ...statsRow(index + 1),
      live_realized_pnl: index === 1000 ? "0" : "1",
    }));
    const watchlist = initialStats.map((row) => ({ wallet_hex: row.wallet }));
    let statsPages = 0;
    let watchlistPages = 0;
    stubSupabase((table, params) => {
      if (table === "service_runtime") return { data: runtime("t1", 1001) };
      if (table === "service_watchlist") {
        watchlistPages += 1;
        expect(params.get("select")).toBe("wallet_hex");
        return { data: page(watchlist, "wallet_hex", params) };
      }
      expect(table).toBe("wallet_live_stats_mv");
      expect(params.get("select")).toBe("*");
      statsPages += 1;
      // The last wallet moves to first in profit order while the wallet keys remain the same.
      const refreshed = initialStats.map((row, index) => ({
        ...row,
        live_realized_pnl: index === 1000 ? "5" : "1",
      }));
      expect(params.get("wallet")).toBe(statsPages === 1 ? null : `gt.${wallet(1000)}`);
      return { data: page(statsPages === 1 ? initialStats : refreshed, "wallet", params) };
    });

    const rows = await fetchWalletStats();
    const watched = await fetchWatchedSet();
    expect(statsPages).toBe(2);
    expect(watchlistPages).toBe(2);
    expect(rows.map((row) => row.wallet)).toEqual(initialStats.map((row) => row.wallet));
    expect(new Set(rows.map((row) => row.wallet)).size).toBe(1001);
    expect(watched).toMatchObject({ status: "available", count: 1001 });
    if (watched.status !== "available") throw new Error("Expected a consistent watched set");
    expect(watched.wallets.size).toBe(1001);

    const tabs = renderToStaticMarkup(createElement(WalletTabs, {
      rows,
      watchedSet: [...watched.wallets],
      openFills: [],
    }));
    expect(tabs).toContain("Watched (1001)");
    const renderedWallets = [...tabs.matchAll(/href="\/wallet\/(0x[0-9a-f]+)"/g)]
      .map((match) => match[1]);
    expect(renderedWallets).toHaveLength(1001);
    expect(new Set(renderedWallets)).toEqual(watched.wallets);
    // The table still sorts by realized profit rather than the fetch cursor.
    expect(renderedWallets[0]).toBe(wallet(1001));

    const kpis = renderToStaticMarkup(createElement(KpiCards, { rows, watched: watched.count }));
    expect(kpis).toContain("1,001 traded");
    expect(kpis).not.toContain("ranked");
    expect(kpis).toContain("+$1,005.00");
    expect(kpis).toMatch(/Settled fills<\/div><div[^>]*>2,002<\/div>/);
    expect(kpis).toMatch(/Open fills<\/div><div[^>]*>1,001<\/div>/);
    expect(kpis).toMatch(/Wins<\/div><div[^>]*>1,001<\/div>/);
    expect(kpis).toContain("+50.20¢");
  });

  it.each([0, 1, 100, 200, 1000, 1001, 2000])(
    "reads complete sets of %i wallets, including full-page boundaries",
    async (count) => {
      const stats = Array.from({ length: count }, (_, index) => statsRow(index + 1));
      const watchlist = stats.map((row) => ({ wallet_hex: row.wallet }));
      const reads: Record<string, number> = {};
      stubSupabase((table, params) => {
        reads[table] = (reads[table] ?? 0) + 1;
        if (table === "service_runtime") return { data: runtime("t1", count) };
        if (table === "service_watchlist") return { data: page(watchlist, "wallet_hex", params) };
        expect(table).toBe("wallet_live_stats_mv");
        return { data: page(stats, "wallet", params) };
      });
      expect(await fetchWalletStats()).toEqual(stats);
      expect(await fetchWatchedSet()).toEqual({
        status: "available",
        count,
        token: "t1",
        wallets: new Set(watchlist.map((row) => row.wallet_hex)),
      });
      const pages = Math.floor(count / 1000) + 1;
      expect(reads).toEqual({
        wallet_live_stats_mv: pages,
        service_runtime: 2,
        service_watchlist: pages,
      });
    },
  );

  it.each(["service_watchlist", "wallet_live_stats_mv"])(
    "fails closed when the second %s page fails",
    async (table) => {
      const stats = Array.from({ length: 1001 }, (_, index) => statsRow(index + 1));
      let pages = 0;
      stubSupabase((relation, params) => {
        if (relation === "service_runtime") return { data: runtime("t1", 1001) };
        expect(relation).toBe(table);
        pages += 1;
        if (pages === 2) return { error: "later page failed" };
        if (table === "service_watchlist") {
          return { data: page(stats.map((row) => ({ wallet_hex: row.wallet })), "wallet_hex", params) };
        }
        return { data: page(stats, "wallet", params) };
      });
      if (table === "service_watchlist") {
        expect(await fetchWatchedSet()).toEqual({
          status: "unavailable", reason: "read_error", message: "later page failed",
        });
      } else {
        await expect(fetchWalletStats()).rejects.toThrow("later page failed");
      }
      expect(pages).toBe(2);
    },
  );

  it.each([false, true])(
    "retries a watchlist replace during traversal once (replace again: %s)",
    async (replaceAgain) => {
      let generation = 0;
      let pages = 0;
      let runtimeReads = 0;
      stubSupabase((table, params) => {
        if (table === "service_runtime") {
          runtimeReads += 1;
          return { data: runtime(`t${generation}`, 1001) };
        }
        expect(table).toBe("service_watchlist");
        pages += 1;
        if (pages === 2 || (replaceAgain && pages === 4)) generation += 1;
        const rows = Array.from({ length: 1001 }, (_, index) => ({
          wallet_hex: wallet(index + 1 + generation),
        }));
        return { data: page(rows, "wallet_hex", params) };
      });
      const result = await fetchWatchedSet();
      expect(pages).toBe(4);
      expect(runtimeReads).toBe(4);
      if (replaceAgain) {
        expect(result).toMatchObject({ status: "unavailable", reason: "token_changed" });
      } else {
        expect(result).toEqual({
          status: "available", count: 1001, token: "t1",
          wallets: new Set(Array.from({ length: 1001 }, (_, index) => wallet(index + 2))),
        });
      }
    },
  );

  it("sends every token and watchlist read with a signal, so Next never replays a repeat", async () => {
    const signals: (AbortSignal | null | undefined)[] = [];
    vi.mocked(getSupabase).mockReturnValue(createClient("https://supabase.test", "test-anon-key", {
      auth: { persistSession: false },
      global: {
        fetch: vi.fn<typeof fetch>(async (input, init) => {
          const table = new URL(String(input)).pathname.split("/").at(-1);
          signals.push(init?.signal);
          const body = table === "service_runtime" ? runtime("t1", 0) : [];
          return new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });
        }),
      },
    }));
    expect(await fetchWatchedSet()).toMatchObject({ status: "available", count: 0 });
    expect(signals).toHaveLength(3); // runtime, watchlist page, runtime
    expect(signals.every((signal) => signal instanceof AbortSignal)).toBe(true);
  });

  it("returns unavailable, never a rejection, when the client cannot be built", async () => {
    vi.mocked(getSupabase).mockImplementationOnce(() => {
      throw new Error("Invalid supabaseUrl: Must be a valid HTTP or HTTPS URL.");
    });
    await expect(fetchWatchedSet()).resolves.toEqual({
      status: "unavailable",
      reason: "read_error",
      message: "Invalid supabaseUrl: Must be a valid HTTP or HTTPS URL.",
    });
  });

  it("rejects a count mismatch after reading all watchlist pages", async () => {
    const rows = Array.from({ length: 1001 }, (_, index) => ({ wallet_hex: wallet(index + 1) }));
    let pages = 0;
    stubSupabase((table, params) => {
      if (table === "service_runtime") return { data: runtime("t1", 1002) };
      expect(table).toBe("service_watchlist");
      pages += 1;
      return { data: page(rows, "wallet_hex", params) };
    });
    expect(await fetchWatchedSet()).toMatchObject({
      status: "unavailable", reason: "count_mismatch",
    });
    expect(pages).toBe(2);
  });
});

describe("overview shows the followed set and paper results, not the ranked universe", () => {
  const followed = wallet(1);
  const openOnly = wallet(2);
  const settledOnly = wallet(3);
  const rankedOnly = wallet(4);
  const row = (index: number, fills: { total: number; settled: number; open: number }): WalletLiveStats => ({
    ...statsRow(index),
    live_total_fills: fills.total,
    live_settled_count: fills.settled,
    live_open_fills: fills.open,
    live_wins: 0,
    live_realized_pnl: "0",
  });
  const rows = [
    row(1, { total: 0, settled: 0, open: 0 }),
    row(2, { total: 1, settled: 0, open: 1 }),
    row(3, { total: 2, settled: 2, open: 0 }),
    row(4, { total: 0, settled: 0, open: 0 }),
  ];
  const links = (markup: string) =>
    [...markup.matchAll(/href="\/wallet\/(0x[0-9a-f]+)"/g)].map((match) => match[1]);

  it("lists followed and open-fill wallets under Watched and has no Bench tab", () => {
    const tabs = renderToStaticMarkup(createElement(WalletTabs, {
      rows,
      watchedSet: [followed],
      openFills: [],
    }));
    expect(tabs).toContain("Watched (2)");
    expect(tabs).toContain("Historical (1)");
    expect(tabs).not.toContain("Bench");
    // The column header itself, not the always-rendered "Unrealized P&L" KPI.
    expect(tabs).toMatch(/<th\b[^>]*><span[^>]*>Unrealized<\/span>/);
    expect(new Set(links(tabs))).toEqual(new Set([followed, openOnly]));
    expect(links(tabs)).not.toContain(settledOnly);
    expect(links(tabs)).not.toContain(rankedOnly);
  });

  it("keeps an unavailable watchlist visibly unavailable", () => {
    const tabs = renderToStaticMarkup(createElement(WalletTabs, {
      rows,
      watchedSet: null,
      openFills: [],
    }));
    expect(tabs).toContain("Watched (—)");
    expect(tabs).toContain("Watchlist projection unavailable");
    expect(tabs).not.toContain("Bench");
    expect(links(tabs)).toEqual([]);
  });

  it("shows the followed count with traded wallets and no ranked count", () => {
    const kpis = renderToStaticMarkup(createElement(KpiCards, { rows, watched: 1 }));
    expect(kpis).toMatch(/Watched<\/div><div[^>]*>1<\/div>/);
    expect(kpis).toContain("2 traded");
    expect(kpis).not.toContain("ranked");
  });
});

describe("paper results: daily realized P&L and open fills", () => {
  // Keys of a wallet starting with each hex digit in turn, so the fills lie in all four key ranges.
  const fill = (index: number, key = `wf|0x${(index % 16).toString(16)}|${String(index).padStart(5, "0")}`) => ({
    idempotency_key: key,
    leader_wallet: wallet(index),
    market_id: `market-${String(index).padStart(5, "0")}`,
    outcome_id: 1,
    side: "buy",
    contracts: "10",
    fill_price: "0.4",
    principal: "4",
    fee: "0.1",
  });
  const settlement = (index: number) => ({
    market_id: `market-${String(index).padStart(5, "0")}`,
    outcome_prices: ["0", "1"],
    settled_at_unix: Date.parse("2026-11-10T12:00:00Z") / 1000,
  });

  const fills = (count: number, key?: (index: number) => string) =>
    Array.from({ length: count }, (_, index) => fill(index + 1, key?.(index + 1)));
  const inOneRange = (index: number) => `wf|0x5|${String(index).padStart(5, "0")}`;
  const inKeyOrder = (rows: ReturnType<typeof fill>[]) =>
    [...rows].sort((a, b) => byText(a.idempotency_key, b.idempotency_key));

  /** Serves the two tables and returns each request's table and key filters. */
  function stubTables(rows: ReturnType<typeof fill>[], settledCount = rows.length, fails?: string) {
    const settled = Array.from({ length: settledCount }, (_, index) => settlement(index + 1));
    const requests: { table: string; filters: string[] }[] = [];
    stubSupabase((table, params) => {
      const key = table === "paper_fills" ? "idempotency_key" : "market_id";
      const filters = params.getAll(key);
      requests.push({ table, filters });
      if (table === fails && filters.some((filter) => filter.startsWith("gt."))) return { error: "later page failed" };
      if (table === "paper_fills") {
        expect(params.get("select")).toBe(
          "idempotency_key,leader_wallet,market_id,outcome_id,side,contracts,fill_price,principal,fee",
        );
        return { data: page(rows, "idempotency_key", params) };
      }
      expect(table).toBe("settled_markets");
      expect(params.get("select")).toBe("market_id,outcome_prices,settled_at_unix");
      return { data: page(settled, "market_id", params) };
    });
    return requests;
  }

  it.each([0, 1000, 1001])("reads every fill and settlement (%i of each)", async (count) => {
    const requests = stubTables(fills(count));
    // Each won fill: 10 × 1 − 4 − 0.1 = 5.9.
    expect(await fetchPaperResults()).toEqual({
      daily: count === 0 ? [] : [{ day: "2026-11-10", pnl: expect.closeTo(count * 5.9, 6) }],
      openFills: [],
    });
    expect(requests.filter(({ table }) => table === "paper_fills")).toHaveLength(4);
    expect(requests.filter(({ table }) => table === "settled_markets")).toHaveLength(Math.floor(count / 1000) + 1);
  });

  it("returns exactly the unsettled fills, including those past the first 1,000 rows of a range", async () => {
    stubTables(fills(1001, inOneRange), 999);
    const { daily, openFills } = await fetchPaperResults();
    expect(daily).toEqual([{ day: "2026-11-10", pnl: expect.closeTo(999 * 5.9, 6) }]);
    expect(openFills).toEqual([fill(1000, inOneRange(1000)), fill(1001, inOneRange(1001))]);
  });

  it("reads each key once, in order, at and around every bound, and bounds every page of a range", async () => {
    const edges = ["other", "wf|0x3f", "wf|0x4", "wf|0x7f", "wf|0x8", "wf|0xbf", "wf|0xc", "wf|0xff", "wf|zz", "zz"];
    const rows = [...fills(1001, inOneRange), ...edges.map((key, index) => fill(2000 + index, key))];
    const requests = stubTables(rows, 0);
    const { openFills } = await fetchPaperResults();
    expect(openFills).toEqual(inKeyOrder(rows));
    expect(requests.filter(({ table }) => table === "paper_fills").map(({ filters }) => filters)).toEqual([
      ["lt.wf|0x4"],
      ["gte.wf|0x4", "lt.wf|0x8"],
      ["gte.wf|0x8", "lt.wf|0xc"],
      ["gte.wf|0xc"],
      [`gt.${inOneRange(999)}`, "lt.wf|0x8"],
    ]);
  });

  it("asks for the four ranges at once and keeps key order when they answer in reverse", async () => {
    const rows = fills(64);
    const held: (() => void)[] = [];
    stubSupabase(async (table, params) => {
      if (table !== "paper_fills") return { data: [] };
      await new Promise<void>((answer) => {
        held.push(answer);
        if (held.length === 4) for (const next of held.reverse()) next();
      });
      return { data: page(rows, "idempotency_key", params) };
    });
    const { openFills } = await fetchPaperResults();
    expect(openFills).toEqual(inKeyOrder(rows));
  });

  it.each(["paper_fills", "settled_markets"])("rejects when a later %s page fails", async (table) => {
    stubTables(fills(1001, inOneRange), 1001, table);
    await expect(fetchPaperResults()).rejects.toThrow("later page failed");
  });

  it.each([
    [0.5 - 0.4 - 0.1, "$0.00", "zero"], // −2.8e-17 in floating point: break-even, never "-$0.00"
    [-0.125, "-$0.13", "neg"], // a half cent rounds away from zero, as formatUsd does
    [-0.005, "-$0.01", "neg"],
    [1.005, "+$1.01", "pos"], // 1.00499… in binary; formatUsd still shows $1.01
  ])("shows a day of %s as %s in its cell and the month total", (pnl, text, sign) => {
    const html = renderToStaticMarkup(createElement(PnlCalendar, { days: [{ day: "2026-11-10", pnl }], today: "2026-11-13" }));
    expect(html.split(`sign-${sign}">${text}</span>`)).toHaveLength(3);
    expect(html.match(/sm:bg-(pos|neg)\/15/g) ?? []).toEqual(sign === "zero" ? [] : [`sm:bg-${sign}/15`]);
  });

  it("says so when nothing has settled", () => {
    const html = renderToStaticMarkup(createElement(PnlCalendar, { days: [], today: "2026-11-13" }));
    expect(html).toContain("No settled P&amp;L yet.");
  });
});
