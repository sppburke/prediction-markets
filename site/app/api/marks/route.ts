// Current-mark proxy for watched-paper unrealized PnL (#398 WS3, #508 terminology). Gamma fetch with an
// in-process 30s TTL cache (no Supabase writes) keyed "market:outcome". Returns { key: mark } for
// the requested keys; a key whose market can't be fetched is simply omitted (the cell shows "—").
import { NextResponse } from "next/server";

const GAMMA_BASE = process.env.GAMMA_BASE_URL ?? "https://gamma-api.polymarket.com";
const TTL_MS = 30_000;
const MARKETS_PER_REQUEST = 50; // Gamma batch cap (mirrors the Rust GammaMarketsClient)

type CacheEntry = { mark: number; at: number };
const cache = new Map<string, CacheEntry>();

interface GammaRow {
  conditionId?: string;
  outcomePrices?: string; // JSON-encoded array of decimal strings, e.g. "[\"0.62\",\"0.38\"]"
}

async function refreshMarkets(markets: string[], now: number): Promise<void> {
  for (let i = 0; i < markets.length; i += MARKETS_PER_REQUEST) {
    const batch = markets.slice(i, i + MARKETS_PER_REQUEST);
    const qs = batch.map((m) => `condition_ids=${encodeURIComponent(m)}`).join("&");
    try {
      const resp = await fetch(`${GAMMA_BASE}/markets?${qs}&limit=500`, { cache: "no-store" });
      if (!resp.ok) continue;
      const rows = (await resp.json()) as GammaRow[];
      for (const row of rows) {
        if (!row.conditionId || !row.outcomePrices) continue;
        let prices: unknown;
        try {
          prices = JSON.parse(row.outcomePrices);
        } catch {
          continue;
        }
        if (!Array.isArray(prices)) continue;
        prices.forEach((p, idx) => {
          const mark = Number(p);
          if (Number.isFinite(mark)) {
            cache.set(`${row.conditionId}:${idx}`, { mark, at: now });
          }
        });
      }
    } catch {
      // best-effort: leave these markets unmarked this round.
    }
  }
}

export async function GET(req: Request) {
  const keys = (new URL(req.url).searchParams.get("keys") ?? "")
    .split(",")
    .map((k) => k.trim())
    .filter(Boolean);

  const now = Date.now();
  const stale = keys.filter((k) => {
    const c = cache.get(k);
    return !c || now - c.at >= TTL_MS;
  });
  const markets = [...new Set(stale.map((k) => k.split(":")[0]).filter(Boolean))];
  if (markets.length > 0) {
    await refreshMarkets(markets, now);
  }

  const result: Record<string, number> = {};
  for (const k of keys) {
    const c = cache.get(k);
    if (c) result[k] = c.mark;
  }
  return NextResponse.json(result);
}
