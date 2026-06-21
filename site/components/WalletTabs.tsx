"use client";

// Live / Historical / Bench wallet tabs (#398 WS3, step 20) + the portfolio Unrealized-P&L KPI
// (step 19). On mount this fetches live marks for every open fill from /api/marks and computes
// per-wallet + portfolio unrealized P&L = Σ side_sign × (mark − fill_price) × contracts.
//
// Tab membership (predicates may overlap by design):
//   Live       = watched by pe-service OR has open fills (+ an Unrealized column)
//   Historical = has any settled fill
//   Bench      = not Live and never settled
// If the watchlist is empty (soft-fail), Live degrades to "has open fills" only.
import { useEffect, useMemo, useState } from "react";

import { formatUsd, toNum } from "@/lib/format";
import type { PaperFill, WalletLiveStats } from "@/lib/types";
import { Stat } from "./Stat";
import { WalletTable } from "./WalletTable";

type TabKey = "live" | "historical" | "bench";

export function WalletTabs({
  rows,
  watchedSet,
  openFills,
}: {
  rows: WalletLiveStats[];
  watchedSet: string[];
  openFills: PaperFill[];
}) {
  const [tab, setTab] = useState<TabKey>("live");
  const [unrealized, setUnrealized] = useState<Map<string, number>>(new Map());

  const watched = useMemo(() => new Set(watchedSet.map((w) => w.toLowerCase())), [watchedSet]);

  useEffect(() => {
    const keys = [...new Set(openFills.map((f) => `${f.market_id}:${f.outcome_id}`))];
    if (keys.length === 0) return;
    let cancelled = false;
    fetch(`/api/marks?keys=${encodeURIComponent(keys.join(","))}`)
      .then((r) => (r.ok ? r.json() : {}))
      .then((marks: Record<string, number>) => {
        if (cancelled) return;
        const byWallet = new Map<string, number>();
        for (const f of openFills) {
          const mark = marks[`${f.market_id}:${f.outcome_id}`];
          const fillPrice = toNum(f.fill_price);
          const contracts = toNum(f.contracts);
          if (mark === undefined || fillPrice === null || contracts === null) continue;
          const sideSign = f.side === "buy" ? 1 : -1;
          const u = sideSign * (mark - fillPrice) * contracts;
          const w = f.leader_wallet.toLowerCase();
          byWallet.set(w, (byWallet.get(w) ?? 0) + u);
        }
        setUnrealized(byWallet);
      })
      .catch(() => {
        /* best-effort: cells stay em-dash */
      });
    return () => {
      cancelled = true;
    };
  }, [openFills]);

  const { live, historical, bench } = useMemo(() => {
    const isLive = (r: WalletLiveStats) =>
      watched.has(r.wallet.toLowerCase()) || (toNum(r.live_open_fills) ?? 0) > 0;
    return {
      live: rows.filter(isLive),
      historical: rows.filter((r) => (toNum(r.live_settled_count) ?? 0) > 0),
      bench: rows.filter((r) => !isLive(r) && (toNum(r.live_settled_count) ?? 0) === 0),
    };
  }, [rows, watched]);

  const unrealizedTotal = useMemo(
    () => [...unrealized.values()].reduce((a, b) => a + b, 0),
    [unrealized],
  );

  const tabs: { key: TabKey; label: string; rows: WalletLiveStats[] }[] = [
    { key: "live", label: `Live (${live.length})`, rows: live },
    { key: "historical", label: `Historical (${historical.length})`, rows: historical },
    { key: "bench", label: `Bench (${bench.length})`, rows: bench },
  ];
  const active = tabs.find((t) => t.key === tab) ?? tabs[0];

  return (
    <div>
      <div className="mb-4 max-w-xs">
        <Stat
          label="Unrealized P&L"
          value={unrealized.size === 0 ? "—" : formatUsd(unrealizedTotal, { sign: true })}
          tone="signed"
          signOf={unrealized.size === 0 ? null : unrealizedTotal}
          sub="open positions · live marks"
        />
      </div>
      <div className="mb-3 flex gap-2 text-xs">
        {tabs.map((t) => (
          <button
            key={t.key}
            type="button"
            onClick={() => setTab(t.key)}
            className={`rounded border px-3 py-1 ${
              t.key === tab
                ? "border-accent text-text"
                : "border-border text-muted hover:text-text"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>
      <WalletTable rows={active.rows} unrealized={tab === "live" ? unrealized : undefined} />
    </div>
  );
}
