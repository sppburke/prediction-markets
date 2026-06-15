"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import {
  formatEdge,
  formatInt,
  formatPct,
  formatTstat,
  formatUsd,
  shortWallet,
  signClass,
  toNum,
  type Numeric,
} from "@/lib/format";
import type { WalletLiveStats } from "@/lib/types";

type Col = {
  key: string;
  label: string;
  group: "hist" | "live" | "id";
  /** Sort accessor (null sorts last). */
  sort: (r: WalletLiveStats) => number | null;
  /** Cell renderer. */
  cell: (r: WalletLiveStats) => React.ReactNode;
  align?: "left" | "right";
};

function num(v: Numeric): number | null {
  return toNum(v);
}

const COLUMNS: Col[] = [
  {
    key: "wallet",
    label: "Wallet",
    group: "id",
    align: "left",
    sort: () => null,
    cell: (r) => (
      <Link href={`/wallet/${r.wallet}`} className="text-accent hover:underline">
        {shortWallet(r.wallet)}
      </Link>
    ),
  },
  { key: "rank", label: "Rank", group: "hist", sort: (r) => num(r.rank), cell: (r) => formatInt(r.rank) },
  { key: "n_trades", label: "Hist trades", group: "hist", sort: (r) => num(r.n_trades), cell: (r) => formatInt(r.n_trades) },
  { key: "hit_rate", label: "Hist win%", group: "hist", sort: (r) => num(r.hit_rate), cell: (r) => formatPct(r.hit_rate) },
  { key: "ls_edge", label: "Hist edge", group: "hist", sort: (r) => num(r.ls_edge), cell: (r) => formatEdge(r.ls_edge) },
  { key: "ls_tstat", label: "Hist t", group: "hist", sort: (r) => num(r.ls_tstat), cell: (r) => formatTstat(r.ls_tstat) },
  { key: "live_total_fills", label: "Live fills", group: "live", sort: (r) => num(r.live_total_fills), cell: (r) => formatInt(r.live_total_fills) },
  { key: "live_settled_count", label: "Settled", group: "live", sort: (r) => num(r.live_settled_count), cell: (r) => formatInt(r.live_settled_count) },
  { key: "live_open_fills", label: "Open", group: "live", sort: (r) => num(r.live_open_fills), cell: (r) => formatInt(r.live_open_fills) },
  { key: "live_win_rate", label: "Live win%", group: "live", sort: (r) => num(r.live_win_rate), cell: (r) => formatPct(r.live_win_rate) },
  {
    key: "live_realized_pnl",
    label: "Live P&L",
    group: "live",
    sort: (r) => num(r.live_realized_pnl),
    cell: (r) => (
      <span className={`sign-${signClass(r.live_realized_pnl)}`}>
        {formatUsd(r.live_realized_pnl, { sign: true })}
      </span>
    ),
  },
  { key: "live_edge", label: "Live $/trade", group: "live", sort: (r) => num(r.live_edge), cell: (r) => formatUsd(r.live_edge, { sign: true }) },
];

const GROUP_TONE: Record<Col["group"], string> = {
  id: "text-muted",
  hist: "text-accent",
  live: "text-text",
};

export function WalletTable({ rows }: { rows: WalletLiveStats[] }) {
  const [sortKey, setSortKey] = useState<string>("live_realized_pnl");
  const [asc, setAsc] = useState(false);

  const sorted = useMemo(() => {
    const col = COLUMNS.find((c) => c.key === sortKey);
    if (!col) return rows;
    const dir = asc ? 1 : -1;
    return [...rows].sort((a, b) => {
      const av = col.sort(a);
      const bv = col.sort(b);
      if (av === null && bv === null) return 0;
      if (av === null) return 1; // nulls last regardless of direction
      if (bv === null) return -1;
      return (av - bv) * dir;
    });
  }, [rows, sortKey, asc]);

  function onSort(key: string) {
    if (key === "wallet") return;
    if (key === sortKey) {
      setAsc((v) => !v);
    } else {
      setSortKey(key);
      setAsc(false);
    }
  }

  return (
    <div className="overflow-x-auto rounded-lg border border-border">
      <table className="w-full min-w-[820px] border-collapse text-xs">
        <thead>
          <tr className="bg-panelAlt text-left text-muted">
            {COLUMNS.map((c) => (
              <th
                key={c.key}
                onClick={() => onSort(c.key)}
                className={`whitespace-nowrap px-3 py-2 font-medium ${
                  c.align === "left" ? "text-left" : "text-right"
                } ${c.key === "wallet" ? "" : "cursor-pointer select-none hover:text-text"}`}
                title={c.group === "hist" ? "Historical (ranker)" : c.group === "live" ? "Live (paper)" : ""}
              >
                <span className={GROUP_TONE[c.group]}>{c.label}</span>
                {sortKey === c.key ? <span className="ml-1">{asc ? "▲" : "▼"}</span> : null}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map((r) => (
            <tr key={r.wallet} className="border-t border-border tabular-nums hover:bg-panelAlt">
              {COLUMNS.map((c) => (
                <td
                  key={c.key}
                  className={`whitespace-nowrap px-3 py-2 ${
                    c.align === "left" ? "text-left" : "text-right"
                  }`}
                >
                  {c.cell(r)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
