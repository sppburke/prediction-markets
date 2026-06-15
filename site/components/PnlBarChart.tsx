"use client";

import { useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { formatUsd, shortWallet, toNum } from "@/lib/format";
import type { WalletLiveStats } from "@/lib/types";

/** Top wallets by absolute live realized P&L, coloured by sign. */
export function PnlBarChart({ rows, top = 15 }: { rows: WalletLiveStats[]; top?: number }) {
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);

  const data = rows
    .map((r) => ({ wallet: shortWallet(r.wallet), pnl: toNum(r.live_realized_pnl) ?? 0 }))
    .filter((d) => d.pnl !== 0)
    .sort((a, b) => Math.abs(b.pnl) - Math.abs(a.pnl))
    .slice(0, top);

  // Reserve height so the panel doesn't jump between server and client render.
  if (!mounted) return <div className="h-72" />;
  if (data.length === 0) {
    return <div className="flex h-72 items-center justify-center text-xs text-muted">No settled P&L yet.</div>;
  }

  return (
    <div className="h-72">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 8, right: 8, bottom: 8, left: 8 }}>
          <XAxis dataKey="wallet" tick={{ fontSize: 10, fill: "#8b97a7" }} interval={0} angle={-35} textAnchor="end" height={60} />
          <YAxis tick={{ fontSize: 10, fill: "#8b97a7" }} width={60} tickFormatter={(v: number) => formatUsd(v)} />
          <Tooltip
            cursor={{ fill: "#1f2a3733" }}
            contentStyle={{ background: "#121821", border: "1px solid #1f2a37", borderRadius: 8, fontSize: 12 }}
            labelStyle={{ color: "#e6edf3" }}
            formatter={(v: number) => [formatUsd(v, { sign: true }), "Realized P&L"]}
          />
          <Bar dataKey="pnl" radius={[2, 2, 0, 0]}>
            {data.map((d) => (
              <Cell key={d.wallet} fill={d.pnl >= 0 ? "#2ecc71" : "#e74c3c"} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
