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
import { toNum } from "@/lib/format";
import type { WalletLiveStats } from "@/lib/types";

/** Historical (ranker hit_rate) vs watched (paper win_rate) win-rate, in percent. */
export function WinRateCompareChart({ row }: { row: WalletLiveStats }) {
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);

  const hist = toNum(row.hit_rate);
  const live = toNum(row.live_win_rate);
  const data = [
    { label: "Historical", pct: hist === null ? null : hist * 100, fill: "#4ea1ff" },
    { label: "Watched", pct: live === null ? null : live * 100, fill: "#2ecc71" },
  ].filter((d) => d.pct !== null) as Array<{ label: string; pct: number; fill: string }>;

  if (!mounted) return <div className="h-56" />;
  if (data.length === 0) {
    return <div className="flex h-56 items-center justify-center text-xs text-muted">No win-rate data yet.</div>;
  }

  return (
    <div className="h-56">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 8, right: 8, bottom: 8, left: 8 }}>
          <XAxis dataKey="label" tick={{ fontSize: 11, fill: "#8b97a7" }} />
          <YAxis
            domain={[0, 100]}
            tick={{ fontSize: 10, fill: "#8b97a7" }}
            width={40}
            tickFormatter={(v: number) => `${v}%`}
          />
          <Tooltip
            cursor={{ fill: "#1f2a3733" }}
            contentStyle={{ background: "#121821", border: "1px solid #1f2a37", borderRadius: 8, fontSize: 12 }}
            labelStyle={{ color: "#e6edf3" }}
            formatter={(v: number) => [`${v.toFixed(1)}%`, "Win rate"]}
          />
          <Bar dataKey="pct" radius={[2, 2, 0, 0]}>
            {data.map((d) => (
              <Cell key={d.label} fill={d.fill} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
