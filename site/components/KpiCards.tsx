import { formatInt, formatPct, formatUsd, toNum } from "@/lib/format";
import type { WalletLiveStats } from "@/lib/types";
import { Panel } from "./Panel";
import { Stat } from "./Stat";

/** Portfolio-level KPIs aggregated from the per-wallet view rows. */
export function KpiCards({ rows }: { rows: WalletLiveStats[] }) {
  let realized = 0;
  let openFills = 0;
  let settled = 0;
  let wins = 0;
  let tradedWallets = 0;

  for (const r of rows) {
    realized += toNum(r.live_realized_pnl) ?? 0;
    openFills += toNum(r.live_open_fills) ?? 0;
    const s = toNum(r.live_settled_count) ?? 0;
    settled += s;
    wins += toNum(r.live_wins) ?? 0;
    if ((toNum(r.live_total_fills) ?? 0) > 0) tradedWallets += 1;
  }
  const winRate = settled > 0 ? wins / settled : null;

  return (
    <Panel title="Live paper portfolio">
      <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-6">
        <Stat label="Wallets" value={formatInt(rows.length)} sub={`${formatInt(tradedWallets)} live`} />
        <Stat label="Realized P&L" value={formatUsd(realized, { sign: true })} tone="signed" signOf={realized} />
        <Stat label="Settled fills" value={formatInt(settled)} />
        <Stat label="Open fills" value={formatInt(openFills)} />
        <Stat label="Wins" value={formatInt(wins)} />
        <Stat label="Win rate" value={formatPct(winRate)} />
      </div>
    </Panel>
  );
}
