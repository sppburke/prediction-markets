import {
  formatCents,
  formatDate,
  formatEdge,
  formatInt,
  formatPct,
  formatPrice,
  formatTstat,
  formatUsd,
  signClass,
} from "@/lib/format";
import type { WalletLiveStats } from "@/lib/types";

function Row({
  label,
  hist,
  live,
  liveTone,
}: {
  label: string;
  hist: string;
  live: string;
  liveTone?: "signed-from-live";
}) {
  return (
    <div className="grid grid-cols-3 items-center gap-2 border-t border-border py-2 text-sm tabular-nums">
      <div className="text-xs text-muted">{label}</div>
      <div className="text-right text-accent">{hist}</div>
      <div className={`text-right ${liveTone ? "" : "text-text"}`}>{live}</div>
    </div>
  );
}

/**
 * Side-by-side historical (ranker) vs watched (paper) comparison — the PR3 headline
 * the legacy portfolio-only SSR dashboard never showed. Edge units differ across
 * regimes (historical ls_edge is dimensionless; paper edge is ¢/settled-trade), so
 * they are labelled, not naively diffed.
 */
export function HistVsLivePanel({ row }: { row: WalletLiveStats }) {
  return (
    <div className="rounded-lg border border-border bg-panel p-4">
      <div className="grid grid-cols-3 gap-2 pb-1 text-[11px] font-semibold uppercase tracking-wider">
        <div className="text-muted">Metric</div>
        <div className="text-right text-accent">Historical</div>
        <div className="text-right text-text">Watched</div>
      </div>
      <Row label="Win rate" hist={formatPct(row.hit_rate)} live={formatPct(row.live_win_rate)} />
      <Row label="Edge (hist · paper ¢/trade)" hist={formatEdge(row.ls_edge)} live={formatCents(row.live_edge, { sign: true })} />
      <Row label="Repriced (hist) / fills (live)" hist={formatInt(row.n_trades)} live={formatInt(row.live_total_fills)} />
      <Row label="Settled" hist="—" live={formatInt(row.live_settled_count)} />
      <Row label="Open" hist="—" live={formatInt(row.live_open_fills)} />
      <Row label="Wins" hist="—" live={formatInt(row.live_wins)} />
      <Row label="t-stat / rank" hist={formatTstat(row.ls_tstat)} live={`#${formatInt(row.rank)}`} />
      <Row label="Avg entry price" hist={formatPrice(row.avg_price)} live="—" />
      <Row label="Repricing coverage" hist={formatPct(row.fill_rate)} live="—" />
      <Row label="Last trade (UTC)" hist={formatDate(row.last_trade_unix)} live="—" />
      <div className="grid grid-cols-3 items-center gap-2 border-t border-border pt-3 text-sm tabular-nums">
        <div className="text-xs font-semibold text-muted">Realized P&amp;L</div>
        <div className="text-right text-muted">—</div>
        <div className={`text-right text-base font-semibold sign-${signClass(row.live_realized_pnl)}`}>
          {formatUsd(row.live_realized_pnl, { sign: true })}
        </div>
      </div>
    </div>
  );
}
