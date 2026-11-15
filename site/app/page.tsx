import { KpiCards } from "@/components/KpiCards";
import { Panel, StateNotice } from "@/components/Panel";
import { PnlBarChart } from "@/components/PnlBarChart";
import { PnlCalendar } from "@/components/PnlCalendar";
import { WalletTabs } from "@/components/WalletTabs";
import { centralDay } from "@/lib/daily-pnl";
import {
  fetchPaperResults,
  fetchWalletStats,
  fetchWatchedSet,
  NotConfiguredError,
} from "@/lib/data";

// Rendered on every request, so one reload shows the latest paper results. The wallet stats come
// from the `wallet_live_stats_mv` materialized view, which pg_cron refreshes every 20 seconds.
export const dynamic = "force-dynamic";

export default async function OverviewPage() {
  // The three reads are independent: start them together, then await each in display order.
  const statsRead = fetchWalletStats();
  const watchedRead = fetchWatchedSet();
  // A failed read shows its message; it must never look like a calendar without P&L.
  const paperRead = fetchPaperResults().catch((e: unknown) => new Error((e as Error).message));

  let rows;
  try {
    rows = await statsRead;
  } catch (e: unknown) {
    const message =
      e instanceof NotConfiguredError
        ? "Supabase not configured — copy site/.env.example to site/.env.local."
        : `Failed to load: ${(e as Error).message}`;
    return <StateNotice kind="error" message={message} />;
  }
  if (rows.length === 0) {
    return <StateNotice kind="empty" message="No wallets in wallet_live_stats yet." />;
  }

  // The count and set come from the same token-guarded projection snapshot. Unavailable is not
  // represented as an empty set: the UI keeps that state visibly distinct from valid empty.
  const watchedProjection = await watchedRead;
  const watched = watchedProjection.status === "available" ? watchedProjection.count : null;
  const watchedSet =
    watchedProjection.status === "available" ? [...watchedProjection.wallets] : null;
  const paper = await paperRead;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-lg font-semibold">Portfolio overview</h1>
        <p className="text-xs text-muted">
          Per-wallet historical (ranker) vs watched (paper) stats. Click a wallet for detail.
        </p>
      </div>
      <KpiCards rows={rows} watched={watched} />
      <Panel title="Daily realized P&L · Central Time">
        {paper instanceof Error ? (
          <p className="text-xs text-neg">Daily P&L unavailable: {paper.message}</p>
        ) : (
          <PnlCalendar days={paper.daily} today={centralDay(Date.now() / 1000)} />
        )}
      </Panel>
      <Panel title="Watched paper realized P&L by wallet">
        <PnlBarChart rows={rows} />
      </Panel>
      <Panel title="Wallets — historical vs watched">
        <WalletTabs
          rows={rows}
          watchedSet={watchedSet}
          openFills={paper instanceof Error ? [] : paper.openFills}
        />
      </Panel>
    </div>
  );
}
