import { KpiCards } from "@/components/KpiCards";
import { Panel, StateNotice } from "@/components/Panel";
import { PnlBarChart } from "@/components/PnlBarChart";
import { PnlCalendar } from "@/components/PnlCalendar";
import { WalletTabs } from "@/components/WalletTabs";
import { centralDay } from "@/lib/daily-pnl";
import {
  fetchDailyPnl,
  fetchOpenFills,
  fetchWalletStats,
  fetchWatchedSet,
  NotConfiguredError,
} from "@/lib/data";

// Server-rendered with ISR: the wallet_live_stats_mv read runs on the server and the result is
// cached + shared across all viewers for up to `revalidate` seconds, instead of re-querying
// Supabase from every browser on every page view.
export const revalidate = 60;

export default async function OverviewPage() {
  let rows;
  try {
    rows = await fetchWalletStats();
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
  const watchedProjection = await fetchWatchedSet();
  const watched = watchedProjection.status === "available" ? watchedProjection.count : null;
  const watchedSet =
    watchedProjection.status === "available" ? [...watchedProjection.wallets] : null;
  const openFills = await fetchOpenFills().catch(() => []);
  // A failed read shows its message; it must never look like a calendar without P&L.
  const daily = await fetchDailyPnl().catch((e: unknown) => new Error((e as Error).message));

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
        {daily instanceof Error ? (
          <p className="text-xs text-neg">Daily P&L unavailable: {daily.message}</p>
        ) : (
          <PnlCalendar days={daily} today={centralDay(Date.now() / 1000)} />
        )}
      </Panel>
      <Panel title="Watched paper realized P&L by wallet">
        <PnlBarChart rows={rows} />
      </Panel>
      <Panel title="Wallets — historical vs watched">
        <WalletTabs rows={rows} watchedSet={watchedSet} openFills={openFills} />
      </Panel>
    </div>
  );
}
