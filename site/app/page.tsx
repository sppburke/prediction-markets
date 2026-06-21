import { KpiCards } from "@/components/KpiCards";
import { Panel, StateNotice } from "@/components/Panel";
import { PnlBarChart } from "@/components/PnlBarChart";
import { WalletTabs } from "@/components/WalletTabs";
import {
  fetchOpenFills,
  fetchServiceRuntime,
  fetchWalletStats,
  fetchWatchedSet,
  NotConfiguredError,
} from "@/lib/data";
import { toNum } from "@/lib/format";

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

  // Secondary: the live watched-count. A failure here must not blank the page, so it falls
  // back to "—".
  let watched: number | null = null;
  try {
    const rt = await fetchServiceRuntime();
    watched = rt ? toNum(rt.watchlist_size) : null;
  } catch {
    watched = null;
  }

  // Secondary (soft-fail): the watched set drives the Live tab; open fills drive unrealized P&L.
  const watchedSet = await fetchWatchedSet().catch(() => new Set<string>());
  const openFills = await fetchOpenFills().catch(() => []);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-lg font-semibold">Portfolio overview</h1>
        <p className="text-xs text-muted">
          Per-wallet historical (ranker) vs live (paper) stats. Click a wallet for detail.
        </p>
      </div>
      <KpiCards rows={rows} watched={watched} />
      <Panel title="Live realized P&L by wallet">
        <PnlBarChart rows={rows} />
      </Panel>
      <Panel title="Wallets — historical vs live">
        <WalletTabs rows={rows} watchedSet={[...watchedSet]} openFills={openFills} />
      </Panel>
    </div>
  );
}
