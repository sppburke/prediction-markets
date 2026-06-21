import Link from "next/link";
import { FillsTable } from "@/components/FillsTable";
import { HistVsLivePanel } from "@/components/HistVsLivePanel";
import { Panel, StateNotice } from "@/components/Panel";
import { WinRateCompareChart } from "@/components/WinRateCompareChart";
import { fetchFills, fetchWalletStat, NotConfiguredError } from "@/lib/data";
import type { PaperFill, WalletLiveStats } from "@/lib/types";

// Server-rendered with ISR (see app/page.tsx): the wallet_live_stats_mv read + fills read run
// on the server and are cached for up to `revalidate` seconds per wallet.
export const revalidate = 60;

export default async function WalletDetailPage({
  params,
}: {
  params: Promise<{ wallet: string }>;
}) {
  const { wallet: raw } = await params;
  const wallet = (raw ?? "").toLowerCase();

  let row: WalletLiveStats | null = null;
  let fills: PaperFill[] = [];
  let error: string | null = null;
  try {
    [row, fills] = await Promise.all([fetchWalletStat(wallet), fetchFills(wallet)]);
  } catch (e: unknown) {
    error =
      e instanceof NotConfiguredError
        ? "Supabase not configured — copy site/.env.example to site/.env.local."
        : `Failed to load: ${(e as Error).message}`;
  }

  return (
    <div className="space-y-6">
      <div>
        <Link href="/" className="text-xs text-accent hover:underline">
          ← all wallets
        </Link>
        <h1 className="mt-1 break-all text-lg font-semibold tabular-nums">{wallet}</h1>
        <p className="text-xs text-muted">Historical (ranker) vs live (paper) detail.</p>
      </div>

      {error ? (
        <StateNotice kind="error" message={error} />
      ) : row === null ? (
        <StateNotice kind="empty" message="No stats for this wallet in wallet_live_stats." />
      ) : (
        <>
          <div className="grid gap-6 lg:grid-cols-2">
            <HistVsLivePanel row={row} />
            <Panel title="Win rate — historical vs live">
              <WinRateCompareChart row={row} />
            </Panel>
          </div>
          <Panel title={`Recent paper fills (${fills.length})`}>
            <FillsTable fills={fills} />
          </Panel>
        </>
      )}
    </div>
  );
}
