"use client";

import { useEffect, useState } from "react";
import { KpiCards } from "@/components/KpiCards";
import { Panel, StateNotice } from "@/components/Panel";
import { PnlBarChart } from "@/components/PnlBarChart";
import { WalletTable } from "@/components/WalletTable";
import { fetchWalletStats, NotConfiguredError } from "@/lib/data";
import type { WalletLiveStats } from "@/lib/types";

export default function OverviewPage() {
  const [rows, setRows] = useState<WalletLiveStats[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    fetchWalletStats()
      .then((data) => {
        if (active) setRows(data);
      })
      .catch((e: unknown) => {
        if (!active) return;
        setError(
          e instanceof NotConfiguredError
            ? "Supabase not configured — copy site/.env.example to site/.env.local."
            : `Failed to load: ${(e as Error).message}`,
        );
      });
    return () => {
      active = false;
    };
  }, []);

  if (error) return <StateNotice kind="error" message={error} />;
  if (rows === null) return <StateNotice kind="loading" message="Loading wallets…" />;
  if (rows.length === 0) return <StateNotice kind="empty" message="No wallets in wallet_live_stats yet." />;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-lg font-semibold">Portfolio overview</h1>
        <p className="text-xs text-muted">
          Per-wallet historical (ranker) vs live (paper) stats. Click a wallet for detail.
        </p>
      </div>
      <KpiCards rows={rows} />
      <Panel title="Live realized P&L by wallet">
        <PnlBarChart rows={rows} />
      </Panel>
      <Panel title="Wallets — historical vs live">
        <WalletTable rows={rows} />
      </Panel>
    </div>
  );
}
