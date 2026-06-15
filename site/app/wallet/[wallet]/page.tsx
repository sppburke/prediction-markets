"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useEffect, useState } from "react";
import { FillsTable } from "@/components/FillsTable";
import { HistVsLivePanel } from "@/components/HistVsLivePanel";
import { Panel, StateNotice } from "@/components/Panel";
import { WinRateCompareChart } from "@/components/WinRateCompareChart";
import { fetchFills, fetchWalletStat, NotConfiguredError } from "@/lib/data";
import type { PaperFill, WalletLiveStats } from "@/lib/types";

export default function WalletDetailPage() {
  const params = useParams<{ wallet: string }>();
  const wallet = (params.wallet ?? "").toLowerCase();

  const [row, setRow] = useState<WalletLiveStats | null | "missing">(null);
  const [fills, setFills] = useState<PaperFill[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!wallet) return;
    let active = true;
    Promise.all([fetchWalletStat(wallet), fetchFills(wallet)])
      .then(([stat, f]) => {
        if (!active) return;
        setRow(stat ?? "missing");
        setFills(f);
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
  }, [wallet]);

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
        <StateNotice kind="loading" message="Loading wallet…" />
      ) : row === "missing" ? (
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
