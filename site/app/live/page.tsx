// Account-scoped live execution view (#508 Phase C). Authorization and all reads are resolved on
// this request through the service-role boundary; no credential columns are queried or rendered.
import Link from "next/link";
import { notFound } from "next/navigation";

import { auth } from "@/auth";
import { Panel, StateNotice } from "@/components/Panel";
import { Stat } from "@/components/Stat";
import { resolveAccess } from "@/lib/authz";
import { formatPrice, formatQty, formatUsd } from "@/lib/format";
import { getServiceRoleSupabase } from "@/lib/supabase-server";
import type { LiveAccountState, LiveFill, LivePosition } from "@/lib/types";

export const dynamic = "force-dynamic";

interface LiveAccount {
  account_id: string;
  is_primary: boolean;
  enabled: boolean;
  requested_live_mode: "off" | "live_tiny";
  effective_live_mode: "off" | "live_tiny";
}

function timestamp(value: number | string | null): string {
  if (value === null) return "—";
  const millis = typeof value === "number" ? value * 1000 : Date.parse(value);
  if (!Number.isFinite(millis)) return "—";
  return new Date(millis).toISOString().slice(0, 16).replace("T", " ");
}

function LiveFillsTable({ fills }: { fills: LiveFill[] }) {
  return (
    <div className="overflow-x-auto rounded-lg border border-border">
      <table className="w-full min-w-[760px] border-collapse text-xs">
        <thead>
          <tr className="bg-panelAlt text-left text-muted">
            <th className="px-3 py-2 font-medium">Entered</th>
            <th className="px-3 py-2 font-medium">Leader</th>
            <th className="px-3 py-2 font-medium">Market</th>
            <th className="px-3 py-2 text-right font-medium">Outcome</th>
            <th className="px-3 py-2 text-right font-medium">Side</th>
            <th className="px-3 py-2 text-right font-medium">Contracts</th>
            <th className="px-3 py-2 text-right font-medium">Fill price</th>
          </tr>
        </thead>
        <tbody>
          {fills.map((fill) => (
            <tr
              key={fill.idempotency_key}
              className="border-t border-border tabular-nums hover:bg-panelAlt"
            >
              <td className="whitespace-nowrap px-3 py-2 text-muted">
                {timestamp(fill.entry_unix ?? fill.inserted_at)}
              </td>
              <td className="px-3 py-2 font-mono text-[11px]">{fill.leader_wallet}</td>
              <td className="px-3 py-2 font-mono text-[11px]">{fill.market_id}</td>
              <td className="px-3 py-2 text-right">{fill.outcome_id}</td>
              <td
                className={`px-3 py-2 text-right ${fill.side === "buy" ? "text-pos" : "text-neg"}`}
              >
                {fill.side}
              </td>
              <td className="px-3 py-2 text-right">{formatQty(fill.contracts)}</td>
              <td className="px-3 py-2 text-right">{formatPrice(fill.fill_price)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function LivePositionsTable({ positions }: { positions: LivePosition[] }) {
  return (
    <div className="overflow-x-auto rounded-lg border border-border">
      <table className="w-full min-w-[640px] border-collapse text-xs">
        <thead>
          <tr className="bg-panelAlt text-left text-muted">
            <th className="px-3 py-2 font-medium">Market</th>
            <th className="px-3 py-2 text-right font-medium">Outcome</th>
            <th className="px-3 py-2 text-right font-medium">Long</th>
            <th className="px-3 py-2 text-right font-medium">Short</th>
            <th className="px-3 py-2 text-right font-medium">Cost basis</th>
          </tr>
        </thead>
        <tbody>
          {positions.map((position) => (
            <tr
              key={`${position.market_id}:${position.outcome_id}`}
              className="border-t border-border tabular-nums"
            >
              <td className="px-3 py-2 font-mono text-[11px]">{position.market_id}</td>
              <td className="px-3 py-2 text-right">{position.outcome_id}</td>
              <td className="px-3 py-2 text-right">{formatQty(position.long_contracts)}</td>
              <td className="px-3 py-2 text-right">{formatQty(position.short_contracts)}</td>
              <td className="px-3 py-2 text-right">{formatUsd(position.cost_basis)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default async function LivePage({
  searchParams,
}: {
  searchParams: Promise<{ account?: string | string[] }>;
}) {
  const session = await auth();
  const access = await resolveAccess(session?.user?.email);
  if (!access) notFound();

  const supabase = getServiceRoleSupabase();
  let accountQuery = supabase
    .from("accounts")
    .select("account_id, is_primary, enabled, requested_live_mode, effective_live_mode")
    .order("is_primary", { ascending: false })
    .order("execution_order")
    .order("account_id");
  if (access.role === "viewer") accountQuery = accountQuery.eq("account_id", access.accountId);

  const { data: accountData, error: accountError } = await accountQuery;
  if (accountError) {
    return <StateNotice kind="error" message={`Failed to load live accounts: ${accountError.message}`} />;
  }
  const accounts = (accountData ?? []) as LiveAccount[];
  if (accounts.length === 0) {
    if (access.role === "viewer") notFound();
    return <StateNotice kind="empty" message="No live accounts have been created." />;
  }

  const rawRequested = (await searchParams).account;
  const requested = typeof rawRequested === "string" ? rawRequested : null;
  const selected =
    access.role === "viewer"
      ? accounts[0]
      : requested
        ? accounts.find((account) => account.account_id === requested)
        : accounts[0];
  if (!selected) notFound();

  const [fillsResult, positionsResult, stateResult] = await Promise.all([
    supabase
      .from("live_fills")
      .select(
        "account_id, idempotency_key, leader_wallet, source_trade_id, market_id, outcome_id, side, contracts, fill_price, entry_unix, event_seq, inserted_at",
      )
      .eq("account_id", selected.account_id)
      .order("inserted_at", { ascending: false })
      .limit(100),
    supabase
      .from("live_positions")
      .select("account_id, market_id, outcome_id, long_contracts, short_contracts, cost_basis")
      .eq("account_id", selected.account_id)
      .order("market_id")
      .order("outcome_id"),
    supabase
      .from("live_account_state")
      .select(
        "account_id, free_collateral, reserved, unredeemed_value, last_reconciled_at, admission_closed_reason",
      )
      .eq("account_id", selected.account_id)
      .maybeSingle(),
  ]);
  const readError = fillsResult.error ?? positionsResult.error ?? stateResult.error;
  if (readError) {
    return <StateNotice kind="error" message={`Failed to load live activity: ${readError.message}`} />;
  }

  const fills = (fillsResult.data ?? []) as LiveFill[];
  const positions = (positionsResult.data ?? []) as LivePosition[];
  const state = (stateResult.data as LiveAccountState | null) ?? null;
  const empty = fills.length === 0 && positions.length === 0 && state === null;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-lg font-semibold">Live account · {selected.account_id}</h1>
        <p className="text-xs text-muted">
          Effective {selected.effective_live_mode} · requested {selected.requested_live_mode} ·{" "}
          {selected.enabled ? "execution enabled" : "execution disabled"}
        </p>
      </div>

      {access.role === "admin" ? (
        <Panel title="Account selector">
          <div className="flex flex-wrap gap-2 text-xs">
            {accounts.map((account) => (
              <Link
                key={account.account_id}
                href={`/live?account=${encodeURIComponent(account.account_id)}`}
                className={`rounded border px-3 py-1 ${
                  account.account_id === selected.account_id
                    ? "border-accent text-text"
                    : "border-border text-muted hover:text-text"
                }`}
              >
                {account.account_id}
                {account.is_primary ? " · primary" : ""}
              </Link>
            ))}
          </div>
        </Panel>
      ) : null}

      {empty ? (
        <StateNotice kind="empty" message="No live activity yet." />
      ) : (
        <>
          <Panel title="Account state">
            {state ? (
              <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
                <Stat label="Free collateral" value={formatUsd(state.free_collateral)} />
                <Stat label="Reserved" value={formatUsd(state.reserved)} />
                <Stat label="Unredeemed" value={formatUsd(state.unredeemed_value)} />
                <Stat
                  label="Last reconciled"
                  value={timestamp(state.last_reconciled_at)}
                  sub={state.admission_closed_reason ?? "admission open"}
                />
              </div>
            ) : (
              <p className="text-xs text-muted">Live account state has not been initialized.</p>
            )}
          </Panel>
          <Panel title={`Open live positions (${positions.length})`}>
            {positions.length > 0 ? (
              <LivePositionsTable positions={positions} />
            ) : (
              <p className="text-xs text-muted">No open live positions.</p>
            )}
          </Panel>
          <Panel title={`Recent live fills (${fills.length})`}>
            {fills.length > 0 ? (
              <LiveFillsTable fills={fills} />
            ) : (
              <p className="text-xs text-muted">No live fills.</p>
            )}
          </Panel>
        </>
      )}
    </div>
  );
}
