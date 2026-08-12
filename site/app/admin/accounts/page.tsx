// Admin-only account inventory and controls (#508 Phase C). Reads explicit display-safe columns;
// credential sealed_bundle is never selected. All writes leave this page through typed RPC routes.
import Link from "next/link";
import { notFound } from "next/navigation";

import { auth } from "@/auth";
import { AccountsAdminPanel } from "@/components/AccountsAdminPanel";
import { StateNotice } from "@/components/Panel";
import { resolveAccess } from "@/lib/authz";
import { getServiceRoleSupabase } from "@/lib/supabase-server";
import type { AccountAdminRow, AccountCredentialMetadata } from "@/lib/types";

export const dynamic = "force-dynamic";

export default async function AccountsAdminPage() {
  const session = await auth();
  const access = await resolveAccess(session?.user?.email);
  if (access?.role !== "admin") notFound();

  const supabase = getServiceRoleSupabase();
  const [accountsResult, credentialsResult] = await Promise.all([
    supabase
      .from("accounts")
      .select(
        "account_id, is_primary, login_email, enabled, execution_order, requested_live_mode, effective_live_mode, live_sizing_mode, live_sizing_dollar_usd, live_sizing_contracts, live_price_impact_cap_bps, created_at, updated_at",
      )
      .order("is_primary", { ascending: false })
      .order("execution_order")
      .order("account_id"),
    supabase
      .from("account_credentials")
      .select("account_id, bundle_version, key_id, fingerprint, updated_at"),
  ]);

  const loadError = accountsResult.error ?? credentialsResult.error;
  if (loadError) {
    return <StateNotice kind="error" message={`Failed to load accounts: ${loadError.message}`} />;
  }

  const credentials = new Map<string, AccountCredentialMetadata>(
    (credentialsResult.data ?? []).map((row) => [
      String(row.account_id),
      {
        bundle_version: Number(row.bundle_version),
        key_id: String(row.key_id),
        fingerprint: String(row.fingerprint),
        updated_at: String(row.updated_at),
      },
    ]),
  );
  const rows = (accountsResult.data ?? []).map((row) => ({
    ...row,
    credentials: credentials.get(String(row.account_id)) ?? null,
  })) as AccountAdminRow[];

  return (
    <div className="space-y-4">
      <div>
        <Link href="/admin" className="text-xs text-accent hover:underline">
          ← service config
        </Link>
        <h1 className="mt-1 text-lg font-semibold">Accounts admin</h1>
        <p className="text-xs text-muted">
          Live account controls and write-only sealed credential rotation. Signed in as{" "}
          <span className="text-text">{session?.user?.email}</span>.
        </p>
      </div>
      <AccountsAdminPanel rows={rows} />
    </div>
  );
}
