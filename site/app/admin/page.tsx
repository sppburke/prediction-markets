// Admin panel (#398 WS3, step 17): edit the Supabase-authoritative `service_config` knobs.
// Server Component — re-resolves admin access on the request, reads rows via the anon client (RLS
// allows anon SELECT), and hands them to the client table for editing.
import Link from "next/link";
import { notFound } from "next/navigation";

import { auth } from "@/auth";
import { resolveAccess } from "@/lib/authz";
import { getSupabase } from "@/lib/supabase";
import type { ServiceConfigRow } from "@/lib/types";
import { AdminConfigTable } from "@/components/AdminConfigTable";
import { Panel, StateNotice } from "@/components/Panel";

// Session-gated; never prerender as static.
export const dynamic = "force-dynamic";

export default async function AdminPage() {
  const session = await auth();
  const access = await resolveAccess(session?.user?.email);
  if (access?.role !== "admin") notFound();

  const supabase = getSupabase();
  if (!supabase) {
    return (
      <Panel title="Admin — service config">
        <StateNotice kind="error" message="Supabase is not configured." />
      </Panel>
    );
  }

  const { data, error } = await supabase
    .from("service_config")
    .select("key, value, value_type, description, updated_by, updated_at")
    .order("key");

  if (error) {
    return (
      <Panel title="Admin — service config">
        <StateNotice kind="error" message={`Failed to load config: ${error.message}`} />
      </Panel>
    );
  }
  const rows = (data ?? []) as ServiceConfigRow[];

  return (
    <Panel title="Admin — service config">
      <p className="mb-3 text-sm text-muted">
        Supabase-authoritative runtime knobs. The live trader polls edits within ≤30 s (no
        restart) and applies them after validation. Signed in as{" "}
        <span className="text-text">{session?.user?.email}</span>.
      </p>
      <p className="mb-4 text-sm">
        <Link href="/admin/accounts" className="text-accent hover:underline">
          Manage live accounts →
        </Link>
      </p>
      <AdminConfigTable rows={rows} />
    </Panel>
  );
}
