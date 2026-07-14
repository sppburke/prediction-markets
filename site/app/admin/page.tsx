// Admin panel (#398 WS3, step 17): edit the Supabase-authoritative `service_config` knobs.
// Server Component — re-checks the session (defence in depth beyond middleware), reads the rows
// via the anon client (RLS allows anon SELECT), and hands them to the client table for editing.
import { redirect } from "next/navigation";

import { auth } from "@/auth";
import { isAllowedEmail } from "@/lib/auth";
import { getSupabase } from "@/lib/supabase";
import type { ServiceConfigRow } from "@/lib/types";
import { AdminConfigTable } from "@/components/AdminConfigTable";
import { Panel, StateNotice } from "@/components/Panel";

// Session-gated; never prerender as static.
export const dynamic = "force-dynamic";

export default async function AdminPage() {
  const session = await auth();
  if (!isAllowedEmail(session?.user?.email)) {
    redirect("/api/auth/signin");
  }

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
      <AdminConfigTable rows={rows} />
    </Panel>
  );
}
