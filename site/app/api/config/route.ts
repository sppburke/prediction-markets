// Admin write endpoint (#398 WS3, step 16, Decision #7): PATCH a single `service_config` row.
// Resolves current admin access on every request, then writes with the service-role key
// (server-side only), stamping `updated_by` + `updated_at`. Any other session → 403.
import { NextResponse } from "next/server";

import { auth } from "@/auth";
import { ALLOWED_EMAIL } from "@/lib/auth";
import { resolveAccess } from "@/lib/authz";
import { getServiceRoleSupabase } from "@/lib/supabase-server";

export async function PATCH(req: Request) {
  const session = await auth();
  const access = await resolveAccess(session?.user?.email);
  if (access?.role !== "admin") {
    return NextResponse.json({ error: "forbidden" }, { status: 403 });
  }

  let parsed: unknown;
  try {
    parsed = await req.json();
  } catch {
    return NextResponse.json({ error: "invalid JSON" }, { status: 400 });
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return NextResponse.json({ error: "JSON object required" }, { status: 400 });
  }
  const body = parsed as { key?: unknown; value?: unknown };
  const { key, value } = body;
  if (typeof key !== "string" || typeof value !== "string") {
    return NextResponse.json({ error: "string `key` and `value` required" }, { status: 400 });
  }

  // UPDATE (not upsert): admins edit existing seeded knobs only; an unknown key affects 0 rows.
  const supabase = getServiceRoleSupabase();
  const { data, error } = await supabase
    .from("service_config")
    .update({
      value,
      updated_by: ALLOWED_EMAIL,
      updated_at: new Date().toISOString(),
    })
    .eq("key", key)
    .select("key");

  if (error) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
  if (!data || data.length === 0) {
    return NextResponse.json({ error: `unknown config key: ${key}` }, { status: 404 });
  }
  return NextResponse.json({ ok: true });
}
