// Admin-only account creation endpoint (#508 Phase C). The SECURITY DEFINER RPC owns the state
// transition and its audit event; this route never writes the accounts table directly.
import { NextResponse } from "next/server";

import { auth } from "@/auth";
import { ALLOWED_EMAIL } from "@/lib/auth";
import { resolveAccess } from "@/lib/authz";
import { getServiceRoleSupabase } from "@/lib/supabase-server";

export async function POST(req: Request) {
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
  const body = parsed as { account_id?: unknown; is_primary?: unknown };
  if (typeof body.account_id !== "string" || typeof body.is_primary !== "boolean") {
    return NextResponse.json(
      { error: "string `account_id` and boolean `is_primary` required" },
      { status: 400 },
    );
  }

  const { error } = await getServiceRoleSupabase().rpc("account_create", {
    p_account_id: body.account_id,
    p_is_primary: body.is_primary,
    p_actor: ALLOWED_EMAIL,
  });
  if (error) return NextResponse.json({ error: error.message }, { status: 500 });
  return NextResponse.json({ ok: true });
}
