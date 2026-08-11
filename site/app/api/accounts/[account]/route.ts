// Admin-only account control endpoint (#508 Phase C). Each action maps one-to-one to a Phase-B
// SECURITY DEFINER RPC; account_set_effective_mode is intentionally absent (service-only).
import { NextResponse } from "next/server";

import { auth } from "@/auth";
import { ALLOWED_EMAIL } from "@/lib/auth";
import { resolveAccess } from "@/lib/authz";
import { getServiceRoleSupabase } from "@/lib/supabase-server";

type RpcCall = { name: string; args: Record<string, unknown> };

function controlRpc(accountId: string, body: Record<string, unknown>): RpcCall | null {
  switch (body.action) {
    case "login_email":
      if (body.login_email !== null && typeof body.login_email !== "string") return null;
      return {
        name: "account_set_login_email",
        args: {
          p_account_id: accountId,
          p_login_email: body.login_email,
          p_actor: ALLOWED_EMAIL,
        },
      };
    case "live_settings":
      if (
        typeof body.enabled !== "boolean" ||
        typeof body.execution_order !== "number" ||
        (body.live_sizing_mode !== null && typeof body.live_sizing_mode !== "string") ||
        (body.live_sizing_dollar_usd !== null &&
          typeof body.live_sizing_dollar_usd !== "string") ||
        (body.live_sizing_contracts !== null &&
          typeof body.live_sizing_contracts !== "string") ||
        typeof body.live_price_impact_cap_bps !== "number"
      ) {
        return null;
      }
      return {
        name: "account_update_live_settings",
        args: {
          p_account_id: accountId,
          p_enabled: body.enabled,
          p_execution_order: body.execution_order,
          p_live_sizing_mode: body.live_sizing_mode,
          p_live_sizing_dollar_usd: body.live_sizing_dollar_usd,
          p_live_sizing_contracts: body.live_sizing_contracts,
          p_live_price_impact_cap_bps: body.live_price_impact_cap_bps,
          p_actor: ALLOWED_EMAIL,
        },
      };
    case "request_mode":
      if (body.requested_mode !== "off" && body.requested_mode !== "live_tiny") return null;
      return {
        name: "account_request_mode",
        args: {
          p_account_id: accountId,
          p_requested_mode: body.requested_mode,
          p_actor: ALLOWED_EMAIL,
        },
      };
    case "promotion_review":
      if (typeof body.reason !== "string" || typeof body.evidence_ref !== "string") return null;
      return {
        name: "account_record_promotion_review",
        args: {
          p_account_id: accountId,
          p_actor: ALLOWED_EMAIL,
          p_reason: body.reason,
          p_evidence_ref: body.evidence_ref,
        },
      };
    case "revoke_promotion_review":
      if (typeof body.reason !== "string") return null;
      return {
        name: "account_revoke_promotion_review",
        args: {
          p_account_id: accountId,
          p_actor: ALLOWED_EMAIL,
          p_reason: body.reason,
        },
      };
    default:
      return null;
  }
}

export async function PATCH(
  req: Request,
  { params }: { params: Promise<{ account: string }> },
) {
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
  const body = parsed as Record<string, unknown>;
  const { account } = await params;
  const call = controlRpc(account, body);
  if (!call) return NextResponse.json({ error: "invalid account action" }, { status: 400 });

  const { error } = await getServiceRoleSupabase().rpc(call.name, call.args);
  if (error) return NextResponse.json({ error: error.message }, { status: 500 });
  return NextResponse.json({ ok: true });
}
