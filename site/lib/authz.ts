// Server-only, request-scoped authorization authority for account/live data (#508 Phase C).
//
// Admin identity is compile-time and never DB-derived. Every viewer binding is re-read through the
// service role on the protected request, so clearing login_email takes effect immediately. React
// cache() deduplicates identical calls inside one request; it does not persist across requests.
import "server-only";

import { cache } from "react";

import {
  isAllowedEmail,
  normalizeLoginEmail,
  type LoginEmailLookup,
} from "./auth";
import { getServiceRoleSupabase } from "./supabase-server";

export type Access = { role: "admin" } | { role: "viewer"; accountId: string };

const lookupLoginEmail: LoginEmailLookup = async (normalizedEmail) => {
  const supabase = getServiceRoleSupabase();
  const { data, error } = await supabase
    .from("accounts")
    .select("account_id, login_email")
    .eq("login_email", normalizedEmail)
    .limit(2);
  return { rows: data ?? [], error };
};

/** Uncached implementation, exported for focused fail-closed unit tests. */
export async function resolveAccessUncached(
  email: string | null | undefined,
  lookup: LoginEmailLookup = lookupLoginEmail,
): Promise<Access | null> {
  if (isAllowedEmail(email)) return { role: "admin" };
  const normalized = normalizeLoginEmail(email);
  if (!normalized) return null;

  try {
    const { rows, error } = await lookup(normalized);
    if (error) return null;
    const matches = rows.filter((row) => row.login_email === normalized);
    if (matches.length !== 1) return null;
    return { role: "viewer", accountId: matches[0].account_id };
  } catch {
    return null;
  }
}

/** Factory exposes the request-cache boundary for a deterministic injected-lookup test. */
export function createCachedAccessResolver(lookup: LoginEmailLookup = lookupLoginEmail) {
  return cache((email: string | null | undefined) => resolveAccessUncached(email, lookup));
}

/** Sole authorization entry point for account/live Server Components and route handlers. */
export const resolveAccess = createCachedAccessResolver();
