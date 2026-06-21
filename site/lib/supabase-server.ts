// Server-only Supabase client holding the SERVICE-ROLE key (bypasses RLS) for the admin write
// path (#398 WS3, step 16, Decision #7). NEVER import this from a client component — `server-only`
// makes that a build error, and the runtime `typeof window` guard is a second backstop (risk #8).
import "server-only";

import { type SupabaseClient, createClient } from "@supabase/supabase-js";

/** Build a service-role Supabase client. Throws if run in the browser or if env is missing. */
export function getServiceRoleSupabase(): SupabaseClient {
  if (typeof window !== "undefined") {
    throw new Error("supabase-server: the service-role client must never run in the browser");
  }
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) {
    throw new Error("supabase-server: NEXT_PUBLIC_SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set");
  }
  return createClient(url, key, { auth: { persistSession: false } });
}
