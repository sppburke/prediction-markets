import { createClient, type SupabaseClient } from "@supabase/supabase-js";

// The site reads Supabase with the PUBLIC anon/publishable key, subject to the
// read-only RLS defined in scripts/supabase_schema.sql. The service-role secret
// key is pe-service's alone and must never appear here.
const url = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
const anonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";

/** True when both public env vars are set. */
export const supabaseConfigured = Boolean(url && anonKey);

let client: SupabaseClient | null = null;

// Lazy + guarded: createClient throws on an empty URL, so we never construct it
// at module load. This keeps `next build` (which prerenders the client pages
// once) green with no env, and components fall back to a configuration notice.
export function getSupabase(): SupabaseClient | null {
  if (!supabaseConfigured) return null;
  if (!client) {
    client = createClient(url, anonKey, {
      auth: { persistSession: false },
    });
  }
  return client;
}
