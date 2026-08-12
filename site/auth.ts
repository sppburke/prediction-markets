// next-auth v5 (Auth.js) config for Google authentication (#398 WS3, #508 Phase C).
//
// Strict Google OAuth, single allowed identity, JWT sessions. Served behind an nginx TLS proxy on
// the DuckDNS host, so `trustHost: true` lets next-auth derive its https origin from the forwarded
// request headers (no bare-IP / port ambiguity). Secrets are read lazily, so `next build` stays
// green without them.
//
// Gates: `authorized` is Edge-safe and checks session presence only; `signIn` admits the hard-coded
// admin or exactly one account.login_email match through the service-role client. Live/account
// authorization is deliberately re-resolved on each protected request by lib/authz.ts.
import NextAuth from "next-auth";
import Google from "next-auth/providers/google";

import { buildAuthCallbacks, type LoginEmailLookup } from "@/lib/auth";
import { getServiceRoleSupabase } from "@/lib/supabase-server";

const lookupLoginEmail: LoginEmailLookup = async (normalizedEmail) => {
  const supabase = getServiceRoleSupabase();
  const { data, error } = await supabase
    .from("accounts")
    .select("account_id, login_email")
    .eq("login_email", normalizedEmail)
    .limit(2);
  return { rows: data ?? [], error };
};

export const authCallbacks = buildAuthCallbacks(lookupLoginEmail);

export const { handlers, auth, signIn, signOut } = NextAuth({
  trustHost: true,
  providers: [
    Google({
      clientId: process.env.AUTH_GOOGLE_CLIENT_ID,
      clientSecret: process.env.AUTH_GOOGLE_CLIENT_SECRET,
    }),
  ],
  session: { strategy: "jwt" },
  callbacks: authCallbacks,
});
