// next-auth v5 (Auth.js) config for the single-email-gated dashboard (#398 WS3).
//
// Strict Google OAuth, single allowed identity, JWT sessions. Served behind an nginx TLS proxy on
// the DuckDNS host, so `trustHost: true` lets next-auth derive its https origin from the forwarded
// request headers (no bare-IP / port ambiguity). Secrets are read lazily, so `next build` stays
// green without them.
//
// Gates: `authorized` makes the middleware redirect any unauthenticated request to the (public)
// sign-in page — the rest of the site stays behind it; `signIn` admits only the allowlisted Google
// account (a wrong account → next-auth's built-in AccessDenied, nothing else renders).
import NextAuth from "next-auth";
import Google from "next-auth/providers/google";

import { isAllowedEmail } from "@/lib/auth";

export const { handlers, auth, signIn, signOut } = NextAuth({
  trustHost: true,
  providers: [
    Google({
      clientId: process.env.AUTH_GOOGLE_CLIENT_ID,
      clientSecret: process.env.AUTH_GOOGLE_CLIENT_SECRET,
    }),
  ],
  session: { strategy: "jwt" },
  callbacks: {
    authorized({ auth: session }) {
      return isAllowedEmail(session?.user?.email);
    },
    signIn({ profile }) {
      return isAllowedEmail(profile?.email);
    },
  },
});
