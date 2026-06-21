// next-auth v5 (Auth.js) config for the single-email-gated admin site (#398 WS3, step 15).
//
// Google provider + JWT sessions. Two gates:
// - `authorized` makes the middleware redirect any unauthenticated request to sign-in (the whole
//   site is private; AC: only the allowed email can load any non-static route).
// - `signIn` rejects every Google account except the allowlisted email → next-auth renders its
//   built-in AccessDenied page (nothing else renders) for a wrong account.
//
// Secrets (AUTH_SECRET, AUTH_GOOGLE_CLIENT_ID/SECRET, AUTH_URL) are read lazily at request time,
// so `next build` stays green without them (risk #7).
import NextAuth from "next-auth";
import Google from "next-auth/providers/google";

import { isAllowedEmail } from "@/lib/auth";

export const { handlers, auth, signIn, signOut } = NextAuth({
  providers: [
    Google({
      clientId: process.env.AUTH_GOOGLE_CLIENT_ID,
      clientSecret: process.env.AUTH_GOOGLE_CLIENT_SECRET,
    }),
  ],
  session: { strategy: "jwt" },
  callbacks: {
    // Middleware gate: unauthenticated → redirect to sign-in (applies to every matched route).
    authorized({ auth: session }) {
      return isAllowedEmail(session?.user?.email);
    },
    // Sign-in gate: only the single allowed Google account may complete sign-in.
    signIn({ profile }) {
      return isAllowedEmail(profile?.email);
    },
  },
});
