// next-auth v5 (Auth.js) config for the single-email-gated admin site (#398 WS3).
//
// Two sign-in providers, both resolving to the same single allowed identity:
// - Google OAuth — for access via a real domain (https). Cannot be used on a bare IP because
//   Google forbids IP redirect URIs.
// - Credentials (password) — a self-hosted login that needs no external redirect, so it works on
//   a bare IP (`http://<vps-ip>:3000`). A correct `SITE_PASSWORD` grants the allowlisted identity.
//
// Gates: `authorized` makes the middleware redirect any unauthenticated request to the (public)
// sign-in page — the rest of the site stays behind it; `signIn` restricts the Google path to the
// allowlisted email (the Credentials path is validated in `authorize`).
//
// `trustHost: true` lets next-auth derive its origin from the request Host (we have no fixed
// AUTH_URL domain on the IP). Secrets are read lazily, so `next build` is green without them.
import NextAuth from "next-auth";
import Credentials from "next-auth/providers/credentials";
import Google from "next-auth/providers/google";

import { ALLOWED_EMAIL, isAllowedEmail, passwordMatches } from "@/lib/auth";

export const { handlers, auth, signIn, signOut } = NextAuth({
  trustHost: true,
  providers: [
    Google({
      clientId: process.env.AUTH_GOOGLE_CLIENT_ID,
      clientSecret: process.env.AUTH_GOOGLE_CLIENT_SECRET,
    }),
    Credentials({
      name: "Password",
      credentials: { password: { label: "Password", type: "password" } },
      authorize(creds) {
        return passwordMatches(creds?.password) ? { id: "admin", email: ALLOWED_EMAIL } : null;
      },
    }),
  ],
  session: { strategy: "jwt" },
  callbacks: {
    // Middleware gate: unauthenticated → redirect to sign-in (applies to every matched route).
    authorized({ auth: session }) {
      return isAllowedEmail(session?.user?.email);
    },
    // The Credentials path is already validated in `authorize`; only gate the Google path here.
    signIn({ account, profile }) {
      if (account?.provider === "google") return isAllowedEmail(profile?.email);
      return true;
    },
  },
});
