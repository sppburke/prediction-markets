"use client";

// Client-side next-auth session context (#398 WS3, step 15). Wraps the app so client components
// can read the session; server gating is enforced separately by `middleware.ts` + `auth()`.
import { SessionProvider } from "next-auth/react";

export function Providers({ children }: { children: React.ReactNode }) {
  return <SessionProvider>{children}</SessionProvider>;
}
