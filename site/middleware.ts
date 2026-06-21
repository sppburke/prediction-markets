// Gate every non-static, non-auth route behind the single-email session (#398 WS3, step 15).
// The `authorized` callback in `auth.ts` redirects unauthenticated requests to sign-in.
export { auth as middleware } from "@/auth";

export const config = {
  // Match everything EXCEPT the auth endpoints (else infinite redirect) and Next static assets.
  matcher: ["/((?!api/auth|_next/static|_next/image|favicon.ico).*)"],
};
