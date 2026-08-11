// Gate every non-static, non-auth route behind any authenticated session (#508 Phase C).
// Account/live handlers re-resolve their stronger authorization through lib/authz.ts.
export { auth as middleware } from "@/auth";

export const config = {
  // Match everything EXCEPT the auth endpoints (else infinite redirect) and Next static assets.
  matcher: ["/((?!api/auth|_next/static|_next/image|favicon.ico).*)"],
};
