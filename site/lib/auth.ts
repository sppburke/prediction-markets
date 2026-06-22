// Single-email allowlist for the admin site (#398 WS3, Decision #7 / risk #9).
//
// There is NO bypass backdoor: this constant is the ONLY source of the allowlist (not the DB), so
// lockout recovery is editing it + redeploying (risk #9). Compared case-insensitively.
export const ALLOWED_EMAIL = "sppburke@gmail.com";

/** True iff `email` (any case) is the single allowed admin. */
export function isAllowedEmail(email: string | null | undefined): boolean {
  return typeof email === "string" && email.toLowerCase() === ALLOWED_EMAIL;
}

/**
 * True iff `input` equals the configured `SITE_PASSWORD` — the password-login secret used for
 * bare-IP access, where Google OAuth cannot redirect (Google forbids IP redirect URIs). Returns
 * `false` when `SITE_PASSWORD` is unset/empty, so a missing password can never grant access.
 */
export function passwordMatches(input: unknown): boolean {
  const expected = process.env.SITE_PASSWORD;
  return (
    typeof input === "string" &&
    typeof expected === "string" &&
    expected.length > 0 &&
    input === expected
  );
}
