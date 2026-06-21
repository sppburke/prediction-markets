// Single-email allowlist for the admin site (#398 WS3, Decision #7 / risk #9).
//
// There is NO bypass backdoor: lockout recovery is via editing this constant (or a direct
// `psql` edit), per risk #9. Compared case-insensitively against the Google profile email.
export const ALLOWED_EMAIL = "sppburke@gmail.com";

/** True iff `email` (any case) is the single allowed admin. */
export function isAllowedEmail(email: string | null | undefined): boolean {
  return typeof email === "string" && email.toLowerCase() === ALLOWED_EMAIL;
}
