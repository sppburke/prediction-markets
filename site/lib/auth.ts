// Authentication helpers for the Google-only site (#398 WS3, #508 Phase C).
//
// There is NO admin bypass backdoor: this constant is the ONLY source of admin identity (not the
// DB), so lockout recovery is editing it + redeploying. Viewer login emails are normalized exactly
// like account_set_login_email's lower(btrim(...)) contract.
export const ALLOWED_EMAIL = "sppburke@gmail.com";

/** True iff `email` (any case) is the single allowed admin. */
export function isAllowedEmail(email: string | null | undefined): boolean {
  return typeof email === "string" && email.toLowerCase() === ALLOWED_EMAIL;
}

/** JavaScript equivalent of Postgres `lower(btrim(email))`; blank input has no identity. */
export function normalizeLoginEmail(email: string | null | undefined): string | null {
  if (typeof email !== "string") return null;
  const normalized = email.trim().toLowerCase();
  return normalized === "" ? null : normalized;
}

export interface LoginEmailRow {
  account_id: string;
  login_email: string | null;
}

export interface LoginEmailLookupResult {
  rows: readonly LoginEmailRow[];
  error: unknown | null;
}

export type LoginEmailLookup = (normalizedEmail: string) => Promise<LoginEmailLookupResult>;

/**
 * Fail-closed sign-in decision with an injected account lookup for network-free tests.
 * The hard-coded admin never depends on an account row; viewers require exactly one match.
 */
export async function canSignInWithEmail(
  email: string | null | undefined,
  lookup: LoginEmailLookup,
): Promise<boolean> {
  if (isAllowedEmail(email)) return true;
  const normalized = normalizeLoginEmail(email);
  if (!normalized) return false;

  try {
    const { rows, error } = await lookup(normalized);
    if (error) return false;
    const matches = rows.filter((row) => row.login_email === normalized);
    return matches.length === 1;
  } catch {
    return false;
  }
}

interface SessionShape {
  expires: string;
  user?: { email?: string | null } | null;
}

/** Build the exact callback set used by Auth.js, while keeping the DB decision injectable. */
export function buildAuthCallbacks(lookup: LoginEmailLookup) {
  return {
    authorized({ auth: session }: { auth: unknown }) {
      // Edge middleware performs no database I/O: every authenticated session may use Paper.
      return Boolean(session);
    },
    signIn({
      user,
      profile,
    }: {
      user?: { email?: string | null };
      profile?: { email?: string | null } | null;
    }) {
      return canSignInWithEmail(user?.email ?? profile?.email, lookup);
    },
    session({ session }: { session: SessionShape }) {
      // Deliberately omit name/image and never project authorization state into the JWT/session.
      return {
        expires: session.expires,
        user: { email: session.user?.email ?? null },
      };
    },
  };
}
