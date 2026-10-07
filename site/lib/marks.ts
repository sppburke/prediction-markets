// Request body for the current-mark proxy (`/api/marks`). The keys travel in a POST body because
// one key per open fill ("market:outcome", ~70 characters) outgrew nginx's 8 KB request line once
// about 110 positions were open: the GET answered 414 and every Unrealized cell showed "—".

/** Matches `fetchOpenFills`' row cap, so one request never asks for more keys than the page loads. */
export const MAX_MARK_KEYS = 5000;

/** Trimmed, de-duplicated keys from `{ keys: string[] }`, or `null` when the body is malformed. */
export function parseMarkKeys(body: unknown): string[] | null {
  if (typeof body !== "object" || body === null) return null;
  const keys = (body as { keys?: unknown }).keys;
  if (!Array.isArray(keys) || keys.length > MAX_MARK_KEYS) return null;
  const parsed = new Set<string>();
  for (const key of keys) {
    if (typeof key !== "string") return null;
    const trimmed = key.trim();
    if (trimmed) parsed.add(trimmed);
  }
  return [...parsed];
}
