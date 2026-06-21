# 30 — Paper-trade analytics pipeline (sink → schema → view → site)

The historical-vs-live analytics stack for the live paper copy-trader (issue
#343). It projects paper trading into Supabase and renders a per-wallet
historical-vs-live view. In the default (analytics-only) mode the local trade
path is unaffected: `paper_state.db` stays authoritative and the event log stays
the durable write-ahead.

When `supabase_authoritative` is set (issue #397) the authority flips: Supabase
becomes the system of record for the money + book (`paper_bankroll`,
`paper_positions`, `paper_fills`, `settled_markets`), SQLite is demoted to a
write-through read cache, and `run_sink` is not spawned (the `commit_fill` /
`apply_resolution` RPCs are the sole writer of `paper_fills`/`settled_markets`).
The event log stays the local crash-recovery WAL. See `_GLOSSARY.md`:
`supabase_authoritative` and `scripts/supabase_paper_state_schema.sql`.

## Data flow

```
pe-service (Rust)                         Supabase                     site/ (Next.js)
  paper fills + settlements  ──sink──▶  paper_fills                      reads
  (best-effort, drop-on-full)           settled_markets   ──view──▶  wallet_live_stats ──anon──▶  overview + per-wallet
  ranker batches (local push) ───────▶  ranking_entries / latest_ranking                            historical-vs-live
```

- **Sink** (`crates/service/src/supabase_sink.rs`, PR2): dual-writes fills +
  settlements; OFF by default (`supabase_sink_enabled`); a Supabase outage never
  blocks or errors the trade path. Fill catch-up uses a contiguous-prefix
  `event_seq` HWM; settlements reconcile by idempotent full re-upsert.
- **Schema + RLS** (`scripts/supabase_schema.sql`, PR2): `paper_fills`,
  `settled_markets`, writer-only `supabase_sink_hwm`, and the `security_invoker`
  `wallet_live_stats` view. Site-exposed tables get anon `select` RLS; the writer
  uses the service-role secret key (RLS-bypassing), the site the anon key.
- **`wallet_live_stats` view**: FULL OUTER join of live paper stats
  (`paper_fills` ⋈ `settled_markets`) against historical ranker stats
  (`latest_ranking`) on `lower(leader_wallet) = lower(wallet_hex)`. Live realized
  P&L mirrors `paper-pnl::value_fill`: `side_sign · (resolved_price − fill_price)
  · contracts`. The FULL OUTER join keeps both admitted-but-not-yet-traded and
  aged-out wallets visible.
- **Site** (`site/`, PR3): thin read-only Next.js viewer (App Router + Tailwind +
  Recharts + `@supabase/supabase-js`). Overview + per-wallet historical-vs-live
  panels and charts. Self-hosted locally on :3000; reads the anon key under RLS.

## Numeric precision

`@supabase/supabase-js` returns `NUMERIC` columns as full-precision strings. The
site rounds every rendered value through a single shared formatter
(`site/lib/format.ts`) — currency to 2 dp, percentages to 1 dp, ranker columns
(`ls_edge`, `ls_tstat`, `fill_rate`, `avg_price`) to declared precisions — so no
raw full-precision string reaches the DOM. Rounding is presentation-only; the
stored columns and the view keep full precision. These are display constants, not
strategy thresholds (which live in `_GLOSSARY.md` / `19-`).

## Operate

- Apply the schema via the IPv4 session pooler (the direct host is IPv6-only); see
  the memory note in the project handoff and `scripts/supabase_schema.sql`.
- Site: `cd site && cp .env.example .env.local && npm ci && npm run build && npm run start`.
- Site env vars are the **public** anon key only (`NEXT_PUBLIC_SUPABASE_URL`,
  `NEXT_PUBLIC_SUPABASE_ANON_KEY`); never the secret key.

## Status / follow-ups

- PR1 (durable settled-set) + PR2 (sink/schema/RLS) + PR3 (this site) shipped.
- PR4 (final) removed the `paper_resolutions.json` sidecar and the legacy
  `render_dashboard_html` SSR path: the settled-set double-credit guard is now
  SQLite-only (`settled_markets`), and this site is the dashboard. The
  `paper_resolutions_path` config key and the `/dashboard` route are gone.
- Per-wallet category breakdown is deferred to a follow-up issue.
