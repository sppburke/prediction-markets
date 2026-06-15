# pe-analytics-site

Historical-vs-live paper-trade analytics site (issue #343 PR3). A thin, read-only
Next.js 15 (App Router) + Tailwind + Recharts viewer over the Supabase
`wallet_live_stats` view. It reads with the **public anon/publishable key** under
read-only RLS (`scripts/supabase_schema.sql`); the local `paper_state.db` and the
service-role secret key stay on `pe-service`.

## What it shows

- **Overview (`/`)** — portfolio KPIs, a live realized-P&L-by-wallet bar chart, and
  a sortable per-wallet table joining historical (ranker: `ls_edge`, `hit_rate`,
  `n_trades`, …) against live (paper: realized P&L, win rate, open/settled fills).
- **Wallet detail (`/wallet/<addr>`)** — the historical-vs-live panel, a win-rate
  comparison chart, and the recent paper-fill tape.

This beats the legacy portfolio-only SSR dashboard (`crates/paper-pnl/dashboard.rs`)
by adding the per-wallet historical-vs-live join the SSR page never had.

## Numeric display

`@supabase/supabase-js` returns Postgres `NUMERIC` as full-precision **strings**.
Every rendered numeric routes through `lib/format.ts`, which rounds to a declared
display precision (currency 2 dp, percentages 1 dp, ranker columns to their own
precision) so no raw full-precision string ever reaches the DOM. Rounding is
presentation-only — the stored columns and the view keep full precision. The
contract is locked by `lib/format.test.ts` (`npm run test`).

## Run locally

```bash
cd site
cp .env.example .env.local   # fill in NEXT_PUBLIC_SUPABASE_URL + _ANON_KEY
npm ci
npm run dev                  # http://localhost:3000
```

Production: `npm run build && npm run start` (defaults to :3000). An optional
`systemd` unit on the local box can run `npm run start`.

## Gate

```bash
npm run lint && npm run test && npm run build
```

CI runs the same via `.github/workflows/site.yml` (path-filtered to `site/**`); it
is independent of the Rust acceptance gate and cannot regress it.
