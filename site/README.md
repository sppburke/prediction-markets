# pe-analytics-site

Historical-vs-watched paper-trade analytics plus account-scoped live execution
visibility and controls (issue #508 Phase C). The Next.js 15 App Router site reads
the shared Paper views with the public anon/publishable key under read-only RLS.
Server-only account authorization, Live reads, and admin controls use the service-role
key; it is never sent to the browser.

## What it shows

- **Overview (`/`)** — portfolio KPIs, a watched-paper realized-P&L-by-wallet bar chart, and
  a sortable per-wallet table joining historical (ranker: `ls_edge`, `hit_rate`,
  `n_trades`, …) against watched paper stats (realized P&L, win rate, fills).
- **Wallet detail (`/wallet/<addr>`)** — the historical-vs-watched panel, a win-rate
  comparison chart, and the recent paper-fill tape.
- **Live (`/live`)** — authorized account-scoped live fills, positions, and account state.
- **Accounts (`/admin/accounts`)** — admin-only account controls and write-only sealed
  credential rotation.

This beats the legacy portfolio-only SSR dashboard (`crates/paper-pnl/dashboard.rs`)
by adding the per-wallet historical-vs-watched join the SSR page never had.

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
cp .env.example .env.local   # fill in public + server-only auth/account settings
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
