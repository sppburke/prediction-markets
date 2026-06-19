# 25 — Wallet Evaluation and Scaling (archived — removed in #370)

> **Status: ARCHIVE.** Nothing here describes live code. The GBM / `portfolio_constructor`
> evaluation-and-scaling methodology this documents was removed in issue #370 (PR3). The
> followed-wallet cohort is now produced by the 72hr ranker's own eligibility filters over
> the full trade universe (`scripts/rank_and_push.sh` → Supabase `latest_ranking`), not a
> curated greedy-selected list — see `docs/26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md`.
> Retained for history.

How to decide which wallets to follow, how many, and when to re-evaluate as capital grows.

## Current production deploy

| Field | Value |
|---|---|
| Cohort size | 10 wallets |
| Strategy | `portfolio_constructor greedy` |
| Validation | PBO across 4 anchors |
| Deploy date | 2026-06-03 |
| Watchlist file | `data/watchlist-production-n10.json` |
| Source eval | `data/eval-results/20260603T111638Z-pbo_n10_validate.json` |

## When to re-evaluate

Trigger a re-evaluation when **any** of these is true:

1. **Monthly cadence** — a new month has completed (new trades have resolved, new data is available for the latest cutoff).
2. **Capital crosses a tier boundary** — see the scaling table below.
3. **Sustained P&L drawdown** — paper bankroll drops >15% from peak. The current cohort may have degraded.
4. **A followed wallet goes inactive** — no trades observed for >30 days. Replace it without waiting for the monthly cycle.

## Re-evaluation process

The exact, verified command sequence lives in the operational runbook:
[`26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md`](26-DATA-REFRESH-AND-REOPTIMIZATION-RUNBOOK.md).
This section covers only the **review gate** (Step 4 of the runbook) — the
decision criteria that determine whether a freshly-constructed cohort is fit to
deploy.

Before reviewing, run runbook Part 1 (backfill) then Part 2 Steps 1–3 (extract →
rank → construct). That produces an eval JSON under `data/eval-results/`.

### Deploy gate — review the eval JSON

Open `data/eval-results/<timestamp>-pbo_n<N>_validate.json` and check:

| Metric | Gate | Notes |
|---|---|---|
| `credible` | must be `true` | PBO ≤ 0.5 AND 0 negative anchors |
| `mean_of_mean_edge` | ≥ $0.01/pos (net-of-haircut) | Below this the edge is noise |
| `n_anchors_negative` | 0 | Any negative anchor = regime risk |
| `pbo.pbo` | ≤ 0.5 | Above 0.5 = overfit signal, do not deploy |

If gating passes, continue to runbook Part 2 Steps 5–6 (export + deploy). If not:
- Try a smaller N (more concentrated = more stable).
- If the most recent anchor is negative, the current month may be a regime shift — hold the existing cohort and re-check in 2 weeks.

---

## Scaling table — cohort size vs capital

As bankroll grows, a larger cohort spreads risk and captures more market coverage. Use `portfolio_constructor` to validate the target N before deploying.

| Bankroll | Target cohort N | Notes |
|---|---|---|
| < $5k | 10 | Current deploy. Concentrated — one wallet's bad month matters. |
| $5k – $20k | 15 – 25 | Run portfolio_constructor with `--target-n 25`; take what passes gating. |
| $20k – $100k | 25 – 50 | At this range, per-wallet diversification matters — avoid over-concentrating in correlated wallets. |
| > $100k | 50 – 100 | Diversification within the validated universe. Add `--min-jaccard` gate. |

These are starting points. Always let the PBO validation determine the actual deployed N — do not force a size that fails gating just to hit a target.

### Why concentrated at small capital

With a $10k paper bankroll and per-trade Kelly fraction of 0.25, a 10-wallet cohort means each wallet accounts for ~10% of trades. This is intentional:

- Fewer wallets = higher signal-to-noise per position.
- At small capital, the minimum position size is a binding constraint — spreading across 50 wallets produces fractional positions that cannot be executed at Polymarket minimums.
- The `portfolio_constructor` greedy algorithm already selects the N that maximises forward edge — it will naturally select a smaller N when the marginal wallet degrades the portfolio.

### When a wallet should be dropped without waiting for re-evaluation

- No trades in 30 days (wallet inactive).
- Wallet identified as infrastructure / market-maker (check `is_infra` in wallet_cache).
- Wallet's recent on-chain activity shows large withdrawals (operator exiting).

In these cases, remove the wallet from `watchlist-production-n<N>.json` manually and restart the service. File a note in the issue tracker so the next monthly re-evaluation accounts for the slot.

---

## Artifact naming convention

| Type | Pattern | Example |
|---|---|---|
| Eval JSON | `data/eval-results/<ISO8601Z>-pbo_n<N>_validate.json` | `20260603T111638Z-pbo_n10_validate.json` |
| Wallet list (txt) | `data/eval-results/watchlist-<ISO8601Z>-pbo_n<N>_validate.txt` | `watchlist-20260603T111638Z-pbo_n10_validate.txt` |
| Service watchlist | `data/watchlist-production-n<N>.json` | `watchlist-production-n10.json` |

Retire old artifacts to `data/archive/` when a new deploy is live. Never delete eval JSONs — they are the audit trail for why a cohort was deployed.
