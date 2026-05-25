# 24 — Skill-Selection Candidate Features (implementation plan for #205)

**Status:** design — first-PR scope frozen; subsequent slices outlined.
**Scope:** bridge the [#205 reference issue](https://github.com/sppburke/prediction-markets/issues/205) (research / parking) into concrete `skill-select` PRs.
**Anchors:** SSRN 6617059 (Gómez-Cram et al., sign-randomization); Bailey & López de Prado (PSR, DSR, PBO); Akey et al. (SSRN 6443103, fee-era + persistence risk); [[project_skill_selection_research]].
**Authority order:** this doc is below `_BASELINE.md` / `_GLOSSARY.md` / `19-WINNER-FOLLOW-STRATEGY.md`; any numeric default it introduces is mirrored into `_GLOSSARY.md` "Skill-selection defaults" in the same PR.

---

## 0. Why this doc exists

`#205` is a research dump — the full candidate feature set + selection methodology — and is explicitly tagged "do not implement directly." `#212` shipped v1 (`pe-skill-select`) using a **narrow subset** of #205 (moments + ROI + win-rate, gated by the sign-randomization skill test, ranked by deflated Sharpe). The first real-data run (2026-05-22, 999-perm, `min_trading_days>=20` gate) gave a **strong negative**: flat-$1 forward = −$291.32 over 1,896 positions, Kelly-f0.10 = −$1.28 (calibration correctly declined). The reading was *"v1 tested the wrong (narrow) object."*

The path forward (decided 2026-05-22): **#207 data → #205 candidate features → weighted composite (train-only tuning + deflation)**.

#207 data (counterparty edges) is currently landing (scan in progress, slice-1c reconcile-volume merged in #228). This doc plans **the #205 candidate-feature work** — what to add to `skill-select`, in what order, with the first PR fully spec'd.

## 1. Current state — what v1 already covers, what's missing vs #205

### 1.1 Covered by v1 (`crates/skill-select/`)

`DeterministicFeatures` (`features.rs:38`) persists per `(cutoff_unix, wallet_hex)`:

- Sample size: `reconstruction_quality`, `closed_trades`, `distinct_markets`, `distinct_events`, `trading_days`.
- PnL/ROI: `total_pnl_usd`, `roi_bps`.
- Outcome rate: `win_rate_bps`.
- Hold duration: `avg_hold_secs`.
- Daily-return moments: `mean_daily_return_bps`, `std_daily_return_bps`, `sharpe_bps`, `skewness_bps`, `excess_kurtosis_bps`, `lcb_5pct_bps`.

Skill gate + ranking (separate modules):
- `skill_test.rs` — event-level sign-randomization (SSRN 6617059), 999 perms, SplitMix64 seed 42.
- `selection.rs` — BH-q FDR gate (`bhq_q_bps=1000`) + deflated-Sharpe heuristic haircut (`√(2 ln m)`), top-N cap, `min_trading_days>=20` gate.
- `forward.rs` — temporal-holdout forward test (flat-$1 + Kelly-f0.10, p = entry-price-bucket win-rate, sparse→flat fallback). **PnL is gross of fees** (`ForwardReport.gross_of_fees=true`).

### 1.2 Gap matrix vs #205

Marked ⛔ where the feature is the bottleneck for the next "real" result.

| #205 group | Feature | In v1? | This doc |
|---|---|---|---|
| **A. Per-bet quality** | EV = p − c | ❌ | **PR 1** ⛔ |
| | CLV (closing line value) | ❌ | Deferred (PR 4 — needs reconstructed pre-resolution consensus price) |
| | Beta-binomial shrunk edge | ❌ | **PR 1** |
| | Exact binary Kelly log-growth | ❌ | **PR 1** |
| | t-stat of per-bet returns | ❌ | **PR 1** |
| | Brier resolution / log loss | ❌ | **PR 1** |
| **B. Skill-vs-luck gates** | Event-level sign-randomization | ✅ (v1 `skill_test.rs`) | — |
| | PSR + MinTRL | partial (DSR-heuristic only) | PR 3 (replace heuristic with PSR + MinTRL) |
| | Deflated Sharpe (false-strategy thm) | partial (heuristic haircut) | PR 3 |
| | BHq / BHy FDR | ✅ (v1 `selection.rs`) | — |
| **C. Activity** | First-entries / active day (recency-weighted) | ❌ | **PR 1** |
| | # distinct events traded (≥10 gate) | ✅ tracked, ❌ gated | **PR 1** (add `min_distinct_events>=10` gate) |
| | Churn (trades/market) | ❌ | PR 2 |
| **D. Capital velocity** | Median time-to-resolution of first entries | partial (`avg_hold_secs`, not first-entry, not median) | **PR 1** (add `median_first_entry_to_resolution_secs`) |
| **E. Executability/capacity** | % first-entries in markets above depth floor | ❌ | Deferred (PR 5 — needs liquidity snapshots at entry) |
| | Avg entry-time liquidity | ❌ | Deferred (PR 5) |
| **F. Concentration** | HHI of profit shares, N_eff, RPC | ❌ | **PR 1** |
| **G. Anti-gaming gates** | Intra-cluster wash (Sirolly Algorithm 1) | ❌ | PR 2 (depends on #207 counterparty-edges scan) |
| | Sybil fan-out (operator ≥5 children) | ❌ | PR 2 (needs funder graph + operator pooling) |
| | Infra/MM exclusion (richer than current heuristic) | partial (`infra` flag in cache) | PR 2 (needs `wallet_fill_roles` from #207 slice 3) |
| | Round-number / Benford anomaly | ❌ | Deferred (weak secondary) |
| **H. Operator-level pooling** | Rank at operator level when funder-graph confident | ❌ | PR 3 (composite stage) |

**PR 1 covers all the additive per-bet/concentration features that need no new tables, no new external data, no counterparty-edge dependency, and no fee data.** Those become bricks the Stage-2 composite (PR 3+) consumes.

## 2. Sequencing — five PRs, two phases

```
Phase A — Candidate features (the bricks)
  PR 1  Per-bet quality (A.1, A.3, A.4, A.5, A.6) + Concentration (F) + activity gates (C.1, C.2, D)
  PR 2  Anti-gaming gates (G) — depends on #207 scan complete + slice 3 (wallet_fill_roles)
                                + funder-graph operator pooling for Sybil

Phase B — Selection methodology (composite + deflation)
  PR 3  Replace deflated-Sharpe heuristic with PSR + MinTRL + true DSR (Bailey & LdP 2014);
        operator-level pooling option
  PR 4  Weighted composite — ONC clustering + clustered MDA + non-negative elastic-net
        weights, PBO/DSR deflation; or Borda/mean-rank as outlier-robust alt
  PR 5  Executability features (E) — depends on liquidity-snapshot table (new infra, separate work)
```

Each PR ends with a **forward-test run on the existing April-2026 holdout**, recorded in the [[project_skill_selection_research]] memory. The minimum viable signal we want from PR 1 alone is: "do the new candidate-feature columns persist forward better than v1's narrow set when used as alternative rankings?" That's a free side-experiment because we already have the forward-test harness.

**PR 1 is the next merge.** PR 2 is blocked on the scan + slice 3. PR 3+ is design + architecture work.

## 3. PR 1 — Scope (frozen)

### 3.1 Features added

All computed inside `extract_features` (`crates/skill-select/src/features.rs`) from the existing `TraderLedger`; no new SQL on the trade side.

| Field added to `DeterministicFeatures` | Type | Formula | Source |
|---|---|---|---|
| `ev_mean_bps` | `i64` | mean over closed trades of `(outcome_$ − entry_price) / 1$`, in bps | trade prints + resolution outcome (0/1) |
| `ev_tstat_bps` | `i64` | `√n · mean(o−c) / sd(o−c)` × 10⁴ (`0` if `n<2` or `std==0`) | derived from per-bet returns |
| `bb_shrunk_edge_bps` | `i64` | beta-binomial shrunk `p̂ = (x+α)/(n+α+β)` minus mean entry price, bps; defaults α=β=1 (Laplace) — wider prior settable via config | per-bet `(o, c)` |
| `kelly_log_growth_bps` | `i64` | exact binary Kelly `g* = ½·SR²` via `g* = p·ln((1−c+p−c)/(1−c)) − ...` — derived from `(p̂, c̄)`; `0` when `f*≤0` | per-bet `(p̂, c̄)` |
| `brier_score_bps` | `i64` | `(1/N)·Σ(c−o)²` × 10⁴ (lower = better calibration) | per-bet `(o, c)` |
| `brier_resolution_bps` | `i64` | Murphy 1973 resolution component `RES = (1/N)·Σnₖ(ōₖ−ō)²` × 10⁴ — bucket entries by entry-price decile | per-bet `(o, c)` |
| `concentration_hhi_bps` | `i32` | `HHI = Σ s_i²` where `s_i = pos_profit_event_i / Σ pos_profit_events` × 10⁴ (`0` if no profitable events) | per-event PnL roll-up |
| `concentration_n_eff_bps` | `i32` | `1/HHI` × 10⁴, capped at `i32::MAX`; `0` if HHI is zero | derived from HHI |
| `concentration_rpc_bps` | `i64` | rank-weighted concentration `Σ r · s_i(r)` × 10⁴ | sorted per-event profit shares |
| `first_entries_per_active_day_bps` | `i32` | `(distinct first-entries / trading_days)` × 10⁴; recency-weighted variant tracked in PR 2 | first entries per market, already in ledger |
| `median_first_entry_to_resolution_secs` | `i64` | median of `(resolution_ts − first_entry_ts)` over distinct markets in train window | trade + resolution |

All `Decimal` math; no `f64` in the persisted columns (per CLAUDE.md "No raw `f64` for money/prices/quantities/probabilities").

### 3.2 New eligibility gate

Add `min_distinct_events: u32` to `SkillConfig` (default **10**, paper threshold). `extract_features` returns `None` when `distinct_events < min_distinct_events`. This is the long-deferred "≥10 events" gate from #205 §C. Mirror the default into `_GLOSSARY.md` "Skill-selection defaults".

### 3.3 Persistence

`wallet_features` table gains 11 new columns (all `INTEGER`/`NUMERIC` per existing style). Use the existing additive-migration pattern in `cache.rs` (or `skill-select/src/db.rs` — whichever owns the table): `ALTER TABLE wallet_features ADD COLUMN ... DEFAULT 0` per new col, idempotent on open. **Re-derive on the next `extract` run** — no online backfill (extract is regenerative; backfill = `extract` again).

### 3.4 Files touched (PR 1)

- `crates/skill-select/src/features.rs` — extend `DeterministicFeatures`; add `extract_per_bet_quality`, `extract_concentration`, `extract_activity_gates`, `extract_capital_velocity` (or extend `extract_features` in-place — pick one). Add unit tests per closed-form math.
- `crates/skill-select/src/db.rs` — schema migration; bump persisted columns; `upsert_wallet_features` row shape.
- `crates/skill-select/src/config.rs` — `min_distinct_events: u32 = 10`, `beta_binomial_alpha: u32 = 1`, `beta_binomial_beta: u32 = 1` (Laplace defaults). Figment env-var overrides per existing style.
- `crates/skill-select/src/extract.rs` — call sites for new fields; ledger filtering unchanged.
- `docs/_GLOSSARY.md` — add the three new defaults to "Skill-selection defaults".
- `docs/24-SKILL-SELECTION-CANDIDATE-FEATURES.md` (this doc) — keep accurate as PRs land.

No new tables, no new external API calls, no fee-data dependency, no #207 dependency, no f64.

### 3.5 Tests

- Unit (`features.rs`): one per new feature with hand-computed expected values on a 3- to 5-trade fixture; one zero-dispersion regression (Brier resolution with single-bucket entries → 0); one `<10 events` rejection.
- Scenario (`scenario_skill_select.rs` if it exists, else create): seed a `WalletCache` with 30 trades for one wallet, run `extract`, assert all 11 new columns land in `wallet_features` with the expected values.

### 3.6 Acceptance

- All v1 columns unchanged on existing test fixtures (no regression).
- 11 new columns populated for the seeded wallet; row shape stable.
- `cargo nextest run -p pe-skill-select` and the doc gate (`cargo test --doc -p pe-skill-select`) pass.
- A re-run of `extract` on `data/wallet_cache.db` produces a `wallet_features` row with the new columns for at least one known-skilled wallet, hand-verified for sanity (single spot check, not exhaustive).
- A forward-test re-run using **any one** of the new columns as the ranking key (instead of deflated-Sharpe) is recorded in memory — purely diagnostic, doesn't gate the merge.

### 3.7 Out of scope for PR 1

- CLV (no pre-resolution consensus price reconstruction yet — separate infra).
- Fee netting in forward PnL (separate fee-backfill task; tracked in `docs/23-FEE-BACKFILL.md` once that's written).
- Sirolly wash / Sybil / richer MM exclusion (needs #207 scan complete + slice 3).
- The Stage-2 weighted composite (PR 4).
- PSR / MinTRL / true DSR replacement (PR 3).
- Operator-level pooling (PR 3).
- Liquidity / depth executability features (PR 5).
- Rayon parallelism for `extract` (orthogonal performance task).

## 4. Later PRs — outlines only

### PR 2 — Anti-gaming gates (G)

**Depends on:** counterparty-edges scan complete (#207) + slice 3 (`wallet_fill_roles`).

Wash gate: Sirolly Algorithm 1 on the counterparty adjacency `b_ij` from `counterparty_edges`. Initial score = closure propensity. ~12 iterations to tol 1e-5. Flag trades with `min(x_i, x_j) ≥ θ=0.9`. Persist a per-wallet `wash_score_bps` + a boolean `wash_excluded` gate. Add a config knob `wash_theta_bps = 9000`.

Sybil fan-out: query `funder_edges` for any wallet with ≥5 funded children whose first-entries cluster within a configurable window. Persist `sybil_fanout_n` + a boolean `sybil_excluded` gate.

MM exclusion: replace the current `infra` heuristic with a maker-share threshold derived from `wallet_fill_roles` (e.g. maker_share > 0.7 over ≥20 fills → excluded). Persist `maker_share_bps`.

### PR 3 — Replace DSR heuristic with PSR + MinTRL + true DSR

Drop the `√(2 ln m)` heuristic haircut; implement Bailey & LdP 2012 (PSR + MinTRL) and 2014 (DSR with the false-strategy threshold `SR₀=√V·[(1−γ)Φ⁻¹(1−1/N)+γΦ⁻¹(1−1/Ne)]`). Selection still BHq-FDR-gated. Optional: rank at operator level when funder-graph confidence > threshold (operator-pooling option from #205 §H).

This is also where the existing **`deflated_sharpe_bps` scaling bug** (memory: "top = 1,779,231 ≈ DSR 178 if ÷10⁴, suspected scaling bug in `selection.rs`") gets fixed for real.

### PR 4 — Weighted composite

ONC clustering of feature correlation `d=√((1−ρ)/2)` → clustered MDA importance under purged + embargoed CPCV → non-negative elastic-net weights (1-SE rule) → PBO deflation over every config tried. Borda/mean-rank as the outlier-robust alternative.

This is the actual #205 "Stage 2" object — the rest of the candidate features only contribute their predictive power through this composite.

### PR 5 — Executability features (E)

Needs a `market_liquidity_snapshots` table sampled at each first-entry's `block_ts`. New infra; separate plan.

## 5. Risks / open questions

1. **CLV is the strongest predictor in betting lit and we don't have it.** Reconstructing pre-resolution consensus needs either (a) a final-N-minute mid-price from CLOB / on-chain order book, or (b) the last on-chain fill as a proxy. Both are infra work; we may pre-validate the composite without CLV and add it later — but we should not claim "covers #205" until CLV lands.
2. **Beta-binomial conjugate prior choice is contested.** Laplace (α=β=1) is the cleanest default; a tighter prior (e.g. α=β=5) shrinks more aggressively. Tracking it as a config knob — PR 1 doesn't pretend to resolve.
3. **t-stat of per-bet returns** is naive (assumes IID); inside a market it isn't. PR 1 logs the naive value; the bootstrap variant (block-resampling at the event level) is a PR-3-era add.
4. **The 999-perm forward-negative is the strongest prior on the table** — every PR after this should include the forward-test number in the merge note so we know whether each addition moved the needle. If PR 1 doesn't move it, that's a real signal, not a bug.
5. **Disk pressure during the counterparty-edges scan** — PR 2 is blocked until disk frees up. Do not start PR 2's wash-score backfill against the live cache while the scan is running.

## 6. Drift vs #205 reference

Recorded after the 2026-05-25 plan-review of #205 against the live tree. These items are not bugs in the reference; they are **drift markers** between #205's specification and the current `pe-skill-select` implementation. Each names its resolution path; do not block PR 1 on them.

### 6.1 Blocking drift

1. **Perm count: 999 vs paper's 10,000.** `pe-skill-select::config.rs::default_permutations = 999` (file:line confirmed 2026-05-25). At 999 perms the p-value floor is `1/(999+1) = 10 bps`. The 2026-05-25 sweep over the full 65,468-wallet cohort tied **7,563 wallets at that floor** under BHq q=0.10, meaning the significance count is partly a resolution artifact rather than a discrimination result. **Resolution:** intentional speed trade-off for v1 — bumping to 10,000 makes `extract` 10× slower per wallet, which on the live cohort is hours rather than ~minutes. Production runs that need finer discrimination should set `PE_SKILL_PERMUTATIONS=10000`; canonization in `_GLOSSARY.md` waits until the longer extract budget is acceptable (or PR 3 lands and re-extract is no longer in the critical path).

2. **DSR is a heuristic, not the calibrated False-Strategy Theorem.** `pe-skill-select::selection.rs:18` explicitly documents the implementation as `√(2 ln m)` Sharpe haircut — a multiple-testing penalty proxy, not Bailey & LdP's trial-Sharpe-variance-adjusted DSR formula. **No `DSR ≥ 0.95` gate is enforced anywhere in the codebase.** **Resolution:** PR 3 of this doc (`PR 3 — Replace DSR heuristic with PSR + MinTRL + true DSR`) explicitly replaces both the heuristic and adds the 0.95 gate. Until PR 3 lands, today's deflated-Sharpe values are *ordinally* correct (the ranking they produce is the right relative order under multiple testing) but the *absolute* deflated numbers and any "significant by DSR" claim are heuristic, not paper-faithful.

### 6.2 Should-fix drift

3. **Wash-cluster exclusion not yet in the Stage-1 cohort.** `pe-skill-select::extract.rs` does NOT filter the cohort by `wash_score`. The clustering machinery exists at `crates/operator-graph/src/clustering.rs:48` (`wash_cluster_match_threshold_pct = 60`), but no wash-exclusion wire reaches skill-select. **Resolution:** PR 2 of this doc (Sirolly Algorithm 1 + `wash_excluded` gate) — itself gated on the #207 counterparty-edges scan completion (currently disk-blocked at 87.6%; resumes when disk frees up). V1 evaluations DO NOT enforce wash exclusion; this is a known statistical-power leak in the current ranking.

4. **`pe-backtest` OOM is dead-path, not patched.** #205 says "pe-backtest OOM dissolved by two-phase offline-score → load-only-top-N." T1 confirms `pe-skill-select` implements the two-phase design (wallet-at-a-time streaming, never holds 269M trades) AND `crates/backtest/src/main.rs:43 let mut all_trades = cache.all_trades();` is unchanged — the original OOM site is still in the original crate. **Resolution:** none planned within this doc — `pe-backtest`'s `all_trades()` path is now dead-path but not yet removed. Filing a tracked cleanup issue would make the dead-path explicit; until then, treat any direct `pe-backtest` run as a known OOM risk.

## 7. References

- #205 (this doc's parent reference issue) — full candidate set + selection methodology.
- #209 (epic) / #206 (event grouping, closed) / #207 (counterparty edges, in flight) / #208 (coverage subcommand, closed).
- #212 (`pe-skill-select` crate v1) and merged PRs #213–#221.
- #228 (reconcile-volume — the #207 §5 PnL-inflation deliverable; gates whether the trade volume PR 1 trusts is materially inflated).
- `_GLOSSARY.md` "Skill-selection defaults" (canonical numeric defaults).
- [[project_skill_selection_research]] memory — running log of forward-test results and the 999-perm forward-negative.
- SSRN 6617059 (Gómez-Cram et al.); Bailey & LdP 2012/2014; López de Prado *AFML* 2018, *MLAM* 2020; Murphy 1973; Kelly 1956; Akey et al. SSRN 6443103.
