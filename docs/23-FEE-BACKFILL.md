# 23 — Fee Backfill (Polymarket per-market taker/maker fees)

**Status:** design. **Update (#326):** the on-chain fee path (Option B — `counterparty_edges.fee_raw`, decoded on-chain) was removed with the operator/funder purge; Option A (Gamma → `market_fees`) is the canonical live source. The Option-B analysis below is retained as historical record.
**Scope:** capture per-market fee schedule + plumb net-of-fees through `reconcile-volume` and `skill-select forward-test`.
**Why now:** the April-2026 holdout is **entirely post-fee** (Polymarket activated trading fees on all markets 2026-03-30 per Akey et al. SSRN 6443103). `forward.rs:21` already flags `gross_of_fees=true` and explicitly defers netting to "a per-market `takerBaseFee` backfill" — this doc plans that backfill. `reconcile-volume` (§5, #228) is also gross; with fees ignored, the on-chain vs Data-API volume comparison silently mixes gross+net.
**Authority order:** below `_BASELINE.md` / `_GLOSSARY.md` / `19-`; new defaults mirrored into `_GLOSSARY.md`.

---

## 0. Verified state (Tier-1, this session)

| Fact | Evidence |
|---|---|
| No `market_fees` / `fee_schedule` / equivalent table exists | `grep -n "market_fees\|fee_schedule" cache.rs` → empty |
| Gamma `/markets` DTO parses `endDate` + `liquidity` only | `gamma.rs:317-329` `GammaMarketRaw` |
| Gamma `/events` `markets[]` DTO parses `conditionId` + `clobTokenIds` only | `events.rs:280-288` `GammaEventMarketRaw` |
| On-chain `fee_raw` IS captured per `OrderFilled` leg | `cache.rs:167` `counterparty_edges.fee_raw TEXT NOT NULL`; decoded in `pe-source-onchain-polygon::order_filled` (Slice 1a, #226) |
| `forward.rs` records gross-of-fees flag, no netting | `forward.rs:52` `pub gross_of_fees: bool` (always `true`); `:186` `gross_of_fees: true` |
| `trades` table has no `side_role` (maker/taker) | Data-API `/trades` doesn't expose this; would have to be inferred or joined from `counterparty_edges` |

## 1. Two sources of truth — pick one for v1, use the other as verification

| | Gamma `takerBaseFee` / `makerBaseFee` (Option A) | On-chain `counterparty_edges.fee_raw` (Option B) |
|---|---|---|
| **What it is** | Per-market fee *rate* posted by Polymarket (basis points of notional) | Realized fee *amount* per leg, in USDC raw (uint256, 1e6 base) |
| **Per-trade attribution** | Implied: assume taker (skilled wallets ≈ takers; MM wallets already excluded via `infra`) | Exact, but requires joining a Data-API trade to its on-chain `OrderFilled` leg(s) via `(block_ts, taker, condition_id, side, amount)` — non-trivial |
| **Coverage** | Every market in Gamma `/markets` / `/events` (we already sweep these) | Every leg already populated in `counterparty_edges` by the current scan |
| **Pre-fee-era (≤ 2026-03-30) handling** | Field is `0` (or absent → treat as `0`) | `fee_raw` is `"0"` for legs before activation |
| **Latency-to-implement** | Low — extend Gamma DTO + new tiny table | Medium-high — non-trivial join logic; on-chain leg fee includes both sides; need to split or pick one |
| **Akey et al. precedent** | Yes — Akey net by per-market fee rate, mid-2026 | — |

**Decision:** **Option A first** (this doc's PRs 1–3). **Option B as a verification cross-check** (PR 4) — sum on-chain fees per trader-market vs Gamma-implied fees; warn on >5% divergence.

The first cut intentionally over-simplifies the role attribution: skill-select's surviving cohort excludes `infra` wallets (the existing MM heuristic), so assuming taker for them is correct in the high 90s of cases. PR 4 then quantifies the error.

## 2. Schema

New `market_fees` table (mirrors the shape of `market_schedules` / `market_resolutions`):

```sql
CREATE TABLE IF NOT EXISTS market_fees (
    condition_id          TEXT    PRIMARY KEY,         -- 0x + 64-hex (normalised via dune::normalise_condition_id)
    taker_base_fee_bps    INTEGER NOT NULL,            -- Gamma takerBaseFee in basis points (0..=10_000)
    maker_base_fee_bps    INTEGER NOT NULL,            -- Gamma makerBaseFee in basis points (0..=10_000)
    fee_active_from_unix  INTEGER,                     -- nullable; first block ts where fee_raw > 0 in counterparty_edges (PR 4 fills)
    fetched_at_unix       INTEGER NOT NULL
);
```

Notes:
- Stored as basis points (not Decimal) — fees are always `0 ≤ x ≤ 10_000` bps and integer math joins faster.
- `fee_active_from_unix` is nullable in PR 1; PR 4 backfills it from `counterparty_edges`. Skill-select's date-window filtering (≤cutoff vs post-cutoff) reads this to decide whether to net.
- No `maker_*_fee` columns beyond `maker_base_fee_bps` — Polymarket's actual fee schedule is one rate per side; if `feeSchedule` later differentiates by counterparty type, add columns then.
- Idempotent additive migration (same pattern as `token_conditions` in #223).

## 3. Sweep mechanics — extend the existing events sweep

`pe-bootstrap events` already paginates Gamma `/events` (`events.rs:run_events`) and writes `market_events` + `token_conditions` in the same pass. Adding a third write target is the natural extension and avoids a third pagination of the same data.

Changes:

1. `GammaEventMarketRaw` (`events.rs:281`) gains two fields:
   ```rust
   #[serde(default)]
   taker_base_fee: Option<rust_decimal::Decimal>,  // Gamma returns this as a number ∈ [0, 1] or as bps integer — DTO needs the flexible deserializer
   #[serde(default)]
   maker_base_fee: Option<rust_decimal::Decimal>,
   ```
   Use the existing `deserialize_decimal_flexible` pattern from `gamma.rs:336-361` (already accepts string / float / int).
2. Per event, per market: convert each fee to bps (multiply by 10_000 if Gamma returns a fraction; otherwise pass-through if already bps — verified against a live `curl` of `/events?limit=1` in the runbook), default to `0` if missing.
3. New helper `cache.upsert_market_fees_batch(rows: &[(String, i32, i32, i64)])` — flushed per page, alongside the existing `upsert_market_events_batch` / `upsert_token_conditions_batch` calls.
4. `EventsReport` gains `fees_upserted: usize`. The orphan gate is unchanged (fee absence is normal for pre-fee-era markets; it must not fail the sweep).

The Gamma `/markets` DTO (`gamma.rs:317`) is **not** extended in PR 1 — that fetch is per-market and slow; we get fee coverage from the bulk `/events` sweep for free. If a market has no event (orphan self-map), its fee stays at the default `0` — operationally acceptable because such markets aren't on the skill-select trade graph.

## 4. Plumbing — where fees get applied

### 4.1 `reconcile-volume` (#228)

`reconcile_volume.rs::run_reconcile_volume` currently computes `data_api_volume_usd` and `on_chain_volume_usd` per market. Net-of-fees changes:

- Load `market_fees` once at entry (`cache.load_market_fees() -> HashMap<String, FeeRow>`).
- For each market's Data-API volume aggregation, apply `taker_base_fee_bps` to each closed trade's notional **only when `closed_at_unix > fee_active_from_unix`** (or, if `fee_active_from_unix` is `NULL`, treat 2026-03-30 as the cutoff per the Akey reference).
- New report fields:
  - `data_api_volume_net_usd` — same as `data_api_volume_usd` minus taker fees over fee-era trades.
  - `markets_in_fee_era` — count of markets with at least one trade post-cutoff.
  - `aggregate_fee_drag_bps` — `1 − (net / gross)` in bps, fee-era markets only.
- Keep both gross and net side-by-side; the inflation ratio (`aggregate_inflation_ratio`) stays gross to preserve the original §5 semantics. A new `aggregate_inflation_ratio_net` is added.

### 4.2 `skill-select forward-test`

`forward.rs::run_forward_test` currently flags `gross_of_fees: true` and stops there. Netting changes:

- Replace `gross_of_fees: bool` with `fees_applied: FeeMode` enum: `{ Gross, NetTakerFlat, NetTakerPerMarket }`.
- Default mode becomes `NetTakerPerMarket`.
- For each forward position:
  - Look up the market's `taker_base_fee_bps` from `market_fees` (default `0` if absent).
  - Entry cost rises by `taker_base_fee_bps · entry_notional / 10_000`.
  - Resolution exit (settlement) is fee-free per Polymarket (settle is on-chain mechanical, no taker leg) — confirm in runbook before merging.
- The Kelly net price `c` (`kelly_sizer`-equivalent) becomes `c_net = c · (1 + taker_base_fee_bps/10_000)` — net cost.
- New report fields: `taker_fees_paid_usd`, `gross_pnl_usd`, `net_pnl_usd`. The headline number switches to net.
- Old `gross_of_fees: true` snapshot is preserved as a comparison baseline in the report.

### 4.3 Other crates

`kelly-sizer`, `trader-index`, `strategy-winner-follow` already assume `c` is net per `_GLOSSARY.md`. **No code changes here** — they consume net `c` from upstream; only the upstream computation changes. Adding a clarifying assertion at the kelly-sizer boundary (`debug_assert!(c_is_net)`) is in scope of PR 3.

## 5. PR sequencing

```
PR 1  Schema + Gamma sweep extension  (no consumers wired)
      - market_fees table; ALTER-ADD-COLUMN-style additive migration
      - events.rs DTO + per-page upsert
      - EventsReport.fees_upserted
      - cache.load_market_fees(), upsert_market_fees_batch()
      - 4 unit tests (DTO parsing for {fraction, bps-int, missing, malformed});
        1 scenario test (sweep populates the table)
      - Verify default for `taker_base_fee_bps` is conservative (probably 0)

PR 2  reconcile-volume net-of-fees
      - load_market_fees() at entry
      - new report fields (gross + net side by side)
      - 2026-03-30 hard cutoff applied where fee_active_from_unix is NULL
      - 3 unit tests (all-gross / all-net / mixed-era)

PR 3  skill-select forward-test net-of-fees
      - FeeMode enum, default NetTakerPerMarket
      - per-position fee, Kelly c_net
      - re-run forward-test on existing 999-perm extract → record net result
        in [[project_skill_selection_research]] memory
      - kelly-sizer debug_assert that c is net

PR 4  Verification: on-chain vs Gamma fee cross-check
      - bootstrap subcommand fee-reconcile
      - sum counterparty_edges.fee_raw per (taker, condition_id), compare
        with Gamma-implied taker fee on the matching Data-API trades
      - backfill market_fees.fee_active_from_unix from earliest fee_raw > 0
      - report divergence buckets (<1%, 1-5%, >5%); >5% triggers warn
```

PRs 1–3 are independent of the counterparty-edges scan (use Gamma only). PR 4 needs the scan complete. PRs 1–3 are unblocked by disk pressure.

## 6. Acceptance per PR

**PR 1**
- `pe-bootstrap events` populates `market_fees` with non-zero rows for at least one post-fee-era market on the live cache.
- `cargo nextest run -p pe-bootstrap` green.
- No regression in `EventsReport` field shape (additive only).

**PR 2**
- `pe-bootstrap reconcile-volume` outputs `data_api_volume_net_usd ≤ data_api_volume_usd` on the partial-scan data.
- `aggregate_fee_drag_bps` falls within `[0, 200]` bps (Polymarket fee schedule is bounded).
- Hand-verified on one known post-cutoff market.

**PR 3**
- Forward-test re-run on the existing 999-perm `wallet_features` produces a net result that is more negative than the gross −$291 (per the Akey-era prior).
- The shift recorded in `[[project_skill_selection_research]]` memory.
- `cargo test --doc -p pe-skill-select` green.

**PR 4**
- The reconciliation tool reports per-bucket counts; any bucket >5% generates a warn log + non-zero exit (exit `2`).
- `fee_active_from_unix` is populated for >95% of `market_fees` rows.

## 7. Risks / open questions

1. **Gamma fee field naming**: this doc assumes `takerBaseFee` and `makerBaseFee` from Akey's citation; the live JSON may use a different camelCase name (`takerFee` / `makerFee` / nested `feeSchedule`). **Verify with `curl 'https://gamma-api.polymarket.com/events?limit=1' | jq '.[0].markets[0]'`** in the PR 1 runbook before freezing the DTO. Fall back to a richer DTO that accepts multiple aliases via `#[serde(alias = ...)]`.
2. **Fraction vs bps encoding**: Gamma might return fees as a fraction (`0.02` = 2%) or as bps (`200`). Detect at parse time and normalise to bps internally.
3. **Maker fees might be rebates (negative)** in some fee-schedule designs; current schema uses `INTEGER NOT NULL` not `i32`. If negative fees are observed, widen to signed integer; for now Polymarket's published schedule is non-negative.
4. **`fee_active_from_unix` per-market vs venue-wide**: Akey says "all markets 2026-03-30" but the crypto/NCAA/Serie-A subset got fees from 2026-02-18. Per-market backfill (PR 4) is the safe source of truth; the 2026-03-30 fallback is a coarse default.
5. **Reconcile-volume re-run cost**: the table grows by one row per traded market (~1M). Backfill from a single events-sweep run is bounded and fast; no migration concerns.
6. **Forward-test gross/net divergence interpretation**: a more-negative net result is *expected*. The skill thesis is not "gross beat net" — it's "selected wallets beat the median wallet, net of fees." Track the net delta but read it as a sanity check, not a rejection signal.

## 8. Out of scope

- Maker rebates / liquidity-provider economics (skill-select excludes those wallets via `infra`).
- Gas costs (paid by the relay, not the trader, on Polymarket's gasless flow).
- Fee schedules on Kalshi (separate venue; not yet integrated).
- Pre-fee-era retrofit (no fees to net before 2026-03-30; the gross PnL there is already net).
- Real-time fee monitoring (this is a backfill + sweep, not a stream).

## 9. References

- Akey et al., SSRN 6443103 — Polymarket fee activation 2026-03-30; net-of-fees PnL methodology.
- `forward.rs` module docs — the deferred-netting marker this doc resolves.
- #228 — `reconcile-volume` §5 PnL-inflation deliverable (gross today; PR 2 above adds net).
- [[project_skill_selection_research]] — running result log for the forward-test net delta.
- `_GLOSSARY.md` — Kelly net price `c` rule (`19-` canonical defaults).
