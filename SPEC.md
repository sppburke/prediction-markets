## Context / intent

#544 shipped exact, replayable activity ingestion, causal bucket commits, wallet fences, durable
history, and a five-step causal positions bracket whose install rule is (issue body, verbatim):
"Compare exact positive ordinary, non-combo asset balances mapped to condition and outcome,
omitting exact zero and applying no resolution-state filter," under the ownership rule "The
ordered activity ledger is the only leader-position owner. Current positions can validate a
captured ledger generation but can never overwrite it."

Activation rehearsal 7 and an offline sweep of the production boot bracket over the live watchlist
proved that rule cannot pass for real wallets: the activity feed does not carry the information
needed to reconstruct balances, and the positions endpoint does. This issue inverts ownership for
**absolute balances** — the venue's positions are the authority at a proved anchor; activity is
the authority for **causal ordering and exact effects after that anchor**; and every balance
effect the feed cannot express exactly is answered by **re-anchoring**, never by inference. It
removes the reconstruction machinery the false premise required. Everything else #544 delivered
stays. Every account stays paper-only, off, and unarmed. #544's activation steps 5–8 stay blocked
on this.

## Evidence-backed findings

Gathered 2026-09-01 (CT) against the live venue and `main` `b7c499b`. Raw captures (activity
pages, both positions partitions, per-market diff, asset-conflict walk, parse sweeps) are archived
as `evidence544_raw_captures.tar.gz`, sha256 `517294a3e30b2225140205d24326241b6b040b61431d8203bf504b8f1da0dcdd`
(first comment). The archive includes the offline bracket harness source (`bracket_sweep.rs`), its
exact invocation (`HARNESS_INVOCATION.md`), the 79-wallet input census, and the per-wallet output.

**F1 — BUY `size` is the nominal order size; the row has no fee field.** Row keys enumerated live:
asset, bio, conditionId, eventSlug, icon, name, outcome, outcomeIndex, price, profileImage,
profileImageOptimized, proxyWallet, pseudonym, side, size, slug, timestamp, title,
transactionHash, type, usdcSize. Wallet `0x3080eef8…`, condition `0x10298d871856…`: BUY 10 +
BUY 10 @0.50, SELL 19.7 @0.24 — the wallet received 19.7 and sold all of it; the reconstructed
ledger keeps 0.3 (1.50%). Condition `0x07f4fa68519d…`: BUY 11.32 + BUY 11.32 @0.53, REDEEM
22.32078 — residue 0.31922 (1.41%). The residues fit no single formula (crypto taker
`0.07·p·(1−p)` = 1.75% at 0.50): shares received are not derivable from the row.

**F2 — Zero-size REDEEM legs are the only record of losing-side burns; partial redemption is
legal.** Same wallet, condition `0x014d48b4f0d3…`: BUY 12.24 of outcome 1; later
`REDEEM outcomeIndex=0 size=0`. Of that wallet's 267 phantom positions: 149 this shape, 73 fee
residue after a non-zero REDEEM, 31 after a full SELL, 14 mixed. The venue's redemption call
takes arbitrary index sets (`third_party/polymarket_client_sdk_v2/src/ctf/types/request.rs:93–108`,
single-index test `tests/ctf.rs:209`); only this repository's own adapter always redeems both
(`crates/venue-polymarket/src/redemption.rs:89`). Public leaders are not bound to it, and the
public row carries no calldata, so the burn scope of a zero-size or outcome-less redemption is
unknowable from the feed.

**F3 — The positions endpoint reports true balances.** `/positions?…sizeThreshold=0&includeArchived=true&…&redeemable={false,true}`
for that wallet: exactly 1 row (open market, 42.24) and 0 redeemable rows. The bracket's reader
already pages both partitions to offset 10,000 with exact `ShareAmount` parsing
(`crates/source-polymarket-public/src/reconciliation.rs:570–690`).

**F4 — Structural.** Offline harness running `CausalPositionValidator::validate_direct` per wallet
against a temporary paper state: 52 of 79 live watchlist wallets processed at review time —
**0 accepted**, 17 fenced, 31 `StableMismatch`, 4 `ConflictingActivityMapping`. Final figure in
the first comment.

**F5 — `ConflictingActivityMapping` is a venue inconsistency on multi-outcome markets.** Wallet
`0x180e62e6…`, one asset id on condition `0xd21e5817…` ("highest temperature in Madrid … 18°C")
carries `outcomeIndex` 1 on four rows and 0 on three. `ActivityAssetMapping::from_rows` rejects
it; the bracket surfaces a generic `Positions` error that boot treats as fatal
(`position_seeder.rs:203` defers only three enumerated kinds).

**F6 — What the ledger feeds.** `crates/copy-signal-engine/src/classifier.rs:81,100`: a leader
BUY is an `Entry` iff `state.long_contracts == ShareAmount::ZERO`. Fee residue makes an entry
after a full exit look like a non-entry forever. The ever-traded history sidecar is a separate
owner and is unaffected.

**F7 — Contracts the redesign respects (verified in code).**
- Bracket activity reads commit before positions are read (`position_seeder.rs:292,308`) and use
  full-history walks (`start=None`, `:356`) because the positions parser requires an activity
  mapping for every asset (`reconciliation.rs:635`). Fixed-end reads are "captured consistency,
  not source finality"; indexing lag reaches 23 s (`docs/15-SOURCES.md:78`). Runtime cursor
  contract: `start = cursor − 1` (`trade_poller.rs:399`).
- `BucketCommitEngine::commit` applies mutations and persists arithmetic fences
  (`bucket_commit.rs:352,382`); partial and late equal-second groups fence at `:290` and `:306`.
- `position_validations` is one row per wallet, overwritten on revalidation and deleted by the
  next position-changing commit (`paper-state/src/schema.rs:186`, `lib.rs:952,1068`) — a current
  membership marker, not history. `CREATE TABLE IF NOT EXISTS` does not add columns to existing
  databases (`lib.rs:461`). Restart recovery rebuilds the ledger from the `leader_positions`
  mirror (`paper_recovery.rs:125`). `activity_groups` retains identity, revision, disposition,
  and proof, not normalized amounts (`schema.rs:101`); raw activity and positions pages enter the
  append-only source log record-once only on the migration/recording path
  (`position_seeder.rs:492`); ordinary anchor groups persist in `activity_groups`; the boot
  binding records path, tail, sequence, hash (`main.rs:362`). `ReconciliationPageEvidence`
  retains URL, hashes, receipt time, schema and parser versions but not `source_id`
  (`reconciliation.rs:67`).
- Admission: the preparer mutex serializes only `prepare` (`watchlist_admission.rs:43,89`);
  after `validate_via_control` it sends `PrepareValidatedAdmissions` (`:116`), whose handler
  rechecks fence and final ledger hash under the orchestrator's single-owner turn
  (`orchestrator.rs:388`; message at `orchestrator_control.rs:35`). The poller sends
  `CommitActivityBucket` independently (`trade_poller.rs:544`) and owns no preparer (`:241`).
- Runtime configuration is an exact 17-key allowlist with SQL parity tests and a guarded
  migration (`runtime_config.rs:22,780`); module constants have precedent
  (`config_poller.rs:25`). `status.json` is the numeric telemetry owner (`status_writer.rs:116`).
- Combo activity becomes `RawOnly` before the ledger (`position-ledger/src/lib.rs:93`);
  `PositionSnapshot` carries no classification; `restore` restores one key (`lib.rs:441`).
- The ranker consumes per-trade `price`, `payoff`, `dollar_size = price × contracts`
  (`scripts/ranker/suff_stats.py:250–268`), never balances: the Forge cache lane is unaffected.
- `InvalidActivityMapping` is never constructed; `ConflictingOutcomeMapping` is emitted
  (`reconciliation.rs:424`).
- Ordinary boot and runtime admissions use the non-recording validator (`main.rs:375,471`); the
  migration-only recording wrapper labels pages without request identity and discards the
  returned sequence (`position_seeder.rs:492`). `activity_groups.proof_json` is committed
  atomically with revision and disposition (`paper-state/src/lib.rs:713`); every new group
  invalidates the current validation (`lib.rs:711`); non-applied dispositions already get
  no-copy records (`lib.rs:769`); cursor semantics are MAX (`lib.rs:941`).
- Ever-traded history consumes first-ever BUYs only; SELLs are non-consuming
  (`docs/_GLOSSARY.md:366`, `bucket_commit.rs:622`).
- Lock order: callers take the preparer mutex before the writer lock
  (`watchlist_capacity.rs:104`); bucket commits take only the writer lock in the orchestrator
  (`orchestrator.rs:424`). Recorder failure is wrapped as `SourceError::Fatal` inside a fetch
  error (`position_seeder.rs:514`); `SourceError` distinguishes `Transient`, `RateLimited`, and
  `Fatal` (`fetcher.rs:38`, `reconciliation.rs:109`).

## Resolved decisions and defaults

- **D1 (owner, 2026-09-01):** positions are the authority for absolute balances at a proved
  anchor; activity is the causal authority after it. No fee modelling, no tolerance comparison,
  no proof removal, no inferred effects.
- **D2 — coverage.** An anchor's `activity_cutoff_unix` is the fixed end of the bracket's **step-3**
  activity read — the last read that precedes positions B. Step 5 only detects already-visible
  post-cutoff activity. Walk (integer-second source times): an event with `source_epoch > cutoff` is post-cutoff and
  applies exactly once whether it indexes before step 5 (bracket defers) or after (applied); an event at or before the
  cutoff that the venue's positions already include is covered and never double-applied; an
  event at or before the cutoff that the positions snapshot does *not* include — positions lagging
  activity indexing, or an event occurring after positions B but stamped at exactly the cutoff
  second — stays absent until the next anchor — the same captured-consistency limit
  the original bracket accepted, now bounded by refresh and visible as anchor age. A wallet with no anchor has cutoff `+∞`: its brackets apply nothing
  arithmetically before positions, so history gaps can no longer fence. Any group with source
  time ≤ the wallet's cutoff is anchor-covered: persisted once in `activity_groups` with
  disposition `anchor_covered` (raw pages enter the source log only on the migration/recording
  path), ever-traded history updated, no ledger mutation, no pending copy, no
  arithmetic fence. A covered group first observed after its anchor (late indexing) records
  `anchor_covered_late`, sets `reanchor_required`, increments the wallet's monotonic
  `coverage_generation`, and invalidates the current validation; that branch runs after
  semantic-revision integrity checks and before both equal-second fence branches. Copy decisions
  for a `reanchor_required` wallet are deferred until its next anchor installs. Cutoffs are
  monotonic across anchors: a candidate whose `activity_cutoff_unix` is below the wallet's stored
  cutoff defers without changing anchor, mirror, validation, or coverage (the existing bracket
  already rejects decreasing fixed ends within one bracket, `position_seeder.rs:422`).
- **D3 — redemptions.** Stamped, non-zero REDEEM keeps the existing exact `LedgerEffect::Redeem`
  arithmetic; underflow after an anchor is a genuine anomaly and still fences. A zero-size or
  outcome-less ordinary REDEEM derives the non-mutating `LedgerEffect::RequiresAnchor`: recorded
  with disposition `reanchor_required_redemption`, no balance change, `reanchor_required` set
  (same generation bump as D2). Combo stays `RawOnly`; zero-share TRADE/SPLIT/MERGE stay
  `RawOnly`. This deletes `RedeemUnattributed`, `AmbiguousRedeem` (error, fence cause, string),
  and the unattributed equal-second guard from PR #553, and the zero-size REDEEM raw-only arm
  from PR #550.
- **D4 — anchor persistence and replay.** Append-only `position_anchors(wallet_hex, anchor_seq,
  anchored_at_unix, activity_cutoff_unix, balances_json /*canonical sorted*/, ledger_hash_after,
  proof_json /*one canonical document: source_id, both positions page-proof hashes with request
  identity and bounds, activity page bounds, observed/received timestamps, schema and parser
  versions, and the source-log generation binding when a recording validator ran*/)`.
  Raw pages are audit evidence bound by hash, not replay inputs. Wallet cursor row gains `activity_cutoff_unix`,
  `coverage_generation`, `reanchor_required` via guarded idempotent `ALTER` (existing-database
  reopen is a scenario). One SQLite transaction per install batch: append anchors, replace each
  wallet's `leader_positions` rows, upsert `position_validations`, write coverage; prebuilt
  in-memory ledgers swap only after commit (bucket-commit precedent). Every newly recorded group stores a versioned effect document
  `{version, effect}` in the existing `activity_groups.proof_json` (`TEXT`, JSON-validated,
  committed atomically with revision and disposition): `Trade {market, outcome, side, price,
  amount}`, `Split {market, amount}`, `Merge {market, amount}`, `Redeem {market, outcome, amount}`,
  and the tag alone for `RequiresAnchor`, `RawOnly`, `Conversion`, `UnknownEffect`. `replay_wallet_ledger` (beside `build_leader_ledger`) rebuilds a wallet by
  ordering its anchors by `anchor_seq`, applying disposition-authorized `activity_groups` effects
  with `source_epoch` in `(cutoff_i, cutoff_{i+1}]` before installing anchor `i+1`, then those
  `> cutoff_final`, always ordered by `(source_epoch, source_trade_id)`; equal-cutoff groups are
  covered; revisions are verified;
  a missing effect document or unknown version is a typed failure. Version contract: legacy rows
  keep their stored dispositions; replay begins at the wallet's first compatible anchor and
  selects the recorded effect/disposition version. No strategy configuration is stored.
- **D5 — install through the existing control message, replaced.** `PrepareValidatedAdmissions`
  becomes `InstallAnchors { installs, acknowledged }` (same batch and acknowledgement shape). The
  bracket's result is `AnchorInstall { wallet, balances, cutoff, proof, expected: ledger hash,
  cursor, anchor_seq, coverage_generation }`. Under one orchestrator turn the handler verifies
  every expectation, cutoff monotonicity (D2), and the current-unfenced predicate for the whole batch (a covered-late or
  redemption event bumps `coverage_generation`, so no separate expected re-anchor state is
  needed), runs the D4
  transaction, then swaps candidates; any mismatch defers the batch. Boot (`validate_direct`)
  installs through the same transaction function under the single-owned boot engine. The
  preparer's flow (`validate_via_control` → `InstallAnchors`) is unchanged in shape.
- **D6 — per-wallet outcomes defer; infrastructure fails.** Retryable per-wallet outcomes, exactly:
  `CausalPositionError::{PositionRevision, InterveningActivity}`;
  `PositionReadError::{MissingActivityMapping, ConflictingActivityMapping,
  ConflictingOutcomeMapping, PositionMappingConflict, DuplicateAsset, SaturatedTerminalPage}`;
  `ActivityReadError::SaturatedTerminalSecond`; and `ActivityReadError::Fetch` /
  `PositionReadError::Fetch` only when the nested `SourceError` is `Transient` or `RateLimited`
  (`crates/source-core/src/lib.rs:41`). All defer without a fence (PR #554's mechanism,
  extended), and `AdmissionPreparer` preserves the typed error rather than stringifying it
  (`watchlist_admission.rs:18`). Boot-fatal: `SourceError::Fatal` (which also wraps recorder
  failure), parse errors, source-log, control-channel, paper-state transaction, and post-loop
  ledger-revision errors. A multi-wallet `prepare` returns failure unless every requested
  admission installs. A universe with zero eligible wallets still fails boot (existing `ensure!`).
  `StableMismatch` and the ledger-versus-positions comparison are deleted: nothing compares any
  more. `InvalidActivityMapping` (never constructed) is deleted.
- **D7 — refresh.** `ANCHOR_REFRESH_SECS` is a service constant (3,600 s, an owner-selected
  operational default recorded only in `docs/_GLOSSARY.md`, not a calibrated value). The trade
  poller is given the existing `AdmissionPreparer` and, after each completed poll round, calls
  the new `AdmissionPreparer::prepare_if_due(wallet, now, refresh_secs) ->
  Result<AnchorRefreshOutcome /*Anchored | Skipped | Deferred*/, AdmissionError>` for **at most
  one** wallet chosen round-robin among wallets whose anchor age exceeds
  the constant or whose `reanchor_required` is set; the method acquires the existing mutex, then
  re-reads anchor sequence, age, and `reanchor_required` and returns `skipped` if no longer due.
  `Deferred` (retryable leaves) continues the poller; an `Err` (control/paper durability) terminates it. Best-effort: stale age is unbounded under
  rate-gate pressure by design and is observable as `oldest_anchor_age_secs` in `status.json`
  (`None` before the first anchor). Readiness unchanged.
- **D8 — no committed harness.** The offline bracket harness stays scratch tooling; its source,
  exact invocation, input census, and outputs are in the evidence archive, and AC9 is an
  operational gate pinned to revision and inputs.

## Chosen architecture

**Product flow (per wallet at boot, at admission, on refresh, on `reanchor_required`).**
1. full-history activity read; record groups; apply only post-cutoff groups;
2. complete positions read A (both partitions);
3. activity read; require no new post-cutoff position-changing group since step 1;
4. complete positions read B; require `A.semantically_equal(B)`;
5. activity read; require no new group since step 3; capture ledger hash, cursor, coverage state.

Result: `AnchorInstall` (D5). Install replaces the wallet's ledger snapshot with A's balances
(exact `ShareAmount`; combo and exact-zero omitted as today) and clears ordinary entries absent
from A. Post-cutoff activity applies causally as today; anything the feed cannot express exactly
routes to `reanchor_required` (D2, D3).

**Existing surfaces extended (no new crate, channel, task, or service).**
- `crates/position-ledger/src/lib.rs`: `LedgerEffect::RequiresAnchor` (non-mutating);
  `replace_wallet_snapshot` (all-or-none). Delete `RedeemUnattributed`, `AmbiguousRedeem`, the
  zero-size REDEEM raw-only arm.
- `crates/service/src/bucket_commit.rs`: coverage branch (D2) and `RequiresAnchor` disposition
  (D3), versioned; `install_anchors` transaction; delete the unattributed equal-second guard.
- `crates/service/src/position_seeder.rs`: bracket yields `AnchorInstall`; `finish` no longer
  compares; deferred set per D6; delete `StableMismatch` and the comparison path.
- `crates/service/src/orchestrator.rs` + `orchestrator_control.rs`: `InstallAnchors` replaces
  `PrepareValidatedAdmissions`; `watchlist_admission.rs` sends it.
- `crates/paper-state`: `position_anchors`; cursor coverage columns with guarded `ALTER`;
  versioned effect document in `activity_groups.proof_json`; `record_position_validations`
  folds into the anchor transaction (no standalone production writer).
- `crates/service/src/paper_recovery.rs`: `replay_wallet_ledger`; recovery loads coverage.
- `crates/service/src/trade_poller.rs`: coverage passed to commits; inline refresh (D7).
- `crates/service/src/status_writer.rs`: `oldest_anchor_age_secs`.
- `crates/source-polymarket-public/src/reconciliation.rs`: delete `InvalidActivityMapping`;
  mapping errors typed per wallet.
- `docs/_GLOSSARY.md` (refresh default), `docs/15-SOURCES.md` (F5; amend the F2 note: burn
  scope unknowable → re-anchor).

**Net-new justification.** One durable table (`position_anchors`: venue-authoritative balances
with proofs — a net-new data shape), three cursor columns (coverage: a net-new invariant), one
constant, one non-mutating effect variant that replaces three redemption code paths.

**Failure handling.** Per-wallet outcomes defer (D6). Compare-and-swap mismatch defers the batch.
Anchor transaction failure leaves ledger, mirror, validation, and coverage untouched (all-or-none).
Late covered group or unknowable redemption → `reanchor_required` → copies deferred until the
next anchor. Refresh starvation → age grows, visible in `status.json`.

**Rejected alternatives.**
- *Fee-modelled reconstruction*: no fee field; residues fit no formula; inexact.
- *Tolerance comparison*: hides transfers/untracked exits; violates exactness.
- *Resolution filtering only*: fixes F2 (149/267) not F1 (104/267).
- *Remove the positions proof*: discards the guarantee #544 exists for.
- *Infer two-leg clearing on every REDEEM* (round-2 candidate): partial redemption is legal
  (F2); clearing would erase a held leg. Re-anchoring answers it with zero inference.
- *Cursor-bounded activity in the bracket*: the positions parser needs the complete mapping (F7).
- *Runtime cadence key*: touches the 17-key allowlist, seed, guarded migration, config hash and
  parity tests for a value nobody tunes at runtime; a constant suffices.
- *A refresh queue/worker*: no queue exists; inline one-wallet-per-round through the existing
  preparer is the smallest shape and needs no new task.
- *Committed harness example*: live-only, not deterministic; the archive is the evidence.

**Why minimum complete.** The proof's install action changes; one non-mutating effect variant
replaces the funded-leg inference; one table and three columns carry coverage; one constant sets
refresh; replay reuses the per-group proof column already committed with each disposition; the
  existing admission message is replaced rather than duplicated; and the change deletes
`StableMismatch`, the comparison path, `RedeemUnattributed`, `AmbiguousRedeem`, the unattributed
guard, and `InvalidActivityMapping`.

## Implementation phases

**Phase 1 — re-anchor bracket (single deployable change).** Paths above; tests in
`crates/position-ledger` (units), `crates/service/tests/scenario_bucket_commit.rs`,
`crates/service/tests/scenario_position_bracket.rs`, orchestrator/poller scenarios, a replay
scenario, and a paper-state upgrade scenario.

Acceptance criteria → deterministic verification (fixed clocks, recorded fixtures, no network):
- AC1 `replace_wallet_snapshot` is all-or-none and idempotent; the canonical capture hash of the
  result equals the hash of the sorted anchored balances → ledger unit + property.
- AC2 An install writes anchor, mirror, validation, and coverage in one transaction; failure
  injected after all four writes and before commit (precedent `paper-state/src/lib.rs:2337`)
  leaves all four and the in-memory ledger untouched; an existing pre-change database reopens with the guarded `ALTER`
  applied exactly once → paper-state + bracket scenarios.
- AC3 A wallet with no anchor: pre-cutoff groups (including a REDEEM/SELL before its BUY, the
  F4 shape) are recorded `anchor_covered`, a covered BUY consumes the market in ever-traded history while
  a covered SELL is non-consuming, and none creates a ledger mutation, pending copy, or fence; after the anchor a post-cutoff BUY/SELL/SPLIT/MERGE and a
  stamped non-zero REDEEM apply exactly (a single-leg redemption leaves the other leg); a
  zero-size or outcome-less REDEEM (live-captured rows) records `reanchor_required_redemption`
  and mutates nothing; an event stamped exactly at the cutoff second is covered → bucket
  scenarios.
- AC4 A covered group first observed after its anchor records `anchor_covered_late` ahead of
  both partial and late equal-second fences, bumps `coverage_generation`, invalidates the
  current validation, leaves no pending continuation, and the classifier's copy path for that
  wallet defers until the next anchor → bucket + orchestrator scenario.
- AC5 Compare-and-swap: (a) positions B, then a poller BUY, then `InstallAnchors` → batch
  deferred, the BUY survives; (b) a covered-late row that changes only coverage state (ledger hash
  and cursor unchanged) → deferred; (c) neither → installs and swaps; (d) a candidate cutoff below the stored cutoff (regressed
  clock fixture) → deferred with anchor, mirror, validation, and coverage unchanged; the asserted
  expectation set is {ledger hash, cursor, anchor_seq, coverage_generation} plus cutoff
  monotonicity and the unfenced predicate → orchestrator scenarios.
- AC6 `replay_wallet_ledger` over recorded anchors plus `activity_groups` under the D4 ordering
  rule (versioned effect documents) reproduces the canonical capture hash with zero network across
  two anchors separated by activity and across a restart (`build_leader_ledger` agrees); a
  missing effect document, unknown effect version, or revision mismatch is a typed failure;
  legacy rows before the first compatible anchor are skipped by contract → replay scenario naming
  the effect schema version and the ordered group query.
- AC7 Every D6 retryable outcome defers without a fence at boot and through the preparer,
  including `ConflictingOutcomeMapping`, duplicate/saturated positions responses, and fetch
  errors nesting `SourceError::Transient` / `RateLimited`; `SourceError::Fatal` (including a
  wrapped recorder failure), parse, source-log, and transaction errors still fail boot; a
  multi-wallet `prepare` with one deferred wallet returns failure → bracket scenarios (extend
  PR #554's).
- AC8 Refresh with a fake clock: `prepare_if_due` anchors a due wallet, returns `skipped` for a
  wallet found no longer due after the mutex-held re-read, returns `Deferred` on a retryable
  outcome and the poller continues, returns `Err` on a paper-state transaction failure and the
  poller terminates; at most one wallet per poll round, one round-robin across all
  due wallets; `oldest_anchor_age_secs` correct and `None` before any anchor → poller scenario.
- AC9 (operational gate, not a unit test): pinned to revision, config hash, source-log tail,
  ranking batch and wallet census, time bounds, and the evidence archive digest (harness source,
  invocation, inputs, outputs) — the offline bracket
  harness over the live watchlist accepts every wallet whose bracket completes under D2–D6;
  counts and archives are posted on this issue before activation; the activation rehearsal
  reaches `installed` with the boot universe equal to that accepted set.

Rollout: #544's operator sequence steps 5–8 (staged build, rehearsal, one restart, verification,
site after one verified projection, Forge lane after purge exit). Rollback: pre-activation the old
binary continues; the new table and columns are additive and ignored by explicit-SQL readers.

## Cross-cutting impact

- **Replay/events:** anchors are versioned, append-only inputs bound to the source-log
  generation; dispositions gain `anchor_covered`, `anchor_covered_late`,
  `reanchor_required_redemption` (versioned); bucket commits, decision continuations, and source
  logs unchanged.
- **Financial semantics:** exact `ShareAmount` throughout; the change deletes inference rather
  than adding arithmetic. Impact cap, Kelly, sizing, fees, concentration untouched.
- **Risk/defaults:** one constant default in `_GLOSSARY.md`. Eligibility unchanged; fence causes
  unchanged except the deleted `AmbiguousRedeem`; the classifier's entry rule is exact at each
  anchor and conservative between anchors.
- **Source contracts:** F1/F2 recorded 2026-09-01 (PRs #551/#553); add F5 and amend F2 per D3.
- **Deployment/config/migration:** additive SQLite table/columns via guarded `ALTER`; no Supabase
  change; site unchanged; Forge cache lane unaffected (F7).
- **Rollback:** as above; `price_impact_cap_bps=100` never reverts.

## Out of scope

Fee modelling; ranker/price-proxy questions (#545); restarting the Forge loop (#545); live
execution; record-once source logs, remaining fence causes, migration machinery.

## Review dispositions

Five adversarial plan-review rounds (reject → reject → reject → reject-on-one-item →
reject-on-cutoff-monotonicity, applied) plus a delta-confirmation pass (approve with one wording
revision, applied). Every
finding was applied except one, declined with reasoning: recording every ordinary boot and
runtime bracket page into the source log. The repository invariant requires the raw payload
hash, source id, timestamps, schema and parser versions — all carried in the anchor proof — not
the bytes; replay consumes recorded balances and effect documents, never pages; the merged #544
already runs ordinary brackets through the non-recording validator by reviewed decision
(`main.rs:375,471`); and hourly refresh would add roughly 75 pages per wallet per hour of source
log with no consumer. That is speculative hardening under the owner's standard. The migration
boot's existing recording path is unchanged.

## Open risks

- Refresh is best-effort under the rate gate; `oldest_anchor_age_secs` is the signal.
- The F5 count across the universe is informational (first comment); deferral handles any count.

## Issue filing / metadata

Repository `sppburke/prediction-markets`. Duplicate search: none open for positions-authority or
fee-adjusted balances (#544 parent; #545 owns ranker/price proxy). Labels: none applied
automatically. Parent: #544 (steps 5–8 blocked on this).
