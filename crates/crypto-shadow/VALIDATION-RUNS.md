# pe-crypto-shadow — validation run log (forensics)

Operational forensics for the BTC latency-arb shadow-measurement harness
(`pe-crypto-shadow`, Strategy 1+, issues #297 / #310 / #319). One section per
4-hour capture run. **This is an immortalized record**: every number below is
Tier-1 (read directly from the run's own SQLite tape / `meta` table / offline
sweep JSON) or explicitly tagged with its lower-tier source and the reason it
cannot be re-derived. The raw tapes for runs 1–2 were **deleted to reclaim disk
after every stat here was extracted** (see *Disposition*); this file is the
surviving record.

Vantage for all runs to date: `vps-ireland` (`82.22.32.225`, Hetzner/FXVPS,
Ireland). Money/edge values are realized **net per share** (a share pays \$1 on
win), i.e. fractions of a dollar on a 0–1 contract, after the documented fee
model.

---

## Run inventory

| Run | Date (UTC) | Binary | Duration | Tape size | Verdict | Disqualifier |
|-----|-----------|--------|----------|-----------|---------|--------------|
| **v2 (pre-#311)** | 2026-06-09, aborted ~T+147 (23:21Z) | #309 (`9d3b366`) | aborted | deleted | INVALID | channel saturation — 78,736 dropped frames, 5m tape dead 73 min |
| **run1** | 2026-06-10 01:39:15Z–05:39:14Z | #311 (`b16624b`, PR #312) | 240.0 min | 7,903,076,352 B (7.36 GiB) | INVALID | AC1: `frames_dropped_clob = 3230` (+ ~25-min CLOB outage) |
| **run2** | 2026-06-10 06:46:44Z–10:46:43Z | #311 (`b16624b`, PR #312) | 240.0 min | 10,786,172,928 B (10.05 GiB) | INVALID | AC1: `frames_dropped_clob = 36428` (steady-state saturation) |
| **run3 (clean attempt)** | launched 2026-06-10 19:53:26Z, ETA ~23:53Z | #317 (`0a74552`, PR #318) | 4 h target | — | *in progress* | — |

Runs 1–2 are the two post-#311 captures that motivated the deeper #317 fix.
The earlier "v2" run (#309 binary) is listed for lineage only — its tape and
`run.log` were deleted on 2026-06-09 and **none of its numbers are recoverable**;
the "78.7k dropped frames" figure quoted in PR #312 belongs to it, **not** to
run1/run2.

### TL;DR verdict

Both runs 1 and 2 ran the full 4 hours and wrote complete tapes, but **both fail
the hard-zero clean-tape acceptance criteria (AC1)** because the #311 binary
still dropped CLOB book frames. They are unusable for a decision-grade #310
sweep. Their realized-edge reads are **not trustworthy** (tiny n, frame-loss,
single-regime) and exist here only as a methodology record. The clean decision
artifact is the run3 (#318 binary) sweep, pending.

---

## Acceptance criteria (clean-tape gate — issue #319)

A tape is clean **iff** a cleanly-shut-down (SIGINT) ≥4 h run ends with **all
three**:

1. **AC1 — `frames_dropped_* = 0`**: every `frames_dropped_{chainlink,clob,bybit,okx,coinbase}`
   key present in `meta` **and** equal to `0`. *Missing keys = crashed / SIGKILL'd
   capture = not-clean* (the keys are stamped only after `run()` returns cleanly).
2. **AC2 — `max_clob_gap_secs < 300`**: largest CLOB inter-frame gap over the run
   (`meta` key, **new in #317** — absent on the pre-#317 run1/run2 tapes).
3. **AC3 — `CLOB_5M_STALENESS_S < 300`**: `(now − MAX(received_at_ms))/1000` over
   `clob_trades WHERE series='5m'` at run end.

`scripts/feed-bakeoff/crypto_shadow_checkin.sh --post` checks all three (since
#320, `2aa36dd`). **Both runs fail on AC1.** Notably, *AC2/AC3 alone would not
have caught either run* (run1 max gap 125.2 s, run2 25.7 s — both < 300 s) — AC1
is the binding constraint.

---

## Root cause and fix lineage

**Symptom (both runs):** the CLOB `5m` series loses frames first, because it
churns the most tokens and loses the drop race.

**Root-cause correction (#317, forensics-proven):** the saturation was **NOT**
"unbounded subscribed-token growth." Both tapes show the actively-captured
market set stays **bounded and flat** across 4 h (run1 4–13, run2 6–16 distinct
`condition_id` per 10-min bucket from `clob_trades`; the broader subscribed
book-frame set is ~17–29 / ~16–26 per memory). The real cause:

- **Head-of-line blocking on the single socket-drain task.** `drive()` performed
  the synchronous `rusqlite` batch flush (`insert_frame_batch`) **and**
  `insert_observations` on the *same* task that polls `rx.recv()`, with no
  `spawn_blocking`. During a flush the bounded channel (8192) was not drained →
  the producer's `try_send` dropped frames.
- **Amplifier: no WS keepalive.** `clob_ws` ignored `Ping` (and the split-stream
  context dropped it) → server-side reconnects (≈144 in run1); each
  full-resubscribe snapshot-burst CPU-stalled the consumer, widening the drop
  window.

**Two manifestations of the one bug:** run1 → a ~25-min near-total CLOB **outage**
(reconnect churn) + 3,230 drops; run2 → **steady-state saturation** (36,428
distributed drops, no outage). Same cause, different shape.

**Fix lineage:**

- **#311 (`b16624b`, PR #312)** — batch-inside-drive one-txn flush (250 ms / 256
  frames, `prepare_cached`, fail-fast); `SubCmd{Add,Prune,ForceReconnect}`
  delivery-gated prune; shared `market_expired` admission guard; per-source
  `frames_dropped_*` `AtomicU64` `meta` stamps; channel 4096→8192. **Made
  saturation *visible* (these runs carry the `frames_dropped_*` keys) but did NOT
  prevent it** — the flush was still synchronous on the drain task. Runs 1–2 are
  the proof it was insufficient.
- **#317 (`0a74552`, PR #318)** — (1) dedicated `spawn_blocking` `writer_loop` fed
  by a bounded `write_channel_capacity = 64` mpsc; `drive` enqueues batched
  `WriteCmd::{FrameBatch,Observations}` and never blocks on SQLite; fail-fast on
  first `DbError` via error-precedence at the writer join. (2) Venue-documented
  CLOB keepalive: app-level text `PING`@10 s + case-insensitive `PONG` read-arm
  filter + 120 s read-idle deadline (half-open detector). (3) Nested fair select
  (outer `biased; shutdown` + inner fair `rx`/`refresh`/`flush`). (4) 3
  `clob_trades` query-side indexes deferred to once-at-end
  `build_clob_trade_indexes` (no schema bump — `SCHEMA_VERSION` stays 4, so these
  old tapes remain openable). (5) New `meta` diagnostic `max_clob_gap_secs`.
  New `_GLOSSARY` defaults: `write_channel_capacity=64`,
  `clob_ping_interval_secs=10`, `clob_read_idle_limit_secs=120`,
  `tape_validity_max_gap_secs=300`.
- **#320 (`2aa36dd`)** — added the AC2 (`max_clob_gap_secs`) query to
  `checkin.sh --post`, completing its all-three-AC check (AC1/AC3 were already in
  `--post` from the #316 live-safe/post split).

---

## Run 1 — CLOB OUTAGE mode

**Tape:** `data/crypto-shadow-tape/crypto_shadow_run1_invalidated.db` (deleted post-extraction).
**Window:** 2026-06-10 01:39:15Z → 05:39:14Z (240.0 min). **Binary:** #311 `b16624b`. **`schema_version` = 4.**

### `meta` (verbatim)

| key | value |
|-----|-------|
| `frames_dropped_clob` | **3230** |
| `frames_dropped_bybit` | 13 |
| `frames_dropped_coinbase` | 31 |
| `frames_dropped_okx` | 10 |
| `frames_dropped_chainlink` | 0 |
| `vantage_rtt` (p50 ms) | clob 19 · gamma 19 · chainlink 20 · coinbase 20 · okx 25 · bybit 41 |
| `lag_clock` | `received_node_v2` |
| `fee_provenance` | `crypto_fees_v2: fee_per_share = 0.07*p*(1-p) (exponent=1); verified 2026-06-08 docs.polymarket.com/trading/fees; entry/taker-buy only; sell-side ambiguous` |
| `max_clob_gap_secs` | *absent (pre-#317 binary)* |

### Capture volume

| table | rows |
|-------|------|
| `raw_ticks` | 10,186,748 → clob 10,000,008 · coinbase 99,482 · okx 50,664 · bybit 36,478 · chainlink 116 |
| `clob_trades` | 101,809 → 5m 85,136 · 15m 16,673 |
| `markets` | 90 → 5m 54 · 15m 36 |
| `observations` | 152 |
| `resolutions` | 52 → yes_won 1: 21 · 0: 31 |

- **CLOB drop rate:** 3230 / (10,000,008 + 3230) = **0.0323 %** (low in aggregate
  but concentrated in the outage).
- **Taker buy:sell skew:** 90,309 buys / 11,500 sells = **7.85 : 1** (thin
  sell-side → relevant to the MM strategy).
- **Active markets / 10-min bucket:** 4–13 (23 buckets) — flat, bounded.
- **Decode stats (from sweep):** `frames_total` 10,186,748 · `clob_book_updates_indexed`
  19,367,150 · `clob_decode_errors` 45 · `exchange_ticks` 186,616 · `chainlink_skipped` 116
  · `book_entries_dropped_after_cap` 0 · `capped_tokens` 0.

### Failure mode — the outage

CLOB book frames were healthy (~20k–77k/min) through minute ~145, then collapsed
to ~0–2 frames/min from minute **~146 through ~170** (11 zero-frame minutes;
recovery ~min 171). The void is punctuated by sparse reconnect frames, so the
**largest single CLOB inter-frame gap is 125.2 s** (repeating ≈ the reconnect
cycle), *not* a single 25-min gap:

| metric (`raw_ticks WHERE source='clob'`, LAG) | value |
|---|---|
| max inter-frame gap | **125.2 s** (125,176 ms) |
| gaps > 10 s | 21 |
| gaps > 60 s | 11 (≈ the reconnect cycles) |
| gaps > 300 s | 0 |

→ A reconstructed AC2 would have **passed** (125.2 < 300); run1 is caught only by
AC1. The outage also **contaminates the lag measurement**: `feed_to_book_lag_ms`
averages **4.3–7.1 s** (vs run2's ~17 ms) — do not read latency off run1.

### Realized reads (DO NOT TRUST — n≈27–48, frame-lossy, single regime)

Directional hit-rate (`observations ⨝ resolutions`, signal "up"⇒YES-wins / "down"⇒NO-wins):

| series | dir | n | hit % |
|---|---|---|---|
| 15m | down | 48 | 77.1 |
| 15m | up | 27 | 44.4 |
| 5m | down | 48 | 72.9 |
| 5m | up | 27 | 48.1 |

Sweep **reference cell** (`3 bps / 300 ms / 1000 ms / top_n 3`, fires 152), realized net/share:

| strategy | 15m down | 15m up | 5m down | 5m up |
|---|---|---|---|---|
| buy_hold (mean, frac+) | +0.093 (75%) | +0.067 (41%) | **+0.1423 (67%)** | +0.024 (37%) |
| scalp (best horizon) | +0.020 @120s | +0.024 @10s | +0.057 @30s | +0.088 @60s |
| mm (mean / fills) | +0.187 / 2 | +0.553 / 1 | +0.089 / 4 | −0.253 / 2 |

MM "fills" are 1–4 per cell → pure noise. `tape_validity.valid = false`;
`fidelity` 152/152 matched, 0 missing/extra, no divergence (replay determinism
holds despite frame loss).

---

## Run 2 — STEADY-STATE SATURATION mode

**Tape:** `data/crypto-shadow-tape/crypto_shadow_run2_invalidated.db` (deleted post-extraction).
**Window:** 2026-06-10 06:46:44Z → 10:46:43Z (240.0 min). **Binary:** #311 `b16624b`. **`schema_version` = 4.**
(Memory records the window as "06:46Z–10:50Z"; 10:50Z is the chain-script check
time, the tape's last frame is 10:46:43Z. `run.log` was **0 bytes** — the
chain-script's Phase-4 SSH hang routed binary output to `/dev/null`; the capture
itself ran end-to-end and wrote the DB.)

### `meta` (verbatim)

| key | value |
|-----|-------|
| `frames_dropped_clob` | **36428** (worse than run1) |
| `frames_dropped_bybit` | 69 |
| `frames_dropped_coinbase` | 107 |
| `frames_dropped_okx` | 125 |
| `frames_dropped_chainlink` | 0 |
| `vantage_rtt` (p50 ms) | clob 18 · gamma 19 · chainlink 21 · coinbase 25 · okx 33 · bybit 41 |
| `lag_clock` | `received_node_v2` |
| `fee_provenance` | *same `crypto_fees_v2` string as run1* |
| `max_clob_gap_secs` | *absent (pre-#317 binary)* |

### Capture volume

| table | rows |
|-------|------|
| `raw_ticks` | 13,649,946 → clob 13,467,810 · coinbase 68,456 · okx 62,547 · bybit 51,018 · chainlink 115 |
| `clob_trades` | 153,873 → 5m 127,483 · 15m 26,388 · NULL-series 2 |
| `markets` | 91 → 5m 55 · 15m 36 |
| `observations` | 196 |
| `resolutions` | 53 → yes_won 1: 24 · 0: 29 |

- **CLOB drop rate:** 36428 / (13,467,810 + 36428) = **0.270 %** (~8× run1),
  **distributed across the whole run** (no outage).
- **Taker buy:sell skew:** 135,227 / 18,646 = **7.25 : 1**.
- **Active markets / 10-min bucket:** 6–16 (25 buckets) — flat, bounded.
- **Decode stats:** `clob_book_updates_indexed` 25,973,480 · `clob_decode_errors`
  46 · `exchange_ticks` 182,015 · `chainlink_skipped` 115 · caps 0.
- The 2 NULL-`series` `clob_trades` (08:07:59–08:08:06Z) are an untagged-market
  anomaly — negligible.

### Failure mode — steady-state, no outage

All 240 minutes present; CLOB frames steady ~25k–81k/min throughout. Largest
inter-frame gap **25.7 s** (gaps > 10 s: 1; > 60 s: 0; > 300 s: 0). Lag is healthy
(`feed_to_book_lag_ms` ~16–20 ms) — confirming the drops are saturation, not an
outage.

- **AC3 caveat:** memory records `CLOB_5M_STALENESS_S = 768 s` (12.8 min) at the
  chain-script check time, i.e. the 5m trade tape died sometime before run end.
  But at the tape's **capture end** the 5m staleness is **0 s** (`MAX(received_at_ms)`
  ≈ `MAX(traded_at_ms)`). The 768 is the gap from the last 5m print to the later
  check, plausibly 5m-market rotation at the instant of the check rather than a
  frozen tape. Run2's true disqualifier is AC1 regardless.

### Realized reads (DO NOT TRUST)

Directional hit-rate (`observations ⨝ resolutions`):

| series | dir | n | hit % |
|---|---|---|---|
| 15m | down | 46 | 56.5 |
| 15m | up | 51 | 58.8 |
| 5m | down | 46 | **87.0** |
| 5m | up | 52 | 55.8 |

Sweep **reference cell** (`3 bps / 300 ms / 1000 ms / top_n 3`, fires 196):

| strategy | 15m down | 15m up | 5m down | 5m up |
|---|---|---|---|---|
| buy_hold (mean, frac+) | −0.095 (50%) | +0.069 (59%) | **+0.2163 (80%)** | −0.055 (48%) |
| scalp (best horizon) | +0.067 @120s | +0.021 @10s | +0.152 @120s | +0.039 @10s |
| mm (mean / fills) | — / 0 | — / 0 | +0.315 / 10 | −0.083 / 8 |

`tape_validity.valid = false`; `fidelity` 196/196 matched, 0 missing/extra, no divergence.

---

## Cross-run signal observations (still DO NOT TRUST)

- **5m-down buy_hold is the only consistently-positive cell** (run1 +0.142 @ 67%,
  run2 +0.216 @ 80%) — the strongest apparent edge, but n≈46–48 on lossy,
  single-regime tapes.
- **The tapes disagree where the sample is thin:** 15m-down buy_hold is +0.093
  (run1) vs −0.095 (run2) — a sign flip from pure sampling noise.
- **MM is fill-starved at the reference config** (0–10 fills per series/direction
  in the 3 bps/300 ms/1000 ms/top_n 3 cell; fills grow at looser thresholds across
  the grid — up to ~249/289 in the most permissive cells) → reference MM cells are
  noise-dominated and the MM model is an explicit upper bound (see fee model).
- **`observations.net_edge_mid` is negative for most cells** (the entry-time
  modeled net edge — the fee hurdle bites, especially short-horizon near 50/50),
  *yet* directional hit-rates and realized buy_hold are often positive. These are
  **different metrics** (entry estimate vs post-resolution realized) and must not
  be conflated. None is a deploy signal.

---

## Sweep methodology (immortalized — applies to the clean run too)

- **Command:** `pe-crypto-shadow sweep --db <copy> [--out <json>]`. Run on a **copy**
  of the tape, never the live DB.
- **Grid (480 cells):** `threshold_bps {1..10} × window_ms {50,100,150,200,300,500,750,1000}
  × cooldown_ms {500,1000,2000} × top_n {2,3}`. `reference_params` =
  3.0 bps / 300 ms / 1000 ms / `min_venues` 2 / `top_n` 3. Scalp horizons {10,30,60,120} s.
  Each cell scores buy_hold / scalp / mm × {5m,15m} × {up,down}.
- **`capture_config_provenance`:** *"capture config assumed = sweep config (tape
  does not record trigger params)."* (A follow-up to stamp capture-time trigger
  params into `meta` is filed against #310.)
- **Fidelity gate:** replay must reproduce the live `observations` row-for-row
  (`live_rows == replay_rows == matched`, `missing == extra == 0`, no
  `first_divergence`). **Independent of frame loss** — passed on both invalid tapes.
- **`tape_validity`:** the sweep self-rejects (`valid=false`) when any
  `frames_dropped_* > 0` per the #311 contract.
- **Fee models (`crypto_fees_v2`, verified 2026-06-08; rebate verified 2026-06-09):**
  - base: `fee_per_share = 0.07·p·(1−p)` (exponent 1); entry/taker-buy only; sell-side officially ambiguous.
  - **buy_hold:** `net = (won?1:0) − entry_ask − taker_fee(entry_ask)`.
  - **scalp:** `net = exit_bid(fire+H) − entry_ask − taker_fee(entry_ask) − taker_fee(exit_bid)` (exit-leg fee charged conservatively).
  - **mm:** `net = (won?1:0) − resting_bid + 0.20·taker_fee(resting_bid)` — an **upper bound ×2** (front-of-queue fills + per-fill rebate idealization of the daily pro-rata pool); rebate rate from per-market `feeSchedule.rebateRate`.

---

## Operational learnings (carry forward to every run)

- **SIGINT only.** Only a clean SIGINT exit reaches the post-loop `meta` stamp
  (`frames_dropped_*`, `max_clob_gap_secs`) + `build_clob_trade_indexes`. A
  SIGKILL/`pkill` leaves a not-clean tape with **missing AC keys**. Drive the 4 h
  via `timeout -s INT 14400`.
- **Never `pkill -f pe-crypto-shadow`** — the ssh shell's own cmdline matches the
  pattern → self-kill. Kill by PID filtered on `ps -o comm=` (`pe-crypto-shado` /
  `timeout` / `bash`).
- **Never read the live DB / run `resolve` mid-capture** — it stalls the batched
  flush behind SQLite's single-writer lock → frame drops → tape invalidated. Use
  `checkin.sh` default (live-safe) during the run; `--post` (all 3 AC + resolve +
  report) **only after the process has exited**.
- **Detach correctly.** Launching over a one-shot ssh with a bare backgrounded
  `setsid` keeps the remote login shell open and hangs the caller (this 0-byte'd
  run2's `run.log`). Use `ssh -f` or append `; exit 0`.
- **Disk:** ~2.7–2.8 GB/hr (run3 measured ~40–41 MB/min sustained ≈ 2.4 GB/hr).
  Need ≥ ~13 GB free for 4 h. The disk watchdog **SIGINTs** (not SIGKILLs) at the
  floor so the tape still finalizes.
- **Chainlink settlement key deferred (\$5,000/mo).** The 5m/15m markets settle on
  the Chainlink BTC/USD Data Stream (API-key-gated; "Ultra fast" \$5k/mo). The
  harness runs **key-free end-to-end**: signal = cross-exchange consensus median
  {bybit,okx,coinbase} (#306); realized/scoring = free Gamma resolutions (#307).
  The decision is whether the edge justifies the \$5k/mo *before* paying.

---

## Gaps — what is NOT recoverable

- **144 server-side reconnects (run1):** cited from the #317 forensics but derived
  from the VPS `run.log`, now wiped. The repeated 125.2 s CLOB gaps are
  Tier-1-consistent with reconnect churn, but the exact count cannot be
  re-derived (`raw_ticks` has no reconnect-event record).
- **Per-run `run.log` drop-warning / "subscribed new CLOB" tallies:** gone —
  run1's `run.log` was wiped with the VPS; run2's was 0 bytes (SSH-hang).
- **`max_clob_gap_secs` binary-stamped value (run1/run2):** never existed
  (pre-#317). The LAG()-recomputed gaps here (125.2 s / 25.7 s) are the substitute.
- **The "v2" #309 run** (78,736 drops, 73-min 5m death): tape + log deleted
  2026-06-09; distinct from run1/run2 and unrecoverable.

---

## Disposition

- **Deleted (disk reclaim, ~17.4 GiB):** `crypto_shadow_run1_invalidated.db`
  (7.36 GiB) and `crypto_shadow_run2_invalidated.db` (10.05 GiB), after every stat
  above was extracted. They were invalidated (AC1) and superseded by the run3
  (#318) tape; their only residual use was offline #310 sweep dry-runs, already
  captured in `sweep_run{1,2}.json`.
- **Retained locally (tiny, irreplaceable once the tapes are gone):**
  `data/crypto-shadow-tape/sweep_run{1,2}.json` (2.1 / 2.8 MiB) — the full 480-cell
  sweep outputs these tables summarize. (`data/` is gitignored, so these are not
  committed; this doc is the immortalized summary.)
- **Supersedes:** the clean run3 (#318 binary, all-3-AC) sweep is the real
  decision artifact for whether the BTC latency-arb edge survives fees. Until it
  passes, **no number in this file is a deploy signal.**
