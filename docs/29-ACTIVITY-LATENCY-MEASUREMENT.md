# 29 — Polymarket `/activity` Attribution-Latency Measurement

**What & why.** *(2026-08-25, #530: the REST-only premise is superseded — the
live-data activity websocket carries attributed trades; see the addendum at the end.
The CLOB market channel remains wallet-anonymous, and this file's REST measurement
stays authoritative for the fallback path.)* Copy-trading a *named* leader on
Polymarket historically used the REST `data-api.polymarket.com/activity?user=<wallet>`
feed — the CLOB WebSocket print is wallet-anonymous (`docs/15-SOURCES.md`, issues
#282/#300). The binding question for
copying **near-resolution first-bets** (first buy in a market placed minutes before it
resolves — a high-conviction signal) is: *how long after a leader's trade can our poller
actually see it?* That visibility lag sets the lowest reliable time-to-resolution (TTR)
floor for the Winner-Follow 72h buy-and-hold cohort.

Harness: `scripts/measure_activity_latency.py` (stdlib-only; re-runnable).

## Method

1. Seed an "active-today" basket from the VOL/PNL `timePeriod=DAY` leaderboard (wallets
   trading right now).
2. Fix `baseline = measurement start` (server clock); only count trades with
   `timestamp > baseline` (fresh during the window, never a backlog).
3. Round-robin poll `/activity?user=W&type=TRADE&start=<baseline>`. For each new
   `transactionHash`:
   - **upper bound** = `first_seen_wallclock − trade.timestamp` (includes our poll jitter);
   - **proven lower bound** = `prior_poll_wallclock − trade.timestamp`, recorded only when
     the *previous* poll of that wallet was already after the trade timestamp yet did not
     contain the trade — i.e. the trade was provably not yet visible then. This is
     **jitter-independent** and is the clean infra-latency number.
4. Clock skew (server − local) read from the HTTP `Date` header and smoothed (~0 here;
   NTP-synced).

`trade.timestamp` is second-granular (±1s rounding noise on individual lags; washes out
over percentiles).

## Result — run 2026-06-14 (15 min, 60 wallets, 4,655 trades, 0 errors)

| metric | n | p50 | p90 | p95 | p99 | max |
|---|---|---|---|---|---|---|
| **Proven indexing lag** (lower bound, jitter-free) | 559 | 1.2s | 3.2s | **3.8s** | 5.2s | 23s |
| Upper bound (incl. poll jitter) | 4,655 | 16s | 26s | 28s | 40s | 66s |

- 99.9% of trades seen ≤ 60s; 100% ≤ 120s — even with the inflated upper bound.
- Achieved poll rate was **~2.3 req/s** (not the 10 target — HTTP RTT dominated), so the
  real wallet-revisit interval was **~26s**, which is what inflates the upper bound. The
  **proven lower bound (~1–4s) is the true `/activity` indexing latency** and is unaffected
  by our cadence.

**Conclusion: Polymarket `/activity` indexes trades in ~1–4s (p95 3.8s, p99 5.2s, rare tail
to ~23s).**

## Implication for the copy-trade TTR floor

End-to-end observe→act latency on the POLL path = `/activity` indexing (~1–4s, p99 5s)
+ the poller's revisit time (round duration + `trade_poll_interval_secs`, 30s in
production) + order placement ≈ **~20–35s typical**. This is the fallback path's
budget; the websocket path below is the primary (#530).

- A **1-minute TTR floor is reliably copyable** (≥ ~40s of margin after worst-case latency).
- Sub-minute breaks down (a 30s-TTR trade observed at +15–20s leaves too little to fill).

→ Historically, the 72h buy-and-hold ranking used **`--min-ttr-hours 0.0167` (60s)** as
the REST reliability floor. Classifier 6 (#739) replaces that production floor with the one
[`MIN_TTR_SECS` value](_GLOSSARY.md#ranking-horizon-floor) used by both pass-two calls and
recorded from the scoring run in the published batch; the copy gate aligns in pe-service release 2
on [#588](https://github.com/sppburke/prediction-markets/issues/588). The measured REST fallback
latency above remains historical evidence, not the current ranking floor. The *capturable* edge
near resolution is governed separately by latency-shifted fill pricing in the ranking.

## Caveats

- Indexing latency was measured on liquid VOL/DAY-leader markets; the indexing *pipeline* is
  infra-level and not expected to vary by market, but fill *liquidity* near resolution is a
  separate question handled in the ranking.
- Measured concurrently with a running `pe-bootstrap backfill` (~20 req/s from the same IP);
  the added ~2.3 req/s caused 0 errors. If anything this biases the latency *high*.
- Re-run before trusting for a new regime: `python3 scripts/measure_activity_latency.py
  --duration-secs 900`.


## Addendum (2026-08-25, issue #530): attributed websocket measurements

`wss://ws-live-data.polymarket.com`, subscription
`{"action":"subscribe","subscriptions":[{"topic":"activity","type":"trades"}]}`,
streams every platform trade with `proxyWallet` (docs/15 entry + re-check policy).

- **Latency** (trade `timestamp` → local receipt, NTP-synced, 120s capture,
  6,293 trades): p50 0.80s / p90 1.25s / p95 1.32s / p99 1.41s / max 1.52s.
  Stable across a 14h soak (median minute-p95 1.31s; worst single minute 6.49s).
- **Continuity**: the stream was live only ~113/840 soak minutes; sockets stay
  ping-alive while the subscription silently lapses (1,442 thirty-second silences
  vs 16 hard disconnects). 2026-08-31 (#546): the stall is connection-local — one
  of several parallel connections freezes while the others deliver every watched
  row; re-subscribing does not revive it; a fresh connection delivers within ~0.8 s;
  the largest activity gap on a healthy connection was 4.775 s. Consequence: three
  independent readers per process with a 30 s normalized-row timeout and direct
  reconnect (`pe-service::activity_ingest`; constants in `_GLOSSARY.md`) and the
  always-on REST poll fallback are load-bearing.
- **Payload**: `proxyWallet`, `conditionId`, `asset`, `outcome`/`outcomeIndex`,
  `price`, `size`, `side`, `timestamp` (string seconds), `transactionHash`;
  `fee` optional per trade; schema otherwise stable all night.
- **End-to-end websocket-primary paper copy** ≈ feed (p95 1.32s) + best-ask fetch
  (p50 64ms from the VPS) + commit round-trip (p50 163ms) ≈ **1.0s p50 / 1.6s p95**,
  the basis for `LATENCY_SHIFT_SECS = 2` (conservative rounding; +1-week re-check
  per `_GLOSSARY.md`).

Harnesses: `scripts/probe_activity_ws.py` (feed discovery/attribution re-check),
`scripts/measure_activity_ws_latency.py` (latency), `scripts/soak_activity_ws.py`
(continuity + bench-wallet reaction capture). Re-run the probe before each deploy
relying on the feed (officially listed endpoint; the first-party client publishes
the subscription/payload contract; no published continuity guarantee — `docs/15`).

## #730 acceptance measurement

Use [#730 AC10](https://github.com/sppburke/prediction-markets/issues/730) for the fill-cohort
size and latency acceptance bounds. The measurements below are the audit recipe, not a claim
that the deployed service has passed. Keep four populations distinct:

- The first post-deploy websocket-fill cohort, with source identity, continuation version and
  applied configuration fixed per row. Report each stage's available count, median, p90 and
  maximum, plus the slowest decision in every bucket containing multiple decisions.
- Every proven first-entry BUY in the same window from a wallet in the service's **recorded
  membership when it traded**, including wallets subsequently removed and every leader price.
  A current-watchlist join or price-band screen cannot define this population.
- BUY units whose first-entry status depends on ordering inside one source second. List these
  separately; each requires a recorded ambiguity refusal or a justified causal suppression.
- Every new paper fill in the window, websocket or REST, enumerated independently of the
  populations above for the converse reconciliation below.

Join `decision_pending` by the canonical source group id to its authenticated websocket receipt,
complete activity-page receipts, `activity_groups` and `entry_gate_results`. Use the gateway
receipt's `received_at` as the websocket origin, not the venue's second-granular timestamp.
Measure history-page receipt, `book.fetched_at_unix_ms` and durable fill completion from that
origin. For a fill, durable completion is the recorded `terminal_transition` clock: the paper
owner renders it **after** the synchronized `FinancialFinal`, through
`supabase_state::terminalize_final_fill_decision` and `orchestrator::render_pending_evidence`.
The Final envelope's `received_at` is sampled before sync and is not that endpoint.
No-copy/no-fill terminal clocks do not establish a synchronized financial fill.

Book receipt and book use are separate endpoints. The early `/book` read can finish before
admission; use `book_staleness_check` for the recorded use check, and
`initial_staleness_gate` for the available decision-start boundary. Report their span separately
from websocket-to-book receipt. A continuation lacking either clock has a missing span; do not
substitute an admission receipt, Prepared timestamp, bucket timestamp or a zero. Report missing
clocks, future/negative spans, provenance exclusions and restart-recovered decisions explicitly.
Report 429 retry time, reads per confirmation, retry-interval waits and urgent-slot waits
separately from page transport and decision work; unavailable wait clocks remain unknown.

For a paired projection, freeze the same recorded fill identities and original stage endpoints
on both sides. Attach the benchmarked scan costs and measured gate waits/serial reads to each
row, subtract only the work the implemented change removes or overlaps, then recompute each
row's projected endpoints before taking population percentiles. Keep queue delay and the
unmeasured tail unchanged; do not subtract one aggregate median from another. Report inputs,
sample counts, missing spans and the matched before/projected distributions. New confirmations
and recovered first entries are separate populations; they are not gains measured by this paired
fill projection. Deployed measurements remain the acceptance authority.

Run the acceptance audit read-only on captured verified log prefixes and a consistent database
snapshot; record deployment revision, config hash, era, window and prefix identities. Prove the
first BUY from complete attributable public `/activity` history and join it to the recorded
membership and local group/gate/decision evidence. Classify every member of the audit population
as filled, correctly refused, refused under the frozen thin-book or VWAP-rounding rules that
Part 2 replaces, or suppressed by a required causal re-anchor. Opposite-outcome entries stay
permitted in Part 1 (the paper hold is keyed by market and outcome): list their fills; a refusal
for holding the other outcome is a miss. Trace a suppression to the
causing groups in `activity_groups` insertion order, not only the recorded trigger id: a mixed
bucket may name a twin while a genuinely new or late group requires the flag. Covered non-twin
arrivals, unknown-condition redemptions and unresolved non-combo identities retain their
[canonical causal rule](_GLOSSARY.md#causal-re-anchor-and-rehearsal-rules-557).
All-twin buckets and known-condition redemptions/combos reaching ordinary routing must not
create re-anchors. Any other miss fails acceptance; an unproved first-entry status is reported
as unknown rather than silently removed. Conversely, reconcile every new paper fill in the window,
websocket or REST, to a proven first-entry BUY by a wallet in the recorded membership when it
traded; a fill without that proof, a same-second-order ambiguity included, fails acceptance.

Inspect the next daily marks against recorded closure proof and available samples under the
canonical closed-mark rule. A usable in-lookback sample with valid closure proof must not produce
an invalid midnight mark; cases lacking that evidence retain their recorded fail-closed cause.
Post the audit results to #730, then the final results to #588 and #530; issue closure waits for
AC16. This recipe authorizes no production mutation or live order.

**Part 2 AC15/AC16.**

AC15 checks the first post-deploy continuation-7 frame fill before and after its REST counterpart
commits. AC16 freezes the earliest qualifying post-deploy frame-fill cohort **before** inspecting
clocks; cohort size and stage bounds come from [#730 AC16](https://github.com/sppburke/prediction-markets/issues/730).
Apply the deployment sequence cutoff to `decision_inputs.admission_receipt.sequence`, including a
pre-deployment frame recovered and admitted after deployment. Keep that frame's original synchronized
receipt as the latency origin. Do not replace a cohort member with a later fill when evidence is missing or a span is invalid.
A re-measurement the owner orders uses a recorded boundary sequence in place of the deployment sequence, with the same bounds.
Keep each row's continuation, source authority, applied configuration hash, Start identity and
source/paper prefix identities. Preserve the Part 1 population and causal audit above, with the
[continuation-7 authority rules](_GLOSSARY.md#continuation-and-commitment-compatibility-588):
frame fills prove admission-time first-entry knowledge; REST fills prove complete-history first
entry. Replaced thin-book/VWAP refusals and a cross-leader paper hold cannot excuse Phase 2 misses.
Frame admission requires the financial Start; before Start a synchronized frame only triggers
Part 1 reconciliation and creates no admission, decision or incident.

Capture only after the cohort closes, never while a latency cohort is collecting: heavy reads and
large writes on the host delay pe-service's durable appends (on 10/5 two AC16 tail samples coincided
with such jobs). The read-only capture below copies `paper_state.db` with the SQLite backup API in one
step (one read transaction; the page copy keeps rowids and committed WAL state; the CLI `.backup`
steps 100 pages at a time and restarts on every production write), then the source frames from the
capture start through the last complete frame, filtered to the source IDs the recipe reads, then
the whole paper log. The capture start is separate from the cohort boundary. It is the deployment's
first source sequence, or an earlier receipt when a frame received before the deployment was recovered
and decided after it: the inspection authenticates the original frame and frozen Gamma identity receipts
of every continuation-7 frame decision and the receipts referenced by every admission or fallback
artifact. The capture checks these and stops, naming the earliest missing sequence, when one
precedes its start; capture again from a known
receipt at or before that sequence. Below the retention boundary (`source_events.log.retention`) the
capture reads pinned frames by their recorded offsets and walks contiguously from the boundary; move
the start earlier for a required receipt before it, the capture stops if retention did not keep a
required receipt, and derive other offsets only within an unpunched range by walking frame lengths
forward from a known receipt (the deploy boot's checkpoint tail or an earlier capture's recorded
start); do not infer them from trade epochs. The capture records its walk start and the newest
receive time among the frames it lacks below it (`source_coverage.json`), read from the dense receipt
records (`<source-log>.boot-checkpoint.receipts`, kept for retired frames too) through the deployed
binary's checksummed, read-only `--receipt-coverage-json` command; set `PE_SERVICE_BIN` as for the
inspection. The inspection refuses a capture that lacks a frame received from its window's first second
on. A re-measurement uses the same
capture start and passes its own cohort boundary to the inspection below. Retain verification
receipts and physical prefix bounds with the capture.

For #737 AC-B, optionally append `<membership-from-paper-seq>`: the first paper record used
for judgments in `[restart, S]`, moved earlier when an older record substantiates a closing
exclusion. Only membership records at or after that paper sequence extend the reference check;
omitting it retains the frame-only check. A missing earlier receipt stops the capture and names
the earliest required source sequence. The `KEEP` set also retains the five membership sources:
ranking, admission, knockout, capacity-config and deferral. AC16 still selects its original sources.

```bash
python3 - <live-paper_state.db> <live-source_events.log> <live-paper.log> <capture-dir> <capture-start-offset> <capture-start-sequence> [<membership-from-paper-seq>] <<'PY'
import ctypes, ctypes.util, json, os, re, sqlite3, struct, subprocess, sys, time, zlib
from pathlib import Path

live_db, live_source, live_paper, out, start_offset, start_seq = sys.argv[1:7]
start_offset, start_seq = int(start_offset), int(start_seq)
membership_from = int(sys.argv[7]) if len(sys.argv) == 8 else None
assert len(sys.argv) in (7, 8) and (membership_from is None or membership_from >= 0)
out = Path(out); out.mkdir(mode=0o700, exist_ok=True)
z = ctypes.CDLL(ctypes.util.find_library("zstd"))
for name, args in (("ZSTD_decompressBound", [ctypes.c_void_p, ctypes.c_size_t]),
                   ("ZSTD_decompress", [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t]),
                   ("ZSTD_isError", [ctypes.c_size_t])):
    fn = getattr(z, name); fn.argtypes = args; fn.restype = ctypes.c_size_t
KEEP = {b"pe-service.activity-frame-admission", b"pe-service.activity-frame-fallback",
        b"pe-service.activity-read-commitment", b"polymarket-activity-ws",
        b"polymarket-public.activity-reconciliation", b"pe-service.watchlist-ranking",
        b"pe-service.watchlist-admission", b"pe-service.watchlist-knockout",
        b"pe-service.watchlist-capacity-config", b"pe-service.watchlist-deferral",
        b"polymarket.gamma.markets"}

# 1. One backup-API step: a single read transaction and a page copy (rowids kept). The CLI
#    `.backup` steps 100 pages at a time and restarts on every production write.
started = time.time()
src = sqlite3.connect(Path(live_db).resolve().as_uri() + "?mode=ro", uri=True)
dst = sqlite3.connect(out / "paper_state.db"); src.backup(dst, pages=-1); dst.close(); src.close()
print("database copy seconds", round(time.time() - started, 1),
      "snapshot started", time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(started)))
# The capture must hold the original frame and identity receipts of every continuation-7 frame decision.
copy = sqlite3.connect((out / "paper_state.db").resolve().as_uri() + "?mode=ro", uri=True)
required = [s for row in copy.execute(
    "SELECT json_extract(frozen_inputs_json,'$.observed_source_receipt.sequence'), "
    "json_extract(frozen_inputs_json,'$.decision_inputs.inputs.identity.receipt.sequence') FROM decision_pending "
    "WHERE json_extract(frozen_inputs_json,'$.version')=7 "
    "AND json_extract(frozen_inputs_json,'$.source_authority')='activity_frame'") for s in row if s is not None]
copy.close()
# Retention punches the log below its boundary except the frames it pins: a start below the boundary reads
# the pinned frames from the start individually and walks contiguously from the boundary.
walk_offset, walk_seq, pins = start_offset, start_seq, []
retention = Path(live_source + ".retention")
if retention.exists():
    authority = json.loads(retention.read_text())["authority"]
    if start_seq < authority["boundary"]["sequence"]:
        walk_offset, walk_seq = authority["boundary"]["offset"], authority["boundary"]["sequence"]
        pins = sorted((p for p in authority["pins"] if start_seq <= p["sequence"] < walk_seq), key=lambda p: p["sequence"])

def frames(f, offset):
    f.seek(offset)
    while True:
        head = f.read(4)
        if len(head) < 4: return
        (size,) = struct.unpack("<I", head); block = f.read(size); crc = f.read(4)
        if len(block) < size or len(crc) < 4: return  # incomplete final frame: stop before it
        assert zlib.crc32(block) == struct.unpack("<I", crc)[0], offset
        yield offset, head + block + crc, block
        offset += 4 + size + 4

def envelope(block):
    capacity = z.ZSTD_decompressBound(block, len(block)); buf = ctypes.create_string_buffer(capacity)
    length = z.ZSTD_decompress(buf, capacity, block, len(block)); assert not z.ZSTD_isError(length)
    match = re.match(rb'\{"seq":(\d+),"source_id":"([^"]+)"', buf.raw[:length])
    return int(match.group(1)), match.group(2), buf.raw[:length]

# 2. Source frames from the capture start to the last complete frame, filtered to the recipe's IDs.
#    Captured after the copy; admission and fallback artifacts must reference frames inside it.
REFERENCING = {b"pe-service.activity-frame-admission", b"pe-service.activity-frame-fallback"}
def artifact_receipts(sid, text):
    if sid not in REFERENCING: return []
    a = json.loads(bytes(json.loads(text)["payload"]))
    return [a["frame_receipt"]["sequence"]] + ([a["identity"]["receipt"]["sequence"]] if a.get("identity") is not None else [])
pinned = set()
with open(live_source, "rb") as f, open(out / "source_filtered.log", "wb") as w:
    w.write(b"EDGE\x01"); kept = 0
    for p in pins:
        _, raw, block = next(frames(f, p["offset"]))
        seq, sid, text = envelope(block)
        assert seq == p["sequence"] and json.loads(text)["this_hash"] == p["hash"], ("pin", p["sequence"])
        pinned.add(seq); required.extend(artifact_receipts(sid, text))
        if sid in KEEP: w.write(raw); kept += 1
    expected = walk_seq; end = walk_offset; walk_hash = None
    for end, raw, block in frames(f, walk_offset):
        seq, sid, text = envelope(block); assert seq == expected, (seq, expected); expected += 1
        if seq == walk_seq: walk_hash = json.loads(text)["this_hash"]
        required.extend(artifact_receipts(sid, text))
        if sid in KEEP: w.write(raw); kept += 1
        end += len(raw)
print("source sequences", walk_seq, expected - 1, "end offset", end, "kept", kept, "pinned", len(pinned))

# 3. The whole paper log through its last complete frame.
with open(live_paper, "rb") as f, open(out / "paper.log", "wb") as w:
    header = f.read(5); assert header[:4] == b"EDGE"; w.write(header)
    for _, raw, block in frames(f, 5):
        w.write(raw)
        if membership_from is None: continue
        seq, _, text = envelope(block)
        if seq < membership_from: continue
        record = json.loads(bytes(json.loads(text)["payload"]))
        if record.get("record") != "membership_changed": continue
        evidence = record["evidence"]
        references = [evidence.get("ranking_receipt"), evidence.get("config_receipt")]
        references.extend(a["receipt"] for a in evidence["admission_receipts"])
        references.extend(a["causal_receipt"] for a in evidence.get("evictions", []))
        required.extend(r["sequence"] for r in references if r is not None)
# Paper references can only be checked after both finite prefixes have been captured.
missing = sorted(s for s in required if s < walk_seq and s not in pinned)
assert not missing or missing[0] >= start_seq, ("capture again from a receipt at or before", missing[0])
assert not missing, ("retention did not keep a required receipt", missing[0])
# 4. Coverage: the dense receipt records keep every sequence's receive time, retired frames included. The deployed
#    binary reads and checksums them; record the walk start and the newest receive time the capture lacks below it.
newest = None
if walk_seq:
    PE_SERVICE = os.environ.get("PE_SERVICE_BIN")
    assert PE_SERVICE and os.path.isfile(PE_SERVICE) and os.access(PE_SERVICE, os.X_OK), "PE_SERVICE_BIN must name the deployed pe-service binary"
    coverage = json.loads(subprocess.run([PE_SERVICE, "--receipt-coverage-json", live_source, str(walk_seq), *map(str, sorted(pinned))],
                                         check=True, stdout=subprocess.PIPE, text=True).stdout)
    assert coverage["walk_hash"] == walk_hash, "receipt records do not match the walk"
    newest = coverage["newest_lacked_received_ms"]
(out / "source_coverage.json").write_text(json.dumps({"walk_start": walk_seq, "newest_lacked_received_ms": newest}))
PY
```

Record identities and run this inspection on the capture, substituting the cohort boundary
sequence (the deployment sequence, or a recorded re-measurement boundary) and cohort size (one fill
for AC15; the AC16 size for latency acceptance). The inspection keeps only each captured frame's
location in memory and reads, CRC-checks and decodes a frame from the log on access; its source
scans read only the frames of the sources they examine. It counts bindings to observations the capture lacks below its walk start
(before its start, or retired and not pinned) as outside and fails if one binds
an admitted frame decision or an audited identity; a target missing from the walk fails. For each market whose earliest captured BUY is not
before the window, it reads recorded BUYs at or before that stamp from `activity_groups` effects
before applying the window: an identity seen in the capture keeps its earliest stamp, as the
whole-prefix reader does, and any other identity, including a captured one whose recorded effect
corrects it to this market, is an earlier or same-second entry in that market. When such an entry is
the market's first entry inside the window, the capture holds no source evidence for it in that
market: the inspection reports it as unknown and acceptance stays unproven. It also reports raw-only
groups it cannot attribute to a market. `ac16-population.json` keeps the cohort boundary under its
historical key `deploy_source_seq`.
It opens SQLite with `mode=ro` and `query_only`, uses autocommit reads on the captured
snapshot (no long transaction), and reads finite log prefixes. It prints evidence and writes
only `ac16-population.json` in the separate audit directory for the scoped queries below.
Supply explicit Central window bounds with offsets, for example `2026-10-05T08:00:00-05:00`
and `2026-10-05T09:00:00-05:00`; use `-06:00` when Central standard time applies. The window end
must not follow the database snapshot start the capture prints (a conservative cutoff): trades in the
later-captured logs may lack decisions in the database copy and appear as misses.
The decoder requires system `libzstd`; it checks framing/CRC, joins ordinary TRADE rows by the
canonical `g2:` component encoding (using `b3sum`), and preserves envelope receipts. It does not
replace the Rust log verifier or authority-specific semantic verification. Do not run normal service boot, `--report`, recovery or checkpoint
preparation as part of this read-only audit. Set `PE_SERVICE_BIN` to the absolute path of the deployed
`pe-service` binary: the inspection decodes membership records and artifacts only through its
read-only `--canonical-membership-json` command, and authenticates frozen Gamma identities through
its read-only `--verify-frame-identity-json` command. This preserves the service's JSON number
parsing and canonical page hashes. The inspection stops without these commands. It also stops, naming the
record, at a membership record that command cannot decode; AC-B, AC-C and AC16 are then incomplete.

```bash
sha256sum paper_state.db source_filtered.log paper.log source_coverage.json
stat -c '%s %n' paper_state.db source_filtered.log paper.log source_coverage.json
python3 - paper_state.db source_filtered.log paper.log <boundary-sequence> <AC16-cohort-size> <window-start-CT> <window-end-CT> <audit-directory> <<'PY'
import calendar, ctypes, ctypes.util, hashlib, json, os, re, sqlite3, statistics, struct, subprocess, sys, time, zlib
from datetime import datetime
from decimal import Decimal
from pathlib import Path

PE_SERVICE = os.environ.get("PE_SERVICE_BIN")
if not PE_SERVICE or not os.path.isfile(PE_SERVICE) or not os.access(PE_SERVICE, os.X_OK):
    sys.exit("PE_SERVICE_BIN must name the deployed pe-service binary")
z = ctypes.CDLL(ctypes.util.find_library("zstd"))
for name, args in (("ZSTD_decompressBound", [ctypes.c_void_p, ctypes.c_size_t]),
                   ("ZSTD_decompress", [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t]),
                   ("ZSTD_isError", [ctypes.c_size_t])):
    fn = getattr(z, name); fn.argtypes = args; fn.restype = ctypes.c_size_t

def decode(block, where):
    capacity = z.ZSTD_decompressBound(block, len(block))
    assert not z.ZSTD_isError(capacity), where
    out = ctypes.create_string_buffer(capacity)
    length = z.ZSTD_decompress(out, capacity, block, len(block))
    assert not z.ZSTD_isError(length), where
    return json.loads(out.raw[:length])

class Prefix(dict):
    """Frame locations by sequence, in log order: offset << 40 | block size << 8 | source index. Each access reads the
    frame from the log, checks its CRC and decodes it, so memory follows the frame count, not the log size; `values`
    given source IDs reads and decodes only those sources' frames."""
    def __init__(self, path):
        super().__init__(); self.path = path; self.fd = os.open(path, os.O_RDONLY); self.sources = []
    def __getitem__(self, seq):
        location = dict.__getitem__(self, seq); offset, size = location >> 40, location >> 8 & 0xFFFFFFFF
        raw = os.pread(self.fd, size + 4, offset + 4)
        assert len(raw) == size + 4 and zlib.crc32(raw[:size]) == struct.unpack("<I", raw[size:])[0], (self.path, offset)
        return decode(raw[:size], (self.path, offset))
    def values(self, *source_ids):
        indexes = {i for i, s in enumerate(self.sources) if s in source_ids}
        return (self[seq] for seq, location in dict.items(self) if not source_ids or location & 0xFF in indexes)

def read_prefix(path):
    envelopes = Prefix(path); digest = hashlib.sha256(); tail = None
    with open(path, "rb") as f:
        header = f.read(5); assert header == b"EDGE\x01", (path, "header"); digest.update(header)
        offset = 5
        while True:
            head = f.read(4)
            if not head: break
            assert len(head) == 4, (path, "incomplete header", offset)
            size = struct.unpack("<I", head)[0]; block = f.read(size); crc = f.read(4)
            assert len(block) == size and len(crc) == 4, (path, "incomplete frame", offset)
            assert zlib.crc32(block) == struct.unpack("<I", crc)[0], (path, offset)
            digest.update(head); digest.update(block); digest.update(crc)
            e = decode(block, (path, offset))
            # Sequences strictly increase in log order (the log verifier's rule), so log order is sequence order.
            assert tail is None or e["seq"] > tail[0], (path, "sequence order", offset)
            tail = (e["seq"], e["this_hash"])
            if e["source_id"] not in envelopes.sources: envelopes.sources.append(e["source_id"])
            index = envelopes.sources.index(e["source_id"]); assert index <= 0xFF, (path, "source count")
            dict.__setitem__(envelopes, e["seq"], offset << 40 | size << 8 | index)
            offset += 4 + size + 4
    print(path, offset, digest.hexdigest(), tail)
    return envelopes

def payload(e):
    return json.loads(bytes(e["payload"]), parse_float=Decimal)

def receipt(index, r):
    e = index[r["sequence"]]; assert e["this_hash"] == r["this_hash"]
    return e

def ns(value):
    base, fraction, zone = re.fullmatch(r"(.{19})(?:\.(\d+))?(Z|[+-]\d\d:\d\d)", value).groups()
    d = datetime.fromisoformat(base + ("+00:00" if zone == "Z" else zone))
    return calendar.timegm(d.utctimetuple()) * 10**9 + int((fraction or "").ljust(9, "0"))

# Rust owns membership decoding. The deployed binary named by PE_SERVICE_BIN decodes each paper
# membership record and referenced artifact exactly as the qualification verifier does (typed
# serde, the sealed-evidence schema and the admission proof manifest) and prints canonical JSON;
# every comparison below reads that output.
def rust_membership_json(kind, raw):
    out = subprocess.run([PE_SERVICE, "--canonical-membership-json", kind], input=raw, capture_output=True)
    if out.returncode != 0:
        lines = [line.strip() for line in out.stderr.decode(errors="replace").splitlines() if line.strip()]
        raise ValueError(lines[-1] if lines else f"exit {out.returncode}")
    return json.loads(out.stdout)

def membership_artifact(e, expected):
    assert (e["source_id"], e["schema_version"], e["parser_version"], e["content_type"]) == (expected, 1, 1, "json"), "artifact envelope"
    try: return rust_membership_json(expected, bytes(e["payload"]))
    except ValueError as error: raise AssertionError("artifact decode: " + str(error))

def membership_record(e, c):
    evidence = c["evidence"]; references = []; errors = []
    def reference(name, r, expected, wallet=None):
        row = {"reference": name, "seq": None, "hash": None, "expected_source_id": expected,
               "status": "mismatched", "error": None}
        references.append(row)
        try:
            row.update(seq=r["sequence"], hash=r["this_hash"])
            if r["sequence"] not in source:
                row.update(status="missing", error="receipt absent from captured source prefix"); return None
            envelope = source[r["sequence"]]
            assert envelope["this_hash"] == r["this_hash"], "receipt hash differs"
            artifact = membership_artifact(envelope, expected)
            if wallet is not None: assert artifact["wallet"] == wallet, "artifact wallet differs"
            row["status"] = "verified"
            return artifact
        except (AssertionError, KeyError, ValueError, TypeError, ArithmeticError) as error:
            row["error"] = str(error); return None
    # Enumerate references before contextual checks: one bad batch identity must not hide
    # an admission or eviction receipt from the export.
    kind = evidence.get("kind")
    ranking = config = None
    if kind == "full_rerank" or evidence.get("ranking_receipt") is not None:
        ranking = reference("ranking_receipt", evidence.get("ranking_receipt"), "pe-service.watchlist-ranking")
    if kind == "capacity_change" or "config_receipt" in evidence:
        config = reference("config_receipt", evidence.get("config_receipt"), "pe-service.watchlist-capacity-config")
    for field, key, expected in (("admission_receipts", "receipt", "pe-service.watchlist-admission"),
                                 ("evictions", "causal_receipt", "pe-service.watchlist-knockout")):
        for i, item in enumerate(evidence.get(field, [])):
            reference(f"{field}[{i}].{key}", item[key], expected, item["wallet"])
    try:
        reasons = {"full_rerank": ["full_rerank"], "capacity_change": ["capacity_change"],
                   "knockout_backfill": ["knockout_inactivity", "knockout_inactivity_hard_cap", "knockout_underperformance"]}
        assert c["reason"] in reasons[kind], "evidence kind differs from reason"
        if ranking is not None:
            assert ranking.get("batch_id") == c["ranking_batch_id"], "ranking batch differs"
            if kind == "full_rerank": assert c["ranking_batch_id"] is not None, "ranking batch missing"
        if kind == "capacity_change":
            assert c["ranking_batch_id"] is None, "capacity unexpectedly names a batch"
            if config is not None:
                assert config["generation"] == evidence["generation"] > 0 and config["target"] == c["capacity"], "capacity identity differs"
        wallets = [a["wallet"] for a in evidence["admission_receipts"]]
        assert len(set(wallets)) == len(wallets) and set(wallets) == set(c["added"]), "admission wallets differ"
        if kind == "knockout_backfill":
            wallets = [a["wallet"] for a in evidence["evictions"]]
            assert len(set(wallets)) == len(wallets) and set(wallets) == set(c["removed"]), "eviction wallets differ"
    except (AssertionError, KeyError, ValueError, TypeError) as error:
        errors.append(str(error))
    return {"seq": e["seq"], "hash": e["this_hash"], "received_at_ns": ns(e["received_at"]),
            **{k: c[k] for k in ("reason", "removed", "added", "capacity", "ranking_batch_id")},
            "kind": evidence.get("kind"), "references": references, "evidence_errors": errors}

audit_unix_ns = time.time_ns(); print("audit clock", audit_unix_ns)
# The capture holds every receipt from its walk start on and only pins below it. Every frame it lacks was received
# before the window's first second, so it holds no window receipt and, as a receipt never precedes its trade, no
# window trade.
coverage = json.loads(Path(sys.argv[2]).with_name("source_coverage.json").read_text())
lacked = coverage["newest_lacked_received_ms"]
assert lacked is None or (lacked + 1) * 10**6 <= ns(sys.argv[6]) // 10**9 * 10**9, ("capture lacks a frame received from the window on", lacked)
source = read_prefix(sys.argv[2]); paper = read_prefix(sys.argv[3])
# Decode every membership record once; the replay and both exports read only this canonical form.
# The shared replay cannot continue past a record the verifier cannot decode, so it stops here.
membership = {}
for e in paper.values():
    if payload(e).get("record") != "membership_changed": continue
    try: membership[e["seq"]] = rust_membership_json("membership_changed", bytes(e["payload"]))
    except ValueError as error:
        sys.exit(f"membership record {e['seq']} {e['this_hash']} does not decode; AC-B, AC-C and AC16 are incomplete: {error}")
window_start = ns(sys.argv[6]) // 10**9; window_end = ns(sys.argv[7]) // 10**9
assert window_start < window_end
audit_dir = Path(sys.argv[8]).resolve(); assert audit_dir.is_dir()
db = sqlite3.connect(Path(sys.argv[1]).resolve().as_uri() + "?mode=ro", uri=True, isolation_level=None)
db.row_factory = sqlite3.Row; db.execute("PRAGMA query_only=ON")
print("Start", [tuple(r) for r in db.execute(
    "SELECT key,value FROM meta WHERE key IN ('financial_start_seq','financial_start_hash')")])
cohort = list(db.execute("""
SELECT f.*, d.source_trade_id, d.semantic_revision, d.wallet_hex,
       d.frozen_inputs_json, d.post_commit_inputs_json, d.state, d.terminal_disposition,
       g.result, g.history_consumed, h.first_epoch
FROM fills AS f
JOIN decision_pending AS d
  ON f.source_receipt_seq = json_extract(d.frozen_inputs_json,'$.observed_source_receipt.sequence')
 AND f.source_receipt_hash = json_extract(d.frozen_inputs_json,'$.observed_source_receipt.this_hash')
LEFT JOIN entry_gate_results AS g ON g.source_trade_id = d.source_trade_id
LEFT JOIN wallet_market_history_v2 AS h
  ON h.wallet_hex = d.wallet_hex AND h.market_id = f.market_id
WHERE json_extract(d.frozen_inputs_json,'$.version') = 7
  AND json_extract(d.frozen_inputs_json,'$.source_authority') = 'activity_frame'
  AND json_extract(d.frozen_inputs_json,'$.decision_inputs.admission_receipt.sequence') >= ?
ORDER BY f.prepared_seq, f.idempotency_key LIMIT ?
""", (int(sys.argv[4]), int(sys.argv[5]))))
print("FROZEN COHORT", [(r["source_trade_id"], r["idempotency_key"], r["prepared_seq"]) for r in cohort])
# The earlier-entry listing covers every in-window frame admission, not only the latency cohort.
admissions = list(db.execute("""
SELECT source_trade_id, wallet_hex, frozen_inputs_json FROM decision_pending
WHERE json_extract(frozen_inputs_json,'$.version') = 7
  AND json_extract(frozen_inputs_json,'$.source_authority') = 'activity_frame'
  AND json_extract(frozen_inputs_json,'$.decision_inputs.admission_receipt.sequence') >= ?
  AND source_epoch >= ? AND source_epoch < ?
""", (int(sys.argv[4]), window_start, window_end)))
assert len(cohort) == int(sys.argv[5]), "cohort incomplete; acceptance unproven"
spans = {p: [] for p in ("initial_staleness_gate", "book_staleness_check", "terminal_transition")}
book_use = []; book_receipt_spans = []; trade_time_spans = []
for row in cohort:
    print("decision/fill snapshot", dict(row))
    c = json.loads(row["frozen_inputs_json"]); t = json.loads(row["post_commit_inputs_json"])
    frame = receipt(source, c["observed_source_receipt"])
    assert frame["source_id"] == "polymarket-activity-ws"
    proof = c["decision_inputs"]; admission = receipt(source, proof["admission_receipt"])
    assert admission["source_id"] == "pe-service.activity-frame-admission"
    artifact = payload(admission)
    inputs = proof["inputs"]; version = inputs["version"]; assert version in (1, 2)
    assert admission["schema_version"] == version and admission["parser_version"] == 1 and admission["content_type"] == "json"
    body = json.dumps(inputs, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    domain = f"prediction-edge/activity-frame-decision/v{version}\0".encode()
    digest = subprocess.check_output(["b3sum"], input=domain + body).decode().split()[0]
    expected = {"version": version, "frame_receipt": inputs["frame_receipt"], "capture_digest": digest}
    if version == 2:
        identity = inputs["identity"]; expected["identity"] = identity
        provenance = identity["provenance"]; asset = provenance["asset"]
        assert inputs["frame_receipt"] == c["observed_source_receipt"]
        assert asset == payload(frame)["asset"].strip(), "frame identity asset differs"
        assert provenance["source_log_sequence"] == identity["receipt"]["sequence"] < admission["seq"], "frame identity receipt sequence differs"
        gamma = receipt(source, identity["receipt"])
        verified = subprocess.run([PE_SERVICE, "--verify-frame-identity-json"],
                                  input=json.dumps({"identity": identity, "source": gamma}).encode(), capture_output=True)
        assert verified.returncode == 0, verified.stderr.decode()
        token = json.loads(verified.stdout)
        assert token["condition_id"] == c["market_id"] and token["outcome"] == c["outcome_id"], "frame asset differs from claimed market"
        assert token["evidence_hash"] == provenance["canonical_page_hash"]
    assert artifact == expected
    assert digest == c["semantic_revision"]
    print(row["source_trade_id"], c["source_authority"], c["applied_configuration_hash"],
          t.get("financial_semantic_version"), proof, row["result"], row["history_consumed"], row["first_epoch"])
    assert t.get("financial_semantic_version") == 3 and row["history_consumed"] == 1
    prepared = paper[row["prepared_seq"]]; pp = payload(prepared)
    final_ref = t.get("terminal", {}).get("final_receipt")
    if final_ref is None:
        print(row["source_trade_id"], "missing Final receipt; AC15 unproven")
    else:
        final = receipt(paper, final_ref); fp = payload(final)
        assert pp["record"] == "financial_prepared" and fp["record"] == "financial_final"
        assert receipt(paper, fp["prepared_receipt"]) == prepared
        economic = pp["payload"]["economic"]; canonical = fp["result"]["canonical"]
        assert pp["payload"]["operation"]["source_trade_id"] == row["source_trade_id"]
        assert canonical["outcome"] in ("applied", "existing")
        assert canonical["applied_prepared_seq"] == row["prepared_seq"]
        assert canonical["quantity"] == economic["sizing"]["expected_shares"]
        assert canonical["principal"] == economic["sizing"]["principal"]
        assert canonical["fee"] == economic["fee"]["expected_fee"]
        assert Decimal(canonical["fill_price"]) == Decimal(economic["sizing"]["expected_vwap"])
        for column, field in (("quantity_str", "quantity"), ("principal_str", "principal"), ("fee_str", "fee")):
            assert Decimal(row[column]) == Decimal(canonical[field]) / 10**6
        assert Decimal(row["fill_price_str"]) == Decimal(canonical["fill_price"])
        assert economic["applied_configuration_hash"] == c["applied_configuration_hash"]
        assert receipt(source, economic["observation"]["source_receipt"]) == frame
        assert receipt(source, economic["observation"]["complete_bound_receipt"]) == frame
        print(row["source_trade_id"], "Prepared/Final equal", final_ref, economic["book_receipt"])
    times = {}
    for purpose in spans:
        clocks = [v for v in t.get("clocks", []) if v["purpose"] == purpose]
        if len(clocks) != 1:
            print(row["source_trade_id"], purpose, "missing/duplicate clock"); continue
        clock = clocks[0]
        instant = clock["unix_millis"] * 10**6 + (clock.get("submillisecond_nanos") or 0)
        value = instant - ns(frame["received_at"])
        if value < 0 or instant > audit_unix_ns or (purpose == "terminal_transition" and final_ref is None):
            print(row["source_trade_id"], purpose, "invalid span", value); continue
        times[purpose] = instant; spans[purpose].append(Decimal(value) / 10**6)
    if all(p in times for p in ("initial_staleness_gate", "book_staleness_check")):
        value = times["book_staleness_check"] - times["initial_staleness_gate"]
        if value >= 0: book_use.append(Decimal(value) / 10**6)
        else: print(row["source_trade_id"], "invalid decision-start to book-use span", value)
    trade_span = Decimal(ns(frame["received_at"]) - ns(frame["observed_at"])) / 10**6
    if trade_span < 0 or ns(frame["received_at"]) > audit_unix_ns:
        print(row["source_trade_id"], "invalid trade-time span", trade_span)
    else:
        trade_time_spans.append(trade_span)
    book = (t.get("book") or {}).get("fetched_at_unix_ms")
    if book is None:
        print(row["source_trade_id"], "missing book-receipt span")
    else:
        span = Decimal(book * 10**6 - ns(frame["received_at"])) / 10**6
        if span < 0 or book * 10**6 > audit_unix_ns:
            print(row["source_trade_id"], "invalid book-receipt span", span)
        else:
            book_receipt_spans.append(span)
            print(row["source_trade_id"], "frame-to-book receipt ms", span)
    print(row["source_trade_id"], "frame receipt minus source time / feed delay ms", trade_span)
for purpose, values in spans.items():
    values.sort(); n = len(values)
    print(purpose, "n", n, "missing/invalid", len(cohort) - n,
          "median/p95/max ms", None if not n else
          (statistics.median(values), values[(95 * n + 99) // 100 - 1], values[-1]))
print("decision-start to book-use ms", len(book_use),
      None if not book_use else statistics.median(book_use), "missing/invalid", len(cohort) - len(book_use))
for purpose, values in (("book receipt", book_receipt_spans), ("trade time / feed delay", trade_time_spans)):
    values.sort(); n = len(values)
    print(purpose, "n", n, "missing/invalid", len(cohort) - n, "median/p95/max ms",
          None if not n else (statistics.median(values), values[(95*n+99)//100-1], values[-1]))
fallbacks = {}; fallback_rows = []; window_fallbacks = []
for e in source.values("pe-service.activity-frame-fallback", "pe-service.activity-read-commitment",
                       "polymarket-public.activity-reconciliation"):
    if e["source_id"] == "pe-service.activity-frame-fallback":
        a = payload(e); r = a["frame_receipt"]; frame = receipt(source, r)
        assert e["schema_version"] == 1 and e["parser_version"] == 1 and a["version"] == 1
        assert frame["source_id"] == "polymarket-activity-ws"
        assert a["reason"] in ("latched", "history_behind", "earlier_unresolved_buy", "wallet_not_ready",
                               "identity_unverified", "copy_expired")
        key = (r["sequence"], r["this_hash"])
        window_fallbacks.append({"seq": e["seq"], "hash": e["this_hash"],
                                 "recorded_at_ns": ns(e["received_at"]),
                                 "frame_seq": r["sequence"], "frame_hash": r["this_hash"],
                                 "reason": a["reason"], "routing_clock": a["routing_clock"],
                                 "frontier": a["frontier"], "latest_incident_basis": a["latest_incident_basis"]})
        if key not in fallbacks:
            fallbacks[key] = a
            fallback_rows.append({"frame_seq": r["sequence"], "frame_hash": r["this_hash"],
                                  "artifact_seq": e["seq"], "reason": a["reason"]})
            print("earliest fallback", e["seq"], e["this_hash"], a, payload(frame))
    elif e["source_id"] in ("pe-service.activity-read-commitment", "polymarket-public.activity-reconciliation"):
        print(e["seq"], e["this_hash"], e["source_id"], payload(e))
start_seq = int(db.execute("SELECT value FROM meta WHERE key='financial_start_seq'").fetchone()[0])
for e in paper.values():
    if e["seq"] > start_seq and payload(e).get("record") == "feed_incident_changed":
        print(e["seq"], e["this_hash"], payload(e))
groups = {r["source_trade_id"]: dict(r) for r in db.execute(
    "SELECT * FROM activity_groups WHERE source_epoch >= ? AND source_epoch < ?",
    (window_start, window_end))}
keys = {}; matched = set(); legs = {}; buys = {}; frame_rows = []; receipts = {}
admitted = set()
for row in db.execute("SELECT frozen_inputs_json FROM decision_pending "
                      "WHERE json_extract(frozen_inputs_json,'$.version')=7 "
                      "AND json_extract(frozen_inputs_json,'$.source_authority')='activity_frame'"):
    c = json.loads(row[0]); r = c["observed_source_receipt"]
    frame = receipt(source, r)  # authenticate the admitted receipt against the captured prefix
    assert frame["source_id"] == "polymarket-activity-ws"
    admitted.add((r["sequence"], r["this_hash"]))
def frame_row(identity, e):
    r = payload(e)
    qualifying = (r.get("side", "").strip().upper() == "BUY"
                  and Decimal(str(r["size"])) > 0
                  and not r.get("isCombo", r.get("is_combo", False)))
    return {"id": identity, "frame_seq": e["seq"], "frame_hash": e["this_hash"],
            "admitted": (e["seq"], e["this_hash"]) in admitted, "qualifying": qualifying}
for e in source.values("polymarket-activity-ws", "polymarket-public.activity-reconciliation"):
    is_frame = e["source_id"] == "polymarket-activity-ws"
    if not is_frame and e["source_id"] != "polymarket-public.activity-reconciliation": continue
    # Dispatch by the recorded contract; historical pages remain identity-join evidence.
    contract = (e["schema_version"], e["parser_version"])
    if contract not in ((2, 2), (3, 2)):
        raise ValueError(f"unsupported reconciliation page contract {contract}")
    for r in ([payload(e)] if is_frame else payload(e)):
        if r.get("type", r.get("activity_type", "TRADE" if is_frame else "")).strip().upper() != "TRADE": continue
        wallet = r.get("proxyWallet", r.get("proxy_wallet", "")).strip().lower()
        tx = r.get("transactionHash", r.get("transaction_hash", "")).strip().lower()
        condition = r.get("conditionId", r.get("condition_id"))
        condition = None if condition is None else condition.strip() or None
        asset = r.get("asset"); asset = None if asset is None else asset.strip() or None
        outcome = r.get("outcomeIndex", r.get("outcome_index")); label = (r.get("outcome") or "").strip()
        side = r.get("side"); side = None if side is None else side.strip() or None
        parts = [b"prediction-edge/source-polymarket-public/activity-group/v2\0", b"TRADE",
                 wallet.encode(), tx.encode(), None if not condition else condition.strip().lower().encode(),
                 None if not asset else asset.strip().encode(),
                 None if outcome is None or (int(outcome) == 999 and not label)
                 else int(outcome).to_bytes(2, "big"), None if not side else side.strip().upper().encode()]
        encoded = b"".join((b"\0" + bytes(8)) if v is None else b"\1" + len(v).to_bytes(8, "big") + v for v in parts)
        if encoded not in keys:
            keys[encoded] = "g2:" + subprocess.check_output(["b3sum"], input=encoded).decode().split()[0]
        key = keys[encoded]
        if (parts[7] == b"BUY" and parts[4] is not None and Decimal(str(r["size"])) > 0
            and not r.get("isCombo", r.get("is_combo", False))):
            epoch = int(r["timestamp"]); epoch = epoch // 1000 if epoch > 9_999_999_999 else epoch  # production normalization
            entry = {"id": key, "wallet": wallet, "market": parts[4].decode(), "epoch": epoch,
                     "tx": tx, "page_seq": e["seq"], "page_hash": e["this_hash"]}
            if key not in buys or epoch < buys[key]["epoch"]: buys[key] = entry
        if is_frame:
            frame_rows.append(frame_row(key, e))
            epoch = int(r["timestamp"]); epoch = epoch // 1000 if epoch > 9_999_999_999 else epoch
            receipts[e["seq"]] = {"seq": e["seq"], "hash": e["this_hash"],
                "received_at_ns": ns(e["received_at"]), "received_at": e["received_at"],
                "wallet": wallet, "market": None if parts[4] is None else parts[4].decode(),
                "outcome": None if parts[6] is None else int.from_bytes(parts[6], "big"), "side": None if parts[7] is None else parts[7].decode(),
                "id": key, "history_group_ids": [], "epoch": epoch,
                "qualifying": frame_rows[-1]["qualifying"]}
            continue
        if key not in groups: continue
        g = groups[key]; assert g["wallet_hex"] == wallet and g["transaction_hash"] == tx
        matched.add(key); legs.setdefault((wallet, tx), set()).add(key)
        print("REST full-identity join", key, e["seq"], e["this_hash"], g, r)
        for row in admissions:
            c = json.loads(row["frozen_inputs_json"])
            if (wallet == row["wallet_hex"] and parts[4] == c["market_id"].encode()
                and parts[7] == b"BUY" and key != row["source_trade_id"] and g["source_epoch"] < c["source_epoch"]):
                print("earlier BUY candidate; verify restamp equivalence", row["source_trade_id"], key, g, r)
print("unmatched group identities; resolve from authenticated corrections or report unknown", sorted(set(groups) - matched))
print("multi-leg controls", [(w, tx, sorted(ids)) for (w, tx), ids in legs.items() if len(ids) > 1])
# Bind corrected/aliased frame identities using authenticated commitment receipts.
outside = 0; outside_ids = set()
for e in source.values("pe-service.activity-read-commitment"):
    if e["source_id"] != "pe-service.activity-read-commitment": continue
    for b in payload(e).get("bindings") or []:
        r = b["stream_receipt"]
        if r["sequence"] < coverage["walk_start"] and r["sequence"] not in source:
            assert (r["sequence"], r["this_hash"]) not in admitted, ("audited frame outside the capture", r)
            outside += 1; outside_ids.add(b["history_group_id"]); continue
        f = receipt(source, r)
        assert f["source_id"] == "polymarket-activity-ws"
        frame_rows.append(frame_row(b["history_group_id"], f))
        twins = receipts[f["seq"]]["history_group_ids"]
        if b["history_group_id"] not in twins: twins.append(b["history_group_id"])
print("bindings to observations outside the capture", outside)
# Replay recorded membership, retaining removed wallets and all prices.
def canonical_wallet(w):
    # WalletAddress::from_hex: "0x" then 40 hex digits in either case; canonical form is lowercase.
    assert isinstance(w, str) and re.fullmatch(r"0x[0-9a-fA-F]{40}", w), "Start membership wallet"
    return w.lower()
start = payload(paper[start_seq]); members = {canonical_wallet(w) for w in start["membership"]}
changes = sorted((e for e in paper.values() if e["seq"] > start_seq and e["seq"] in membership),
                 key=lambda e: e["seq"])
# The service's replay applies one pinned repair (crates/service/src/paper_recovery.rs HISTORICAL_MEMBERSHIP_PIN):
# six wallets that paper era act-557-62ed205-2's sequence-28 full rerank omitted leave the membership just before
# that record applies. Mirror it as the service does: identity checks over the era from Start, where any partial
# match stops the inspection, and a stop for a pinned era that reaches sequence 28 without any membership record.
REPAIR = {"activation_id": "act-557-62ed205-2", "seq": 28,
          "this_hash": "d76e36115b72ef6b842c425f8bb082522a65ea85c0187a0d8d8c2591a6ea2b5b",
          "raw_payload_hash": "f472fd1aabb73cabc61f3f2baabf1d559a07165b115d05391b06dd20b7e228db",
          "wallets": ("0x3925c4477052d34dc440068286ae693b728242fc", "0x803a112b5eb1404eea463b26a2318cfcb3b9219e",
                      "0xb26dfe6953c9814f03b7f16ac4c717305b4673f7", "0xf25de1a7357bbe92adf84ea58eff101375789d7a",
                      "0x27f738fe203827445690339104aae35b20bc44b0", "0x40604cb1f958c03bea0b18aa43e4cb0d62f33ec3")}
def pinned(e):
    return (start["activation_id"] == REPAIR["activation_id"] and e["seq"] == REPAIR["seq"],
            e["this_hash"] == REPAIR["this_hash"], e["raw_payload_hash"] == REPAIR["raw_payload_hash"])
repairs = {e["seq"] for e in paper.values() if changes and e["seq"] >= start_seq and any(pinned(e))}
for s in repairs:
    assert all(pinned(paper[s])) and s in membership and membership[s]["reason"] == "full_rerank", ("membership repair identity", s)
assert changes or not (start["activation_id"] == REPAIR["activation_id"] and max(paper) >= REPAIR["seq"]), "membership repair: pinned era without its record"
intervals = []; opened = {w: ns(paper[start_seq]["received_at"]) for w in members}
for e in changes:
    at = ns(e["received_at"]); c = membership[e["seq"]]
    if e["seq"] in repairs:
        print("historical membership repair", e["seq"], e["this_hash"], REPAIR["wallets"])
        for w in REPAIR["wallets"]: intervals.append((w, opened.pop(w), at)); members.remove(w)
    for w in c["removed"]: intervals.append((w, opened.pop(w), at)); members.remove(w)
    for w in c["added"]: assert w not in members; members.add(w); opened[w] = at
intervals.extend((w, at, window_end * 10**9) for w, at in opened.items())
first = {}
for b in buys.values():
    k = (b["wallet"], b["market"]); first[k] = min(first.get(k, b["epoch"]), b["epoch"])
unattributable = {}; restamped = []; unknown = []
def captured(sid, wallet, market):
    # Captured evidence for this pair: a recorded market correction is not the captured identity.
    b = buys.get(sid); return b is not None and (b["wallet"], b["market"]) == (wallet, market)
for (wallet, market), epoch in list(first.items()):
    if epoch < window_start: continue  # stamps only move earlier: this first entry precedes the window
    # Recorded BUYs at or before the captured first stamp: an identity seen in the capture keeps its
    # earliest stamp, as the whole-prefix reader does; any other identity is an earlier or
    # same-second entry in this market.
    earlier = list(db.execute(
            "SELECT source_trade_id, source_epoch FROM activity_groups WHERE wallet_hex=? AND activity_type='TRADE' "
            "AND json_extract(proof_json,'$.effect.kind')='trade' AND json_extract(proof_json,'$.effect.side')='Buy' "
            "AND json_extract(proof_json,'$.effect.market')=? AND CAST(json_extract(proof_json,'$.effect.amount') AS INTEGER) > 0 "
            "AND source_epoch <= ?", (wallet, market, epoch)))
    for sid, recorded in earlier:
        if captured(sid, wallet, market) and recorded < buys[sid]["epoch"]:
            restamped.append((sid, buys[sid]["epoch"], recorded)); buys[sid]["epoch"] = recorded
        first[wallet, market] = min(first[wallet, market], recorded)
    entry = first[wallet, market]
    if not window_start <= entry < window_end: continue
    # An in-window first entry recorded only outside the capture has no source evidence here.
    unknown += [(wallet, market, sid, recorded) for sid, recorded in earlier
                if recorded == entry and not captured(sid, wallet, market)]
    (raw,) = db.execute("SELECT count(*) FROM activity_groups WHERE wallet_hex=? AND activity_type='TRADE' "
                        "AND json_extract(proof_json,'$.effect.kind')='raw_only' AND source_epoch < ?",
                        (wallet, entry)).fetchone()
    if raw: unattributable[wallet] = max(unattributable.get(wallet, 0), raw)
print("captured BUY identities with an earlier recorded stamp (identity, captured, recorded)", restamped)
print("in-window first entries without captured source evidence (unknown; acceptance unproven)", unknown)
print("earlier raw-only TRADE groups that cannot be attributed to a market, by wallet", unattributable)
population = []
for b in buys.values():
    if not window_start <= b["epoch"] < window_end or b["epoch"] != first[b["wallet"], b["market"]]: continue
    overlaps = [(lo, hi) for w, lo, hi in intervals if w == b["wallet"]
                and lo < (b["epoch"] + 1)*10**9 and hi > b["epoch"]*10**9]
    if overlaps:
        b["membership_boundary_ambiguous"] = not any(lo <= b["epoch"]*10**9 and hi >= (b["epoch"]+1)*10**9 for lo, hi in overlaps)
        population.append(b)
audited_ids = {r[0] for r in db.execute(
    "SELECT source_trade_id FROM decision_pending WHERE source_epoch >= ? AND source_epoch < ? "
    "AND json_extract(frozen_inputs_json,'$.version')=7", (window_start, window_end))}
audited_ids.update(b["id"] for b in population)
assert not outside_ids & audited_ids, ("capture starts after bindings audited identities need", sorted(outside_ids & audited_ids)[:5])
# AC-C's receipt census is independent of source-time first-entry population membership.
# Count directly from the captured prefix, never from frame_rows or the JSON being exported.
raw_receipt_count = len({(e["seq"], e["this_hash"]) for e in source.values("polymarket-activity-ws")
                         if e["source_id"] == "polymarket-activity-ws"})
print("unique captured feed receipts", raw_receipt_count, "exported receipts", len(receipts))
assert raw_receipt_count == len(receipts), "receipt export dropped captured feed inputs"
raw_window_receipt_count = len({(e["seq"], e["this_hash"]) for e in source.values("polymarket-activity-ws")
    if e["source_id"] == "polymarket-activity-ws" and ns(sys.argv[6]) <= ns(e["received_at"]) < ns(sys.argv[7])})
assert raw_window_receipt_count == sum(ns(sys.argv[6]) <= r["received_at_ns"] < ns(sys.argv[7]) for r in receipts.values())
print("unique in-window feed receipts", raw_window_receipt_count)
membership_changes = [{"seq": e["seq"], "hash": e["this_hash"], "at_ns": ns(e["received_at"]),
                       "removed": membership[e["seq"]]["removed"], "added": membership[e["seq"]]["added"]}
                      for e in changes]
membership_records = [membership_record(e, membership[e["seq"]])
                      for e in paper.values() if e["seq"] in membership]
deferrals = []
for e in source.values("pe-service.watchlist-deferral"):
    if e["source_id"] != "pe-service.watchlist-deferral": continue
    assert (e["schema_version"], e["parser_version"], e["content_type"]) == (1, 1, "json")
    a = payload(e); assert a["version"] == 1
    # Deferrals are audit-only (no Rust decoder); check the writer's serialized shape.
    assert isinstance(a["deferrals"], list), "deferrals array"
    for d in a["deferrals"]:
        assert isinstance(d, dict) and set(d) == {"wallet", "stage", "class", "kind", "message"}, "deferral fields"
        assert isinstance(d["wallet"], str) and re.fullmatch(r"0x[0-9a-fA-F]{40}", d["wallet"]), "deferral wallet"
        assert all(isinstance(d[k], str) for k in ("stage", "kind", "message")), "deferral text"
        assert d["class"] in ("wallet_transient", "wallet_persistent", "shared"), "deferral class"
    deferrals.append({"seq": e["seq"], "hash": e["this_hash"], "received_at_ns": ns(e["received_at"]),
                      "deferrals": a["deferrals"]})
frame_rows = [f for f in frame_rows if f["id"] in audited_ids]
frame_keys = {(f["frame_seq"], f["frame_hash"]) for f in frame_rows}
fallback_rows = [f for f in fallback_rows if (f["frame_seq"], f["frame_hash"]) in frame_keys]
(audit_dir / "ac16-population.json").write_text(json.dumps({
    "deploy_source_seq": int(sys.argv[4]), "window_start": window_start, "window_end": window_end,
    "receipts": list(receipts.values()), "window_fallbacks": window_fallbacks,
    "raw_receipt_count": raw_receipt_count, "raw_window_receipt_count": raw_window_receipt_count,
    "membership_changes": membership_changes,
    "membership_records": membership_records, "deferrals": deferrals,
    "frames": frame_rows, "fallbacks": fallback_rows, "buys": population}, sort_keys=True))
db.close()
PY
```

**#737 AC-B membership evidence (release 1).** The same `ac16-population.json` now exports
`membership_records`: every captured paper `membership_changed` record's sequence, hash,
receive nanoseconds, reason, deltas, capacity, ranking batch and sealed evidence kind. Each
reference names its expected source ID and is `verified`, `missing` or `mismatched` against
the captured source prefix. Verification requires the same sequence and hash, the owner's
source ID, schema 1 / parser 1 / JSON envelope, and decoding as the owner's typed artifact by
`pe-service --canonical-membership-json`, which decodes the record, its sealed evidence and each
artifact exactly as the qualification verifier does, including the admission proof manifest;
every identity comparison reads its canonical output. The artifact structs contain no further
`AppendReceipt` references: admission proof documents are retained JSON preimages and knockout
fill sequence fields are provenance, not sequence/hash receipts. `deferrals` exports every
captured deferral artifact with its sequence, hash and receive nanoseconds, and its full
`deferrals` array, including wallet, class, kind and message. Neither export uses AC16's filters.

Run this extracted reference check in the audit directory. It emits one row per membership
record, with `pass` only when every referenced receipt is verified and the wallet/batch/generation
identities agree; otherwise it emits `incomplete` and the
failing references and evidence errors. Use only records at or before S for AC-B judgments,
including any earlier exclusion record selected by `<membership-from-paper-seq>`. This check
establishes receipt completeness; the Rust verifier remains the authority for policy semantics.
AC-B's live-wallet accounting and operator status/journal commands are in docs/35.

```bash
sqlite3 -readonly -header -json paper_state.db <<'SQL'
WITH membership_inputs AS (
  SELECT value AS j FROM json_each(readfile('ac16-population.json'),'$.membership_records')
)
SELECT j->>'$.seq' AS seq, j->>'$.hash' AS hash, j->>'$.received_at_ns' AS received_at_ns,
       j->>'$.reason' AS reason, j->>'$.kind' AS kind,
       CASE WHEN json_array_length(j,'$.evidence_errors')=0
             AND NOT EXISTS (SELECT 1 FROM json_each(j,'$.references')
                             WHERE value->>'$.status' IS NOT 'verified')
            THEN 'pass' ELSE 'incomplete' END AS verdict,
       (SELECT json_group_array(json(value)) FROM json_each(j,'$.references')
        WHERE value->>'$.status' IS NOT 'verified') AS failing_references,
       j->'$.evidence_errors' AS evidence_errors
FROM membership_inputs ORDER BY j->>'$.seq';
SQL
```

The extracted closing-batch check consumes `ac-b-closing-batch.json`, with this shape (all
times are integer epoch milliseconds; each read records `SELECT max(batch_id) FROM ranking_batches`):

```json
{"s_unix_ms": 1800000060000,
 "reads": [{"started_unix_ms": 1800000050000, "completed_unix_ms": 1800000050100, "batch_id": 86},
           {"started_unix_ms": 1800000060100, "completed_unix_ms": 1800000060200, "batch_id": 87}],
 "publications": [{"batch_id": 87, "committed_unix_ms": 1800000060050,
                   "evidence": "retained publication-commit evidence path and checksum"}]}
```

`publications` is optional and contains retained evidence of actual transaction commitment;
a batch row's `created_at` alone is insufficient. The last read completed at or before S and
the first read started at or after S must agree. When they differ, the earlier batch is selected
only when the later batch ID is exactly the earlier ID plus one and retained evidence shows that
later batch committed strictly after S. Otherwise AC-B remains incomplete: a skipped ID may have
committed before S. Missing brackets, empty maxima, malformed read bounds and conflicting tied
reads also remain incomplete. Reads that straddle S provide no bracket. Retain the input and result.

```bash
sqlite3 -readonly -header -json paper_state.db <<'SQL'
WITH closing_batch_inputs AS (
  SELECT readfile('ac-b-closing-batch.json') AS j
), reads AS (
  SELECT value AS r, value->>'$.started_unix_ms' AS started,
         value->>'$.completed_unix_ms' AS completed, value->>'$.batch_id' AS batch
  FROM closing_batch_inputs, json_each(j,'$.reads')
), before_s AS (
  SELECT * FROM reads, closing_batch_inputs WHERE completed <= j->>'$.s_unix_ms'
    AND completed=(SELECT max(completed) FROM reads WHERE completed <= j->>'$.s_unix_ms')
), after_s AS (
  SELECT * FROM reads, closing_batch_inputs WHERE started >= j->>'$.s_unix_ms'
    AND started=(SELECT min(started) FROM reads WHERE started >= j->>'$.s_unix_ms')
), brackets AS (
  SELECT (SELECT min(batch) FROM before_s) AS earlier_batch,
         (SELECT min(batch) FROM after_s) AS later_batch,
         json_type(j,'$.s_unix_ms')='integer' AND json_type(j,'$.reads')='array'
         AND NOT EXISTS (SELECT 1 FROM reads WHERE json_type(r,'$.started_unix_ms') IS NOT 'integer'
           OR json_type(r,'$.completed_unix_ms') IS NOT 'integer' OR started > completed
           OR json_type(r,'$.batch_id') IS NOT 'integer')
         AND (SELECT count(DISTINCT batch) FROM before_s)=1
         AND (SELECT count(DISTINCT batch) FROM after_s)=1 AS valid,
         j FROM closing_batch_inputs
), result AS (
  SELECT *, valid AND (earlier_batch=later_batch OR (later_batch = earlier_batch + 1
    AND EXISTS (SELECT 1 FROM json_each(j,'$.publications')
      WHERE value->>'$.batch_id'=later_batch AND json_type(value,'$.committed_unix_ms')='integer'
        AND value->>'$.committed_unix_ms' > j->>'$.s_unix_ms'
        AND json_type(value,'$.evidence')='text' AND length(trim(value->>'$.evidence')) > 0))) AS proved
  FROM brackets
)
SELECT CASE WHEN proved THEN earlier_batch END AS closing_batch_id,
       CASE WHEN proved THEN 'pass' ELSE 'incomplete' END AS verdict,
       earlier_batch, later_batch FROM result;
SQL
```

**#737 AC-C receipt census (release 1).** Pin this recipe by the deployed revision and
its `sha256sum` before the swap. Record the deployment boundary source sequence, invocation ID,
`pe-service listening` UTC time, and `window_end` (listening + 7,200 s). After the window
closes, run the capture above once; its printed database snapshot start is the capture cutoff
and must be later than `window_end`. Retain the activation sequence and verified source/paper
prefix identities, plus the exact capture, inspection, journal and census command lines.
Run the inspection with that deployment sequence, **AC16 cohort size 0**, listening as its
window start and `window_end` as its end: its cohort assertion requires an empty latency cohort.
The existing source-time first-entry checks and scoped SQL below still run unchanged.

The export's `receipts` preserves every unique captured feed receipt before the population
filter, with the original authenticated receive text and nanoseconds, normalized identity and
trade epoch, content qualification, and authenticated binding targets in `history_group_ids`.
`window_fallbacks` preserves every captured fallback before the frame-key filter, including
frontier and historical latch evidence. Despite its name this array is unfiltered; the census applies the
receive window. The inspection independently counts feed receipts straight from the captured
prefix, asserts agreement with the export, and repeats that check for the receive window.

Receipt selection is `[listening, window_end)`, whatever its trade-time delay or membership at
trade time. Routing evidence continues through the capture cutoff: a queued receipt may route
after `window_end`. Capture the journal for this invocation through that cutoff, retaining the
journal timestamp and decoding `MESSAGE` as the service's **flattened** JSON event:

```bash
journalctl -u pe-service _SYSTEMD_INVOCATION_ID=<id> --since '<listening UTC>' --until '<capture-cutoff UTC>' -o json > journal.json
python3 - journal.json ignored.json <<'PY'
import json, re, sys
from decimal import Decimal
from pathlib import Path

ignored = []
def require(line, fields):
    assert all(k in line and line[k] is not None for k in fields), ("missing ignored field", fields, line)
for entry in Path(sys.argv[1]).read_text().splitlines():
    entry = json.loads(entry)
    try:
        line = json.loads(entry.get("MESSAGE", ""))
    except (ValueError, TypeError):
        continue
    if not isinstance(line, dict) or line.get("message") != "frame admission ignored": continue
    require(entry, ["__REALTIME_TIMESTAMP"])
    assert re.fullmatch(r"\d+", entry["__REALTIME_TIMESTAMP"]), entry
    require(line, ["receipt_sequence", "receipt_hash", "wallet", "market", "outcome", "source_trade_id", "reason"])
    assert type(line["receipt_sequence"]) is int and line["receipt_sequence"] >= 0, line
    assert re.fullmatch(r"[0-9a-f]{64}", line["receipt_hash"]), line
    assert type(line["outcome"]) is int, line
    reason = line["reason"]
    assert reason in ("identity_seen", "rest_owned", "market_consumed", "not_entry", "not_copy_eligible"), line
    if reason == "identity_seen":
        pairs = (["earlier_receipt_sequence", "earlier_receipt_hash"],
                 ["continuation_source_trade_id", "continuation_semantic_revision"])
        assert any(any(k in line for k in pair) for pair in pairs), line
        for pair in pairs:
            if any(k in line for k in pair): require(line, pair)
    elif reason == "rest_owned":
        require(line, ["semantic_revision", "disposition"])
    else:
        require(line, ["action", "balance", "short_balance"])
        for field in ("balance", "short_balance"):
            assert isinstance(line[field], str) and Decimal(line[field]).is_finite(), line
        line["balance_positive"] = Decimal(line["balance"]) > 0
        if reason == "market_consumed": require(line, ["consuming_source_trade_id", "first_epoch"])
    line["__REALTIME_TIMESTAMP"] = entry["__REALTIME_TIMESTAMP"]
    ignored.append(line)
Path(sys.argv[2]).write_text(json.dumps(ignored, sort_keys=True))
PY
```

Run this extracted SQL in the audit directory on the captured database. Set `listening_ns`,
`window_end_ns` and `capture_cutoff_ns` to exact epoch nanoseconds for the recorded UTC bounds
(the inspection's `ns` conversion preserves fractional seconds). The SQL joins JSON inputs
directly. A decision counts only for its authenticated admitted receipt; a second reader's echo
needs its own ignored line. Binding targets extend a receipt's identity, never its receipt key.
Each qualifying receipt needs exactly one disposition: (a) frame decision; (b) confirmed ignored
reason; or (c) `earlier_unresolved_buy` with a separately authenticated smaller-sequence BUY
and the fallback receipt's **own** history group/gate, plus its decision if that gate admits.
The blocker is reported with whether both receipts traded in the same second; receive-time ties
never override durable sequence order. Other fallbacks, missing evidence and duplicate dispositions
fail. An unexplained `not_copy_eligible` is incomplete: only a captured structural removal with
no subsequent addition before routing, or a still-active fence recorded after receipt and before
routing, confirms loss of live eligibility. A structural addition after either loss invalidates
that explanation. Structural removals preclude live re-entry until structural addition; an
active fence in this snapshot precludes live re-entry while it remains active. Same-second fence
ordering that the persisted second clock cannot establish remains incomplete.

```bash
sqlite3 -readonly -header -json paper_state.db \
  -cmd '.parameter init' \
  -cmd '.parameter set :listening_ns <listening-epoch-ns>' \
  -cmd '.parameter set :window_end_ns <window-end-epoch-ns>' \
  -cmd '.parameter set :capture_cutoff_ns <capture-cutoff-epoch-ns>' <<'SQL'
WITH census_inputs AS (
  SELECT readfile('ac16-population.json') AS population, readfile('ignored.json') AS ignored
), receipts AS (
  SELECT j->>'$.seq' AS seq, j->>'$.hash' AS hash, j->>'$.id' AS id,
         j->>'$.wallet' AS wallet, j->>'$.market' AS market, j->>'$.outcome' AS outcome,
         j->>'$.epoch' AS epoch, j->>'$.received_at_ns' AS received_at_ns,
         j->>'$.qualifying' AS qualifying, j
  FROM (SELECT value AS j FROM census_inputs, json_each(population,'$.receipts'))
), selected AS (
  SELECT * FROM receipts WHERE qualifying=1 AND received_at_ns >= :listening_ns
    AND received_at_ns < :window_end_ns
), identities AS (
  SELECT seq, hash, id FROM receipts
  UNION SELECT r.seq, r.hash, b.value FROM receipts r, json_each(r.j,'$.history_group_ids') b
), ignored AS (
  SELECT value AS j, value->>'$.receipt_sequence' AS seq, value->>'$.receipt_hash' AS hash,
         CAST(value->>'$.__REALTIME_TIMESTAMP' AS INTEGER)*1000 AS routed_at_ns
  FROM census_inputs, json_each(ignored)
), membership AS (
  SELECT value AS j, value->>'$.at_ns' AS at_ns FROM census_inputs,
       json_each(population,'$.membership_changes')
), losses AS (
  SELECT r.seq, r.hash, i.routed_at_ns, m.at_ns AS lost_at_ns
  FROM selected r JOIN ignored i USING(seq,hash) JOIN membership m
    ON m.at_ns > r.received_at_ns AND m.at_ns <= i.routed_at_ns
  WHERE EXISTS (SELECT 1 FROM json_each(m.j,'$.removed') WHERE value=r.wallet)
  UNION ALL
  SELECT r.seq, r.hash, i.routed_at_ns, f.fenced_at_unix*1000000000
  FROM selected r JOIN ignored i USING(seq,hash) JOIN wallet_fences f ON f.wallet_hex=r.wallet
  WHERE f.fenced_at_unix*1000000000 > r.received_at_ns
    AND f.fenced_at_unix*1000000000+999999999 <= i.routed_at_ns
), confirmed_losses AS (
  SELECT l.* FROM losses l JOIN selected r USING(seq,hash)
  WHERE NOT EXISTS (SELECT 1 FROM membership m, json_each(m.j,'$.added') a
    WHERE a.value=r.wallet AND m.at_ns >= l.lost_at_ns AND m.at_ns <= l.routed_at_ns)
), dispositions AS (
  SELECT r.seq, r.hash, 'a' AS class, 1 AS confirmed, 'frame decision' AS reason,
         NULL AS blocking_seq, NULL AS same_trade_second
  FROM selected r JOIN decision_pending d
    ON json_extract(d.frozen_inputs_json,'$.version')=7
   AND json_extract(d.frozen_inputs_json,'$.source_authority')='activity_frame'
   AND json_extract(d.frozen_inputs_json,'$.observed_source_receipt.sequence')=r.seq
   AND json_extract(d.frozen_inputs_json,'$.observed_source_receipt.this_hash')=r.hash
   AND d.wallet_hex=r.wallet
   AND EXISTS (SELECT 1 FROM identities x WHERE x.seq=r.seq AND x.hash=r.hash AND x.id=d.source_trade_id)
  UNION ALL
  SELECT r.seq, r.hash, 'b',
    CASE WHEN i.j->>'$.wallet'!=r.wallet OR i.j->>'$.market'!=r.market
           OR i.j->>'$.outcome'!=r.outcome OR i.j->>'$.source_trade_id'!=r.id
           OR i.routed_at_ns+999 < r.received_at_ns THEN 0
      WHEN i.j->>'$.reason'='identity_seen' THEN
        EXISTS (SELECT 1 FROM receipts earlier JOIN identities x ON x.seq=earlier.seq AND x.hash=earlier.hash
          JOIN identities own ON own.seq=r.seq AND own.hash=r.hash AND own.id=x.id
          WHERE earlier.seq=i.j->>'$.earlier_receipt_sequence' AND earlier.hash=i.j->>'$.earlier_receipt_hash'
            AND earlier.seq<r.seq AND earlier.wallet=r.wallet)
        OR EXISTS (SELECT 1 FROM decision_pending d JOIN identities x ON x.id=d.source_trade_id
          WHERE x.seq=r.seq AND x.hash=r.hash AND d.wallet_hex=r.wallet
            AND d.source_trade_id=i.j->>'$.continuation_source_trade_id'
            AND d.semantic_revision=i.j->>'$.continuation_semantic_revision')
      WHEN i.j->>'$.reason'='rest_owned' THEN EXISTS (
        SELECT 1 FROM activity_groups g WHERE g.source_trade_id=r.id AND g.wallet_hex=r.wallet
          AND g.semantic_revision=i.j->>'$.semantic_revision' AND g.disposition=i.j->>'$.disposition')
      WHEN i.j->>'$.reason'='market_consumed' THEN EXISTS (
        SELECT 1 FROM wallet_market_history_v2 h WHERE h.wallet_hex=r.wallet AND h.market_id=r.market
          AND h.source_trade_id=i.j->>'$.consuming_source_trade_id' AND h.first_epoch=i.j->>'$.first_epoch')
      WHEN i.j->>'$.reason'='not_entry' THEN i.j->>'$.action'!='Entry'
        AND (i.j->>'$.action'!='Add' OR i.j->>'$.balance_positive'=1)
      WHEN i.j->>'$.reason'='not_copy_eligible' THEN CASE WHEN EXISTS (
        SELECT 1 FROM confirmed_losses l WHERE l.seq=r.seq AND l.hash=r.hash AND l.routed_at_ns=i.routed_at_ns)
        THEN 1 ELSE -1 END
      ELSE 0 END,
    i.j->>'$.reason', NULL, NULL
  FROM selected r JOIN ignored i USING(seq,hash) WHERE i.routed_at_ns <= :capture_cutoff_ns
  UNION ALL
  SELECT r.seq, r.hash, 'c',
    f.value->>'$.reason'='earlier_unresolved_buy'
    AND EXISTS (SELECT 1 FROM receipts e WHERE e.wallet=r.wallet AND e.market=r.market
      AND e.qualifying=1 AND e.seq<r.seq)
    AND EXISTS (SELECT 1 FROM identities x JOIN activity_groups g ON g.source_trade_id=x.id
      JOIN entry_gate_results gate ON gate.source_trade_id=g.source_trade_id
      WHERE x.seq=r.seq AND x.hash=r.hash AND g.wallet_hex=r.wallet AND gate.wallet_hex=r.wallet
        AND gate.market_id=r.market AND (gate.result!='admitted' OR EXISTS (
          SELECT 1 FROM decision_pending d WHERE d.source_trade_id=g.source_trade_id AND d.wallet_hex=r.wallet))),
    f.value->>'$.reason',
    (SELECT min(e.seq) FROM receipts e WHERE e.wallet=r.wallet AND e.market=r.market AND e.qualifying=1 AND e.seq<r.seq),
    (SELECT e.epoch=r.epoch FROM receipts e WHERE e.wallet=r.wallet AND e.market=r.market
      AND e.qualifying=1 AND e.seq<r.seq ORDER BY e.seq LIMIT 1)
  FROM selected r JOIN census_inputs p JOIN json_each(p.population,'$.window_fallbacks') f
    ON f.value->>'$.frame_seq'=r.seq AND f.value->>'$.frame_hash'=r.hash
  WHERE f.value->>'$.recorded_at_ns' <= :capture_cutoff_ns
), counts AS (
  SELECT r.seq, r.hash, r.wallet, r.market, r.outcome, r.id, r.epoch,
         count(d.class) AS disposition_count, group_concat(d.class) AS disposition_class,
         min(d.confirmed) AS confirmed, group_concat(d.reason) AS reasons,
         max(d.blocking_seq) AS blocking_seq, max(d.same_trade_second) AS same_trade_second
  FROM selected r LEFT JOIN dispositions d USING(seq,hash) GROUP BY r.seq,r.hash
)
SELECT seq, hash, wallet, market, outcome, id, epoch, disposition_count,
       coalesce(disposition_class,'missing') AS disposition_class,
       CASE WHEN disposition_count!=1 THEN 'fail' WHEN confirmed=-1 THEN 'incomplete'
            WHEN confirmed=1 THEN 'pass' ELSE 'fail' END AS verdict,
       CASE WHEN disposition_count=0 THEN 'no disposition at capture cutoff'
            WHEN disposition_count>1 THEN 'multiple dispositions: ' || reasons
            WHEN confirmed=-1 THEN 'not_copy_eligible: live-set loss unproven'
            WHEN confirmed!=1 THEN 'unconfirmed or unconverged: ' || reasons ELSE reasons END AS reason,
       blocking_seq, same_trade_second
FROM counts ORDER BY seq;
SQL
```

Retain every output row. Any fail reopens AC-C; any incomplete leaves it unproven.
**Fewer than 10 receipts meeting (a) is insufficient evidence, not a pass**, even if every
census row passes. This receipt census does not replace the source-time first-entry audit below.

The `FROZEN COHORT` line is the identity list to retain for all later calculations. Report every
stage's available count, median, nearest-rank p95 (rank `ceil(0.95 × n)`) and maximum, missing
clocks and invalid spans. A short cohort or any missing required span leaves latency acceptance
unproven. Separately report frame-to-book receipt from the authenticated `book_receipt`,
decision-start to `book_staleness_check`, trade-time spans and feed delay; do not substitute
whole-second epochs, revision times, admission clocks, Prepared or Final envelope timestamps
for missing stage clocks. `terminal_transition` is the post-sync fill endpoint. Annotate recovered
work and queueing; retain the first synchronized frame receipt rather than another reader's echo.

For AC15, retain the first row's full frozen continuation, terminal, gate/history rows, Prepared
receipt, Final receipt and exact financial values on both captures. After REST, compare those
same identities and bytes; one leader-ledger effect may be added,
with no second decision, consumption or financial operation. Before deployment, the production
recovery scenarios must prove boot replay both before and after reconciliation; this inspection
never invokes mutating recovery. Verify frame-first copy ownership by wallet, transaction,
recorded asset and side: matching REST groups, restamped twins and partial fills add no copy.
An independent asset or side arriving alone, including after restart, retains ordinary routing.
REST-first exact identities and consumed markets remain refused. Admitted frames leave no
reconciliation obligation; runtime produces no feed audits, contradiction/absence incidents or
latch releases. Version-two admissions freeze authenticated asset identity and require no feed
audit at qualification; version-one admissions retain the historical checker. A mismatched asset
and claimed market must fall back without consuming history. Historical records remain decodable
and selected frozen proofs remain verifiable. At boot, scenario counters must show authentication
only for commitments proving ordinary retirement, supporting surviving ordinary obligations or
verifying open continuations, counted separately, with no stored frontier restore.
Poll requests must remain cursor/obligation bounded rather than forcing feed-audit complete reads;
causal brackets retain their full reads. After restart, frames must fall back until a fresh read
publishes a frontier. A later frame beyond the freshness bound of the first admission can enter
when its own frontier is current. A downtime BUY followed by another BUY in that market must copy
only the first when eligible; transient read failure delays resumption until a successful fresh read,
then qualifying entries copy without an admission cycle.

For REST-decided first entries, search the verified source prefix for the trade's frame, including
authenticated corrections/restamp equivalence. A delayed frame with another asset is an independent
trade under the copy-ownership key; there is no admission-time counterpart search. No recorded frame means **feed-missed**. Otherwise
use the earliest authenticated `pe-service.activity-frame-fallback` artifact per `frame_receipt`,
ordered by artifact source sequence: report its `reason`, `routing_clock`, evaluated `frontier`
and `latest_incident_basis`. Derive wallet/market from the referenced frame, not artifact fields.
Keep historical `latched`, `history_behind`, `earlier_unresolved_buy`, `wallet_not_ready` and
`identity_unverified` and `copy_expired` separate;
a frame with no justified routing artifact is unexplained, never inferred from current status.

Later-discovered earlier entries require `activity_groups` **and** recorded REST page rows, because
late/raw-only `proof_json` can omit market and side. Read the candidate groups and a multi-leg
control from the same captured snapshot. The following commands run in the audit directory
containing `ac16-population.json`; point `paper_state.db` at that captured snapshot.
Each command is an autocommit read scoped to the frozen window or decision receipts.

List **every** continuation-7 complete-read first-entry decision in the window whose read commitment
is at or after the cohort boundary (`deploy_source_seq`: the deployment sequence, or a recorded
re-measurement boundary). Match frames by full identity and authenticated binding targets from
the prefix export. Keep every receipt for feed-presence accounting; for routing choose the
admitted receipt, otherwise the earliest qualifying positive-share, non-combo BUY, otherwise
the earliest receipt, then that receipt's earliest fallback. An excluded zero-share observation
before a qualifying one never owns its routing reason.
`unexplained` means a recorded frame has no fallback artifact and requires investigation:

```bash
sqlite3 -readonly -header -json paper_state.db <<'SQL'
WITH p AS (SELECT readfile('ac16-population.json') AS j),
frames AS (
 SELECT value->>'$.id' AS id, value->>'$.frame_seq' AS seq,
        value->>'$.frame_hash' AS hash,
        row_number() OVER (PARTITION BY value->>'$.id'
          ORDER BY value->>'$.admitted' DESC, value->>'$.qualifying' DESC,
                   value->>'$.frame_seq') AS rn
 FROM p, json_each(p.j,'$.frames')),
fallbacks AS (SELECT value AS j FROM p, json_each(p.j,'$.fallbacks'))
SELECT d.source_trade_id, d.wallet_hex, d.source_epoch,
       f.seq AS frame_seq, f.hash AS frame_hash,
       CASE WHEN f.id IS NULL THEN 'feed-missed' ELSE coalesce((
         SELECT j->>'$.reason' FROM fallbacks
         WHERE j->>'$.frame_seq'=f.seq AND j->>'$.frame_hash'=f.hash
         ORDER BY j->>'$.artifact_seq' LIMIT 1), 'unexplained') END AS routing
FROM p, decision_pending d
JOIN entry_gate_results g ON g.source_trade_id=d.source_trade_id AND g.result='admitted'
LEFT JOIN frames f ON f.id=d.source_trade_id AND f.rn=1
WHERE json_extract(d.frozen_inputs_json,'$.version')=7
  AND json_extract(d.frozen_inputs_json,'$.source_authority')='complete_read'
  AND json_extract(d.frozen_inputs_json,'$.read_commitment.sequence') >= p.j->>'$.deploy_source_seq'
  AND d.source_epoch >= p.j->>'$.window_start' AND d.source_epoch < p.j->>'$.window_end'
ORDER BY d.source_epoch, d.source_trade_id;
SQL
```

Emit the unexplained first-entry BUY set, which **must be empty**. Join successful BUY fills
by the exact `wf|<wallet>|<source_trade_id>|` prefix of the canonical idempotency key;
terminal financial fill evidence leaves its own `idempotency_key` null. The export includes every
earliest BUY second in each wallet/market from the recorded inputs, without a price screen,
and replays membership removals/additions. It retains same-second pieces and flags membership
changes within a venue timestamp's second for investigation. Include frame-only observations
before their REST echo. Prove complete attributable history for this population using the
recorded full-read bounds; absent coverage is unknown and cannot establish acceptance.
Inspect every joined refusal or suppression separately for correctness under its frozen authority;
neither an obsolete thin-book/VWAP rule nor an unjustified causal disposition excuses a miss.

```bash
sqlite3 -readonly -header -json paper_state.db <<'SQL'
WITH buys AS (SELECT value AS j FROM json_each(readfile('ac16-population.json'),'$.buys'))
SELECT b.j, g.result, g.history_consumed, h.first_epoch, a.disposition, a.proof_json
FROM buys b
LEFT JOIN decision_pending d ON d.source_trade_id=b.j->>'$.id'
LEFT JOIN fills f ON f.side='buy'
 AND substr(f.idempotency_key,1,length('wf|' || (b.j->>'$.wallet') || '|' || (b.j->>'$.id') || '|'))
     = 'wf|' || (b.j->>'$.wallet') || '|' || (b.j->>'$.id') || '|'
LEFT JOIN entry_gate_results g ON g.source_trade_id=b.j->>'$.id'
LEFT JOIN wallet_market_history_v2 h ON h.wallet_hex=b.j->>'$.wallet' AND h.market_id=b.j->>'$.market'
LEFT JOIN activity_groups a ON a.source_trade_id=b.j->>'$.id'
LEFT JOIN no_copy_dispositions n ON n.source_trade_id=b.j->>'$.id'
WHERE f.idempotency_key IS NULL
  AND (d.state='terminal' AND (d.terminal_disposition='no_fill' OR d.terminal_disposition LIKE 'no_copy:%')) IS NOT TRUE
  AND coalesce(n.reason,'')=''
  AND (g.result IS NULL OR g.result='admitted')
  AND coalesce(a.disposition,'') NOT IN ('reanchor_required_late_group','anchor_covered_late')
ORDER BY b.j->>'$.epoch', b.j->>'$.wallet', b.j->>'$.id';
SQL
```

Inspect bounded group and multi-leg controls from the same window:

```bash
sqlite3 -readonly -header -json paper_state.db <<'SQL'
WITH p AS (SELECT readfile('ac16-population.json') AS j)
SELECT source_trade_id, activity_type, wallet_hex, transaction_hash, source_epoch,
       semantic_revision, disposition, proof_json FROM activity_groups, p
WHERE source_epoch >= p.j->>'$.window_start' AND source_epoch < p.j->>'$.window_end'
ORDER BY activity_groups.rowid;
WITH p AS (SELECT readfile('ac16-population.json') AS j)
SELECT wallet_hex, transaction_hash, count(DISTINCT source_trade_id) AS legs
FROM activity_groups, p
WHERE source_epoch >= p.j->>'$.window_start' AND source_epoch < p.j->>'$.window_end'
GROUP BY wallet_hex, transaction_hash
HAVING count(DISTINCT source_trade_id) > 1 ORDER BY wallet_hex, transaction_hash;
SQL
```

Decode the referenced authenticated `polymarket-public.activity-reconciliation` pages with the
same prefix reader. Normalize through the recorded parser/schema contract and derive the full
`g2:` identity with `SourceActivityGroupId::derive`: activity type, wallet, transaction hash,
condition, asset, outcome and side. Join that key to `activity_groups.source_trade_id`; never
join on transaction hash alone. Keep page receipt and row identity alongside each match, using
verified bindings/restamp pairs for corrected identities and equivalent twins. Before using this
join, check a printed `multi-leg controls` transaction against the control query: each distinct
leg must match only its own full identity; retain the transaction, pages and resulting keys as evidence.
Without that control the earlier-entry audit is unproven. List same-wallet, frame-consumed-market
BUYs whose verified identity differs from the admitted trade and whose REST epoch precedes it;
they do not change that frame decision. List homogeneous same-second pieces, mixed
outcomes, both-outcome exposure and all routing/refusal causes for every first-entry BUY in recorded
membership, including removed wallets and all prices. Unexplained misses, duplicate history or
ledger effects or copy-ownership violations fail AC16. Post results to
#588 and #530 and close #730 only after AC16.

Admission captures contain only the admitting wallet's preceding unresolved frames, the frame
market's consumption fact, compact ledger capture and append-only activity row boundary, that
market's anchor balances and post-anchor effects; classification uses the rebuilt position.
Authenticate the scoped balances/effects against the durable anchor and group prefix and verify
first consumption against `wallet_market_history_v2`'s transaction-written owner. The receipt-ordered
continuation alone retains the bounded body; the compact source admission artifact retains
its version, frame receipt and `capture_digest`, equal to the domain-separated frame revision; configuration, sizing
basis and quality come from continuation facts, while payload and parser/schema contracts come
from the authenticated frame. Coverage, eligibility, clocks, frontier and frozen paper prefix
remain admission-time evidence. Resolved frames and unrelated wallets, positions and consumed
markets do not contribute to capture size.

The poller's coalesced unresolved receipts and the bucket owner's ordering barrier must agree after
admission, ordinary retirement and restart, using the canonical
[receipt-priority rule](_GLOSSARY.md#continuation-and-commitment-compatibility-588) in
`_GLOSSARY.md`. Admission removes its own barrier immediately. Ordinary retirement is acknowledged by the owner.
Frontier publication rechecks this barrier for remaining observations with authenticated source
time at or before the fixed end, including fallbacks without a decision row. An empty read alone
cannot advance past an unresolved fallback. Qualification authenticates one reconstructed read at a
time and retains only bindings and restamp pairs for cohort selection.
