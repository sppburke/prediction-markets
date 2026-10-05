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

→ The 72h buy-and-hold ranking uses **`--min-ttr-hours 0.0167` (60s)** as the reliability
floor. (An earlier exploratory re-run this session passed a 6h floor on the command line —
before this latency was measured — which is far stricter than the ~10–20s copy latency
warrants; the script default has always been 60s.) The *capturable* edge near resolution is governed separately
by latency-shifted fill pricing in the ranking, not by this floor.

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
Keep each row's continuation, source authority, applied configuration hash, Start identity and
source/paper prefix identities. Preserve the Part 1 population and causal audit above, with the
[continuation-7 authority rules](_GLOSSARY.md#continuation-and-commitment-compatibility-588):
frame fills prove admission-time first-entry knowledge; REST fills prove complete-history first
entry. Replaced thin-book/VWAP refusals and a cross-leader paper hold cannot excuse Phase 2 misses.

Use captured, verified finite log prefixes and a consistent `paper_state.db` snapshot containing
its committed WAL state. The deployment capture must identify the first source sequence after
activation; do not infer it from trade epochs. Retain verification receipts and physical prefix
bounds with the capture. Record identities and run this inspection, substituting the deployment
source sequence and cohort size (one fill for AC15; the AC16 size for latency acceptance).
It opens SQLite with `mode=ro` and `query_only`, reads logs as bytes, and prints to stdout.
The decoder requires system `libzstd`; it checks framing/CRC, joins ordinary TRADE rows by the
canonical `g2:` component encoding (using `b3sum`), and preserves envelope receipts. It does not
replace the Rust log verifier or authority-specific semantic verification. Do not run normal service boot, `--report`, recovery or checkpoint
preparation as part of this read-only audit.

```bash
sha256sum paper_state.db source_events.log paper.log
stat -c '%s %n' paper_state.db source_events.log paper.log
python3 - paper_state.db source_events.log paper.log <deploy-source-sequence> <AC16-cohort-size> <<'PY'
import calendar, ctypes, ctypes.util, hashlib, json, re, sqlite3, statistics, struct, subprocess, sys, time, zlib
from datetime import datetime
from decimal import Decimal
from pathlib import Path

z = ctypes.CDLL(ctypes.util.find_library("zstd"))
for name, args in (("ZSTD_decompressBound", [ctypes.c_void_p, ctypes.c_size_t]),
                   ("ZSTD_decompress", [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t]),
                   ("ZSTD_isError", [ctypes.c_size_t])):
    fn = getattr(z, name); fn.argtypes = args; fn.restype = ctypes.c_size_t

def read_prefix(path):
    raw = Path(path).read_bytes(); assert raw[:5] == b"EDGE\x01", (path, "header")
    offset = 5; envelopes = {}
    while offset < len(raw):
        assert offset + 4 <= len(raw), (path, "incomplete header", offset)
        size = struct.unpack_from("<I", raw, offset)[0]; end = offset + 4 + size
        assert end + 4 <= len(raw), (path, "incomplete frame", offset)
        block = raw[offset + 4:end]
        assert zlib.crc32(block) == struct.unpack_from("<I", raw, end)[0], (path, offset)
        capacity = z.ZSTD_decompressBound(block, len(block))
        assert not z.ZSTD_isError(capacity), (path, offset)
        out = ctypes.create_string_buffer(capacity)
        length = z.ZSTD_decompress(out, capacity, block, len(block))
        assert not z.ZSTD_isError(length), (path, offset)
        e = json.loads(out.raw[:length]); assert e["seq"] not in envelopes
        envelopes[e["seq"]] = e; offset = end + 4
    tail = envelopes[max(envelopes)] if envelopes else None
    print(path, len(raw), hashlib.sha256(raw).hexdigest(),
          None if tail is None else (tail["seq"], tail["this_hash"]))
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

audit_unix_ns = time.time_ns(); print("audit clock", audit_unix_ns)
source = read_prefix(sys.argv[2]); paper = read_prefix(sys.argv[3])
db = sqlite3.connect(Path(sys.argv[1]).resolve().as_uri() + "?mode=ro", uri=True)
db.row_factory = sqlite3.Row; db.execute("PRAGMA query_only=ON"); db.execute("BEGIN")
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
assert len(cohort) == int(sys.argv[5]), "cohort incomplete; acceptance unproven"
spans = {p: [] for p in ("initial_staleness_gate", "book_staleness_check", "terminal_transition")}
book_use = []
for row in cohort:
    print("decision/fill snapshot", dict(row))
    c = json.loads(row["frozen_inputs_json"]); t = json.loads(row["post_commit_inputs_json"])
    frame = receipt(source, c["observed_source_receipt"])
    assert frame["source_id"] == "polymarket-activity-ws"
    proof = c["decision_inputs"]; admission = receipt(source, proof["admission_receipt"])
    assert admission["source_id"] == "pe-service.activity-frame-admission"
    artifact = payload(admission)
    body = json.dumps(proof["inputs"], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    digest = subprocess.check_output(["b3sum"], input=b"prediction-edge/activity-frame-decision/v1\0" + body).decode().split()[0]
    assert artifact == {"version": 1, "frame_receipt": proof["inputs"]["frame_receipt"], "capture_digest": digest}
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
    print(row["source_trade_id"], "feed delay ms",
          Decimal(ns(frame["received_at"]) - ns(frame["observed_at"])) / 10**6)
for purpose, values in spans.items():
    values.sort(); n = len(values)
    print(purpose, "n", n, "missing/invalid", len(cohort) - n,
          "median/p95/max ms", None if not n else
          (statistics.median(values), values[(95 * n + 99) // 100 - 1], values[-1]))
print("decision-start to book-use ms", len(book_use),
      None if not book_use else statistics.median(book_use), "missing/invalid", len(cohort) - len(book_use))
fallbacks = {}
for e in sorted(source.values(), key=lambda e: e["seq"]):
    if e["source_id"] == "pe-service.activity-frame-fallback":
        a = payload(e); r = a["frame_receipt"]; frame = receipt(source, r)
        assert e["schema_version"] == 1 and e["parser_version"] == 1 and a["version"] == 1
        assert frame["source_id"] == "polymarket-activity-ws"
        assert a["reason"] in ("latched", "history_behind", "earlier_unresolved_buy", "wallet_not_ready")
        key = (r["sequence"], r["this_hash"])
        if key not in fallbacks:
            fallbacks[key] = a
            print("earliest fallback", e["seq"], e["this_hash"], a, payload(frame))
    elif e["source_id"] in ("pe-service.activity-read-commitment", "polymarket-public.activity-reconciliation"):
        print(e["seq"], e["this_hash"], e["source_id"], payload(e))
start_seq = int(db.execute("SELECT value FROM meta WHERE key='financial_start_seq'").fetchone()[0])
for e in paper.values():
    if e["seq"] > start_seq and payload(e).get("record") == "feed_incident_changed":
        print(e["seq"], e["this_hash"], payload(e))
groups = {r["source_trade_id"]: dict(r) for r in db.execute("SELECT * FROM activity_groups")}
keys = {}; matched = set(); legs = {}
for e in source.values():
    if e["source_id"] != "polymarket-public.activity-reconciliation": continue
    # Dispatch by the recorded contract; historical pages remain identity-join evidence.
    contract = (e["schema_version"], e["parser_version"])
    if contract not in ((2, 2), (3, 2)):
        raise ValueError(f"unsupported reconciliation page contract {contract}")
    for r in payload(e):
        if r.get("type", r.get("activity_type", "")).strip().upper() != "TRADE": continue
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
        if key not in groups: continue
        g = groups[key]; assert g["wallet_hex"] == wallet and g["transaction_hash"] == tx
        matched.add(key); legs.setdefault((wallet, tx), set()).add(key)
        print("REST full-identity join", key, e["seq"], e["this_hash"], g, r)
        for row in cohort:
            c = json.loads(row["frozen_inputs_json"])
            if (wallet == row["wallet_hex"] and parts[4] == c["market_id"].encode()
                and parts[7] == b"BUY" and key != row["source_trade_id"] and g["source_epoch"] < c["source_epoch"]):
                print("earlier BUY candidate; verify restamp equivalence", row["source_trade_id"], key, g, r)
print("unmatched group identities; resolve from authenticated corrections or report unknown", sorted(set(groups) - matched))
print("multi-leg controls", [(w, tx, sorted(ids)) for (w, tx), ids in legs.items() if len(ids) > 1])
db.close()
PY
```

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
same identities and bytes; one authenticated binding and one leader-ledger effect may be added,
with no second decision, consumption or financial operation. Before deployment, the production
recovery scenarios must prove boot replay both before and after reconciliation; this inspection
never invokes mutating recovery. For each frame audit, join `stream_receipt` (sequence **and** hash)
to its frozen frame receipt and `history_group_id` to `activity_groups.source_trade_id`, retaining
`semantic_revision`, `page_occurrence_index`, `page_raw_hash`, the commitment receipt and its
`read_proof`. Authenticate each indexed page occurrence against its source envelope. For negative
audits, join `FeedIncidentChanged.incident.frame_receipt` to the same frame and authenticate
`deciding_commitment_receipt` plus any `counterpart_identity`; preserve the proof even with empty
bindings. A retained authenticated match survives until its recorded target commits, even when a
later mature full-history read is empty. After a binding or negative counterpart identity is fixed,
only authenticated restamp equivalence can change its identifier; another leg of the same transaction
must retain its own routing. After an absence incident, the first later authenticated same-transaction
group is the frame's late counterpart: one ledger effect, no second decision. Its first binding
fixes that counterpart durably; only authenticated restamp equivalence can change its identifier. List unresolved audits separately. Reconstruct engagement/release order from the active
paper era, including each `engagement_receipt`, and compare the frozen admission latch basis;
current status is supplementary evidence only.

For REST-decided first entries, search the verified source prefix for the trade's frame, including
authenticated corrections/restamp equivalence. No recorded frame means **feed-missed**. Otherwise
use the earliest authenticated `pe-service.activity-frame-fallback` artifact per `frame_receipt`,
ordered by artifact source sequence: report its `reason`, `routing_clock`, evaluated `frontier`
and `latest_incident_basis`. Derive wallet/market from the referenced frame, not artifact fields.
Keep `latched`, `history_behind`, `earlier_unresolved_buy` and `wallet_not_ready` separate;
a frame with no justified routing artifact is unexplained, never inferred from current status.

Later-discovered earlier entries require `activity_groups` **and** recorded REST page rows, because
late/raw-only `proof_json` can omit market and side. Read the candidate groups and a multi-leg
control from the same snapshot:

```bash
sqlite3 -readonly -header -json paper_state.db <<'SQL'
BEGIN;
SELECT source_trade_id, activity_type, wallet_hex, transaction_hash, source_epoch,
       semantic_revision, disposition, proof_json FROM activity_groups ORDER BY rowid;
SELECT wallet_hex, transaction_hash, count(DISTINCT source_trade_id) AS legs
FROM activity_groups GROUP BY wallet_hex, transaction_hash
HAVING count(DISTINCT source_trade_id) > 1 ORDER BY wallet_hex, transaction_hash;
COMMIT;
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
they neither latch nor change that frame decision. List homogeneous same-second pieces, mixed
outcomes, both-outcome exposure and all routing/refusal causes for every first-entry BUY in recorded
membership, including removed wallets and all prices. Unexplained misses, duplicate history or
ledger effects, unaudited frame decisions and unlatched contradictions fail AC16. Post results to
#588 and #530 and close #730 only after AC16.

Admission captures contain only the admitting wallet's preceding unresolved frames, the frame
market's consumption fact, compact ledger capture and append-only activity row boundary, that
market's anchor balances and post-anchor effects; classification uses the rebuilt position.
Authenticate the scoped balances/effects against the durable anchor and group prefix and verify
first consumption against `wallet_market_history_v2`'s transaction-written owner. The receipt-ordered
continuation alone retains the bounded body; the compact source admission artifact retains
its version, frame receipt and `capture_digest`, equal to the domain-separated frame revision; configuration, sizing
basis and quality come from continuation facts, while payload and parser/schema contracts come
from the authenticated frame. Coverage, eligibility, clocks, frontier and sealed paper latch basis
remain admission-time evidence. Resolved frames and unrelated wallets, positions and consumed
markets do not contribute to capture size.

The poller's coalesced unresolved receipts and the bucket owner's ordering barrier must agree after
admission, audit, release and restart: an admitted receipt supersedes earlier same-identity excluded
observations; otherwise the first synchronized receipt remains. Every retirement is acknowledged by
the owner. Frontier publication rechecks this barrier for all observations with authenticated source
time at or before the fixed end, including fallbacks without a decision row. An empty read alone
cannot advance past an unresolved fallback. Qualification authenticates one reconstructed read at a
time and retains only bindings and restamp pairs for cohort selection.
