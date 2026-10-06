#!/usr/bin/env python3
"""Pass 2: latency-shifted re-rank of the ranking candidates (#536): pass one's edge-floor
wallets on schema one; on schema two, every wallet whose in-horizon positions can still meet
the survival gates (#588).

The pass-1 ranker (`rank_72hr_buyandhold.py`) scores each first-buy at the LEADER's
entry price. When we copy, we observe the leader's trade ~Δ seconds late and enter at
whatever the market is then. This pass re-prices every candidate position at the
CLOB minute historical-price reference: the LATEST sample at-or-before `entry+Δ`
(at-or-before per repository precedent — forward selection would be look-ahead),
staleness bounded by the window, strictly before actual resolution. Positions with no
fresh-enough sample are NOT REPRICED and count against the wallet's repricing
coverage (the `fill_rate` wire column, name retained for compatibility). For schema
two, coverage is judged over the copy scope only: `n_total` counts the wallet's
positions whose shifted scheduled horizon qualifies (no price changes that), and
coverage is repriced / (that count minus positions a reference price proves out of
band). A position outside the horizon needs no reference window. The prior
print-tape proxy was retired at the #536 owner-gated cutover: it measured a
wallet-sampled trade tape (side-blind, up to 120s late) instead of the market's
state at our entry; the pinned batch-70 experiment on the issue quantified the
difference (survivors 299 → 558; late-print drift ≈ +1-2¢ against winners).

Fail-closed publication gate: every candidate pair's needed reference window must be
terminal in `ranker_price_pages` ('complete' or valid-'empty', written by
`pe-bootstrap prices-history --targets-csv`); any un-terminal remainder exits 75 —
the supervised tempfail lane — so a partially fetched cycle can never publish a
selectively biased batch. Selection statistics are never rewritten by that
operational gate: validated-empty, unmapped, stale, and invalid-price positions
simply count as not repriced.

Inputs: pass-1 `ranked_72hr_buyandhold.csv` + `qualifying_positions_72hr.csv` (must
include `outcome_id`) + the cache's `ranker_price_points`/`ranker_price_pages` and
bound payout token lists (schema two) or `token_conditions` (schema one). Outputs: `latency_shift_ranked.csv`, `latency_shift_basket.txt`,
`oracle_outcomes.csv` (per-position chosen sample or typed not-repriced reason — the
replay/provenance artifact), and `oracle_manifest.json` (the versioned run manifest
whose canonical hash the publisher stores in `ranking_batches.config_hash`).
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sqlite3
import statistics
import sys
import time
import tempfile
from itertools import groupby

import duckdb
import pandas as pd

from ranker_duck import limit_memory

from ranker_decay import (
    DEFAULT_HALF_LIFE_DAYS,
    decay_weights,
    parse_as_of,
    weighted_stats,
)

# Versioned oracle identity for the run manifest. Bump ORACLE_VERSION on any change to
# the lookup rule or repricing semantics; the fetch side's parser identity is
# `pe_bootstrap::prices_history::RANKER_PRICE_PARSER_VERSION` (mirrored here).
ORACLE_NAME = "clob-minute-reference"
ORACLE_VERSION = 6
ORACLE_FIDELITY_MINUTES = 1
ORACLE_PARSER_VERSION = 1


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="data/wallet_cache.db")
    p.add_argument("--ranked-csv", required=True, help="pass-1 ranked_72hr_buyandhold.csv")
    p.add_argument("--positions-csv", required=True,
                   help="pass-1 qualifying_positions_72hr.csv (must include outcome_id)")
    p.add_argument("--out-dir", default="data/eval-results")
    p.add_argument("--latency-shift-secs", type=float, default=2.0,
                   help="Δ: re-price at the latest reference sample at <= entry+Δ "
                        "(2s = the measured websocket-path copy latency, #530)")
    p.add_argument("--emit-targets", default=None, metavar="PATH",
                   help="write the per-token merged backward fetch windows "
                        "(token_id,start_ts,end_ts) for the candidate positions and exit "
                        "(consumed by `pe-bootstrap prices-history --targets-csv`).")
    p.add_argument("--fill-window-secs", type=float, default=120.0,
                   help="reference-sample staleness bound: the chosen sample must be at "
                        "most this many seconds before entry+Δ; older => NOT REPRICED. "
                        "Must be positive (the bound also anchors the fetch windows).")
    p.add_argument("--slip-cents", type=float, default=1.0,
                   help="entry slippage in cents on the repriced fill basis")
    p.add_argument("--half-life-days", type=float, default=DEFAULT_HALF_LIFE_DAYS,
                   help="exponential recency-decay half-life in days for the net edge/t-stat "
                        "(a fill one half-life old weighs 0.5). <= 0 disables decay (flat = legacy). "
                        "coverage / hit_rate / activity gates stay raw. Same weight as pass-1.")
    p.add_argument("--as-of", default=None,
                   help="decay age anchor (ISO date/datetime or unix epoch). Default: max repriced entry_ts.")
    p.add_argument("--floor-tstat", type=float, default=2.0,
                   help="pass-1 candidate floor AND pass-2 survival floor on net t-stat")
    p.add_argument("--min-fill-rate", type=float, default=0.5,
                   help="drop wallets whose repriced fraction (repricing coverage; the "
                        "`fill_rate` wire column) is below this")
    p.add_argument("--min-active-months", type=int, default=3)
    p.add_argument("--min-avg-per-month", type=float, default=20.0)
    p.add_argument("--min-trl", type=int, default=0,
                   help="minimum track-record length: survival requires >= this many REPRICED "
                        "positions (docs/_GLOSSARY ranker_prod_min_trl). 0 = off. Mirrors pass-1; "
                        "the run28 production shape uses 20 with the per-month gates zeroed.")
    p.add_argument("--min-ttr-secs", type=int, default=60,
                   help="schema-two minimum scheduled horizon at entry+shift")
    p.add_argument("--ttr-max-secs", type=int, default=259200,
                   help="schema-two exclusive maximum scheduled horizon at entry+shift")
    p.add_argument("--price-min", type=float, default=0.15,
                   help="schema-two repriced band lower bound (inclusive)")
    p.add_argument("--price-max", type=float, default=0.85,
                   help="schema-two repriced band upper bound (exclusive)")
    p.add_argument("--before-ranking-json",
                   help="cycle-start snapshot of the active published batch; required for schema two")
    p.add_argument("--cycle-manifest-file",
                   help="frozen source/cache manifest; required for schema two")
    p.add_argument("--cache-stage-record",
                   help="finalized side-cache record whose digest binds publication")
    p.add_argument("--target-n", type=int, default=25)
    p.add_argument("--git-sha", default="unknown",
                   help="code revision recorded in the run manifest (the wrapper passes it)")
    p.add_argument("--pipeline-versions-file", required=False,
                   help="JSON emitted by pe-bootstrap pipeline-versions; required when writing the run manifest")
    return p.parse_args()


def load_candidates(ranked_csv: str, floor_tstat: float):
    """Stream pass one's edge-floor candidates into the spilled wallet relation."""
    with open(ranked_csv, newline="") as f:
        for r in csv.DictReader(f):
            try:
                if (r.get("eligible", "").strip().lower() in ("true", "1")
                        and float(r["tstat_net"]) >= floor_tstat
                        and float(r["mean_net"]) > 0):
                    yield (r["wallet"],)
            except (ValueError, KeyError):
                continue


def in_horizon(ttr_secs: int, a: argparse.Namespace) -> bool:
    """Schema two's copy scope before any price: the shifted scheduled horizon."""
    return a.min_ttr_secs <= ttr_secs - a.latency_shift_secs < a.ttr_max_secs


def load_survivable_wallets(con, a: argparse.Namespace) -> None:
    """An in-horizon count bounds repricing; coverage can only shrink its denominator.

    Keep every position wallet, including those that cannot meet nf > 1/min_trl,
    with its first CSV ordinal for the unchanged false-verdict tie order (#588).
    """
    con.execute(
        "CREATE TABLE wallets AS SELECT wallet, MIN(ordinal) AS first_ordinal, "
        "COUNT(*) FILTER (WHERE in_scope) AS n_total, "
        "COUNT(*) FILTER (WHERE in_scope) > 1 AND "
        "COUNT(*) FILTER (WHERE in_scope) >= ? AS candidate "
        "FROM raw_positions WHERE wallet != '' GROUP BY wallet", [a.min_trl],
    )


def append_rows(con, table: str, columns: list[str], rows: list[tuple]) -> None:
    """Append a bounded batch without executemany's per-row transactions."""
    if rows:
        con.append(table, pd.DataFrame(rows, columns=columns, dtype=object))
        rows.clear()


def query_rows(con, sql: str):
    """A separate cursor lets consumers issue queries while streaming this result."""
    cursor = con.cursor()
    try:
        cursor.execute(sql)
        while batch := cursor.fetchmany(4096):
            yield from batch
    finally:
        cursor.close()


def write_before_after_diff(path: str, before_path: str, rows,
                            target_n: int) -> None:
    """Write a deterministic exact field-level cycle comparison."""
    with open(before_path, encoding="utf-8") as source:
        before_value = json.load(source)
    if not isinstance(before_value, list):
        raise ValueError("before-ranking snapshot must be a JSON array")

    def normalized(row: dict, *, after: bool) -> dict:
        wallet = str(row.get("wallet", row.get("wallet_hex", ""))).lower()
        if not wallet:
            raise ValueError("before/after ranking row omitted wallet")
        score_key = "tstat_net_ls" if after else "score"
        score = row.get(score_key)
        survives = bool(row.get("survives", False))
        rank = int(row["rank"]) if row.get("rank") not in (None, "") else None
        eligible = bool(row.get("eligible", survives))
        membership = bool(row.get("membership", survives and rank is not None
                                  and rank <= target_n))
        return {
            "wallet": wallet,
            "eligibility": eligible,
            "score": None if score in (None, "") else float(score),
            "survival": survives,
            "rank": rank,
            "membership": membership,
        }

    before = {item["wallet"]: item for item in (
        normalized(row, after=False) for row in before_value
    )}
    # The prior published snapshot is bounded by the batch size. Stream the new
    # population in wallet order, carrying its already assigned ranking ordinal.
    pending = iter(sorted(before))
    before_wallet = next(pending, None)

    def empty(wallet):
        return {"wallet": wallet, "eligibility": False, "score": None,
                "survival": False, "rank": None, "membership": False}

    with open(path, "w", encoding="utf-8") as destination:
        destination.write("[")
        separator = ""

        def emit(wallet, right):
            nonlocal separator
            left = before.get(wallet, empty(wallet))
            diff = {"wallet": wallet,
                    "before": {key: left[key] for key in (
                        "eligibility", "score", "survival", "rank", "membership")},
                    "after": {key: right[key] for key in (
                        "eligibility", "score", "survival", "rank", "membership")}}
            destination.write(separator)
            json.dump(diff, destination, sort_keys=True, separators=(",", ":"))
            separator = ","

        for row in rows:
            enriched = dict(row)
            enriched["membership"] = bool(row["survives"] and row["rank"] <= target_n)
            right = normalized(enriched, after=True)
            wallet = right["wallet"]
            while before_wallet is not None and before_wallet < wallet:
                emit(before_wallet, empty(before_wallet))
                before_wallet = next(pending, None)
            emit(wallet, right)
            if before_wallet == wallet:
                before_wallet = next(pending, None)
        while before_wallet is not None:
            emit(before_wallet, empty(before_wallet))
            before_wallet = next(pending, None)
        destination.write("]\n")


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ─── Reference-oracle helpers (#536) ─────────────────────────────────────────────

def map_pair_tokens(db: str, pairs: list[tuple[str, str]]) -> dict[tuple[str, str], str]:
    """(market_id, outcome_id) → CLOB token_id. Schema two reads each market's payout
    evidence token list, whose order pass one's outcome and the payout vector follow
    (#690); schema one reads token_conditions (positional outcome_index; NULL rows
    skipped, never mispriced). Unmapped pairs are simply absent — their positions are
    not repriceable (honest)."""
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    schema = int(con.execute("PRAGMA user_version").fetchone()[0])
    if schema == -2:
        con.close()
        raise ValueError("unfinished bulk root (schema -2); resume cache-populate-activity-v2 --bulk-root before any other command")
    con.execute("PRAGMA busy_timeout=30000;")
    out: dict[tuple[str, str], str] = {}
    markets = sorted({m for m, _ in pairs})
    want = set(pairs)
    for i in range(0, len(markets), 800):
        chunk = markets[i:i + 800]
        ph = ",".join("?" * len(chunk))
        if schema >= 2:
            rows = [(cid, str(index), token["token_id"])
                    for cid, tokens in con.execute(
                        f"SELECT market_id, tokens_json FROM clob_payout_evidence_v2 "
                        f"WHERE market_id IN ({ph})", chunk)
                    for index, token in enumerate(json.loads(tokens)) if token.get("token_id")]
        else:
            rows = [(cid, str(int(oi)), tid) for cid, oi, tid in con.execute(
                f"SELECT condition_id, outcome_index, token_id FROM token_conditions "
                f"WHERE condition_id IN ({ph}) AND outcome_index IS NOT NULL", chunk)]
        for cid, oi, tid in rows:
            if (cid, oi) in want:
                out[(cid, oi)] = tid
    con.close()
    return out


def merge_ranges(ranges):
    """Stream the disjoint inclusive union of already sorted ranges."""
    current = None
    for lo, hi in ranges:
        if current is not None and lo <= current[1] + 1:
            current = (current[0], max(current[1], hi))
        else:
            if current is not None:
                yield current
            current = (lo, hi)
    if current is not None:
        yield current


def subtract_ranges(needed, covered):
    """Stream needed minus covered, both sorted, with inclusive bounds."""
    coverage = iter(merge_ranges(covered))
    interval = next(coverage, None)
    for lo, hi in merge_ranges(needed):
        cursor = lo
        while interval is not None and interval[1] < cursor:
            interval = next(coverage, None)
        while interval is not None and interval[0] <= hi:
            c_lo, c_hi = interval
            if c_lo > cursor:
                yield cursor, c_lo - 1
            cursor = max(cursor, c_hi + 1)
            if cursor > hi:
                break
            interval = next(coverage, None)
        if cursor <= hi:
            yield cursor, hi


def pair_windows(entries, shift: float, fill_window: float):
    """Merged backward windows from sorted entry times, padded one second each side."""
    return merge_ranges((int(entry + shift - fill_window) - 1,
                         int(entry + shift) + 1) for entry in entries)


RANKED_FIELDS = ["wallet", "n_total", "n_filled", "fill_rate", "active_months",
                 "mean_net_ls", "tstat_net_ls", "n_eff", "hit_rate", "eligible", "survives"]
POSITION_FIELDS = ["ordinal", "wallet", "market_id", "outcome_id", "entry_ts",
                   "ttr_secs", "resolved_at", "payoff", "in_scope"]
PRICED_FIELDS = POSITION_FIELDS + ["token_id", "sample_t", "sample_price", "outcome",
                                  "net", "staleness"]


def load_positions(con, a, schema_version):
    if schema_version < 2:
        con.execute("CREATE TABLE candidates (wallet VARCHAR)")
        batch = []
        for row in load_candidates(a.ranked_csv, a.floor_tstat):
            batch.append(row)
            if len(batch) == 4096:
                append_rows(con, "candidates", ["wallet"], batch)
        append_rows(con, "candidates", ["wallet"], batch)
        con.execute("CREATE TABLE wallets AS SELECT DISTINCT wallet, 0::BIGINT AS first_ordinal, "
                    "0::BIGINT AS n_total, true AS candidate FROM candidates")
        con.execute("DROP TABLE candidates")
        if not con.execute("SELECT COUNT(*) FROM wallets").fetchone()[0]:
            return
    with open(a.positions_csv, newline="") as source:
        if "outcome_id" not in (csv.DictReader(source).fieldnames or []):
            raise ValueError("positions CSV lacks outcome_id — re-run pass 1 with the "
                             "rank_72hr_buyandhold.py ranker (it writes outcome_id).")
    # Python's conversions and horizon rule remain the owners. CSV ordinals are
    # assigned before disabling insertion order for the spilling sorts/aggregates.
    con.create_function("position_int", lambda value: int(value), ["VARCHAR"], "BIGINT")
    con.create_function("position_float", lambda value: float(value), ["VARCHAR"], "DOUBLE")
    con.create_function("position_scope", lambda ttr: schema_version < 2 or in_horizon(int(ttr), a),
                        ["VARCHAR"], "BOOLEAN")
    con.execute(
        "CREATE TABLE raw_positions AS SELECT ROW_NUMBER() OVER () AS ordinal, "
        "wallet, market_id, outcome_id, entry_ts, ttr_secs, resolved_at, payoff, "
        "CASE WHEN wallet != '' THEN position_scope(ttr_secs) ELSE false END AS in_scope "
        "FROM read_csv(?, header=true, all_varchar=true, parallel=false, "
        "force_not_null=['wallet','market_id','outcome_id','entry_ts','ttr_secs','resolved_at','payoff'])",
        [a.positions_csv],
    )
    con.execute("SET preserve_insertion_order=false")
    if schema_version >= 2:
        load_survivable_wallets(con, a)
    con.execute(
        "CREATE TABLE positions AS SELECT ordinal, r.wallet, market_id, outcome_id, "
        "position_int(entry_ts) AS entry_ts, position_int(ttr_secs) AS ttr_secs, "
        "position_int(resolved_at) AS resolved_at, position_float(payoff) AS payoff, in_scope "
        "FROM raw_positions r JOIN wallets w ON r.wallet=w.wallet WHERE w.candidate"
    )
    con.execute("DROP TABLE raw_positions")
    con.execute(
        "CREATE TABLE pairs AS SELECT market_id, outcome_id, MIN(entry_ts) AS first_entry, "
        "MAX(entry_ts) AS last_entry, NULL::VARCHAR AS token_id FROM positions "
        "GROUP BY market_id, outcome_id"
    )
    con.execute("CREATE TABLE pair_tokens (market_id VARCHAR, outcome_id VARCHAR, token_id VARCHAR)")
    cursor = con.cursor()
    try:
        cursor.execute("SELECT market_id, outcome_id FROM pairs ORDER BY market_id, outcome_id")
        while pairs := cursor.fetchmany(800):
            tokens = map_pair_tokens(a.db, pairs)
            batch = [(mid, oid, token) for (mid, oid), token in tokens.items()]
            append_rows(con, "pair_tokens", ["market_id", "outcome_id", "token_id"], batch)
    finally:
        cursor.close()
    con.execute("UPDATE pairs SET token_id=t.token_id FROM pair_tokens t "
                "WHERE pairs.market_id=t.market_id AND pairs.outcome_id=t.outcome_id")
    con.execute("DROP TABLE pair_tokens")


def reprice_positions(con, conn, a, schema_version):
    con.execute("CREATE TABLE priced_positions AS SELECT *, NULL::VARCHAR AS token_id, "
                "NULL::BIGINT AS sample_t, NULL::VARCHAR AS sample_price, NULL::VARCHAR AS outcome, "
                "NULL::DOUBLE AS net, NULL::DOUBLE AS staleness FROM positions WHERE false")
    source = query_rows(con, "SELECT p.*, t.token_id, t.first_entry, t.last_entry "
                        "FROM positions p JOIN pairs t USING (market_id, outcome_id) "
                        "ORDER BY p.market_id, p.outcome_id, p.ordinal")
    batch = []
    pair_count = con.execute("SELECT COUNT(*) FROM pairs").fetchone()[0]
    for index, (_key, positions) in enumerate(groupby(source, key=lambda p: (p[2], p[3])), 1):
        bounds = None
        for row in positions:
            ordinal, wallet, mid, oid, entry, ttr, resolved_at, payoff, in_scope, token, first, last = row
            if bounds is None:
                lo = int(first + a.latency_shift_secs - a.fill_window_secs) - 1
                hi = int(last + a.latency_shift_secs) + 1
                has_points = token is not None and conn.execute(
                    "SELECT 1 FROM ranker_price_points WHERE token_id=? AND t>=? AND t<=? LIMIT 1",
                    (token, lo, hi)).fetchone() is not None
                bounds = (lo, hi)
            sample_t = sample_price = net = staleness = None
            target = entry + a.latency_shift_secs
            if not in_scope:
                reason = "scheduled_horizon"
            elif token is None:
                reason = "unmapped"
            else:
                sample = conn.execute(
                    "SELECT t, price FROM ranker_price_points WHERE token_id=? AND t>=? "
                    "AND t<=? AND t<=? ORDER BY t DESC LIMIT 1", (token, *bounds, target),
                ).fetchone()
                if sample is None:
                    reason = "future_only" if has_points else "no_sample"
                elif target - sample[0] > a.fill_window_secs:
                    reason = "stale"
                elif not sample[0] < resolved_at:
                    reason = "post_resolution"
                else:
                    try:
                        fill_price = float(sample[1])
                    except (TypeError, ValueError):
                        fill_price = None
                    if fill_price is None or not (0.0 < fill_price < 1.0):
                        reason = "invalid_price"
                    else:
                        eff = min(fill_price + a.slip_cents / 100.0, 0.999)
                        if schema_version >= 2 and not (a.price_min <= eff < a.price_max):
                            reason = "price_band"
                        else:
                            net = (payoff - eff) / eff
                            staleness = target - sample[0]
                            sample_t, sample_price = sample
                            reason = "repriced"
            batch.append((*row[:9], token, sample_t, sample_price, reason, net, staleness))
            if len(batch) == 4096:
                append_rows(con, "priced_positions", PRICED_FIELDS, batch)
        if index % 2000 == 0:
            log(f"  {index}/{pair_count} market-outcomes processed")
    append_rows(con, "priced_positions", PRICED_FIELDS, batch)
    con.execute("DROP TABLE positions")
    con.execute("ALTER TABLE priced_positions RENAME TO positions")


def score_wallets(con, a, as_of):
    con.execute("CREATE TABLE scores (wallet VARCHAR, n_total BIGINT, n_filled VARCHAR, "
                "fill_rate VARCHAR, active_months VARCHAR, mean_net_ls VARCHAR, tstat_net_ls VARCHAR, "
                "n_eff VARCHAR, hit_rate VARCHAR, eligible BOOLEAN, survives BOOLEAN, "
                "first_market VARCHAR, first_outcome VARCHAR, first_ordinal BIGINT)")
    source = query_rows(con, "SELECT wallet, in_scope, outcome, net, entry_ts, payoff, "
                        "market_id, outcome_id, ordinal FROM positions "
                        "ORDER BY wallet, market_id, outcome_id, ordinal")
    batch = []
    for wallet, positions in groupby(source, key=lambda p: p[0]):
        nets, ets, payoffs, months = [], [], [], set()
        n_total = n_band = 0
        first_key = None
        for _, in_scope, outcome, net, entry, payoff, mid, oid, ordinal in positions:
            if not in_scope:
                continue
            if first_key is None:
                first_key = (mid, oid, ordinal)
            n_total += 1
            n_band += outcome == "price_band"
            if outcome == "repriced":
                nets.append(net)
                ets.append(entry)
                payoffs.append(payoff)
                g = time.gmtime(entry)
                months.add((g.tm_year, g.tm_mon))
        if first_key is None:
            continue
        nf, am = len(nets), len(months)
        scope = n_total - n_band
        fr = nf / scope if scope else 0.0
        mean, _, n_eff, t = weighted_stats(nets, decay_weights(ets, as_of, a.half_life_days))
        hr = statistics.fmean(payoffs) if payoffs else float("nan")
        eligible = (nf > 1 and am >= a.min_active_months
                    and (nf / am if am else 0) >= a.min_avg_per_month
                    and nf >= a.min_trl and fr >= a.min_fill_rate)
        survives = eligible and not math.isnan(t) and t >= a.floor_tstat and mean > 0
        batch.append((wallet, n_total, str(nf), str(round(fr, 4)), str(am),
                      str(round(mean, 6)) if not math.isnan(mean) else "",
                      str(round(t, 4)) if not math.isnan(t) else "", str(round(n_eff, 4)),
                      str(round(hr, 4)) if not math.isnan(hr) else "", eligible, survives, *first_key))
        if len(batch) == 4096:
            append_rows(con, "scores", RANKED_FIELDS + ["first_market", "first_outcome", "first_ordinal"], batch)
    append_rows(con, "scores", RANKED_FIELDS + ["first_market", "first_outcome", "first_ordinal"], batch)
    con.execute(
        "CREATE VIEW ranked AS SELECT w.wallet, COALESCE(s.n_total, w.n_total) AS n_total, "
        + ", ".join(f"COALESCE(s.{field}, '') AS {field}" for field in RANKED_FIELDS[2:9])
        + ", COALESCE(s.eligible, false) AS eligible, COALESCE(s.survives, false) AS survives, "
        "ROW_NUMBER() OVER (ORDER BY COALESCE(s.survives, false) DESC, "
        "COALESCE(TRY_CAST(s.tstat_net_ls AS DOUBLE), -9) DESC, s.wallet IS NOT NULL DESC, "
        "s.first_market, s.first_outcome, COALESCE(s.first_ordinal, w.first_ordinal)) AS rank "
        "FROM wallets w LEFT JOIN scores s ON w.wallet=s.wallet "
        + ("" if a.schema_two else "WHERE s.wallet IS NOT NULL")
    )


def ranked_rows(con, *, by_wallet=False):
    sql = "SELECT " + ", ".join(RANKED_FIELDS) + ", rank FROM ranked ORDER BY "
    sql += "lower(wallet)" if by_wallet else "rank"
    for row in query_rows(con, sql):
        yield dict(zip(RANKED_FIELDS + ["rank"], row))


def main() -> int:
    a = parse_args()
    if a.fill_window_secs <= 0:
        log("FATAL: --fill-window-secs must be positive — the staleness bound also "
            "anchors the backward fetch windows (#536 review)")
        return 1
    os.makedirs(a.out_dir, exist_ok=True)
    probe = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True)
    try:
        schema_version = int(probe.execute("PRAGMA user_version").fetchone()[0])
        if schema_version == -2:
            raise ValueError("unfinished bulk root (schema -2); resume cache-populate-activity-v2 --bulk-root before any other command")
    finally:
        probe.close()
    if (schema_version >= 2 and not a.emit_targets
            and (not a.before_ranking_json or not a.cycle_manifest_file
                 or not a.cache_stage_record or a.as_of is None)):
        # An explicit anchor keeps decay independent of which wallets are evaluated.
        log("FATAL: schema two requires cycle-start ranking, cache manifests and --as-of")
        return 1
    with tempfile.TemporaryDirectory(prefix="pass2-", dir=a.out_dir) as spill_dir:
        with duckdb.connect() as con:
            limit_memory(con, spill_dir)
            return rerank(con, a, schema_version)


def rerank(con, a, schema_version):
    slip = a.slip_cents / 100.0
    a.schema_two = schema_version >= 2
    try:
        load_positions(con, a, schema_version)
    except ValueError as error:
        log(f"FATAL: {error}")
        return 1
    universe_count, candidate_count = con.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE candidate) FROM wallets").fetchone()
    if a.schema_two:
        log(f"survivable position wallets: {candidate_count} of {universe_count}; the rest "
            f"cannot meet the survival gates at any price and are not evaluated")
    else:
        log(f"pass-1 edge-floor candidates: {candidate_count} wallets")
    if not universe_count:
        log("no candidates; nothing to re-rank")
        return 1 if a.schema_two else 76
    npos = con.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
    pair_count, unmapped = con.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE token_id IS NULL) FROM pairs").fetchone()
    log(f"candidate positions: {npos} across {pair_count} (market,outcome) pairs")
    if unmapped:
        log(f"token mapping missing for {unmapped}/{pair_count} pairs — those positions "
            "are not repriceable")
    shift, fill_window = a.latency_shift_secs, a.fill_window_secs
    if a.emit_targets:
        rows_out = token_count = 0
        source = query_rows(con, "SELECT t.token_id, p.entry_ts FROM positions p "
                            "JOIN pairs t USING (market_id, outcome_id) "
                            "WHERE p.in_scope AND t.token_id IS NOT NULL ORDER BY t.token_id, p.entry_ts")
        with open(a.emit_targets, "w", newline="") as destination:
            destination.write("token_id,start_ts,end_ts\n")
            for token, positions in groupby(source, key=lambda p: p[0]):
                token_count += 1
                for lo, hi in pair_windows((p[1] for p in positions), shift, fill_window):
                    destination.write(f"{token},{lo},{hi}\n")
                    rows_out += 1
        log(f"wrote {rows_out} merged target window(s) for {token_count} token(s) to {a.emit_targets}")
        return 0

    with sqlite3.connect(f"file:{a.db}?mode=ro", uri=True) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        source = query_rows(con, "SELECT p.market_id, p.outcome_id, t.token_id, p.entry_ts "
                            "FROM positions p JOIN pairs t USING (market_id, outcome_id) "
                            "WHERE p.in_scope AND t.token_id IS NOT NULL "
                            "ORDER BY p.market_id, p.outcome_id, p.entry_ts")
        uncovered_pairs = 0
        for key, positions in groupby(source, key=lambda p: p[:3]):
            needed = pair_windows((p[3] for p in positions), shift, fill_window)
            covered = conn.execute("SELECT start_ts, end_ts FROM ranker_price_pages "
                                   "WHERE token_id=? AND fidelity_minutes=? ORDER BY start_ts, end_ts",
                                   (key[2], ORACLE_FIDELITY_MINUTES))
            if next(subtract_ranges(needed, covered), None) is not None:
                uncovered_pairs += 1
        if uncovered_pairs:
            log(f"TEMPFAIL(75): {uncovered_pairs} pair(s) have un-terminal reference "
                "coverage — the targeted fetch stage retries the remainder next attempt")
            return 75
        log(f"reference coverage terminal for all {pair_count} candidate pairs — scoring")
        reprice_positions(con, conn, a, schema_version)

    outcomes_path = os.path.join(a.out_dir, "oracle_outcomes.csv")
    with open(outcomes_path, "w", newline="") as destination:
        writer = csv.writer(destination)
        writer.writerow(["wallet", "market_id", "outcome_id", "token_id", "entry_ts", "payoff",
                         "resolved_at", "sample_t", "sample_price", "outcome"])
        writer.writerows(query_rows(con, "SELECT wallet, market_id, outcome_id, token_id, entry_ts, "
                                    "payoff, resolved_at, sample_t, sample_price, outcome FROM positions "
                                    "ORDER BY market_id, outcome_id, ordinal"))
    as_of = parse_as_of(a.as_of)
    if as_of is None:
        as_of = con.execute("SELECT COALESCE(MAX(entry_ts), 0) FROM positions "
                            "WHERE outcome='repriced'").fetchone()[0]
    decay = "flat (no decay)" if a.half_life_days <= 0 else f"half_life={a.half_life_days}d"
    log(f"scoring: {decay}, as_of={as_of}")
    score_wallets(con, a, as_of)
    ranked_path = os.path.join(a.out_dir, "latency_shift_ranked.csv")
    with open(ranked_path, "w", newline="") as destination:
        writer = csv.DictWriter(destination, fieldnames=RANKED_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(ranked_rows(con))
    diff_path = os.path.join(a.out_dir, "before_after_diff.json")
    if a.before_ranking_json:
        write_before_after_diff(diff_path, a.before_ranking_json,
                                ranked_rows(con, by_wallet=True), a.target_n)

    # Versioned run manifest: canonical JSON whose sha256 the publisher stores in
    # `ranking_batches.config_hash` (#536 replay binding). The publish request is
    # deliberately NOT part of the manifest (it would hash-cycle through config_hash).
    if not a.pipeline_versions_file:
        raise ValueError("--pipeline-versions-file is required when writing the run manifest")
    with open(a.pipeline_versions_file, encoding="utf-8") as versions_file:
        pipeline_versions = json.load(versions_file)
    required_versions = {
        "source", "activity_schema", "activity_parser", "clob_resolution_schema",
        "clob_resolution_parser", "cache_schema", "configuration",
    }
    missing_versions = sorted(required_versions - pipeline_versions.keys())
    if missing_versions:
        raise ValueError(f"pipeline versions omitted {missing_versions}")
    manifest = {
        "oracle": ORACLE_NAME,
        "oracle_version": ORACLE_VERSION,
        "fidelity_minutes": ORACLE_FIDELITY_MINUTES,
        "parser_version": ORACLE_PARSER_VERSION,
        "lookup": "latest sample at-or-before entry+shift",
        "latency_shift_secs": a.latency_shift_secs,
        "staleness_bound_secs": a.fill_window_secs,
        "slip_cents": a.slip_cents,
        "half_life_days": a.half_life_days,
        "as_of": as_of,
        "floor_tstat": a.floor_tstat,
        "min_coverage": a.min_fill_rate,
        "min_trl": a.min_trl,
        "min_active_months": a.min_active_months,
        "min_avg_per_month": a.min_avg_per_month,
        "schema_version": schema_version,
        "price_band": {"minimum_inclusive": a.price_min,
                       "maximum_exclusive": a.price_max},
        "scheduled_horizon": {"minimum_secs": a.min_ttr_secs,
                              "maximum_secs_exclusive": a.ttr_max_secs},
        "git_sha": a.git_sha,
        "versions": {
            **{key: pipeline_versions[key] for key in sorted(required_versions)},
            "ranker": ORACLE_VERSION,
            "oracle_parser": ORACLE_PARSER_VERSION,
        },
        "inputs": {
            "ranked_csv_sha256": sha256_file(a.ranked_csv),
            "positions_csv_sha256": sha256_file(a.positions_csv),
            "before_ranking_sha256": (
                sha256_file(a.before_ranking_json) if a.before_ranking_json else None
            ),
            "cycle_manifest_sha256": (
                sha256_file(a.cycle_manifest_file) if a.cycle_manifest_file else None
            ),
            "cache_stage_record_sha256": (
                sha256_file(a.cache_stage_record) if a.cache_stage_record else None
            ),
            # The cycle's target file (stage 2a) when present — binds which windows
            # the reference store was asked to cover (#536 review).
            "oracle_targets_sha256": (
                sha256_file(os.path.join(a.out_dir, "oracle_targets.csv"))
                if os.path.exists(os.path.join(a.out_dir, "oracle_targets.csv"))
                else None
            ),
        },
        "outputs": {
            "latency_shift_ranked_sha256": sha256_file(ranked_path),
            "oracle_outcomes_sha256": sha256_file(outcomes_path),
            "before_after_diff_sha256": (
                sha256_file(diff_path) if a.before_ranking_json else None
            ),
        },
    }
    manifest_path = os.path.join(a.out_dir, "oracle_manifest.json")
    rendered = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    with open(manifest_path, "w") as f:
        f.write(rendered + "\n")
    log(f"manifest sha256={hashlib.sha256(rendered.encode()).hexdigest()[:16]}… "
        f"written to {manifest_path}")

    count_rows = con.execute("SELECT COUNT(*) FROM ranked").fetchone()[0]
    if not count_rows:
        log("no candidate positions overlapped the positions CSV — wrote empty ranking")
        return 0
    count_survivors = con.execute("SELECT COUNT(*) FROM ranked WHERE survives").fetchone()[0]
    log(f"repriced survivors (coverage>={a.min_fill_rate}, t>={a.floor_tstat}, "
        f"mean>0, >={a.min_active_months}mo, >={a.min_avg_per_month}/mo): {count_survivors}")
    basket_size = a.target_n if a.target_n >= 0 else max(0, count_survivors + a.target_n)
    basket = [r for r in ranked_rows(con) if r["survives"] and r["rank"] <= basket_size]
    basket_path = os.path.join(a.out_dir, "latency_shift_basket.txt")
    with open(basket_path, "w") as f:
        f.write(f"# latency_shift_basket — Δ={shift}s slip={slip} floor_t={a.floor_tstat} "
                f"half_life={a.half_life_days}d min_coverage={a.min_fill_rate} "
                f"candidates={universe_count} survivors={count_survivors}\n")
        for r in basket:
            f.write(r["wallet"] + "\n")
    count = con.execute("SELECT COUNT(*) FROM positions WHERE staleness IS NOT NULL").fetchone()[0]
    if count:
        offsets = (count // 2, int(count * 0.9), count - 1)
        fd = [con.execute("SELECT staleness FROM positions WHERE staleness IS NOT NULL "
                          "ORDER BY staleness LIMIT 1 OFFSET ?", [offset]).fetchone()[0]
              for offset in offsets]
        log(f"reference-sample staleness (s before entry+Δ): p50={fd[0]:.0f} "
            f"p90={fd[1]:.0f} max={fd[2]:.0f}")
    if basket:
        log(f"basket={len(basket)}  mean coverage={statistics.fmean([float(r['fill_rate']) for r in basket]):.3f}  "
            f"mean tstat_ls={statistics.fmean([float(r['tstat_net_ls']) for r in basket]):.2f}  "
            f"mean meanNet_ls={statistics.fmean([float(r['mean_net_ls']) for r in basket]):+.4f}")
    log(f"wrote {ranked_path}  and  {basket_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
