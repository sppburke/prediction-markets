#!/usr/bin/env python3
"""Pass 2: price certified first entries at the first eligible taker-print second.

The highest fee-paying first-entry price at the first eligible second in
[entry + shift, entry + window] sets the copy price, plus slippage and a taker fee.
Missing prints stay unpriced;
coverage is published as fill_rate. Survival uses priced sample size, positive
mean, the t floor and the wallet's unfiltered window maker share.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import shutil
import sqlite3
import statistics
import sys
import time
from itertools import groupby

import pandas as pd

from ranker_duck import get_engine, limit_memory, duck_first_entry_prints_v2

from ranker_decay import (
    DEFAULT_HALF_LIFE_DAYS,
    decay_weights,
    parse_as_of,
    weighted_stats,
)

# The projection's oracle_version=6 identifies its format, independently of pricing.
ORACLE_NAME = "first-entry-taker-print"
ORACLE_VERSION = 7
TAKER_FEE_RATE = 0.05
MAKER_SHARE_CAP = 0.5


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
                   help="first eligible print is at or after entry + Δ")
    p.add_argument("--fill-window-secs", type=float, default=120.0,
                   help="last eligible print is at entry + this many seconds (inclusive)")
    p.add_argument("--slip-cents", type=float, default=0.5,
                   help="entry slippage in cents on the repriced fill basis")
    p.add_argument("--half-life-days", type=float, default=DEFAULT_HALF_LIFE_DAYS,
                   help="exponential recency-decay half-life in days for the net edge/t-stat "
                        "(a fill one half-life old weighs 0.5). <= 0 disables decay (flat = legacy). "
                        "coverage / hit_rate / activity gates stay raw. Same weight as pass-1.")
    p.add_argument("--as-of", default=None,
                   help="decay age anchor (ISO date/datetime or unix epoch). Default: max repriced entry_ts.")
    p.add_argument("--floor-tstat", type=float, default=3.0,
                   help="pass-1 candidate floor AND pass-2 survival floor on net t-stat")
    p.add_argument("--min-trl", type=int, default=0,
                   help="minimum track-record length: survival requires >= this many REPRICED "
                        "positions (docs/_GLOSSARY ranker_prod_min_trl). 0 = off. Mirrors pass-1; "
                        "production uses 20.")
    p.add_argument("--min-ttr-secs", type=int, default=60,
                   help="schema-two minimum scheduled horizon at entry+shift")
    p.add_argument("--ttr-max-secs", type=int, default=86400,
                   help="schema-two exclusive maximum scheduled horizon at entry+shift")
    p.add_argument("--price-min", type=float, default=0.15,
                   help="schema-two repriced band lower bound (inclusive)")
    p.add_argument("--price-max", type=float, default=0.65,
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


def in_horizon(entry_ts: int, ttr_secs: int, a: argparse.Namespace) -> bool:
    """Use the scheduled marker's next UTC midnight, with an exclusive ceiling."""
    marker = entry_ts + ttr_secs
    day_end = (marker // 86400 + 1) * 86400
    if not (-2**63 <= marker < 2**63 and -2**63 <= day_end < 2**63):
        return False
    remaining = day_end - entry_ts - a.latency_shift_secs
    if not -2**63 <= remaining < 2**63:
        return False
    return ((a.min_ttr_secs == 0 or remaining >= a.min_ttr_secs)
            and (a.ttr_max_secs == 0 or remaining < a.ttr_max_secs))


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


RANKED_FIELDS = ["wallet", "n_total", "n_filled", "fill_rate", "active_months",
                 "mean_net_ls", "tstat_net_ls", "n_eff", "hit_rate", "maker_share", "eligible", "survives"]
POSITION_FIELDS = ["ordinal", "wallet", "market_id", "outcome_id", "entry_ts",
                   "ttr_secs", "resolved_at", "payoff", "in_scope"]


def load_positions(con, a):
    with open(a.positions_csv, newline="") as source:
        if "outcome_id" not in (csv.DictReader(source).fieldnames or []):
            raise ValueError("positions CSV lacks outcome_id — re-run pass 1 with the "
                             "rank_72hr_buyandhold.py ranker (it writes outcome_id).")
    # Python's conversions and horizon rule remain the owners. CSV ordinals are
    # assigned before disabling insertion order for the spilling sorts/aggregates.
    con.create_function("position_int", lambda value: int(value), ["VARCHAR"], "BIGINT")
    con.create_function("position_float", lambda value: float(value), ["VARCHAR"], "DOUBLE")
    con.create_function("position_scope", lambda entry, ttr: in_horizon(int(entry), int(ttr), a),
                        ["VARCHAR", "VARCHAR"], "BOOLEAN")
    con.execute(
        "CREATE TABLE raw_positions AS SELECT ROW_NUMBER() OVER () AS ordinal, "
        "wallet, market_id, outcome_id, entry_ts, ttr_secs, resolved_at, payoff, "
        "CASE WHEN wallet != '' THEN position_scope(entry_ts, ttr_secs) ELSE false END AS in_scope "
        "FROM read_csv(?, header=true, all_varchar=true, parallel=false, "
        "force_not_null=['wallet','market_id','outcome_id','entry_ts','ttr_secs','resolved_at','payoff'])",
        [a.positions_csv],
    )
    con.execute("SET preserve_insertion_order=false")
    load_survivable_wallets(con, a)
    con.execute(
        "CREATE TABLE positions AS SELECT ordinal, r.wallet, market_id, outcome_id, "
        "position_int(entry_ts) AS entry_ts, position_int(ttr_secs) AS ttr_secs, "
        "position_int(resolved_at) AS resolved_at, position_float(payoff) AS payoff, in_scope "
        "FROM raw_positions r JOIN wallets w ON r.wallet=w.wallet WHERE w.candidate"
    )
    con.execute(
        "CREATE TABLE pairs AS SELECT market_id, outcome_id, MIN(entry_ts) AS first_entry, "
        "MAX(entry_ts) AS last_entry FROM positions WHERE in_scope "
        "GROUP BY market_id, outcome_id"
    )
    duck_first_entry_prints_v2(con, a.latency_shift_secs, a.fill_window_secs)
    con.execute("DROP TABLE raw_positions")


def reprice_positions(con, a):
    con.execute("CREATE TABLE priced_positions AS SELECT *, NULL::BIGINT AS sample_t, "
                "NULL::DOUBLE AS sample_price, NULL::VARCHAR AS outcome, "
                "NULL::DOUBLE AS net, NULL::DOUBLE AS print_delay FROM positions WHERE false")
    # One pair belongs wholly to one bucket, bounding each as-of join's memory.
    # A wallet's own first entry predates its lookup window, so eligible prints
    # necessarily come from other wallets in the certified first-entry projection.
    for bucket in range(8):
        con.execute(
            "INSERT INTO priced_positions WITH matched AS ("
            "SELECT c.*, p.sample_t, p.sample_price, "
            "LEAST(p.sample_price + ?, 0.999) AS eff "
            "FROM (SELECT * FROM positions WHERE hash(market_id) % 8 = ?) c "
            "ASOF LEFT JOIN (SELECT * FROM first_entry_prints WHERE hash(market_id) % 8 = ?) p "
            "ON c.market_id=p.market_id AND c.outcome_id=p.outcome_id "
            "AND c.entry_ts + ? <= p.sample_t), classified AS ("
            "SELECT *, CASE WHEN NOT in_scope THEN 'scheduled_horizon' "
            "WHEN sample_t IS NULL OR sample_t > entry_ts + ? THEN 'no_print' "
            "WHEN NOT (? <= eff AND eff < ?) THEN 'price_band' ELSE 'repriced' END AS reason "
            "FROM matched) SELECT " + ", ".join(POSITION_FIELDS) + ", "
            "CASE WHEN reason IN ('repriced','price_band') THEN sample_t END, "
            "CASE WHEN reason IN ('repriced','price_band') THEN sample_price END, reason, "
            "CASE WHEN reason='repriced' THEN payoff / (eff * (1 + ? * (1 - eff))) - 1 END, "
            "CASE WHEN reason='repriced' THEN sample_t - entry_ts END FROM classified",
            [a.slip_cents / 100.0, bucket, bucket, a.latency_shift_secs,
             a.fill_window_secs, a.price_min, a.price_max, TAKER_FEE_RATE],
        )
    con.execute("DROP TABLE positions")
    con.execute("ALTER TABLE priced_positions RENAME TO positions")
    con.execute("DROP TABLE first_entry_prints")


def score_wallets(con, a, as_of):
    con.execute("CREATE TABLE scores (wallet VARCHAR, n_total BIGINT, n_filled VARCHAR, "
                "fill_rate VARCHAR, active_months VARCHAR, mean_net_ls VARCHAR, tstat_net_ls VARCHAR, "
                "n_eff VARCHAR, hit_rate VARCHAR, maker_share VARCHAR, eligible BOOLEAN, survives BOOLEAN, "
                "first_market VARCHAR, first_outcome VARCHAR, first_ordinal BIGINT)")
    source = query_rows(con, "SELECT wallet, in_scope, outcome, net, entry_ts, payoff, "
                        "market_id, outcome_id, ordinal, maker_share FROM positions "
                        "JOIN maker_shares USING (wallet) "
                        "ORDER BY wallet, market_id, outcome_id, ordinal")
    batch = []
    for wallet, positions in groupby(source, key=lambda p: p[0]):
        nets, ets, payoffs, months = [], [], [], set()
        n_total = n_band = 0
        first_key = None
        for _, in_scope, outcome, net, entry, payoff, mid, oid, ordinal, maker_share in positions:
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
        eligible = nf > 1 and nf >= a.min_trl and maker_share < MAKER_SHARE_CAP
        survives = eligible and not math.isnan(t) and t >= a.floor_tstat and mean > 0
        batch.append((wallet, n_total, str(nf), str(round(fr, 4)), str(am),
                      str(round(mean, 6)) if not math.isnan(mean) else "",
                      str(round(t, 4)) if not math.isnan(t) else "", str(round(n_eff, 4)),
                      str(round(hr, 4)) if not math.isnan(hr) else "", str(maker_share), eligible, survives, *first_key))
        if len(batch) == 4096:
            append_rows(con, "scores", RANKED_FIELDS + ["first_market", "first_outcome", "first_ordinal"], batch)
    append_rows(con, "scores", RANKED_FIELDS + ["first_market", "first_outcome", "first_ordinal"], batch)
    con.execute(
        "CREATE VIEW ranked AS SELECT w.wallet, COALESCE(s.n_total, w.n_total) AS n_total, "
        + ", ".join(f"COALESCE(s.{field}, '') AS {field}" for field in RANKED_FIELDS[2:9])
        + ", COALESCE(s.maker_share, CAST(m.maker_share AS VARCHAR), '') AS maker_share, "
        "COALESCE(s.eligible, false) AS eligible, COALESCE(s.survives, false) AS survives, "
        "ROW_NUMBER() OVER (ORDER BY COALESCE(s.survives, false) DESC, "
        "COALESCE(TRY_CAST(s.tstat_net_ls AS DOUBLE), -9) DESC, s.wallet IS NOT NULL DESC, "
        "s.first_market, s.first_outcome, COALESCE(s.first_ordinal, w.first_ordinal)) AS rank "
        "FROM wallets w LEFT JOIN scores s ON w.wallet=s.wallet "
        "LEFT JOIN maker_shares m ON m.wallet=w.wallet"
    )


def ranked_rows(con, *, by_wallet=False):
    sql = "SELECT " + ", ".join(RANKED_FIELDS) + ", rank FROM ranked ORDER BY "
    sql += "lower(wallet)" if by_wallet else "rank"
    for row in query_rows(con, sql):
        yield dict(zip(RANKED_FIELDS + ["rank"], row))


def main() -> int:
    a = parse_args()
    if a.latency_shift_secs <= 0 or a.fill_window_secs <= 0:
        # A positive shift keeps the wallet's own first entry out of its print window.
        log("FATAL: --latency-shift-secs and --fill-window-secs must be positive")
        return 1
    os.makedirs(a.out_dir, exist_ok=True)
    probe = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True)
    try:
        schema_version = int(probe.execute("PRAGMA user_version").fetchone()[0])
        if schema_version == -2:
            raise ValueError("unfinished bulk root (schema -2); resume cache-populate-activity-v2 --bulk-root before any other command")
    finally:
        probe.close()
    if (schema_version >= 2
            and (not a.before_ranking_json or not a.cycle_manifest_file
                 or not a.cache_stage_record or a.as_of is None)):
        # An explicit anchor keeps decay independent of which wallets are evaluated.
        log("FATAL: schema two requires cycle-start ranking, cache manifests and --as-of")
        return 1
    if schema_version < 2:
        if next(load_candidates(a.ranked_csv, a.floor_tstat), None) is None:
            log("no candidates; nothing to re-rank")
            return 76
        log("FATAL: pass 2 prices the schema-two export's first-entry prints; migrate the cache first")
        return 1
    spill_dir = os.path.join(a.out_dir, "pass2-spill")
    if os.path.lexists(spill_dir):
        if os.path.islink(spill_dir) or not os.path.isdir(spill_dir):
            raise ValueError("pass-two spill path must be a directory, not a symlink")
        shutil.rmtree(spill_dir)
    os.mkdir(spill_dir)
    try:
        with get_engine(schema_version=2) as con:
            limit_memory(con, spill_dir)
            return rerank(con, a, schema_version)
    finally:
        shutil.rmtree(spill_dir)


def rerank(con, a, schema_version):
    slip = a.slip_cents / 100.0
    try:
        load_positions(con, a)
    except ValueError as error:
        log(f"FATAL: {error}")
        return 1
    universe_count, candidate_count = con.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE candidate) FROM wallets").fetchone()
    log(f"survivable position wallets: {candidate_count} of {universe_count}; the rest "
        f"cannot meet the survival gates at any price and are not evaluated")
    if not universe_count:
        log("no candidates; nothing to re-rank")
        return 1
    npos = con.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
    pair_count = con.execute("SELECT COUNT(*) FROM pairs").fetchone()[0]
    log(f"candidate positions: {npos} across {pair_count} (market,outcome) pairs")
    shift = a.latency_shift_secs
    reprice_positions(con, a)

    outcomes_path = os.path.join(a.out_dir, "oracle_outcomes.csv")
    with open(outcomes_path, "w", newline="") as destination:
        writer = csv.writer(destination)
        writer.writerow(["wallet", "market_id", "outcome_id", "entry_ts", "payoff",
                         "resolved_at", "sample_t", "sample_price", "outcome"])
        writer.writerows(query_rows(con, "SELECT wallet, market_id, outcome_id, entry_ts, "
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
        "latency_shift_secs": a.latency_shift_secs,
        "fill_window_secs": a.fill_window_secs,
        "slip": slip,
        "fee_rate": TAKER_FEE_RATE,
        "maker_share_cap": MAKER_SHARE_CAP,
        "slip_cents": a.slip_cents,
        "half_life_days": a.half_life_days,
        "as_of": as_of,
        "floor_tstat": a.floor_tstat,
        "min_trl": a.min_trl,
        "schema_version": schema_version,
        "price_band": {"minimum_inclusive": a.price_min,
                       "maximum_exclusive": a.price_max},
        "scheduled_horizon": {"minimum_secs": a.min_ttr_secs,
                              "maximum_secs_exclusive": a.ttr_max_secs},
        "git_sha": a.git_sha,
        "versions": {
            **{key: pipeline_versions[key] for key in sorted(required_versions)},
            "ranker": ORACLE_VERSION,
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
    log(f"repriced survivors (t>={a.floor_tstat}, mean>0, n>={a.min_trl}, "
        f"maker_share<{MAKER_SHARE_CAP}): {count_survivors}")
    basket_size = a.target_n if a.target_n >= 0 else max(0, count_survivors + a.target_n)
    basket = [r for r in ranked_rows(con) if r["survives"] and r["rank"] <= basket_size]
    basket_path = os.path.join(a.out_dir, "latency_shift_basket.txt")
    with open(basket_path, "w") as f:
        f.write(f"# latency_shift_basket — Δ={shift}s slip={slip} floor_t={a.floor_tstat} "
                f"half_life={a.half_life_days}d "
                f"candidates={universe_count} survivors={count_survivors}\n")
        for r in basket:
            f.write(r["wallet"] + "\n")
    count = con.execute("SELECT COUNT(*) FROM positions WHERE print_delay IS NOT NULL").fetchone()[0]
    if count:
        offsets = (count // 2, int(count * 0.9), count - 1)
        fd = [con.execute("SELECT print_delay FROM positions WHERE print_delay IS NOT NULL "
                          "ORDER BY print_delay LIMIT 1 OFFSET ?", [offset]).fetchone()[0]
              for offset in offsets]
        log(f"first-entry print delay (s after entry): p50={fd[0]:.0f} "
            f"p90={fd[1]:.0f} max={fd[2]:.0f}")
    if basket:
        log(f"basket={len(basket)}  mean coverage={statistics.fmean([float(r['fill_rate']) for r in basket]):.3f}  "
            f"mean tstat_ls={statistics.fmean([float(r['tstat_net_ls']) for r in basket]):.2f}  "
            f"mean meanNet_ls={statistics.fmean([float(r['mean_net_ls']) for r in basket]):+.4f}")
    log(f"wrote {ranked_path}  and  {basket_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
