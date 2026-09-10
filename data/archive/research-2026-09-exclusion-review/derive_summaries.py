#!/usr/bin/env python3
"""Derive the committed research summaries for docs/37 from the privately retained raw captures.

Inputs (private, git-ignored; see README.md for the location and hashes):
  raw-forge/forge_ro{,2,3,4,5,6,7}.out.json   read-only ranker-host captures (row copies stay private)
  recapture-pages/summary.json                 per-page metadata of the 52 recaptured public pages
Committed inputs read from this directory:
  tombstone-review.csv, archive-density.json, tombstone-audit-summary.json

Outputs written to this directory:
  cohort_summary.csv   one derived row per screened wallet (27 = 24 cohort + 3 context)
  aggregates.json      class-level counts used by the record
  controls.json        density-window results for the control wallets

Usage: python3 derive_summaries.py <private-root>
"""
import csv
import json
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PRIV = Path(sys.argv[1]).resolve()
RAW = PRIV / "raw-forge"


def load(name):
    with open(RAW / name) as f:
        return json.load(f)


ro, ro2, ro3, ro4, ro5, ro6, ro7 = (load(f"forge_ro{s}.out.json") for s in ("", "2", "3", "4", "5", "6", "7"))
review = {r["wallet"]: r for r in csv.DictReader(open(HERE / "tombstone-review.csv"))}
audit = {r["wallet"]: r for r in json.load(open(HERE / "tombstone-audit-summary.json"))}
tomb = {r["wallet_hex"]: r for r in ro["cohort_tomb"]}
arch_w = {r["wallet_hex"]: r for r in ro["archive_cohort_wallets"]}
arch_m = {r["wallet_hex"]: r for r in ro["archive_cohort_manifest"]}
windows = ro3["cohort"]
recap = json.load(open(PRIV / "recapture-pages" / "summary.json"))["wallets"]

RETAIN = {
    "0x1f19c48aee80ec95396d91f0d21ac249b8a7f57a",
    "0x06dc51826bc524d9a83770e7de9dd7e005b04524",
}


def verdict(w, r, win):
    if r["original_csv_reason"] != "breadth>=2000":
        return "context (not in cohort)"
    if w in RETAIN:
        return "retain with evidence"
    if win.get("n", 0) == 0:
        return "unresolved: never ingested"
    return "recommend reclassification for requalification"


rows = []
for w, r in review.items():
    win = windows.get(w, {})
    a = arch_w.get(w) or {}
    m = arch_m.get(w) or {}
    rc = recap.get(w) or []
    full = [p for p in rc if p.get("n") == 500]
    rows.append({
        "wallet": w,
        "profile": r["public_profile_name"],
        "in_cohort": r["original_csv_reason"] == "breadth>=2000",
        "csv_reason": r["original_csv_reason"],
        "csv_distinct_markets": r["original_distinct_markets"],
        "tombstone_reason": tomb[w]["reason"],
        "tombstone_purged_at_unix": tomb[w]["purged_at_unix"],
        "archive_manifest_purged_at_unix": m.get("purged_at_unix"),
        "archive_is_infra": a.get("is_infra"),
        "archive_is_active": a.get("is_active"),
        "archive_discovered_at_unix": a.get("discovered_at_unix"),
        "archive_trades": win.get("n", 0),
        "archive_first_ts": win.get("first_ts"),
        "archive_last_ts": win.get("last_ts"),
        "min_500_window_secs": win.get("min_window_span"),
        "dense_windows": win.get("dense_windows"),
        "oldest500_span_secs": win.get("oldest500_span"),
        "newest500_span_secs": win.get("newest500_span"),
        "at_purge_page_hours": r["historical_page_500_span_hours"],
        "current_page_hours": r["current_page_500_span_hours"],
        "recaptured_full_pages": len(full),
        "recaptured_min_span_hours": min((p["span_hours"] for p in full), default=None),
        "recaptured_any_fires": any(p["rule_fires"] for p in full),
        "sampled_n": r["n_first_buys_30d"],
        "sampled_t_proxy": r["decayed_t_proxy"],
        "both_outcomes_fraction": r["both_outcomes_fraction"],
        "exclusion_verdict": verdict(w, r, win),
        "follower_eligibility": "not established",
    })
with open(HERE / "cohort_summary.csv", "w", newline="") as f:
    wr = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    wr.writeheader()
    wr.writerows(rows)

sample = [r for r in ro4["sample"] if r["n"] >= 500]
dense = [r for r in sample if r["dense"] > 0]
non_csv = ro6["non_csv_infra_with_trades"]
agg = {
    "observed_at": {k: v["observed_at"] for k, v in {"ro": ro, "ro2": ro2, "ro3": ro3, "ro4": ro4, "ro5": ro5, "ro6": ro6, "ro7": ro7}.items()},
    "live_schema_user_version": ro["user_version"][0]["user_version"],
    "live_tombstones_by_reason": ro["tomb_by_reason"],
    "live_wallet_rows_by_flags": ro["is_infra_count"],
    "live_tombstone_and_wallet_row_overlap": ro["tomb_and_wallet_row"][0]["n"],
    "live_flagged_wallets": {"at_ro": 53, "at_ro5": ro5["flagged_wallets"], "at_ro6": ro6["flagged_count"], "at_ro7": ro7["flagged_count"], "trade_rows_held_at_ro5": ro5["flagged_trade_rows"]},
    "cohort_live_state": {"tombstones": len(ro["cohort_tomb"]), "wallet_rows": len(ro["cohort_wallets"]), "trade_groups": len(ro["cohort_trades"])},
    "active_universe": ro2["active_trade_volume"][0],
    "archive_manifest_by_reason": ro2["archive_manifest_by_reason"],
    "csv_intersection": {k: ro6[k] for k in ("csv_addr_count", "csv_tombstoned", "csv_tombstoned_by_reason", "csv_not_tombstoned", "infra_tombstones_total", "infra_tombstones_outside_csv", "exchange_tombstones", "archive_infra_csv_members", "archive_infra_non_csv")},
    "cohort_archived_trades_total": sum(r["archive_trades"] for r in rows if r["in_cohort"]),
    "cohort_verdicts": {v: sum(1 for r in rows if r["in_cohort"] and r["exclusion_verdict"] == v) for v in sorted({r["exclusion_verdict"] for r in rows if r["in_cohort"]})},
    "any_window_sample": {"seed": ro4["seed"], "population": ro4["eligible_pop"][0]["n"], "sampled": len(ro4["sample"]), "with_500_trades": len(sample), "any_window_dense": len(dense), "oldest500_fires": sum(1 for r in sample if r["oldest500"] < 3600), "newest500_fires": sum(1 for r in sample if r["newest500"] < 3600), "sampled_trades": sum(r["n"] for r in sample), "dense_wallets_trades": sum(r["n"] for r in dense), "median_trades": statistics.median(r["n"] for r in sample)},
    "non_csv_infra_with_archived_trades": {"n": len(non_csv), "with_dense_window": sum(1 for r in non_csv if r.get("dense_windows", 0) > 0), "without_dense_window": sum(1 for r in non_csv if r.get("n", 0) >= 500 and r.get("dense_windows", 0) == 0), "under_500_trades": sum(1 for r in non_csv if r.get("n", 0) < 500)},
    "pending_publication_marker_exists": ro7["pending_marker_exists"],
    "recent_ranking_csv_overlap": ro7["ranked_csv_overlap"],
}
json.dump(agg, open(HERE / "aggregates.json", "w"), indent=1)
controls = {
    "exchange_contracts": ro6["exchange_tombstones"],
    "biggest_csv_members_by_archived_trades": ro6["biggest_csv_members"],
    "probe_flagged_non_csv_examples": ro3["controls"],
    "non_csv_infra_with_archived_trades": non_csv,
}
json.dump(controls, open(HERE / "controls.json", "w"), indent=1)
print(json.dumps({"rows": len(rows), "verdicts": agg["cohort_verdicts"], "cohort_trades": agg["cohort_archived_trades_total"]}))
