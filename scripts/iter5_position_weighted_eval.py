#!/usr/bin/env python3
"""Iter 5: position-size-weighted forward edge (Kelly-realized).

Flat-$1 treats every (wallet, market, outcome) as a single $1 bet. But the
wallets bet variable sizes, and a "good wallet at scale" is what we copy.
Position-weighted edge:
    edge_pw = sum(usd_size * (outcome - vwap)) / sum(usd_size)
where usd_size = sum of contracts * vwap_entry per position group.

For each of the candidate rankings (default-composite single, default-composite
intersection_K, GBM single, GBM intersection_K), report BOTH metrics
at each anchor.

If the ranking that wins on flat-$1 LOSES on position-weighted, the
production answer changes.
"""
import sys, sqlite3, json
from pathlib import Path
from statistics import mean, stdev
from datetime import datetime
from dataclasses import dataclass

sys.path.insert(0, '/home/sean/git/pm-ranker-iter2/scripts')
from composite_tuner import objective, data

DB = '/home/sean/git/prediction-markets/data/wallet_cache.db'
BIN = '/home/sean/git/pm-ranker-iter2/target/release/pe-skill-select'
GBM_JSON = Path('/home/sean/git/pm-ranker-iter2/data/ranker-iter-logs/iter2-gbm-topn.json')

DEFAULT_WEIGHTS = {
    "SHARPE_BPS": 1500, "EV_MEAN_BPS": 833, "EV_TSTAT_BPS": 833,
    "BB_SHRUNK_EDGE_BPS": 833, "KELLY_LOG_GROWTH_BPS": 833,
    "BRIER_SCORE_BPS": -833, "BRIER_RESOLUTION_BPS": 833,
    "CONCENTRATION_HHI_BPS": -500, "CONCENTRATION_N_EFF_BPS": 500,
    "CONCENTRATION_RPC_BPS": -500,
    "FIRST_ENTRIES_PER_ACTIVE_DAY_BPS": 1000,
    "MEDIAN_FIRST_ENTRY_TO_RESOLUTION_SECS": -1000,
}
TOP_N = 5000
FWD_SECS = 30 * 86400


@dataclass(frozen=True)
class WeightedPos:
    wallet_hex: str
    vwap_entry: float
    outcome: float
    usd_size: float  # contracts * vwap_entry


def load_weighted_positions(cutoff_unix, fwd_end_unix, wallets):
    """Like data.load_oos_positions but also returns usd_size = sum(contracts*price)."""
    if not wallets:
        return []
    out = []
    lst = list(wallets)
    chunk = 900
    with sqlite3.connect(f'file:{DB}?mode=ro', uri=True) as conn:
        for start in range(0, len(lst), chunk):
            piece = lst[start:start+chunk]
            qmarks = ",".join("?" * len(piece))
            sql = f"""
                SELECT
                    t.wallet_hex,
                    SUM(CAST(t.price_str AS REAL) * t.contracts) /
                        NULLIF(SUM(t.contracts), 0) AS vwap_entry,
                    SUM(CAST(t.price_str AS REAL) * t.contracts) AS usd_size,
                    CASE WHEN r.winning_outcome_id = t.outcome_id THEN 1.0 ELSE 0.0 END AS outcome
                FROM trades t
                JOIN market_resolutions r USING (market_id)
                WHERE t.wallet_hex IN ({qmarks})
                  AND t.side = 'buy'
                  AND t.timestamp_unix > ?
                  AND t.timestamp_unix <= ?
                  AND r.winning_outcome_id IS NOT NULL
                  AND r.resolved_at_unix >= t.timestamp_unix
                GROUP BY t.wallet_hex, t.market_id, t.outcome_id
                HAVING vwap_entry > 0.0 AND vwap_entry < 1.0
            """
            params = [*piece, cutoff_unix, fwd_end_unix]
            for w, vwap, usd, outcome in conn.execute(sql, params):
                out.append(WeightedPos(w, float(vwap), float(outcome), float(usd)))
    return out


def per_pos_metrics(cutoff, fwd_end, hexes):
    """Returns (n_pos, flat_edge, weighted_edge, total_usd, total_pnl)."""
    pos = load_weighted_positions(cutoff, fwd_end, hexes)
    if not pos:
        return 0, 0.0, 0.0, 0.0, 0.0
    flat = sum((p.outcome - p.vwap_entry) / p.vwap_entry for p in pos) / len(pos)
    total_usd = sum(p.usd_size for p in pos)
    total_pnl = sum(p.usd_size * (p.outcome - p.vwap_entry) / p.vwap_entry for p in pos)
    weighted = total_pnl / total_usd if total_usd > 0 else 0.0
    return len(pos), flat, weighted, total_usd, total_pnl


def main():
    cutoffs = data.distinct_cutoffs(DB)
    gbm_top = {}
    if GBM_JSON.exists():
        gbm_top = {int(k): v for k, v in json.loads(GBM_JSON.read_text()).items()}

    comp_cache = {}
    def comp_top(c):
        if c not in comp_cache:
            _, hexes = objective.invoke_composite(BIN, DB, c, DEFAULT_WEIGHTS, top_n=TOP_N)
            comp_cache[c] = hexes
        return comp_cache[c]

    print("=== Iter 5: flat-$1 vs position-weighted forward edge ===\n")

    for K in [3, 5]:
        print(f"\n=== K = {K} ===")
        print(f"{'anchor':12s}  {'method':24s}  {'n_pos':>7s}  "
              f"{'flat_$':>9s}  {'wtd_$':>9s}  {'total_usd':>12s}  {'total_pnl':>12s}")
        for i, anchor in enumerate(cutoffs):
            if i < K - 1:
                continue
            fwd_end = anchor + FWD_SECS
            window = cutoffs[i - K + 1: i + 1]
            anchor_str = datetime.utcfromtimestamp(anchor).date().isoformat()

            # Composite single
            n, f, w, usd, pnl = per_pos_metrics(anchor, fwd_end, comp_top(anchor))
            print(f"{anchor_str:12s}  {'comp_single':24s}  {n:>7d}  "
                  f"${f:>+8.4f}  ${w:>+8.4f}  ${usd:>11,.0f}  ${pnl:>+11,.0f}")

            # Composite intersection_K
            inter = set(comp_top(window[0]))
            for c in window[1:]:
                inter &= set(comp_top(c))
            n, f, w, usd, pnl = per_pos_metrics(anchor, fwd_end, list(inter))
            print(f"{anchor_str:12s}  {f'comp_inter_{K}':24s}  {n:>7d}  "
                  f"${f:>+8.4f}  ${w:>+8.4f}  ${usd:>11,.0f}  ${pnl:>+11,.0f}")

            # GBM single
            if anchor in gbm_top:
                n, f, w, usd, pnl = per_pos_metrics(anchor, fwd_end, gbm_top[anchor])
                print(f"{anchor_str:12s}  {'gbm_single':24s}  {n:>7d}  "
                      f"${f:>+8.4f}  ${w:>+8.4f}  ${usd:>11,.0f}  ${pnl:>+11,.0f}")

                # GBM intersection_K
                ginter = set(gbm_top[window[0]]) if window[0] in gbm_top else set()
                if ginter:
                    for c in window[1:]:
                        if c in gbm_top:
                            ginter &= set(gbm_top[c])
                        else:
                            ginter = set()
                            break
                if ginter:
                    n, f, w, usd, pnl = per_pos_metrics(anchor, fwd_end, list(ginter))
                    print(f"{anchor_str:12s}  {f'gbm_inter_{K}':24s}  {n:>7d}  "
                          f"${f:>+8.4f}  ${w:>+8.4f}  ${usd:>11,.0f}  ${pnl:>+11,.0f}")
            print()


if __name__ == '__main__':
    main()
