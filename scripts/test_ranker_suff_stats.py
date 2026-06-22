#!/usr/bin/env python3
"""Drift guard for the ``suff_stats`` substrate (issue #421, PR1).

Two layers:
  * end-to-end on a hermetic in-memory DuckDB (synthetic trades / resolutions / schedules
    created under the production view names) through the REAL production join
    ``ranker_duck.duck_extract_positions`` -> ``suff_stats.materialize``. Proves no-drift
    reuse, voided-market exclusion, sell-side exclusion, invalid-price exclusion, and the
    derived schema/columns.
  * pure ``derive_columns`` / ``concurrency`` unit tests on hand-built frames (no DuckDB).

Run: ``python3 scripts/test_ranker_suff_stats.py``
"""
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402

from ranker import suff_stats  # noqa: E402


def _make_con():
    """Fresh in-memory DuckDB with synthetic trades/resolutions/schedules under the production
    view names ``duck_extract_positions`` reads. Built via explicit DDL + parameterised INSERT
    so NULLs (voided markets) and column types are exact (avoids pandas-3.0 str-dtype scans)."""
    con = duckdb.connect()
    con.execute("CREATE TABLE trades(wallet_hex VARCHAR, market_id VARCHAR, outcome_id BIGINT, "
                "timestamp_unix BIGINT, side VARCHAR, price_str VARCHAR, contracts BIGINT)")
    con.executemany(
        "INSERT INTO trades VALUES (?,?,?,?,?,?,?)",
        [
            # 0xa / M1: a SELL before the buy (must be ignored), then the first BUY (won).
            ("0xa", "M1", 1, 500, "sell", "0.40", 10),
            ("0xa", "M1", 1, 1000, "buy", "0.40", 10),
            # 0xa / M2: BUY, lost (winning outcome = 2).
            ("0xa", "M2", 1, 2000, "buy", "0.60", 5),
            # 0xa / M4: VOIDED market (winning_outcome_id NULL) -> excluded.
            ("0xa", "M4", 1, 2500, "buy", "0.50", 8),
            # 0xb / M3: BUY, won.
            ("0xb", "M3", 1, 1500, "buy", "0.55", 20),
            # 0xb / M5: invalid price (>1) -> excluded by the valid-price filter.
            ("0xb", "M5", 1, 1600, "buy", "1.50", 3),
        ],
    )
    con.execute("CREATE TABLE market_resolutions(market_id VARCHAR, winning_outcome_id BIGINT, "
                "resolved_at_unix BIGINT)")
    con.executemany(
        "INSERT INTO market_resolutions VALUES (?,?,?)",
        [
            ("M1", 1, 5000),
            ("M2", 2, 9500),
            ("M3", 1, 3400),
            ("M4", None, 9999),  # voided: no winning outcome -> excluded by the INNER join
            ("M5", 1, 4000),
        ],
    )
    con.execute("CREATE TABLE market_schedules(market_id VARCHAR, end_date_unix BIGINT)")
    con.executemany(
        "INSERT INTO market_schedules VALUES (?,?)",
        [("M1", 4600), ("M2", 9200), ("M3", 3300), ("M4", 8000), ("M5", 4500)],
    )
    return con


class MaterializeEndToEndTest(unittest.TestCase):
    def setUp(self) -> None:
        self.ss = suff_stats.materialize(_make_con(), ["0xa", "0xb"]).set_index("market")

    def test_columns_and_index(self) -> None:
        ss = suff_stats.materialize(_make_con(), ["0xa", "0xb"])
        self.assertEqual(list(ss.columns), suff_stats.SUFF_STATS_COLUMNS)
        self.assertTrue(ss.index.equals(pd.RangeIndex(len(ss))))

    def test_qualifying_set(self) -> None:
        # M4 (voided) and M5 (invalid price) excluded; M1/M2/M3 kept.
        self.assertEqual(set(self.ss.index), {"M1", "M2", "M3"})

    def test_default_wallets_is_full_universe(self) -> None:
        # wallets=None -> all_wallets(con) discovers the universe from the trades view.
        ss = suff_stats.materialize(_make_con()).set_index("market")
        self.assertEqual(set(ss.index), {"M1", "M2", "M3"})

    def test_payoff(self) -> None:
        self.assertEqual(self.ss.loc["M1", "payoff"], 1.0)  # bought 1, won 1
        self.assertEqual(self.ss.loc["M2", "payoff"], 0.0)  # bought 1, won 2
        self.assertEqual(self.ss.loc["M3", "payoff"], 1.0)

    def test_first_buy_ignores_sell(self) -> None:
        self.assertEqual(self.ss.loc["M1", "entry_ts"], 1000)  # not the t=500 sell

    def test_ttr_ref_is_scheduled_close(self) -> None:
        # ttr_ref = entry_ts + (end_date - entry_ts) = end_date_unix (scheduled_only).
        self.assertEqual(self.ss.loc["M1", "ttr_ref"], 4600)
        self.assertEqual(self.ss.loc["M2", "ttr_ref"], 9200)

    def test_resolved_at(self) -> None:
        self.assertEqual(self.ss.loc["M1", "resolved_at"], 5000)

    def test_dollar_size_and_eff(self) -> None:
        self.assertAlmostEqual(self.ss.loc["M1", "dollar_size"], 0.40 * 10)
        self.assertAlmostEqual(self.ss.loc["M1", "_eff"], min(0.40 + 0.01, 0.999))

    def test_concurrency(self) -> None:
        # 0xa labels: M1 [1000,4600], M2 [2000,9200]. At M1 entry only M1 live -> 1; at M2
        # entry both live -> 2. 0xb has a single label -> 1.
        self.assertEqual(self.ss.loc["M1", "c_t"], 1.0)
        self.assertEqual(self.ss.loc["M2", "c_t"], 2.0)
        self.assertEqual(self.ss.loc["M3", "c_t"], 1.0)


def _raw() -> pd.DataFrame:
    """A hand-built 9-column frame in the shape ``duck_extract_positions`` returns."""
    return pd.DataFrame({
        "wallet": ["0xa", "0xa"],
        "market_id": ["m1", "m2"],
        "outcome_id": [1, 2],
        "entry_ts": [1000, 2000],
        "ttr_secs": [3600, 7200],
        "price": [0.4, 0.6],
        "contracts": [10, 5],
        "payoff": [1.0, 0.0],
        "resolved_at": [5000, 9500],
    })


class DeriveColumnsTest(unittest.TestCase):
    def test_schema_and_values(self) -> None:
        ss = suff_stats.derive_columns(_raw())
        self.assertEqual(list(ss.columns), suff_stats.SUFF_STATS_COLUMNS)
        self.assertTrue(ss.index.equals(pd.RangeIndex(len(ss))))
        self.assertEqual(ss.loc[0, "ttr_ref"], 1000 + 3600)
        self.assertAlmostEqual(ss.loc[0, "dollar_size"], 0.4 * 10)
        self.assertAlmostEqual(ss.loc[1, "_eff"], min(0.6 + 0.01, 0.999))

    def test_with_concurrency_false_is_nan(self) -> None:
        ss = suff_stats.derive_columns(_raw(), with_concurrency=False)
        self.assertTrue(ss["c_t"].isna().all())

    def test_custom_slip(self) -> None:
        ss = suff_stats.derive_columns(_raw(), slip=0.05)
        self.assertAlmostEqual(ss.loc[0, "_eff"], 0.45)


class ConcurrencyTest(unittest.TestCase):
    def test_overlapping_labels(self) -> None:
        ss = pd.DataFrame({
            "wallet": ["w", "w", "w"],
            "entry_ts": [10, 15, 30],
            "ttr_ref": [20, 25, 40],
        })
        np.testing.assert_array_equal(suff_stats.concurrency(ss), [1.0, 2.0, 1.0])

    def test_independent_wallets_do_not_share(self) -> None:
        ss = pd.DataFrame({
            "wallet": ["a", "b"],
            "entry_ts": [10, 12],
            "ttr_ref": [100, 100],
        })
        np.testing.assert_array_equal(suff_stats.concurrency(ss), [1.0, 1.0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
