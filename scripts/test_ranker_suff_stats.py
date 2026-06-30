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

import ranker_duck  # noqa: E402
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
            # 0xb / M3: BUY, won. A later pre-resolution trade (t=3000 < resolved 3400) moves the
            # close proxy to 0.80 while the FIRST buy (t=1500) stays the entry.
            ("0xb", "M3", 1, 1500, "buy", "0.55", 20),
            ("0xb", "M3", 1, 3000, "buy", "0.80", 5),
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


def _make_clv_con():
    """``_make_con()`` plus the OPTIONAL CLOB views the true_clv path joins (issue #429 PR4):
    ``token_conditions`` (token→outcome map) + ``market_price_history`` (the CLOB series). Built as
    TABLEs under the production view names — ``information_schema`` lists both tables and views, so
    ``suff_stats._relation_exists`` finds them."""
    con = _make_con()
    con.execute("CREATE TABLE token_conditions(token_id VARCHAR, condition_id VARCHAR, "
                "outcome_index BIGINT, fetched_at_unix BIGINT)")
    con.executemany(
        "INSERT INTO token_conditions VALUES (?,?,?,?)",
        [
            ("tokY", "M3", 1, 9),     # M3 bought outcome 1 -> tokY
            ("tokN", "M2", None, 9),  # M2 token mapped but NULL outcome_index -> skipped
        ],
    )
    # M3 close_ref = COALESCE(end_date 3300, resolved_at 3400) = 3300.
    con.execute("CREATE TABLE market_price_history(market_id VARCHAR, token_id VARCHAR, "
                "t BIGINT, price VARCHAR, source VARCHAR)")
    con.executemany(
        "INSERT INTO market_price_history VALUES (?,?,?,?,?)",
        [
            ("M3", "tokY", 3000, "0.70", "clob"),
            ("M3", "tokY", 3100, "0.85", "clob"),    # last clob <= close_ref -> the true close 0.85
            ("M3", "tokY", 3300, "0.10", "trades"),  # latest t but source!='clob' -> excluded
            ("M3", "tokY", 4000, "0.99", "clob"),    # after close -> excluded by t <= close_ref
            ("M2", "tokN", 3000, "0.50", "clob"),    # M2 outcome_index NULL -> excluded by the join
        ],
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

    def test_close_proxy_is_last_pre_resolution_price(self) -> None:
        # M1/M2: single buy each -> proxy == entry. M3: a later t=3000 trade at 0.80 (< resolved
        # 3400) moves the proxy off the 0.55 first-buy entry. proxy_clv reads this column.
        self.assertAlmostEqual(self.ss.loc["M1", "close_proxy"], 0.40)
        self.assertAlmostEqual(self.ss.loc["M2", "close_proxy"], 0.60)
        self.assertAlmostEqual(self.ss.loc["M3", "close_proxy"], 0.80)

    def test_true_clv_close_all_nan_when_clob_views_absent(self) -> None:
        # _make_con() registers no CLOB views -> the true_clv merge is all-NaN (graceful: true_clv
        # then degrades to an empty ranking, never crashes / mis-prices).
        self.assertIn("true_clv_close", self.ss.columns)
        self.assertTrue(self.ss["true_clv_close"].isna().all())


class TrueClvMaterializeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.ss = suff_stats.materialize(_make_clv_con(), ["0xa", "0xb"]).set_index("market")

    def test_true_clv_close_is_clob_close_pinned(self) -> None:
        # M3/tokY: arg_max over source='clob' points with t <= close_ref(=end_date 3300) -> the
        # t=3100 mid 0.85 — NOT the trades@3300 (source filter) nor the clob@4000 (after close).
        # Distinct from close_proxy=0.80 (last trade) -> the two columns compute independently.
        self.assertAlmostEqual(self.ss.loc["M3", "true_clv_close"], 0.85)
        self.assertAlmostEqual(self.ss.loc["M3", "close_proxy"], 0.80)

    def test_null_outcome_index_and_unmapped_are_nan(self) -> None:
        # M2 has a token row but NULL outcome_index -> skipped; M1 has no token map -> NaN.
        self.assertTrue(np.isnan(self.ss.loc["M2", "true_clv_close"]))
        self.assertTrue(np.isnan(self.ss.loc["M1", "true_clv_close"]))


class TrueClvCloseClampTest(unittest.TestCase):
    """B5 (#436): the true_clv close is pinned to ``t <= LEAST(end_date, resolved_at)``, so for an
    EARLY-resolved market (``resolved_at < end_date``) it cannot use a post-resolution — and, for an
    in-sample position, post-``as_of`` — CLOB tick. Runs ``_TRUE_CLV_SQL`` directly on a hermetic
    fixture (the only true_clv consumer is in-sample scoring, where ``resolved_at <= as_of``)."""

    @staticmethod
    def _con():
        con = duckdb.connect()
        con.execute("CREATE TABLE market_resolutions(market_id VARCHAR, winning_outcome_id BIGINT, "
                    "resolved_at_unix BIGINT)")
        con.executemany("INSERT INTO market_resolutions VALUES (?,?,?)",
                        [("EARLY", 1, 1000), ("NORMAL", 1, 3000)])
        con.execute("CREATE TABLE market_schedules(market_id VARCHAR, end_date_unix BIGINT)")
        con.executemany("INSERT INTO market_schedules VALUES (?,?)",
                        [("EARLY", 2000), ("NORMAL", 2500)])   # EARLY resolves (1000) BEFORE end (2000)
        con.execute("CREATE TABLE token_conditions(token_id VARCHAR, condition_id VARCHAR, "
                    "outcome_index BIGINT, fetched_at_unix BIGINT)")
        con.executemany("INSERT INTO token_conditions VALUES (?,?,?,?)",
                        [("tE", "EARLY", 1, 9), ("tN", "NORMAL", 1, 9)])
        con.execute("CREATE TABLE market_price_history(market_id VARCHAR, token_id VARCHAR, "
                    "t BIGINT, price VARCHAR, source VARCHAR)")
        con.executemany("INSERT INTO market_price_history VALUES (?,?,?,?,?)", [
            ("EARLY", "tE", 900, "0.60", "clob"),    # last clob <= resolved 1000 -> the true close
            ("EARLY", "tE", 1500, "0.99", "clob"),   # post-resolution: old COALESCE(end 2000) leaked it
            ("NORMAL", "tN", 2400, "0.70", "clob"),  # last clob <= end 2500 -> the true close
            ("NORMAL", "tN", 2800, "0.80", "clob"),  # > end 2500 -> excluded (clamp is a no-op here)
        ])
        return con

    def test_early_resolution_close_capped_at_resolution(self) -> None:
        out = suff_stats.true_clv_close_prices(self._con()).set_index("market_id")
        # EARLY: LEAST(end 2000, resolved 1000) = 1000 -> 0.60, NOT the post-resolution 0.99 the bare
        # COALESCE(end_date, resolved_at) anchor would have leaked.
        self.assertAlmostEqual(out.loc["EARLY", "true_clv_close"], 0.60)
        # NORMAL (resolved 3000 >= end 2500): anchor stays end_date 2500 -> 0.70 (clamp is a no-op).
        self.assertAlmostEqual(out.loc["NORMAL", "true_clv_close"], 0.70)


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


class MaterializeAsTest(unittest.TestCase):
    """Phase-A memory path (issue #468 follow-up): ``duck_extract_positions(materialize_as=…)``
    writes a DuckDB temp TABLE (returns None) carrying the SAME positions as the ``.df()`` path —
    so the CLV joins can run in DuckDB instead of two pandas merges; a non-identifier name is rejected."""

    def test_temp_table_matches_df_path(self) -> None:
        con = _make_con()
        df = ranker_duck.duck_extract_positions(con, ["0xa", "0xb"], **suff_stats._PERMISSIVE)
        ret = ranker_duck.duck_extract_positions(
            con, ["0xa", "0xb"], **suff_stats._PERMISSIVE, materialize_as="_pe_positions")
        self.assertIsNone(ret)
        tbl = con.execute("SELECT * FROM _pe_positions").df()
        self.assertEqual(len(df), len(tbl))
        key = ["wallet", "market_id", "outcome_id"]
        self.assertEqual(sorted(map(tuple, df[key].itertuples(index=False))),
                         sorted(map(tuple, tbl[key].itertuples(index=False))))

    def test_bad_identifier_rejected(self) -> None:
        with self.assertRaises(ValueError):
            ranker_duck.duck_extract_positions(
                _make_con(), ["0xa"], **suff_stats._PERMISSIVE,
                materialize_as="x; DROP TABLE trades")


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

    def test_close_proxy_defaults_nan_without_merge(self) -> None:
        # derive_columns is pure; a raw frame WITHOUT the materialize-side close_proxy merge gets
        # all-NaN close_proxy (proxy_clv then yields no scores rather than crashing).
        ss = suff_stats.derive_columns(_raw())
        self.assertIn("close_proxy", ss.columns)
        self.assertTrue(ss["close_proxy"].isna().all())

    def test_close_proxy_propagates_when_present(self) -> None:
        raw = _raw()
        raw["close_proxy"] = [0.7, 0.3]
        ss = suff_stats.derive_columns(raw)
        self.assertEqual(list(ss["close_proxy"]), [0.7, 0.3])

    def test_true_clv_close_defaults_nan_without_merge(self) -> None:
        # Pure derive_columns: a raw frame WITHOUT the materialize-side true_clv merge gets all-NaN
        # true_clv_close (true_clv then yields no scores rather than crashing).
        ss = suff_stats.derive_columns(_raw())
        self.assertIn("true_clv_close", ss.columns)
        self.assertTrue(ss["true_clv_close"].isna().all())

    def test_true_clv_close_propagates_when_present(self) -> None:
        raw = _raw()
        raw["true_clv_close"] = [0.9, 0.2]
        ss = suff_stats.derive_columns(raw)
        self.assertEqual(list(ss["true_clv_close"]), [0.9, 0.2])

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
