"""Tests for scripts/ranker/bench_composition.py (item 3.4, docs/32)."""
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ranker import bench_composition as bc  # noqa: E402


def _matrix() -> pd.DataFrame:
    """24-period synthetic panel: losing baseline; strong low-variance hybrid; big
    high-variance online_weighting; a true_clv hybrid with the HIGHEST paired t (must be
    barred); one NaN no-signal period on the hybrid (zero-filled by the rule)."""
    rng = np.random.default_rng(3)
    n = 24
    base = rng.normal(-10.0, 2.0, n)
    hybrid = rng.normal(40.0, 5.0, n)
    hybrid[3] = np.nan  # pre-#475 no-signal period -> zero-filled by the rule
    ow = rng.normal(300.0, 400.0, n)
    clv = rng.normal(60.0, 1.0, n)  # tightest arm — highest t, but true_clv-ranked
    return pd.DataFrame({
        "t_stat_baseline|none|policy_full_rerank|b|churn0.0": base,
        "t_stat_baseline|none|policy_hybrid_displacement|b|churn0.75": hybrid,
        "eb_shrinkage_skill|none|policy_online_weighting|b|churn0.75": ow,
        "true_clv|none|policy_hybrid_displacement|b|churn0.75": clv,
    })


BASE = "t_stat_baseline|none|policy_full_rerank|b|churn0.0"


class PairedTTest(unittest.TestCase):
    def test_matches_scipy_ttest_rel_on_zero_filled_panel(self) -> None:
        m = _matrix()
        table = bc.paired_t_vs_baseline(m, BASE)
        z = m.fillna(0.0)
        for cfg in m.columns:
            if cfg == BASE:
                continue
            expected = stats.ttest_rel(z[cfg], z[BASE]).statistic
            self.assertAlmostEqual(table.loc[cfg, "paired_t"], expected, places=10)

    def test_missing_baseline_raises(self) -> None:
        with self.assertRaises(ValueError):
            bc.paired_t_vs_baseline(_matrix(), "nope")


class ComposeBenchTest(unittest.TestCase):
    def test_slots_fixed_by_family_and_clv_barred(self) -> None:
        out = bc.compose_bench(_matrix(), BASE)
        bench = out["bench"]
        # The true_clv hybrid has the HIGHEST paired t but is INELIGIBLE (A3): the
        # hybrid slot must go to the t_stat arm.
        table = out["table"]
        self.assertEqual(table.index[0],
                         "true_clv|none|policy_hybrid_displacement|b|churn0.75")
        self.assertEqual(bench["policy_hybrid_displacement"]["config"],
                         "t_stat_baseline|none|policy_hybrid_displacement|b|churn0.75")
        self.assertEqual(bench["policy_online_weighting"]["config"],
                         "eb_shrinkage_skill|none|policy_online_weighting|b|churn0.75")

    def test_family_with_no_eligible_arm_is_none(self) -> None:
        m = _matrix().drop(columns=["eb_shrinkage_skill|none|policy_online_weighting|b|churn0.75"])
        out = bc.compose_bench(m, BASE)
        self.assertIsNone(out["bench"]["policy_online_weighting"])


if __name__ == "__main__":
    unittest.main()
