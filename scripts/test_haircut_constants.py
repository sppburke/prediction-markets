#!/usr/bin/env python3
"""Drift guard for the canonical fee + slippage constants in
`scripts/composite_tuner/data.py`.

These constants mirror the canonical Rust + docs values:
- `polymarket_fee_rate = 0.04` → `POLYMARKET_FEE_RATE_BPS = 400`
- `slippage_rate = 0.01` → `SLIPPAGE_RATE_BPS = 100`

documented in `docs/_GLOSSARY.md` and implemented in
`crates/strategy-winner-follow/src/config.rs::default_polymarket_fee_rate()` /
`default_slippage_rate()` and applied at
`crates/strategy-winner-follow/src/evaluate.rs:95-112`.

If either assertion below fires, the implementor changed the Python constant
WITHOUT a co-update to `docs/_GLOSSARY.md`. Two action paths:
1. The change is intentional → update `docs/_GLOSSARY.md` and the pinned
   literal here together; also update the Rust defaults to match.
2. The change is accidental → revert the Python constant.

This test runs in CI as part of `.github/workflows/ci.yml` (Python step). It
catches Python ↔ glossary drift only; Rust ↔ glossary co-update is by
convention until the broader shared-TOML drift guard lands (tracker #248
out-of-scope: option (a)).

Run: `python3 scripts/test_haircut_constants.py`
  or: `pytest scripts/test_haircut_constants.py -v`
"""
import sys
import unittest
from pathlib import Path

# Make `composite_tuner` importable from the package dir.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from composite_tuner.data import POLYMARKET_FEE_RATE_BPS, SLIPPAGE_RATE_BPS


class HaircutConstantsTest(unittest.TestCase):
    """Pinned-literal asserts; co-update both sides when the canonical values move."""

    def test_polymarket_fee_rate_bps(self):
        # Pinned to docs/_GLOSSARY.md polymarket_fee_rate = 0.04 (March 2026 model).
        # Co-update: docs/_GLOSSARY.md AND scripts/composite_tuner/data.py.
        self.assertEqual(POLYMARKET_FEE_RATE_BPS, 400)

    def test_slippage_rate_bps(self):
        # Pinned to docs/_GLOSSARY.md slippage_rate = 0.01 (100 bps).
        # Co-update: docs/_GLOSSARY.md AND scripts/composite_tuner/data.py.
        self.assertEqual(SLIPPAGE_RATE_BPS, 100)


if __name__ == "__main__":
    unittest.main(verbosity=2)
