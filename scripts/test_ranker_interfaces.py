#!/usr/bin/env python3
"""Drift guard for the ranker harness plugin contracts (issue #421, PR1).

Verifies the typed ``Protocol`` skeleton in ``scripts/ranker/__init__.py``: each Protocol is
``runtime_checkable`` and matches on member presence, the reference ``EBShrinkageSkill``
satisfies ``Estimator``, and ``Criteria`` is a frozen dataclass with the spec'd fields.

Run: ``python3 scripts/test_ranker_interfaces.py``
"""
import sys
import unittest
from dataclasses import FrozenInstanceError, fields, is_dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ranker import (  # noqa: E402
    CapacityFilter,
    Criteria,
    Deflator,
    Demoter,
    Estimator,
    IntegrityFilter,
    Selector,
    SetTransitionPolicy,
    SignalCombiner,
    Validator,
)
from ranker.estimators import EBShrinkageSkill  # noqa: E402

ALL_PROTOCOLS = (
    Estimator, SignalCombiner, Deflator, Selector, SetTransitionPolicy,
    IntegrityFilter, CapacityFilter, Demoter, Validator,
)


class ProtocolConformanceTest(unittest.TestCase):
    def test_eb_shrinkage_is_estimator(self) -> None:
        self.assertIsInstance(EBShrinkageSkill(), Estimator)

    def test_estimator_requires_name_and_score(self) -> None:
        class HasBoth:
            name = "x"

            def score(self, ss, *, as_of, weights):  # noqa: ANN001, D401
                ...

        class MissingName:
            def score(self, ss, *, as_of, weights):  # noqa: ANN001, D401
                ...

        class MissingScore:
            name = "x"

        self.assertIsInstance(HasBoth(), Estimator)
        self.assertNotIsInstance(MissingName(), Estimator)
        self.assertNotIsInstance(MissingScore(), Estimator)

    def test_all_protocols_runtime_checkable(self) -> None:
        bare = object()  # satisfies none of the protocols (each declares >= 1 member)
        for proto in ALL_PROTOCOLS:
            with self.subTest(proto=proto.__name__):
                self.assertNotIsInstance(bare, proto)


class CriteriaTest(unittest.TestCase):
    def test_is_frozen_dataclass_with_expected_fields(self) -> None:
        self.assertTrue(is_dataclass(Criteria))
        names = {f.name for f in fields(Criteria)}
        self.assertEqual(
            names,
            {"active_within_secs", "ttr_hours", "price_min", "price_max",
             "half_life_days", "min_trl"},
        )

    def test_frozen(self) -> None:
        c = Criteria(active_within_secs=86_400, ttr_hours=72.0, price_min=0.15,
                     price_max=0.85, half_life_days=30.0, min_trl=10)
        with self.assertRaises(FrozenInstanceError):
            c.ttr_hours = 24.0  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main(verbosity=2)
