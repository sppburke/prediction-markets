"""Greedy max-edge / min-overlap wallet selector for Stage-2 portfolio construction.

stdlib-only — no numpy/pandas/composite_tuner imports.
No sibling-module imports: overlap_fn is injected by the caller so this file
can be loaded standalone via sys.path in CI without importing overlap.py.
"""
from dataclasses import dataclass, field


@dataclass
class SelectionResult:
    """Output of a GreedySelector.select() call."""
    wallets: list          # selected wallet hex addresses in selection order
    scores: list           # per-wallet GBM edge score (float)
    objectives: list       # per-wallet greedy objective value at selection time
    market_union: object   # frozenset of all (market_id, outcome_id) covered


class GreedySelector:
    """Greedy max-edge / min-overlap wallet selector.

    At each step selects the wallet maximising:
        score * (1 - overlap_lambda * overlap_fn(candidate, selected_union, market_sets))

    Args:
        overlap_fn: callable(candidate_markets, selected_union, market_sets) -> float in [0,1].
                    Injected by the caller (e.g. overlap.marginal_overlap); no import needed.
        overlap_lambda: weight on the overlap penalty (default 1.0).
                        0 => pure top-N by score, 1 => balanced edge/diversity.
    """

    def __init__(self, overlap_fn, overlap_lambda=1.0):
        self._overlap_fn = overlap_fn
        self._lambda = overlap_lambda

    def select(self, scores, market_sets, max_n, min_edge_score=0.0):
        """Select a diversified subset of wallets.

        Args:
            scores: dict[wallet_hex -> float] of edge scores (higher = better).
            market_sets: dict[wallet_hex -> frozenset[(market_id, outcome_id)]].
                         Wallets absent from market_sets are treated as having empty sets.
            max_n: maximum number of wallets to select.
            min_edge_score: greedy objective threshold; stop when best remaining
                            objective <= this value (default 0.0 = stop at non-positive).

        Returns:
            SelectionResult
        """
        candidates = [w for w in scores if scores[w] > min_edge_score]
        # Sort descending by score for deterministic tie-breaking.
        candidates.sort(key=lambda w: (-scores[w], w))

        selected = []
        selected_scores = []
        selected_objs = []
        selected_union = frozenset()

        for _ in range(max_n):
            if not candidates:
                break

            best_w = None
            best_obj = None
            for w in candidates:
                mset = market_sets.get(w, frozenset())
                overlap = self._overlap_fn(mset, selected_union, market_sets)
                obj = scores[w] * (1.0 - self._lambda * overlap)
                if best_obj is None or obj > best_obj:
                    best_obj = obj
                    best_w = w

            if best_w is None or best_obj <= min_edge_score:
                break

            selected.append(best_w)
            selected_scores.append(scores[best_w])
            selected_objs.append(best_obj)
            mset = market_sets.get(best_w, frozenset())
            selected_union = selected_union | mset
            candidates.remove(best_w)

        return SelectionResult(
            wallets=selected,
            scores=selected_scores,
            objectives=selected_objs,
            market_union=selected_union,
        )
