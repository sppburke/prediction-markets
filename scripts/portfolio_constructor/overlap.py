"""Market-overlap utilities for Stage-2 portfolio construction.

stdlib-only — no numpy/pandas/composite_tuner imports.
No sibling-module imports: this file is loaded standalone via sys.path in CI.
"""


def jaccard(a, b):
    """Jaccard similarity between two frozensets (or any sets).

    Returns 0.0 when both are empty (treats two empty universes as maximally
    dissimilar — conservative for the greedy selector: an empty candidate
    produces zero overlap penalty, which is correct).
    """
    if not a and not b:
        return 0.0
    union_size = len(a | b)
    if union_size == 0:
        return 0.0
    return len(a & b) / union_size


def marginal_overlap(candidate_markets, selected_union, market_sets):
    """Jaccard overlap of `candidate_markets` against `selected_union`.

    Args:
        candidate_markets: frozenset[(market_id, outcome_id)] for the candidate wallet.
        selected_union: frozenset union of market sets of all already-selected wallets.
        market_sets: unused at this layer (kept for API symmetry with richer providers).

    Returns a float in [0.0, 1.0].
    """
    _ = market_sets  # not used at the jaccard layer
    return jaccard(candidate_markets, selected_union)
