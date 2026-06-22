"""Multiple-testing deflation for the ranker bake-off (issue #421).

The bake-off scores many wallets AND many configs; both are multiple-testing, so a raw
Sharpe/edge that looks significant is inflated by selection (the winner's-curse this harness
exists to kill). The Deflated Sharpe Ratio (Bailey & Lopez de Prado, JPM 2014) corrects an
observed SR for the number of trials N, non-normality (skew/kurtosis), and sample length.

Conforms to the ``Deflator`` Protocol: per-wallet (``n_trials`` = candidate count) AND
grid-level (``n_trials`` = the pre-registered ``N_GRID``).
"""
import math

import pandas as pd
from scipy.stats import norm

# Euler-Mascheroni constant, for the expected-maximum-of-N-Gaussians false-strategy threshold.
_EULER_MASCHERONI = 0.577_215_664_901_532_9
# Floor for the SR sampling-variance denominator (guards extreme skew/kurtosis -> non-positive).
_VAR_FLOOR = 1e-12


def expected_max_sharpe(n_trials: int, sr_variance: float) -> float:
    """Expected maximum of ``n_trials`` i.i.d. null Sharpe estimates (Bailey-LdP 2014, SR0).

    ``SR0 = sqrt(sr_variance) * [(1-g)*Phi^-1(1 - 1/N) + g*Phi^-1(1 - 1/(N*e))]`` with ``g`` the
    Euler-Mascheroni constant and ``N = n_trials``. This is the false-strategy threshold a real SR
    must beat. Returns ``0.0`` for ``n_trials <= 1`` (no selection) or non-positive variance.
    Monotonically increasing in ``n_trials`` (more trials -> a higher bar).
    """
    if n_trials <= 1 or sr_variance <= 0:
        return 0.0
    n = float(n_trials)
    term = ((1.0 - _EULER_MASCHERONI) * norm.ppf(1.0 - 1.0 / n)
            + _EULER_MASCHERONI * norm.ppf(1.0 - 1.0 / (n * math.e)))
    return math.sqrt(sr_variance) * term


def deflated_sharpe_ratio(sr: float, n_obs: int, skew: float, kurt: float, *,
                          sr0: float = 0.0) -> float:
    """Deflated Sharpe Ratio = ``P(true SR > sr0)`` (Bailey-LdP 2014).

    ``DSR = Phi( (sr - sr0) * sqrt(n_obs - 1) / sqrt(1 - skew*sr + (kurt-1)/4 * sr^2) )``.
    ``sr``/``sr0`` are per-observation (NOT annualised). ``kurt`` is the non-excess kurtosis
    (normal = 3). Returns NaN if ``n_obs < 2`` (the SR sampling variance is undefined).
    """
    if n_obs < 2:
        return float("nan")
    denom = math.sqrt(max(1.0 - skew * sr + (kurt - 1.0) / 4.0 * sr * sr, _VAR_FLOOR))
    z = (sr - sr0) * math.sqrt(n_obs - 1.0) / denom
    return float(norm.cdf(z))


class DeflatedSharpe:
    """``Deflator`` wrapper: adds a ``dsr`` column = ``P(true SR > expected-max-null SR)``.

    Expects ``scores`` to carry per-row columns ``sr`` (per-observation Sharpe), ``n_obs``,
    ``skew``, ``kurt``. The null ``SR0`` is the expected maximum over ``n_trials`` given the
    observed cross-row SR dispersion (``scores['sr'].var``), so deflation strengthens as more
    configs/wallets are tried. ``as_of`` is part of the Protocol but unused here (DSR is
    point-in-time over the supplied scores).
    """

    name = "deflated_sharpe"

    def deflate(self, scores: pd.DataFrame, *, n_trials: int, as_of: int) -> pd.DataFrame:
        sr_var = float(scores["sr"].var(ddof=1)) if len(scores) > 1 else 0.0
        sr0 = expected_max_sharpe(n_trials, sr_var)
        out = scores.copy()
        out["dsr"] = [
            deflated_sharpe_ratio(float(r.sr), int(r.n_obs), float(r.skew), float(r.kurt), sr0=sr0)
            for r in scores.itertuples(index=False)
        ]
        return out


# Registered by `.name` (issue #421 "Architecture" — each module registered by .name).
DEFLATOR_REGISTRY: "dict[str, type]" = {DeflatedSharpe.name: DeflatedSharpe}
