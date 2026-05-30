"""Kelly sizing and simulation for Stage-2 portfolio construction.

stdlib-only — uses `statistics` instead of numpy so this module is loadable
in the numpy-less CI Python step.  No sibling-module imports.

Math mirrors sizing_april_sim.py and forward_curated_selector.py exactly:
  full_kelly_fraction  <- sizing_april_sim.py:95  (f*=mu/var, clipped [0,1])
  exante_kelly_fraction <- forward_curated_selector.py:68  (base * t2/(t2+1))
  sim_fractional       <- sizing_april_sim.py:108  (compound)
  sim_flat             <- sizing_april_sim.py:125  (fixed-dollar)
"""
import math
import statistics
from dataclasses import dataclass


@dataclass
class SizingConfig:
    """Parameters for size_portfolio()."""
    mode: str           # 'fractional' or 'flat'
    kelly_fraction: float   # portfolio_sizing_kelly_fraction (scales applied f*)
    min_position_usd: float  # floor; bets whose stake < this are skipped
    bankroll_usd: float      # starting bankroll for the simulation


@dataclass
class SizingResult:
    """Output of size_portfolio()."""
    terminal_bankroll: float
    net_profit: float
    monthly_return_pct: float
    n_placed: int
    n_skipped: int
    applied_kelly_fraction: float   # the ex-ante fraction actually used
    mode: str


def full_kelly_fraction(returns):
    """Growth-optimal Kelly fraction f* = mu/sigma^2, clipped [0, 1].

    Uses population variance (ddof=0) matching numpy's np.var default.
    Returns 0.0 for empty, all-same, or non-positive-mean series.
    """
    if not returns:
        return 0.0
    mu = statistics.fmean(returns)
    # Population variance (ddof=0) — same as numpy default np.var().
    n = len(returns)
    if n < 2:
        return 0.0
    var = statistics.fmean([(r - mu) ** 2 for r in returns])
    if var <= 0.0:
        return 0.0
    return max(0.0, min(1.0, mu / var))


def exante_kelly_fraction(returns):
    """Ex-ante Kelly with t-stat shrinkage: f* * t^2/(t^2+1).

    Mirrors forward_curated_selector.py:kelly_base_and_shrink but uses
    sample variance (ddof=1) which is what that function uses (np.var(ddof=1)).
    Returns 0.0 when n < 5 (insufficient data for a reliable estimate).
    """
    n = len(returns)
    if n < 5:
        return 0.0
    mu = statistics.fmean(returns)
    # Sample variance (ddof=1) — matches forward_curated_selector.py:7.
    var = statistics.variance(returns)  # statistics.variance uses ddof=1
    if var <= 0.0:
        return 0.0
    f_star = max(0.0, mu / var)
    if f_star <= 0.0:
        return 0.0
    # t-stat: mu / (std / sqrt(n))
    std = math.sqrt(var)
    t = mu / (std / math.sqrt(n))
    if t <= 0.0:
        return 0.0
    m = t * t / (t * t + 1.0)
    return min(1.0, f_star * m)


def sim_fractional(returns, f, b0, min_pos):
    """Compound bankroll b0 at fraction f per bet; skip bets whose stake < min_pos.

    Returns (terminal_bankroll, n_placed, n_skipped).
    Returns (0.0, placed, skipped) if bankroll hits zero.
    """
    b = b0
    placed = skipped = 0
    for r in returns:
        stake = f * b
        if stake < min_pos:
            skipped += 1
            continue
        b += stake * r
        placed += 1
        if b <= 0.0:
            return 0.0, placed, skipped
    return b, placed, skipped


def sim_flat(returns, stake, b0, min_pos):
    """Fixed-dollar stake per bet (no compounding); skip if stake < min_pos.

    Returns (terminal_bankroll, n_placed, n_skipped).
    """
    if stake < min_pos:
        return b0, 0, len(returns)
    b = b0
    placed = skipped = 0
    for r in returns:
        if b < stake:
            skipped += 1
            continue
        b += stake * r
        placed += 1
    return b, placed, skipped


def size_portfolio(fwd_returns, exante_returns, cfg):
    """Run the chosen sizing simulation and return a SizingResult.

    Args:
        fwd_returns: list of per-position returns in the forward window.
        exante_returns: list of per-position returns in the prior window,
                        used to estimate the ex-ante Kelly fraction.
        cfg: SizingConfig.

    The applied Kelly fraction = cfg.kelly_fraction * exante_kelly_fraction(exante_returns).
    For 'flat' mode the stake is fixed at applied_f * cfg.bankroll_usd.
    For 'fractional' mode the stake compounds at applied_f * current_bankroll.
    """
    f_exante = exante_kelly_fraction(exante_returns)
    applied_f = cfg.kelly_fraction * f_exante

    if cfg.mode == 'fractional':
        terminal, placed, skipped = sim_fractional(
            fwd_returns, applied_f, cfg.bankroll_usd, cfg.min_position_usd
        )
    elif cfg.mode == 'flat':
        stake = applied_f * cfg.bankroll_usd
        terminal, placed, skipped = sim_flat(
            fwd_returns, stake, cfg.bankroll_usd, cfg.min_position_usd
        )
    else:
        raise ValueError(f"Unknown sizing mode: {cfg.mode!r}; expected 'fractional' or 'flat'")

    net_profit = terminal - cfg.bankroll_usd
    monthly_pct = (terminal / cfg.bankroll_usd - 1.0) * 100.0 if cfg.bankroll_usd > 0 else 0.0

    return SizingResult(
        terminal_bankroll=terminal,
        net_profit=net_profit,
        monthly_return_pct=monthly_pct,
        n_placed=placed,
        n_skipped=skipped,
        applied_kelly_fraction=applied_f,
        mode=cfg.mode,
    )
