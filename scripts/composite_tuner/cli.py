"""CLI entry: `python -m composite_tuner.cli [args]` or
`scripts/composite_tuner/cli.py [args]`. Writes a TOML fragment of the
chosen weights + a JSON report of the full study.

Smoke mode (`--smoke`): n_trials=20, pbo_perms=10, n_cutoffs=2 (~5 min wall).
Full mode (defaults): n_trials=300, pbo_perms=100, all cutoffs (~hours).
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

# Allow `python scripts/composite_tuner/cli.py` from the repo root by adding
# the parent dir to sys.path so `from composite_tuner import ...` works.
_SELF_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_SELF_DIR.parent))

from composite_tuner.tuner import TunerConfig, run_tuner  # noqa: E402
from composite_tuner.pbo import pbo_summary  # noqa: E402
from composite_tuner.objective import WEIGHT_NAMES  # noqa: E402


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        prog="composite-tuner",
        description=(
            "Optuna+PBO weight optimisation for pe-skill-select composite."
        ),
    )
    p.add_argument("--db-path", required=True, help="path to wallet_cache.db")
    p.add_argument(
        "--binary",
        default="./target/release/pe-skill-select",
        help="path to pe-skill-select binary (default ./target/release/pe-skill-select)",
    )
    p.add_argument("--smoke", action="store_true", help="fast mode: 20 trials, 10 PBO perms, 2 cutoffs")
    p.add_argument("--n-trials", type=int, default=None)
    p.add_argument("--n-cutoffs", type=int, default=None, help="use most-recent N cutoffs (None=all)")
    p.add_argument("--pbo-perms", type=int, default=None)
    p.add_argument("--fwd-days", type=int, default=30)
    p.add_argument("--top-n", type=int, default=5000)
    p.add_argument("--bhq-q-bps", type=int, default=1000)
    p.add_argument("--min-trading-days", type=int, default=20)
    p.add_argument("--z-lcb", type=float, default=1.645)
    p.add_argument("--sampler", default="tpe", choices=["tpe", "cmaes"])
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--storage", default=None, help="Optuna SQLite URL (None=in-memory)")
    p.add_argument("--output-json", default=None)
    p.add_argument("--output-toml", default=None)
    return p.parse_args(argv)


def format_report(result) -> str:
    lines = []
    lines.append("=" * 70)
    lines.append("composite-tuner result")
    lines.append("=" * 70)
    lines.append(f"Best mean OOS edge-LCB: {result.best_mean_lcb:+.6f}")
    lines.append(f"Best trial number: #{result.best_trial_number}")
    lines.append(f"Trials completed: {result.n_trials_completed}")
    lines.append(f"CV windows: {len(result.windows)}")
    lines.append("")
    lines.append("Best weights:")
    for name in WEIGHT_NAMES:
        v = result.best_weights.get(name, 0)
        lines.append(f"  PE_SKILL_COMPOSITE_W_{name:42s} = {v:+6d}")
    lines.append("")
    lines.append("Per-window scores (best trial):")
    for ws in result.window_scores_best:
        lines.append(
            f"  {ws.window_label:30s}  n_pos={ws.n_positions:7d}  lcb={ws.edge_lcb:+.6f}"
        )
    lines.append("")
    lines.append(pbo_summary(result.pbo))
    return "\n".join(lines)


def result_to_json(result) -> dict:
    return {
        "config": {
            "n_trials": result.config.n_trials,
            "n_cutoffs": result.config.n_cutoffs,
            "pbo_perms": result.config.pbo_perms,
            "fwd_days": result.config.fwd_days,
            "top_n": result.config.top_n,
            "bhq_q_bps": result.config.bhq_q_bps,
            "min_trading_days": result.config.min_trading_days,
            "sampler": result.config.optuna_sampler,
            "seed": result.config.optuna_seed,
        },
        "best": {
            "trial_number": result.best_trial_number,
            "mean_lcb": result.best_mean_lcb,
            "weights": result.best_weights,
            "per_window": [
                {
                    "label": ws.window_label,
                    "n_selected": ws.n_selected,
                    "n_positions": ws.n_positions,
                    "edge_lcb": ws.edge_lcb,
                }
                for ws in result.window_scores_best
            ],
        },
        "pbo": {
            "pbo": result.pbo.pbo,
            "median_oos_rank": result.pbo.median_oos_rank,
            "n_perms": result.pbo.n_perms,
            "n_trials": result.pbo.n_trials,
            "n_windows": result.pbo.n_windows,
        },
        "trial_history": [
            {"trial": t, "mean_lcb": s, "weights": w}
            for (t, s, w) in result.trial_history
        ],
    }


def result_to_toml(result) -> str:
    """Emit a TOML fragment suitable for paste into a SkillConfig TOML.
    The fields here mirror SkillConfig::composite_w_* names — figment
    consumes them directly when `pe-skill-select <subcmd> <toml_path>`
    is run with this file as the config.
    """
    lines = []
    lines.append(f"# composite-tuner — best trial #{result.best_trial_number}")
    lines.append(f"# Mean OOS edge-LCB: {result.best_mean_lcb:+.6f}")
    lines.append(pbo_summary(result.pbo).replace("\n", "\n# "))
    lines.append("")
    for name in WEIGHT_NAMES:
        toml_key = f"composite_w_{name.lower()}"
        v = result.best_weights.get(name, 0)
        lines.append(f"{toml_key} = {v}")
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    if args.smoke:
        n_trials = args.n_trials or 20
        pbo_perms = args.pbo_perms or 10
        n_cutoffs = args.n_cutoffs or 2
    else:
        n_trials = args.n_trials or 300
        pbo_perms = args.pbo_perms or 100
        n_cutoffs = args.n_cutoffs
    cfg = TunerConfig(
        db_path=args.db_path,
        binary=args.binary,
        n_trials=n_trials,
        n_cutoffs=n_cutoffs,
        pbo_perms=pbo_perms,
        fwd_days=args.fwd_days,
        top_n=args.top_n,
        bhq_q_bps=args.bhq_q_bps,
        min_trading_days=args.min_trading_days,
        z_lcb=args.z_lcb,
        optuna_sampler=args.sampler,
        optuna_seed=args.seed,
        storage=args.storage,
    )
    try:
        result = run_tuner(cfg)
    except Exception as e:
        print(f"composite-tuner: fatal {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    print(format_report(result))
    if args.output_json:
        Path(args.output_json).write_text(json.dumps(result_to_json(result), indent=2))
        print(f"[json] {args.output_json}")
    if args.output_toml:
        Path(args.output_toml).write_text(result_to_toml(result))
        print(f"[toml] {args.output_toml}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
