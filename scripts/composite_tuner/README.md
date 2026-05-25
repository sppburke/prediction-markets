# composite-tuner

Optuna+PBO weight-optimisation harness for `pe-skill-select composite`.
Closes [#238](https://github.com/sppburke/prediction-markets/issues/238).

## What it does

Tunes the 12 `CompositeWeights` (`PE_SKILL_COMPOSITE_W_*`) to maximise the
**realized OOS edge-LCB** across a **walk-forward CV** of `wallet_features`
cutoffs, then runs **Bailey & López de Prado PBO** calibration on the trial
score matrix to honestly report overfitting risk.

The Rust `pe-skill-select composite` binary is the rank oracle (single source
of truth for the composite math). The Python tool drives it via env vars and
parses stdout. **OOS edge is computed directly in Python from `wallet_cache.db`** —
faster than shelling out to `forward-test` per trial.

## Prerequisites

- `optuna` installed in `.venv-analysis/`:
  ```
  .venv-analysis/bin/python3 -m pip install optuna
  ```
- `pe-skill-select` release binary built:
  ```
  cargo build --release -p pe-skill-select
  ```
- `wallet_features` has rows at **multiple `cutoff_unix` values** — PBO needs
  ≥ 4 folds. Generate via:
  ```
  for ts in <list of unix timestamps>; do
      PE_SKILL_CUTOFF_UNIX=$ts ./target/release/pe-skill-select extract
  done
  ```

## Smoke run (~5 min wall-clock)

```
.venv-analysis/bin/python3 scripts/composite_tuner/cli.py \
    --db-path data/wallet_cache.db --smoke
```

Smoke mode: 20 trials, 10 PBO permutations, 2 cutoffs. Use during dev /
sanity checking. PBO is degenerate at 2 cutoffs (n_windows < 4) but the
end-to-end run completes.

## Full run (~hours, depending on n_cutoffs and n_trials)

```
.venv-analysis/bin/python3 scripts/composite_tuner/cli.py \
    --db-path data/wallet_cache.db \
    --n-trials 300 --pbo-perms 100 \
    --output-toml data/composite-tuner/best-weights.toml \
    --output-json data/composite-tuner/study-report.json
```

## Output

- **stdout**: human-readable report (best weights, per-window LCB, PBO line).
- **`--output-toml`**: TOML fragment with `composite_w_*` keys — paste into a
  SkillConfig TOML and `pe-skill-select composite <toml>` uses the new weights.
- **`--output-json`**: full trial history + per-window scores + PBO logits
  for downstream plotting / analysis.

## Reading the PBO result

```
PBO=0.230 (OK)  median_OOS_rank=0.638  [n_trials=300, n_windows=8, n_perms=100]
```

- **PBO ≤ 0.50** = the best in-sample trial is, on average, above the OOS
  median → real signal.
- **PBO > 0.50** = the best in-sample trial is systematically below OOS
  median → tuning is selecting in-sample noise. Reject the chosen weights;
  rely on simpler priors (e.g. EV-only) instead.
- **`median_OOS_rank` near 0.5** = OOS performance is random across permutations
  → no signal. Near 1.0 = strong signal.

## Files

- `data.py` — SQLite reads (`distinct_cutoffs`, `load_oos_positions`).
- `cv.py` — walk-forward window generation from existing cutoffs.
- `objective.py` — subprocess oracle (`invoke_composite`), OOS scorer
  (`edge_lcb`, `score_window`), top-level `evaluate_weights`.
- `pbo.py` — Bailey & López de Prado PBO calibration on a trial score matrix.
- `tuner.py` — Optuna study orchestration + per-trial bookkeeping.
- `cli.py` — argparse entry + report formatting.

## Caveats

- **Gross of fees.** Inherits the same gross-of-fees assumption as
  `pe-skill-select forward-test`. Apply uniform fee subtraction at the
  comparison step if needed.
- **Single-source rank.** Only tunes the `composite` ranker's weights —
  doesn't compare against alternative ranker classes (e.g. GBM, RF). That's
  a Phase 4 follow-up if Phase 3 shows weight-tuning has gradient.
- **PBO requires ≥ 4 windows.** With fewer windows the PBO half-split is
  degenerate and the result is flagged as N/A.
