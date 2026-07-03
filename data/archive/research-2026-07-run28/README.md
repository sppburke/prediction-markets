# run28 — 2026 weekly full-grid bake-off (archived artifacts)

Findings document: `docs/33-RUN28-2026-WEEKLY-BAKEOFF-RESULTS.md`. Evidence class:
**bench-composition / descriptive only** per `docs/32` §1 (amendment A4) — never
certification.

Provenance: operator-ordered run on a rented Lambda 80-core / 440 GB box
(2026-07-02 → 2026-07-03 UTC), box repo at `a1d7601` (identical to the merge base of
this archive's PR). Box terminated after rsync; these files are the complete surviving
record. Cache shipped from the local box; integrity checksum in `box_cache.sha`.

| file | what it is |
|---|---|
| `decision.json` | harness verdict: WINNER, PBO 0.0103, Hansen-SPA p=0.005, CPR go, 24/24 clean periods; AKM/MRSW/FCR uncertainty block |
| `leaderboard.csv` | 768 configs × per-config moments (cum_return, weekly mean/sd/sr/se, CLV diagnostics) |
| `return_matrix.csv` | 24 weekly USD P&L rows × 768 config columns (the frozen panel; column sums == leaderboard cum_return to 3e-11) |
| `capacity_leaderboard.csv` | derived: capacity-feasible $/week ranking at the live bankroll (see docs/33 §4 for the metric) |
| `mtm_coverage.json` | per-config `positions_in_window`, open-at-horizon, MTM mark rates, resolution-lag stats |
| `manifest.json` | pre-registered grid: axes + all 768 config keys, created_at, forward_criteria_filter=true |
| `survivorship.json` | the standing optimistic-bias disclosure (#385 purge; current-snapshot universe) |
| `true_clv_preflight.json` | true_clv exclusion record: coverage 25% < 30% gate (#429 permanent ceiling) |
| `clv_diagnostic.json` | advisory CLV t-stats — struck from narratives per docs/32 A3; kept for the record |
| `progress.jsonl` | phase timings: materialize 268s, screen 692s, grid 34,165s (9.49 h) |
| `analysis_capacity.py` | the capacity + TTR + policy analysis that produced capacity_leaderboard.csv (run against this dir) |
| `verify_ttr_pairing.py` | independent adversarial verification of the TTR pairing (written by a separate verifier; structural parse, no shared code) |
| `launch_run28.sh` / `run28.log` | exact launcher CLI + run log of the authoritative run |
| `launch_run28b.sh` / `run28b.log` | the failed thread+numba-nogil speed experiment (GIL-bound at 1.2 cores; killed; do not retry) |
| `bootstrap.sh` / `bootstrap.log` | box provisioning script + log (toolchain, venv, data ship) |
| `box_cache.sha` | sha256 of the shipped `wallet_cache.db` slice |
| `build-pe-bootstrap.log` | box-side release build log |

Reproduce the analysis: `.venv-analysis/bin/python analysis_capacity.py` from inside
this directory (needs pandas/numpy; NW t-stats independently match statsmodels OLS-HAC
`maxlags=2, use_correction=False` to 1e-15).
