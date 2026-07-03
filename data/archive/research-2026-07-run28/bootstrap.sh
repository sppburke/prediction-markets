#!/bin/bash
set -eo pipefail
cd ~/prediction-markets
echo "STEP uv-install $(date +%T)"
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
echo "STEP python-3.11.15 $(date +%T)"
uv python install 3.11.15
echo "STEP venv $(date +%T)"
uv venv --python 3.11.15 .venv-analysis
echo "STEP pip-deps $(date +%T)"
uv pip install --python .venv-analysis/bin/python -r scripts/requirements.txt
echo "STEP verify-python-imports $(date +%T)"
.venv-analysis/bin/python -c "import numpy,pandas,duckdb,scipy,arch,statsmodels,numba; print('py-imports OK', numpy.__version__, pandas.__version__, duckdb.__version__, numba.__version__)"
echo "STEP rustup $(date +%T)"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none
source "$HOME/.cargo/env"
echo "STEP rust-toolchain-pin $(date +%T)"
rustup show   # installs the rust-toolchain.toml pin (1.95.0)
echo "STEP cargo-build-pe-backtest $(date +%T)"
cargo build --release -p pe-backtest 2>&1 | tail -3
echo "STEP verify-binary $(date +%T)"
ls -la target/release/pe-backtest && target/release/pe-backtest --help 2>&1 | head -3 || echo "(pe-backtest --help nonzero, checking binary exists)"
echo "BOOTSTRAP_DONE $(date +%T)"
