//! `pe-backtest` binary entry point.

use pe_backtest::config::BacktestConfig;
use pe_backtest::error::BacktestError;
use pe_backtest::{funder_graph, simulation};
use pe_bootstrap::cache::WalletCache;
use pe_strategy_winner_follow::WinnerFollowConfig;
use pe_trader_index::{LedgerConfig, RankerConfig};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), BacktestError> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let config = BacktestConfig::from_env()?;

    // Load wallet trade cache.
    let cache = WalletCache::open(&config.cache_path)?;
    let all_wallet_addresses = cache.all_wallet_addresses();
    let all_trades = cache.all_trades();

    info!(
        wallets = all_wallet_addresses.len(),
        trades = all_trades.len(),
        "cache loaded"
    );

    if all_trades.is_empty() {
        tracing::warn!("no trades in cache — populate with pe-bootstrap first");
        return Ok(());
    }

    // Phase 0: build funder graph.
    let operator_identities = funder_graph::build_funder_graph(
        config.etherscan_api_key.as_deref(),
        &all_wallet_addresses,
        &all_trades,
    )
    .await?;

    // Phase 1: walk-forward simulation.
    std::fs::create_dir_all(&config.output_dir)?;

    let report = simulation::run_simulation(
        &config,
        all_trades,
        operator_identities,
        &RankerConfig::default(),
        &LedgerConfig::default(),
        &WinnerFollowConfig::default(),
    )?;

    info!(
        total_pnl_usd = %report.total_pnl_usd,
        sharpe_ratio = %report.sharpe_ratio,
        max_drawdown_pct = %report.max_drawdown_pct,
        total_copies = report.total_copies,
        win_rate_pct = %report.win_rate_pct,
        "backtest complete — results in {:?}",
        config.output_dir,
    );

    Ok(())
}
