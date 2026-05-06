//! `pe-backtest` binary entry point.

use std::collections::HashSet;

use pe_backtest::config::BacktestConfig;
use pe_backtest::error::BacktestError;
use pe_backtest::{funder_graph, simulation};
use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::dune::DuneClient;
use pe_strategy_winner_follow::{WinnerFollowConfig, WinnerFollowStrategy};
use pe_trader_index::{LedgerConfig, RankerConfig};
use time::OffsetDateTime;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), BacktestError> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let config = BacktestConfig::from_env()?;

    // Load wallet trade cache (mutable so Dune resolutions can be written).
    let mut cache = WalletCache::open(&config.cache_path)?;
    let all_wallet_addresses = cache.all_wallet_addresses();
    let all_trades = cache.all_trades();
    let snapshots = cache.load_all_snapshots()?;

    // Fetch on-chain resolutions via Dune before running the simulation so that
    // financial/quantitative markets (absent from the Gamma API) are resolved.
    if let Some(api_key) = &config.dune_api_key {
        let all_market_ids: HashSet<String> = cache.all_market_ids().into_iter().collect();
        let already_resolved = cache.resolved_market_ids();
        let unresolved: HashSet<String> = all_market_ids
            .difference(&already_resolved)
            .cloned()
            .collect();
        if unresolved.is_empty() {
            info!("backtest: all markets already resolved — skipping Dune fetch");
        } else {
            // Scope the Dune scan to our actual trade history window, not all of
            // history from epoch 0. A 30-day buffer before the earliest trade
            // captures resolutions for markets entered near our data horizon.
            const THIRTY_DAYS_SECS: i64 = 30 * 86_400;
            let min_trade_ts = cache.min_trade_unix()?.saturating_sub(THIRTY_DAYS_SECS);
            info!(
                unresolved = unresolved.len(),
                from_unix = min_trade_ts,
                "backtest: fetching Dune resolutions"
            );
            let dune = DuneClient::new(api_key.clone());
            match dune
                .fetch_resolutions(&unresolved, min_trade_ts, config.dune_namespace.as_deref())
                .await
            {
                Ok(rows) => {
                    let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
                    let mut inserted = 0usize;
                    for (market_id, winner, resolved_at_unix) in rows {
                        cache.insert_resolution(
                            &market_id,
                            winner,
                            resolved_at_unix,
                            fetched_at,
                        )?;
                        inserted += 1;
                    }
                    info!(
                        inserted,
                        unresolved = unresolved.len(),
                        "backtest: Dune resolutions fetched"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "backtest: Dune resolution fetch failed — continuing without on-chain resolutions"
                    );
                }
            }
        }
    }

    let resolutions = pe_bootstrap::gamma::load_resolutions(&cache)?;

    info!(
        wallets = all_wallet_addresses.len(),
        trades = all_trades.len(),
        snapshots = snapshots.len(),
        resolutions = resolutions.len(),
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

    let ranker_config = RankerConfig {
        min_reconstruction_quality: config.ranker_min_quality,
        active_min_closed_trades: config.ranker_active_min_closed,
        active_min_distinct_markets: config.ranker_active_min_markets,
        incubator_min_closed_trades: config.ranker_incubator_min_closed,
        incubator_min_distinct_markets: config.ranker_incubator_min_markets,
        ..RankerConfig::default()
    };
    info!(
        min_quality = ranker_config.min_reconstruction_quality,
        active_min_closed = ranker_config.active_min_closed_trades,
        active_min_markets = ranker_config.active_min_distinct_markets,
        incubator_min_closed = ranker_config.incubator_min_closed_trades,
        incubator_min_markets = ranker_config.incubator_min_distinct_markets,
        "ranker config (backtest-adjusted)"
    );

    let strategy = WinnerFollowStrategy::new(WinnerFollowConfig::default());
    let report = simulation::run_simulation(
        &config,
        all_trades,
        operator_identities,
        &snapshots,
        &resolutions,
        &ranker_config,
        &LedgerConfig::default(),
        &strategy,
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
