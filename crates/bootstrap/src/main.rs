use pe_bootstrap::{
    BootstrapConfig, backfill, cache::WalletCache, config, discovery, migrate,
    parse_seed_as_of_env, run, seed_historical_snapshots, weekly,
};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let first_arg = std::env::args().nth(1);

    // Subcommand dispatch (issue #166). Checked BEFORE `--print-config` /
    // positional TOML path so a subcommand always wins. Subcommands accept an
    // optional `[toml-path]` second positional arg.
    let subcommand = first_arg.as_deref().and_then(|s| match s {
        "migrate" | "discovery" | "backfill" | "weekly" => Some(s),
        _ => None,
    });
    if let Some(sub) = subcommand {
        let toml_arg = std::env::args().nth(2).map(std::path::PathBuf::from);
        let bootstrap_config = match config::load(toml_arg.as_deref()) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("bootstrap: config error: {e}");
                std::process::exit(1);
            }
        };
        let mut cache = match WalletCache::open(&bootstrap_config.cache_path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("bootstrap: cache open failed: {e}");
                std::process::exit(1);
            }
        };
        let exit = match sub {
            "migrate" => match migrate::run_migrate_from_config(&bootstrap_config, &mut cache) {
                Ok(r) => {
                    tracing::info!(
                        wallet_set = r.wallet_set_rows,
                        trades = r.trades_rows,
                        dune_csv = r.dune_csv_rows,
                        infra = r.infra_rows,
                        seeded_polymarket = r.last_polymarket_fetch_seeded,
                        seeded_funder = r.last_funder_fetch_seeded,
                        activated = r.activated,
                        "migrate: complete"
                    );
                    0
                }
                Err(e) => {
                    eprintln!("migrate: fatal: {e}");
                    1
                }
            },
            "discovery" => match discovery::run_discovery(&bootstrap_config, &mut cache).await {
                Ok(r) => {
                    tracing::info!(
                        uploaded = r.uploaded,
                        new_wallets = r.new_wallets,
                        activated = r.activated,
                        "discovery: complete"
                    );
                    0
                }
                Err(e) => {
                    eprintln!("discovery: fatal: {e}");
                    1
                }
            },
            "backfill" => match backfill::run_backfill(&bootstrap_config, &mut cache).await {
                Ok(r) => {
                    tracing::info!(
                        due = r.due,
                        fetched = r.fetched,
                        failed = r.failed,
                        activated = r.activated,
                        "backfill: complete"
                    );
                    0
                }
                Err(e @ pe_bootstrap::error::BootstrapError::PartialFetch { .. }) => {
                    // Soft-fail: pipeline ran, successful wallets stamped, post-fetch
                    // steps (resolutions, refresh_trade_counts, apply_activation_rules)
                    // completed. Failed wallets stayed NULL and will be retried by the
                    // next backfill run. Non-zero exit so systemd / operators notice.
                    eprintln!("backfill: partial: {e} — failed wallets will retry on next run");
                    2
                }
                Err(e) => {
                    eprintln!("backfill: fatal: {e}");
                    1
                }
            },
            "weekly" => match weekly::run_weekly(&bootstrap_config, &mut cache).await {
                Ok(r) => {
                    tracing::info!(
                        due = r.due,
                        processed = r.processed,
                        failed = r.failed,
                        to_block = r.to_block,
                        "weekly: complete"
                    );
                    0
                }
                Err(e) => {
                    eprintln!("weekly: fatal: {e}");
                    1
                }
            },
            _ => unreachable!(
                "subcommand filter restricts to migrate / discovery / backfill / weekly"
            ),
        };
        std::process::exit(exit);
    }

    if first_arg.as_deref() == Some("--print-config") {
        match toml::to_string_pretty(&BootstrapConfig::default()) {
            Ok(s) => {
                print!("{s}");
                return;
            }
            Err(e) => {
                eprintln!("bootstrap: --print-config failed: {e}");
                std::process::exit(1);
            }
        }
    }

    let config_path = first_arg.map(std::path::PathBuf::from);
    let bootstrap_config = match config::load(config_path.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("bootstrap: config error: {e}");
            std::process::exit(1);
        }
    };

    // Historical-seed mode: when PE_SEED_AS_OF_DATES is set, run the parameterized
    // Dune query at each as-of date and write a snapshot row-set into the cache.
    // This path is exclusive of the regular discover/fetch/build pipeline.
    let seed_env = std::env::var("PE_SEED_AS_OF_DATES").unwrap_or_default();
    if !seed_env.trim().is_empty() {
        let dates = match parse_seed_as_of_env(&seed_env) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("bootstrap: PE_SEED_AS_OF_DATES parse error: {e}");
                std::process::exit(1);
            }
        };
        match seed_historical_snapshots(&bootstrap_config, &dates).await {
            Ok(rows) => {
                tracing::info!(
                    snapshots = dates.len(),
                    rows,
                    "bootstrap: historical seed complete"
                );
                return;
            }
            Err(e) => {
                eprintln!("bootstrap: historical seed fatal: {e}");
                std::process::exit(1);
            }
        }
    }

    match run(&bootstrap_config).await {
        Ok(watchlist) => {
            tracing::info!(
                active = watchlist.active_count,
                output = %bootstrap_config.output_path.display(),
                "bootstrap: complete"
            );
        }
        Err(e) => {
            eprintln!("bootstrap: fatal: {e}");
            std::process::exit(1);
        }
    }
}
