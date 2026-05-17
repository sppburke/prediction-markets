use pe_bootstrap::{
    BootstrapConfig, backfill, cache::WalletCache, config, discovery, lock, migrate,
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
        "discovery" | "backfill" | "weekly" => Some(s),
        _ => None,
    });
    if let Some(sub) = subcommand {
        let toml_arg = std::env::args().nth(2).map(std::path::PathBuf::from);
        let bootstrap_config = match config::load(toml_arg.as_deref()) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "bootstrap: config error");
                std::process::exit(1);
            }
        };
        let mut cache = match WalletCache::open(&bootstrap_config.cache_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "bootstrap: cache open failed");
                std::process::exit(1);
            }
        };
        let exit = match sub {
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
                    tracing::error!(error = %e, "discovery: fatal");
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
                    tracing::warn!(
                        error = %e,
                        "backfill: partial — failed wallets will retry on next run"
                    );
                    2
                }
                Err(e) => {
                    tracing::error!(error = %e, "backfill: fatal");
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
                    tracing::error!(error = %e, "weekly: fatal");
                    1
                }
            },
            _ => unreachable!("subcommand filter restricts to discovery / backfill / weekly"),
        };
        std::process::exit(exit);
    }

    if first_arg.as_deref() == Some("--print-config") {
        match toml::to_string_pretty(&BootstrapConfig::default()) {
            Ok(s) => {
                // --print-config is intentional non-log stdout: dumps a TOML config
                // template for the operator to redirect/edit. Keep as `print!`.
                print!("{s}");
                return;
            }
            Err(e) => {
                tracing::error!(error = %e, "bootstrap: --print-config failed");
                std::process::exit(1);
            }
        }
    }

    // Issue #191 Item 2 — one-shot V1-attribution backfill subcommand.
    // Clears the V1 topic from `enumerated_topic_hashes` + V1-keyed entries
    // from `chunk_progress` so the next normal `pe-bootstrap` run re-does V1
    // enumeration via the standard chunked path, populating
    // `polymarket_contracts_seen` bit 0 for every wallet with V1 activity.
    //
    // Optional `[toml-path]` second positional arg matches the subcommand
    // dispatch above. `--dry-run` flag (any third positional arg literally
    // matching) prints what WOULD be cleared without modifying.
    if first_arg.as_deref() == Some("--backfill-v1-attribution") {
        let toml_arg = std::env::args().nth(2).map(std::path::PathBuf::from);
        let dry_run = std::env::args().any(|a| a == "--dry-run");
        let cfg = match config::load(toml_arg.as_deref()) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "backfill-v1-attribution: config error");
                std::process::exit(1);
            }
        };
        let mut cache = match WalletCache::open(&cfg.cache_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "backfill-v1-attribution: cache open failed");
                std::process::exit(1);
            }
        };
        let _lock = match lock::CacheMutationLock::acquire(&cfg.cache_path) {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(error = %e, "backfill-v1-attribution: lock acquire failed");
                std::process::exit(1);
            }
        };
        if dry_run {
            // Preview path: load cursors, compute what WOULD be removed.
            let (_, topics) = match migrate::load_enum_state(&cache) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "backfill-v1-attribution: load_enum_state failed");
                    std::process::exit(1);
                }
            };
            let progress = match migrate::load_chunk_progress(&cache) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!(error = %e, "backfill-v1-attribution: load_chunk_progress failed");
                    std::process::exit(1);
                }
            };
            let v1_topic_hex = format!(
                "{}",
                pe_source_onchain_polygon::contracts::TOPIC_ORDER_FILLED_V1
            );
            let v1_prefix = format!("{v1_topic_hex}|");
            let topic_present = topics.contains(&v1_topic_hex);
            let chunks_to_remove = progress
                .keys()
                .filter(|k| k.starts_with(&v1_prefix))
                .count();
            print!(
                "DRY RUN: would clear V1 topic from enumerated_topic_hashes (present={topic_present}) \
                 and {chunks_to_remove} entries from chunk_progress.\n\
                 Re-run without --dry-run to apply.\n"
            );
            return;
        }
        match migrate::reset_v1_topic_cursors(&mut cache) {
            Ok((topic_removed, chunks_removed)) => {
                print!(
                    "Cleared V1 topic from enumerated_topic_hashes (was_present={topic_removed}) \
                     and {chunks_removed} entries from chunk_progress.\n\
                     Run pe-bootstrap normally to re-enumerate V1; the per-chunk UPSERT will \
                     populate polymarket_contracts_seen bit 1 for every wallet that has V1 \
                     OrderFilled activity.\n"
                );
                return;
            }
            Err(e) => {
                tracing::error!(error = %e, "backfill-v1-attribution: reset failed");
                std::process::exit(1);
            }
        }
    }

    let config_path = first_arg.map(std::path::PathBuf::from);
    let bootstrap_config = match config::load(config_path.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "bootstrap: config error");
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
                tracing::error!(
                    error = %e,
                    env_var = "PE_SEED_AS_OF_DATES",
                    "bootstrap: env-var parse error"
                );
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
                tracing::error!(error = %e, "bootstrap: historical seed fatal");
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
            tracing::error!(error = %e, "bootstrap: fatal");
            std::process::exit(1);
        }
    }
}
