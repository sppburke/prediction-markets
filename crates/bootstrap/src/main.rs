use alloy::providers::ProviderBuilder;
use pe_bootstrap::{
    BootstrapConfig, FUNDER_DISCOVERY_TO_BLOCK, backfill,
    cache::WalletCache,
    config, coverage, discovery, enumerate,
    error::BootstrapError,
    fetch, fetch_resolutions_and_schedules, funder, infra_probe, lock, migrate, pile,
    seed_historical::{self, parse_seed_as_of_env},
    watchlist_phase, weekly,
};
use pe_source_onchain_polygon::{
    AlloyChainLogFetcher, BlockRange, contracts::CTF_EXCHANGE_V1_DEPLOY_BLOCK,
};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args: Vec<String> = std::env::args().collect();
    let first_arg = args.get(1).map(|s| s.as_str());

    // ── Subcommand dispatch ──────────────────────────────────────────────────
    // Precedence: named subcommands > flag-style args > positional TOML path.
    //
    // Exit-code convention (uniform across all subcommands):
    //   0 = success (clean)
    //   1 = fatal error
    //   2 = partial (soft-fail): pipeline ran, some wallets/fetches failed;
    //       cache is durable, re-run to retry failed items.

    // ── Subcommands that take [--strict] [<toml-path>] ──────────────────────
    let known_sub = matches!(
        first_arg,
        Some(
            "all"
                | "enumerate"
                | "fetch"
                | "funder"
                | "watchlist"
                | "resolutions"
                | "events"
                | "seed-historical"
                | "discovery"
                | "backfill"
                | "weekly"
                | "classify-infra"
                | "coverage"
        )
    );

    if let Some(sub) = first_arg.filter(|_| known_sub) {
        // Single-pass flag parser. Tracks the actual string slices consumed
        // as flag values so the TOML positional search doesn't mistake
        // `--dump-ledgers /path` for a config file path.
        let rest: Vec<&str> = args[2..].iter().map(|s| s.as_str()).collect();
        let mut strict = false;
        let mut dry_run = false;
        let mut dump_ledgers_path: Option<std::path::PathBuf> = None;
        let mut stage: Option<&str> = None;
        let mut as_of_arg: Option<&str> = None;
        let mut flag_values: std::collections::HashSet<&str> = std::collections::HashSet::new();

        let mut i = 0;
        while i < rest.len() {
            let a = rest[i];
            if a == "--strict" {
                strict = true;
            } else if a == "--dry-run" {
                dry_run = true;
            } else if a == "--dump-ledgers" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                dump_ledgers_path = Some(std::path::PathBuf::from(rest[i]));
            } else if let Some(v) = a.strip_prefix("--dump-ledgers=") {
                dump_ledgers_path = Some(std::path::PathBuf::from(v));
            } else if a == "--stage" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                stage = Some(rest[i]);
            } else if let Some(v) = a.strip_prefix("--stage=") {
                stage = Some(v);
            } else if a == "--as-of" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                as_of_arg = Some(rest[i]);
            } else if let Some(v) = a.strip_prefix("--as-of=") {
                as_of_arg = Some(v);
            }
            i += 1;
        }

        // TOML path: last non-flag positional not consumed as a flag value.
        let toml_arg: Option<std::path::PathBuf> = rest
            .iter()
            .rfind(|&&a| !a.starts_with("--") && !flag_values.contains(a))
            .map(|p| std::path::PathBuf::from(*p));

        let bootstrap_config = match config::load(toml_arg.as_deref()) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "bootstrap: config error");
                std::process::exit(1);
            }
        };

        // `coverage` (issue #208) is a read-only probe: open the cache
        // READ_ONLY, never CREATE/migrate it, and never take the
        // CacheMutationLock. Handle it before the shared read-write open below
        // so it stays off the mutating path entirely.
        //   exit 0 = clean (no gaps), 2 = partial (a gap was detected),
        //   1 = fatal (cache/IO error) — per the convention above.
        if sub == "coverage" {
            let exit = match coverage::run_coverage(&bootstrap_config.cache_path) {
                Ok(report) => {
                    if report.is_clean() {
                        0
                    } else {
                        2
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "coverage: fatal");
                    1
                }
            };
            std::process::exit(exit);
        }

        let mut cache = match WalletCache::open(&bootstrap_config.cache_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "bootstrap: cache open failed");
                std::process::exit(1);
            }
        };

        let exit = match sub {
            // ── New decomposed subcommands ───────────────────────────────────
            "all" => handle_all(&bootstrap_config, &mut cache, strict).await,

            "enumerate" => match enumerate::run_enumerate(&bootstrap_config, &mut cache).await {
                Ok(r) => {
                    tracing::info!(
                        wallets_discovered = r.wallets_discovered,
                        chunks_scanned = r.chunks_scanned,
                        topics_completed = r.topics_completed,
                        skipped = r.skipped,
                        "enumerate: complete"
                    );
                    0
                }
                Err(e) => {
                    tracing::error!(error = %e, "enumerate: fatal");
                    1
                }
            },

            "fetch" => {
                let wallets = match wallets_from_cache(&mut cache) {
                    Ok(w) => w,
                    Err(e) => {
                        tracing::error!(error = %e, "fetch: cache read failed");
                        std::process::exit(1);
                    }
                };
                match fetch::run_fetch(&bootstrap_config, &mut cache, &wallets).await {
                    Ok(r) => {
                        tracing::info!(
                            attempted = r.attempted,
                            failed = r.failed,
                            "fetch: complete"
                        );
                        0
                    }
                    Err(e @ BootstrapError::PartialFetch { .. }) if !strict => {
                        tracing::warn!(error = %e, "fetch: partial — failed wallets will retry");
                        2
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "fetch: fatal");
                        1
                    }
                }
            }

            "funder" => {
                // Issue #203: the bulk funder lookup runs against the Polygon RPC
                // via batched `eth_getLogs` (the incremental `weekly` path keeps
                // Etherscan). Requires `polygon_rpc_url`.
                let rpc_url = match &bootstrap_config.polygon_rpc_url {
                    Some(u) => u.clone(),
                    None => {
                        tracing::error!("funder: PE_BOOTSTRAP_POLYGON_RPC_URL not set");
                        std::process::exit(1);
                    }
                };
                let pending =
                    match cache.wallets_needing_funder_lookup(bootstrap_config.funder_limit) {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::error!(error = %e, "funder: cache read failed");
                            std::process::exit(1);
                        }
                    };
                if pending.is_empty() {
                    tracing::info!("funder: no wallets pending funder lookup");
                    std::process::exit(0);
                }
                let http_url = match rpc_url.parse::<reqwest::Url>() {
                    Ok(u) => u,
                    Err(e) => {
                        tracing::error!(error = %e, "funder: invalid polygon_rpc_url");
                        std::process::exit(1);
                    }
                };
                let fetcher = AlloyChainLogFetcher {
                    provider: ProviderBuilder::new().connect_http(http_url),
                    min_chunk: 1,
                };
                let block_range = BlockRange {
                    from: CTF_EXCHANGE_V1_DEPLOY_BLOCK,
                    to: FUNDER_DISCOVERY_TO_BLOCK,
                };
                match funder::run_funder_eth_logs(
                    &mut cache,
                    &pending,
                    block_range,
                    &fetcher,
                    bootstrap_config.funder_block_chunk,
                    bootstrap_config.funder_topic_batch_size,
                )
                .await
                {
                    Ok(r) if r.failed > 0 && !strict => {
                        tracing::warn!(
                            failed = r.failed,
                            "funder: partial — failed wallets will retry on next run"
                        );
                        2
                    }
                    Ok(r) if r.failed > 0 => {
                        tracing::error!(failed = r.failed, "funder: partial (--strict)");
                        1
                    }
                    Ok(r) => {
                        tracing::info!(
                            pending = r.pending,
                            processed = r.processed,
                            edges_total = r.edges_total,
                            "funder: complete"
                        );
                        0
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "funder: fatal");
                        1
                    }
                }
            }

            "watchlist" => {
                let wallets = match wallets_from_cache(&mut cache) {
                    Ok(w) => w,
                    Err(e) => {
                        tracing::error!(error = %e, "watchlist: cache read failed");
                        std::process::exit(1);
                    }
                };
                match watchlist_phase::run_watchlist(
                    &bootstrap_config,
                    &mut cache,
                    &wallets,
                    dump_ledgers_path.as_deref(),
                )
                .await
                {
                    Ok(r) => {
                        tracing::info!(
                            ledger_count = r.ledger_count,
                            active = r.active_count,
                            output = %r.output_path.display(),
                            "watchlist: complete"
                        );
                        0
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "watchlist: fatal");
                        1
                    }
                }
            }

            "resolutions" => {
                let all_ids = cache.all_market_ids();
                let ids: Vec<String> = match stage {
                    // When --stage is given, filter to a specific resolution source.
                    // For now all stages run through fetch_resolutions_and_schedules;
                    // per-stage filtering is a follow-up (see issue #195 follow-ups).
                    Some(s) => {
                        tracing::info!(stage = s, "resolutions: running stage (full pipeline)");
                        all_ids
                    }
                    None => all_ids,
                };
                match fetch_resolutions_and_schedules(&bootstrap_config, &mut cache, &ids).await {
                    Ok(report) if report.has_failures() => {
                        // Issue #201: optional stages soft-failed; surface as
                        // partial (exit 2) so the skip is visible to operators.
                        tracing::warn!(
                            stages_failed = ?report.stages_failed,
                            "resolutions: partial — some optional stages soft-failed; re-run to retry"
                        );
                        2
                    }
                    Ok(_) => {
                        tracing::info!("resolutions: complete");
                        0
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "resolutions: fatal");
                        1
                    }
                }
            }

            "events" => match pe_bootstrap::events::run_events(&bootstrap_config, &mut cache).await
            {
                Ok(report) => {
                    tracing::info!(
                        events_seen = report.events_seen,
                        conditions_mapped = report.conditions_mapped,
                        total_traded_markets = report.total_traded_markets,
                        orphan_self_mapped = report.orphan_self_mapped,
                        "events: complete"
                    );
                    0
                }
                Err(e) => {
                    tracing::error!(error = %e, "events: fatal");
                    1
                }
            },

            "seed-historical" => {
                let dates = if let Some(as_of) = as_of_arg {
                    match parse_seed_as_of_env(as_of) {
                        Ok(d) => d,
                        Err(e) => {
                            tracing::error!(error = %e, "--as-of parse error");
                            std::process::exit(1);
                        }
                    }
                } else {
                    // Fall back to PE_SEED_AS_OF_DATES env var (legacy path).
                    let env = std::env::var("PE_SEED_AS_OF_DATES").unwrap_or_default();
                    match parse_seed_as_of_env(&env) {
                        Ok(d) => d,
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                env_var = "PE_SEED_AS_OF_DATES",
                                "bootstrap: env-var parse error"
                            );
                            std::process::exit(1);
                        }
                    }
                };
                if dates.is_empty() {
                    tracing::warn!(
                        "seed-historical: no dates specified — pass --as-of YYYY-MM-DD,... \
                         or set PE_SEED_AS_OF_DATES"
                    );
                    std::process::exit(0);
                }
                match seed_historical::run_seed_historical(&bootstrap_config, &mut cache, &dates)
                    .await
                {
                    Ok(r) => {
                        tracing::info!(
                            dates_attempted = r.dates_attempted,
                            dates_skipped = r.dates_skipped,
                            rows_inserted = r.rows_inserted,
                            "seed-historical: complete"
                        );
                        0
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "seed-historical: fatal");
                        1
                    }
                }
            }

            // ── Existing subcommands (UNCHANGED names) ───────────────────────
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
                Err(e @ BootstrapError::PartialFetch { .. }) => {
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
                Err(e @ BootstrapError::PartialFetch { .. }) => {
                    // Issue #195: weekly now returns PartialFetch (exit 2) when any
                    // wallets fail, matching backfill's exit-code convention.
                    tracing::warn!(
                        error = %e,
                        "weekly: partial — failed wallets will retry on next run"
                    );
                    2
                }
                Err(e) => {
                    tracing::error!(error = %e, "weekly: fatal");
                    1
                }
            },

            "classify-infra" => {
                // Issue #197: retroactive sweep that mirrors the cold-start
                // probe semantics over cached trades. `--dry-run` previews
                // the would-flag set without writing.
                let threshold = std::env::var("PE_BOOTSTRAP_INFRA_SPAN_SECS")
                    .ok()
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(infra_probe::DEFAULT_INFRA_SPAN_SECS);
                match cache.classify_infra_retroactive(threshold, dry_run) {
                    Ok(r) => {
                        tracing::info!(
                            scanned = r.scanned,
                            flagged = r.flagged,
                            dry_run = r.dry_run,
                            threshold_secs = threshold,
                            "classify-infra: complete"
                        );
                        0
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "classify-infra: fatal");
                        1
                    }
                }
            }

            _ => unreachable!("known_sub filter restricts to known subcommand names"),
        };
        std::process::exit(exit);
    }

    // ── Flag-style args ──────────────────────────────────────────────────────
    if first_arg == Some("--print-config") {
        match toml::to_string_pretty(&BootstrapConfig::default()) {
            Ok(s) => {
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
    if first_arg == Some("--backfill-v1-attribution") {
        let toml_arg = args.get(2).map(std::path::PathBuf::from);
        let dry_run = args.iter().any(|a| a == "--dry-run");
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

    // ── No-arg / positional TOML path → default "all" ───────────────────────
    // When PE_SEED_AS_OF_DATES is set, treat as `seed-historical` (legacy path).
    let config_path = first_arg.map(std::path::PathBuf::from);
    let bootstrap_config = match config::load(config_path.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "bootstrap: config error");
            std::process::exit(1);
        }
    };

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
        let mut cache = match WalletCache::open(&bootstrap_config.cache_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "bootstrap: cache open failed");
                std::process::exit(1);
            }
        };
        match seed_historical::run_seed_historical(&bootstrap_config, &mut cache, &dates).await {
            Ok(r) => {
                tracing::info!(
                    snapshots = r.dates_attempted,
                    rows = r.rows_inserted,
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

    // No-arg → run "all" with strict=false (soft-fail default).
    let mut cache = match WalletCache::open(&bootstrap_config.cache_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "bootstrap: cache open failed");
            std::process::exit(1);
        }
    };
    let exit = handle_all(&bootstrap_config, &mut cache, false).await;
    std::process::exit(exit);
}

/// Orchestrate all bootstrap phases in sequence.
///
/// Exit codes: 0 = clean, 1 = fatal, 2 = soft-fail (partial fetch/funder; cache
/// durable; re-run to retry). The `strict` flag promotes soft-fail to fatal.
async fn handle_all(config: &BootstrapConfig, cache: &mut WalletCache, strict: bool) -> i32 {
    // Step 0: one-shot migration (synchronous).
    if let Err(e) = migrate::auto_migrate_legacy(config, cache) {
        tracing::error!(error = %e, "all: migrate fatal");
        return 1;
    }

    // Step 1: enumerate wallets.
    if let Err(e) = enumerate::run_enumerate(config, cache).await {
        tracing::error!(error = %e, "all: enumerate fatal");
        return 1;
    }

    let wallets = match wallets_from_cache(cache) {
        Ok(w) => w,
        Err(e) => {
            tracing::error!(error = %e, "all: wallets read failed");
            return 1;
        }
    };

    // Step 2: fetch Polymarket trades.
    let mut soft_fail = false;
    match fetch::run_fetch(config, cache, &wallets).await {
        Ok(_) => {}
        Err(BootstrapError::PartialFetch { .. }) if !strict => {
            soft_fail = true;
        }
        Err(e) => {
            tracing::error!(error = %e, "all: fetch fatal");
            return 1;
        }
    }

    // Step 3: funder-graph discovery (optional).
    // Issue #203: the bulk path uses batched `eth_getLogs` against the Polygon
    // RPC (the `weekly` path keeps Etherscan), so it gates on `polygon_rpc_url`.
    if config.fetch_funder_graph {
        if let Some(rpc_url) = &config.polygon_rpc_url {
            let pending = match cache.wallets_needing_funder_lookup(config.funder_limit) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!(error = %e, "all: funder lookup list failed");
                    return 1;
                }
            };
            if !pending.is_empty() {
                let http_url = match rpc_url.parse::<reqwest::Url>() {
                    Ok(u) => u,
                    Err(e) => {
                        tracing::error!(error = %e, "all: invalid polygon_rpc_url");
                        return 1;
                    }
                };
                let fetcher = AlloyChainLogFetcher {
                    provider: ProviderBuilder::new().connect_http(http_url),
                    min_chunk: 1,
                };
                let block_range = pe_source_onchain_polygon::BlockRange {
                    from: CTF_EXCHANGE_V1_DEPLOY_BLOCK,
                    to: FUNDER_DISCOVERY_TO_BLOCK,
                };
                match funder::run_funder_eth_logs(
                    cache,
                    &pending,
                    block_range,
                    &fetcher,
                    config.funder_block_chunk,
                    config.funder_topic_batch_size,
                )
                .await
                {
                    Ok(r) if r.failed > 0 && !strict => {
                        soft_fail = true;
                    }
                    Ok(r) if r.failed > 0 => {
                        tracing::error!(failed = r.failed, "all: funder partial (--strict)");
                        return 1;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!(error = %e, "all: funder fatal");
                        return 1;
                    }
                }
            }
        } else {
            tracing::warn!(
                "PE_BOOTSTRAP_FETCH_FUNDER_GRAPH=1 but PE_BOOTSTRAP_POLYGON_RPC_URL not set — skipping"
            );
        }
    }

    // Step 4: watchlist build.
    match watchlist_phase::run_watchlist(config, cache, &wallets, None).await {
        Ok(r) => {
            tracing::info!(
                active = r.active_count,
                output = %r.output_path.display(),
                "all: watchlist complete"
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "all: watchlist fatal");
            return 1;
        }
    }

    // Step 5: resolutions (optional).
    if config.fetch_resolutions {
        let ids = cache.all_market_ids();
        match fetch_resolutions_and_schedules(config, cache, &ids).await {
            Ok(report) if report.has_failures() => {
                // Issue #201: optional stages soft-failed → partial (exit 2),
                // not fatal. Polygon (primary) failures still propagate as Err.
                tracing::warn!(
                    stages_failed = ?report.stages_failed,
                    "all: resolutions partial — optional stages soft-failed"
                );
                soft_fail = true;
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(error = %e, "all: resolutions fatal");
                return 1;
            }
        }
    }

    if soft_fail { 2 } else { 0 }
}

/// Parse the `SRC_WALLET_SET_JSON`-bit wallet list from `cache` into `Vec<WalletAddress>`.
fn wallets_from_cache(
    cache: &mut WalletCache,
) -> Result<Vec<pe_core_types::WalletAddress>, BootstrapError> {
    let hexes = cache.wallets_with_source_bit(pile::SRC_WALLET_SET_JSON)?;
    Ok(hexes
        .iter()
        .filter_map(|h| {
            pe_core_types::WalletAddress::from_hex(h)
                .map_err(|e| {
                    tracing::warn!(address = %h, error = %e, "bootstrap: skipping unparseable wallet");
                })
                .ok()
        })
        .collect())
}
