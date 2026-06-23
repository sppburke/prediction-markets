use pe_bootstrap::{
    BootstrapConfig, backfill, cache::WalletCache, config, coverage, error::BootstrapError, fetch,
    fetch_resolutions_and_schedules, infra_probe, migrate, pile, purge, run_schedule_backfill,
    watchlist_phase, winner_discovery,
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
                | "fetch"
                | "watchlist"
                | "resolutions"
                | "schedules"
                | "events"
                | "backfill"
                | "classify-infra"
                | "coverage"
                | "winner-discovery"
                | "prices-history"
                | "purge"
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

            "schedules" => {
                let all_ids = cache.all_market_ids();
                match run_schedule_backfill(&bootstrap_config, &mut cache, &all_ids).await {
                    Ok(inserted) => {
                        tracing::info!(inserted, "schedules: complete");
                        0
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "schedules: fatal");
                        1
                    }
                }
            }

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

            "winner-discovery" => {
                match winner_discovery::run_winner_discovery(&bootstrap_config, &mut cache).await {
                    Ok(r) => {
                        tracing::info!(
                            leaderboard_unique = r.leaderboard_unique,
                            leaderboard_activated = r.leaderboard_activated,
                            radion_unique = r.radion_unique,
                            radion_activated = r.radion_activated,
                            datadash_unique = r.datadash_unique,
                            datadash_activated = r.datadash_activated,
                            "winner-discovery: complete"
                        );
                        0
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "winner-discovery: fatal");
                        1
                    }
                }
            }

            "prices-history" => {
                match pe_bootstrap::prices_history::run_prices_history(
                    &bootstrap_config,
                    &mut cache,
                )
                .await
                {
                    Ok(r) => {
                        tracing::info!(
                            start_dates_updated = r.start_dates_updated,
                            tokens_fetched = r.tokens_fetched,
                            tokens_failed = r.tokens_failed,
                            points_written = r.points_written,
                            "prices-history: complete"
                        );
                        // Per-token soft-fails (non-fatal fetch errors) → partial (exit 2): the run
                        // is durable + resumable, re-run to retry the skipped tokens.
                        if r.tokens_failed > 0 { 2 } else { 0 }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "prices-history: fatal");
                        1
                    }
                }
            }

            "purge" => match purge::run_purge(&bootstrap_config, &mut cache, dry_run) {
                Ok(r) => {
                    tracing::info!(
                        proven_losers = r.proven_losers_deleted,
                        dead_weight = r.dead_weight_deleted,
                        trades = r.trades_deleted,
                        snapshots = r.snapshots_deleted,
                        tombstones = r.tombstones_written,
                        dry_run = r.dry_run,
                        "purge: complete"
                    );
                    0
                }
                Err(e) => {
                    tracing::error!(error = %e, "purge: fatal");
                    1
                }
            },

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

    // ── No-arg / positional TOML path → default "all" ───────────────────────
    let config_path = first_arg.map(std::path::PathBuf::from);
    let bootstrap_config = match config::load(config_path.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "bootstrap: config error");
            std::process::exit(1);
        }
    };

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

    // Step 1: discover wallets via the Polymarket leaderboard (all categories)
    // + Radion (when activated). Replaces the retired Dune `enumerate` (#335).
    match winner_discovery::run_winner_discovery(config, cache).await {
        Ok(r) => {
            tracing::info!(
                leaderboard_unique = r.leaderboard_unique,
                leaderboard_activated = r.leaderboard_activated,
                radion_unique = r.radion_unique,
                radion_activated = r.radion_activated,
                datadash_unique = r.datadash_unique,
                datadash_activated = r.datadash_activated,
                "all: winner-discovery complete"
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "all: winner-discovery fatal");
            return 1;
        }
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

    // Step 3: watchlist build.
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

    // Step 4: resolutions (optional).
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
