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
                | "activate-next"
                | "purge-infra"
                | "clear-infra-exclusion"
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
        let mut reset_clob_cursor = false;
        let mut defer_activation = false;
        let mut confirm = false;
        let mut batch_id: Option<&str> = None;
        let mut audit_csv: Option<std::path::PathBuf> = None;
        let mut wallet_arg: Option<&str> = None;
        let mut flag_values: std::collections::HashSet<&str> = std::collections::HashSet::new();

        let mut i = 0;
        while i < rest.len() {
            let a = rest[i];
            if a == "--strict" {
                strict = true;
            } else if a == "--reset-clob-cursor" {
                reset_clob_cursor = true;
            } else if a == "--dry-run" {
                dry_run = true;
            } else if a == "--defer-activation" {
                defer_activation = true;
            } else if a == "--confirm" {
                confirm = true;
            } else if a == "--batch-id" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                batch_id = Some(rest[i]);
            } else if let Some(v) = a.strip_prefix("--batch-id=") {
                batch_id = Some(v);
            } else if a == "--audit-csv" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                audit_csv = Some(std::path::PathBuf::from(rest[i]));
            } else if let Some(v) = a.strip_prefix("--audit-csv=") {
                audit_csv = Some(std::path::PathBuf::from(v));
            } else if a == "--wallet" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                wallet_arg = Some(rest[i]);
            } else if let Some(v) = a.strip_prefix("--wallet=") {
                wallet_arg = Some(v);
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

        // Serialize standalone mutators before opening the shared cache RW.
        // Discovery owns its narrower per-source lock internally; these commands
        // have no nested acquisition and hold this guard for their full mutation.
        let _cache_mutation_lock = if matches!(
            sub,
            "activate-next" | "backfill" | "purge" | "purge-infra" | "clear-infra-exclusion"
        ) {
            match pe_bootstrap::lock::CacheMutationLock::acquire(&bootstrap_config.cache_path) {
                Ok(lock) => Some(lock),
                Err(e) => {
                    tracing::error!(error = %e, subcommand = sub, "bootstrap: cache mutation lock failed");
                    std::process::exit(1);
                }
            }
        } else {
            None
        };

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
                // `--reset-clob-cursor` is an emergency-only lever for discarding
                // an interrupted opaque cursor. Stop competing writers first.
                if reset_clob_cursor {
                    if let Err(e) =
                        cache.delete_source_cursor(pe_bootstrap::clob::CLOB_CLOSED_CURSOR_KEY)
                    {
                        tracing::error!(error = %e, "resolutions: failed to reset CLOB cursor");
                        std::process::exit(1);
                    }
                    tracing::info!(
                        "resolutions: --reset-clob-cursor → deleted source_cursor.clob_closed; \
                         CLOB will start from page 1"
                    );
                }
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
                let result =
                    fetch_resolutions_and_schedules(&bootstrap_config, &mut cache, &ids).await;
                // Coverage of the CLOB token→condition map (issue #429) — a DB-state
                // metric, logged after the run regardless of the stage outcome.
                log_token_coverage(&cache, bootstrap_config.clob_token_coverage_warn_pct);
                match result {
                    Ok(report) if report.has_failures() => {
                        // Issue #201: optional stages soft-failed; or #429: CLOB
                        // token-order divergences quarantined markets (both folded
                        // into has_failures). Surface as partial (exit 2) so the
                        // anomaly is visible to operators.
                        tracing::warn!(
                            stages_failed = ?report.stages_failed,
                            clob_order_mismatches = report.clob_order_mismatches,
                            "resolutions: partial — soft-failed stages and/or CLOB token-order divergences; re-run to retry"
                        );
                        2
                    }
                    Ok(_) => {
                        tracing::info!("resolutions: complete");
                        0
                    }
                    Err(e) => {
                        let exit_code = resolutions_error_exit_code(&e);
                        if exit_code == BootstrapError::TEMPFAIL_EXIT_CODE {
                            tracing::warn!(
                                error = %e,
                                exit_code,
                                "resolutions: audit incomplete"
                            );
                        } else {
                            tracing::error!(error = %e, exit_code, "resolutions: fatal");
                        }
                        exit_code
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
                    let exit_code = e.exit_code();
                    if exit_code == BootstrapError::TEMPFAIL_EXIT_CODE {
                        tracing::warn!(error = %e, exit_code, "events: temporary failure");
                    } else {
                        tracing::error!(error = %e, exit_code, "events: fatal");
                    }
                    exit_code
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

            "backfill" => match backfill::run_backfill_with_policy(
                &bootstrap_config,
                &mut cache,
                if defer_activation {
                    pile::ActivationPolicy::Deferred
                } else {
                    pile::ActivationPolicy::Immediate
                },
            )
            .await
            {
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
                match winner_discovery::run_winner_discovery_with_policy(
                    &bootstrap_config,
                    &mut cache,
                    if defer_activation {
                        pile::ActivationPolicy::Deferred
                    } else {
                        pile::ActivationPolicy::Immediate
                    },
                )
                .await
                {
                    Ok(r) => {
                        tracing::info!(
                            leaderboard_unique = r.leaderboard_unique,
                            leaderboard_activated = r.leaderboard_activated,
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
                        // Price-series coverage of the resolved-with-winner universe (issue #429
                        // PR3) — a DB-state metric, logged after the backfill regardless of soft-fails.
                        log_price_series_coverage(
                            &cache,
                            bootstrap_config.prices_history_coverage_warn_pct,
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

            "activate-next" => {
                let Some(batch_id) = batch_id else {
                    tracing::error!("activate-next: --batch-id is required");
                    std::process::exit(1);
                };
                match pile::activate_next(&mut cache, batch_id) {
                    Ok(batch) => {
                        if let Some(path) = audit_csv.as_deref()
                            && let Err(e) = pile::write_activation_audit_csv(&batch, path)
                        {
                            tracing::error!(
                                error = %e,
                                batch_id = batch.batch_id,
                                activated = batch.wallet_hexes.len(),
                                audit = %path.display(),
                                "activate-next: activation committed but CSV materialization failed; rerun the same batch id to regenerate without activating another cohort"
                            );
                            std::process::exit(1);
                        }
                        let activated = batch.wallet_hexes.len();
                        if activated == 0 {
                            tracing::warn!(
                                batch_id = batch.batch_id,
                                "activate-next: no inactive non-infrastructure wallets remain; skipping"
                            );
                        } else if activated < batch.requested_count {
                            tracing::warn!(
                                batch_id = batch.batch_id,
                                activated,
                                requested = batch.requested_count,
                                reused = batch.reused,
                                "activate-next: candidate pile depleted; activated remaining wallets"
                            );
                        } else {
                            tracing::info!(
                                batch_id = batch.batch_id,
                                activated,
                                reused = batch.reused,
                                "activate-next: controlled batch complete"
                            );
                        }
                        0
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "activate-next: fatal");
                        1
                    }
                }
            }

            "purge-infra" => match purge::run_infra_purge(&bootstrap_config, &mut cache, dry_run) {
                Ok(r) => {
                    tracing::info!(
                        infrastructure = r.infrastructure_deleted,
                        trades = r.trades_deleted,
                        snapshots = r.snapshots_deleted,
                        wallet_features = r.wallet_features_deleted,
                        tombstones = r.tombstones_written,
                        dry_run = r.dry_run,
                        "purge-infra: complete"
                    );
                    0
                }
                Err(e) => {
                    tracing::error!(error = %e, "purge-infra: fatal");
                    1
                }
            },

            "clear-infra-exclusion" => {
                if !confirm {
                    tracing::error!("clear-infra-exclusion: --confirm is required");
                    std::process::exit(1);
                }
                let Some(wallet) = wallet_arg else {
                    tracing::error!("clear-infra-exclusion: --wallet <hex> is required");
                    std::process::exit(1);
                };
                let normalized = match pe_core_types::WalletAddress::from_hex(wallet) {
                    Ok(address) => address.to_string(),
                    Err(e) => {
                        tracing::error!(error = %e, "clear-infra-exclusion: invalid wallet");
                        std::process::exit(1);
                    }
                };
                match cache.clear_infra_exclusion(&normalized) {
                    Ok(true) => {
                        tracing::warn!(
                            wallet = normalized,
                            "clear-infra-exclusion: exclusion cleared"
                        );
                        0
                    }
                    Ok(false) => {
                        tracing::error!(
                            wallet = normalized,
                            "clear-infra-exclusion: matching infra exclusion not found"
                        );
                        1
                    }
                    Err(e) => {
                        tracing::error!(error = %e, wallet = normalized, "clear-infra-exclusion: fatal");
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
/// Exit codes: 0 = clean, 1 = fatal, 2 = soft-fail (partial fetch; cache
/// durable; re-run to retry). The `strict` flag promotes soft-fail to fatal.
async fn handle_all(config: &BootstrapConfig, cache: &mut WalletCache, strict: bool) -> i32 {
    // Step 0: one-shot migration (synchronous).
    if let Err(e) = migrate::auto_migrate_legacy(config, cache) {
        tracing::error!(error = %e, "all: migrate fatal");
        return 1;
    }

    // Step 1: discover wallets via the Polymarket leaderboard (all categories)
    // + datadash. Replaces the retired Dune `enumerate` (#335).
    match winner_discovery::run_winner_discovery(config, cache).await {
        Ok(r) => {
            tracing::info!(
                leaderboard_unique = r.leaderboard_unique,
                leaderboard_activated = r.leaderboard_activated,
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
        let result = fetch_resolutions_and_schedules(config, cache, &ids).await;
        log_token_coverage(cache, config.clob_token_coverage_warn_pct);
        match result {
            Ok(report) if report.has_failures() => {
                // Issue #201: optional stages soft-failed; or #429: CLOB token-order
                // divergences quarantined markets (both folded into has_failures) →
                // partial (exit 2), not fatal. The primary CLOB fetch still
                // propagates failures as Err.
                tracing::warn!(
                    stages_failed = ?report.stages_failed,
                    clob_order_mismatches = report.clob_order_mismatches,
                    "all: resolutions partial — soft-failed stages and/or CLOB token-order divergences"
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

/// Log the CLOB token→condition coverage of resolved-with-winner markets and
/// warn when it falls below `warn_pct` (issue #429). Coverage is a DB-state
/// metric, so it is queried after the resolutions run. Integer percentage —
/// the workspace lints `float_arithmetic`, so no `f64` is used.
fn log_token_coverage(cache: &WalletCache, warn_pct: u8) {
    let (total, mapped) = cache.token_coverage_report();
    if total == 0 {
        return;
    }
    let pct = mapped.saturating_mul(100) / total;
    if warn_pct > 0 && pct < i64::from(warn_pct) {
        tracing::warn!(
            mapped,
            total,
            coverage_pct = pct,
            warn_pct,
            "resolutions: CLOB token→condition coverage below threshold — coverage \
             self-heals on the next cycle's full walk (issue #519)"
        );
    } else {
        tracing::info!(
            mapped,
            total,
            coverage_pct = pct,
            "resolutions: CLOB token→condition coverage"
        );
    }
}

/// Log the CLOB price-series coverage (issue #429 PR3) after a `prices-history` run: warn when the
/// usable-series share of resolved-with-winner markets falls below `warn_pct`, else info. Integer
/// math only (the workspace lints `float_arithmetic`). A partial backfill or a starved token map
/// trips the warn; a complete CLOB-only backfill clears it near the ~63.6% usable-series ceiling PR2
/// measured.
fn log_price_series_coverage(cache: &WalletCache, warn_pct: u8) {
    let cov = cache.price_series_coverage_report();
    if cov.total == 0 {
        return;
    }
    let usable_pct = cov.usable.saturating_mul(100) / cov.total;
    let with_series_pct = cov.with_series.saturating_mul(100) / cov.total;
    if warn_pct > 0 && usable_pct < i64::from(warn_pct) {
        tracing::warn!(
            usable = cov.usable,
            with_series = cov.with_series,
            total = cov.total,
            usable_pct,
            with_series_pct,
            warn_pct,
            "prices-history: usable CLOB price-series coverage below threshold — re-run \
             `pe-bootstrap prices-history` to backfill missing series (issue #429 PR3)"
        );
    } else {
        tracing::info!(
            usable = cov.usable,
            with_series = cov.with_series,
            total = cov.total,
            usable_pct,
            with_series_pct,
            "prices-history: CLOB price-series coverage"
        );
    }
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

fn resolutions_error_exit_code(error: &BootstrapError) -> i32 {
    match error {
        BootstrapError::ResolutionAuditIncomplete { .. } => BootstrapError::TEMPFAIL_EXIT_CODE,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::resolutions_error_exit_code;
    use pe_bootstrap::error::BootstrapError;

    #[test]
    fn resolution_audit_incomplete_is_the_only_resolutions_tempfail() {
        for error in [
            BootstrapError::ResolutionAuditIncomplete {
                blocked: 1,
                clipped: 0,
            },
            BootstrapError::ResolutionAuditIncomplete {
                blocked: 0,
                clipped: 1,
            },
        ] {
            assert_eq!(
                resolutions_error_exit_code(&error),
                BootstrapError::TEMPFAIL_EXIT_CODE
            );
        }
        assert_eq!(
            resolutions_error_exit_code(&BootstrapError::Clob {
                message: "fatal".to_owned(),
            }),
            1
        );
    }
}
