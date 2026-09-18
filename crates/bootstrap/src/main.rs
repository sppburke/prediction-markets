use pe_bootstrap::{
    BootstrapConfig, backfill,
    cache::WalletCache,
    cache_migration::{
        CacheActivationRequest, PriorCacheBinding, SupabasePublicationProbe,
        activate_cache_v2_with_handoff, finalize_cache_v2, migrate_cache_v2,
        populate_activity_bulk_root_v2_with_clock, populate_activity_fresh_v2_with_clock,
        populate_activity_v2, restore_prior_cache, stage_cache_cycle_v2, verify_frozen_payload_v1,
    },
    config, coverage,
    error::BootstrapError,
    fetch, fetch_resolutions_and_schedules, infra_probe, migrate, pile, purge,
    reclamation_evidence, run_schedule_backfill, watchlist_phase, winner_discovery,
};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args: Vec<String> = std::env::args().collect();
    let first_arg = args.get(1).map(|s| s.as_str());

    // ── Subcommand dispatch ──────────────────────────────────────────────────
    // Precedence: named subcommands > flag-style args > positional TOML path.
    //
    // Exit-code vocabulary (commands emit a subset, not uniform behavior):
    //   0  = success (clean)
    //   1  = permanent failure (fatal)
    //   2  = partial (soft-fail): pipeline ran, some wallets/fetches failed;
    //        cache is durable, re-run to retry failed items.
    //   75 = temporary failure (tempfail): a bounded retryable operation
    //        exhausted its in-process retries; the production loop supervisor
    //        retries the cycle. Emitted by `events`, `resolutions`, and
    //        `cache-populate-activity-v2` (transient or rate-limited reads).

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
                | "recover-reclamation"
                | "reclamation-evidence"
                | "cache-migrate-v2"
                | "cache-verify-frozen-v1"
                | "cache-populate-activity-v2"
                | "cache-populate-payout-v2"
                | "cache-finalize-v2"
                | "cache-activate"
                | "cache-restore-prior"
                | "cache-stage-v2"
                | "pipeline-versions"
        )
    );

    if let Some(sub) = first_arg.filter(|_| known_sub) {
        if sub == "pipeline-versions" {
            println!(
                "{}",
                serde_json::json!({
                    "source": "polymarket-public-activity",
                    "activity_schema": pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                    "activity_parser": pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
                    "clob_resolution_schema": pe_source_polymarket_public::CLOB_RESOLUTION_SCHEMA_VERSION,
                    "clob_resolution_parser": pe_source_polymarket_public::CLOB_RESOLUTION_PARSER_VERSION,
                    "cache_schema": pe_bootstrap::cache::CACHE_SCHEMA_VERSION_V2,
                    "configuration": 1,
                })
            );
            std::process::exit(0);
        }
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
        let mut targets_csv: Option<std::path::PathBuf> = None;
        let mut wallet_arg: Option<&str> = None;
        let mut db_arg: Option<std::path::PathBuf> = None;
        let mut manifest_arg: Option<std::path::PathBuf> = None;
        let mut frozen_payload_arg: Option<std::path::PathBuf> = None;
        let mut stage_record_arg: Option<std::path::PathBuf> = None;
        let mut fixed_db_arg: Option<std::path::PathBuf> = None;
        let mut backup_arg: Option<std::path::PathBuf> = None;
        let mut displaced_backup_arg: Option<std::path::PathBuf> = None;
        let mut publication_request_arg: Option<std::path::PathBuf> = None;
        let mut pending_pointer_arg: Option<std::path::PathBuf> = None;
        let mut expected_sha256_arg: Option<String> = None;
        let mut stage_evidence_sha256_arg: Option<String> = None;
        let mut prior_sha256_arg: Option<String> = None;
        let mut prior_schema_arg: Option<i64> = None;
        let mut held_loop_lock_fd_arg: Option<u32> = None;
        let mut held_loop_lock_pid_arg: Option<u32> = None;
        let mut held_run_lock_fd_arg: Option<u32> = None;
        let mut held_run_lock_pid_arg: Option<u32> = None;
        let mut fixed_end_arg: Option<i64> = None;
        let mut generation_arg: Option<u64> = None;
        let mut full_read_wallets_arg: Option<&str> = None;
        let mut fresh_generation_arg: Option<Result<u64, String>> = None;
        let mut prior_arg: Option<std::path::PathBuf> = None;
        let mut side_arg: Option<std::path::PathBuf> = None;
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
            } else if a == "--held-loop-lock-fd" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                held_loop_lock_fd_arg = rest[i].parse().ok();
            } else if a == "--held-loop-lock-pid" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                held_loop_lock_pid_arg = rest[i].parse().ok();
            } else if a == "--held-run-lock-fd" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                held_run_lock_fd_arg = rest[i].parse().ok();
            } else if a == "--held-run-lock-pid" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                held_run_lock_pid_arg = rest[i].parse().ok();
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
            } else if a == "--targets-csv" {
                // A bare flag must never silently fall through to the legacy
                // prices-history workflow (#536 review). Usage error is FATAL
                // (exit 1): this binary's exit 2 means "partial, retry", and a
                // supervisor must never retry a malformed invocation.
                if i + 1 >= rest.len() {
                    eprintln!("error: --targets-csv requires a file path");
                    std::process::exit(1);
                }
                i += 1;
                flag_values.insert(rest[i]);
                targets_csv = Some(std::path::PathBuf::from(rest[i]));
            } else if let Some(v) = a.strip_prefix("--targets-csv=") {
                targets_csv = Some(std::path::PathBuf::from(v));
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
            } else if a == "--db" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                db_arg = Some(std::path::PathBuf::from(rest[i]));
            } else if let Some(v) = a.strip_prefix("--db=") {
                db_arg = Some(std::path::PathBuf::from(v));
            } else if a == "--manifest" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                manifest_arg = Some(std::path::PathBuf::from(rest[i]));
            } else if let Some(v) = a.strip_prefix("--manifest=") {
                manifest_arg = Some(std::path::PathBuf::from(v));
            } else if a == "--frozen-payload" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                frozen_payload_arg = Some(std::path::PathBuf::from(rest[i]));
            } else if let Some(v) = a.strip_prefix("--frozen-payload=") {
                frozen_payload_arg = Some(std::path::PathBuf::from(v));
            } else if a == "--stage-record" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                stage_record_arg = Some(std::path::PathBuf::from(rest[i]));
            } else if let Some(v) = a.strip_prefix("--stage-record=") {
                stage_record_arg = Some(std::path::PathBuf::from(v));
            } else if a == "--fixed-db" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                fixed_db_arg = Some(std::path::PathBuf::from(rest[i]));
            } else if let Some(v) = a.strip_prefix("--fixed-db=") {
                fixed_db_arg = Some(std::path::PathBuf::from(v));
            } else if a == "--backup" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                backup_arg = Some(std::path::PathBuf::from(rest[i]));
            } else if let Some(v) = a.strip_prefix("--backup=") {
                backup_arg = Some(std::path::PathBuf::from(v));
            } else if a == "--displaced-backup" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                displaced_backup_arg = Some(std::path::PathBuf::from(rest[i]));
            } else if let Some(v) = a.strip_prefix("--displaced-backup=") {
                displaced_backup_arg = Some(std::path::PathBuf::from(v));
            } else if a == "--publication-request" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                publication_request_arg = Some(std::path::PathBuf::from(rest[i]));
            } else if let Some(v) = a.strip_prefix("--publication-request=") {
                publication_request_arg = Some(std::path::PathBuf::from(v));
            } else if a == "--pending-pointer" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                pending_pointer_arg = Some(std::path::PathBuf::from(rest[i]));
            } else if let Some(v) = a.strip_prefix("--pending-pointer=") {
                pending_pointer_arg = Some(std::path::PathBuf::from(v));
            } else if a == "--expected-sha256" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                expected_sha256_arg = Some(rest[i].to_owned());
            } else if let Some(v) = a.strip_prefix("--expected-sha256=") {
                expected_sha256_arg = Some(v.to_owned());
            } else if a == "--stage-evidence-sha256" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                stage_evidence_sha256_arg = Some(rest[i].to_owned());
            } else if let Some(v) = a.strip_prefix("--stage-evidence-sha256=") {
                stage_evidence_sha256_arg = Some(v.to_owned());
            } else if a == "--prior-sha256" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                prior_sha256_arg = Some(rest[i].to_owned());
            } else if let Some(v) = a.strip_prefix("--prior-sha256=") {
                prior_sha256_arg = Some(v.to_owned());
            } else if a == "--prior-schema" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                prior_schema_arg = rest[i].parse().ok();
            } else if let Some(v) = a.strip_prefix("--prior-schema=") {
                prior_schema_arg = v.parse().ok();
            } else if a == "--fixed-end" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                fixed_end_arg = rest[i].parse().ok();
            } else if let Some(v) = a.strip_prefix("--fixed-end=") {
                fixed_end_arg = v.parse().ok();
            } else if a == "--generation" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                generation_arg = rest[i].parse().ok();
            } else if let Some(v) = a.strip_prefix("--generation=") {
                generation_arg = v.parse().ok();
            } else if a == "--fresh-generation" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                fresh_generation_arg = Some(rest[i].parse().map_err(|_| rest[i].to_owned()));
            } else if let Some(v) = a.strip_prefix("--fresh-generation=") {
                fresh_generation_arg = Some(v.parse().map_err(|_| v.to_owned()));
            } else if a == "--full-read-wallets" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                full_read_wallets_arg = Some(rest[i]);
            } else if let Some(v) = a.strip_prefix("--full-read-wallets=") {
                full_read_wallets_arg = Some(v);
            } else if a == "--prior" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                prior_arg = Some(std::path::PathBuf::from(rest[i]));
            } else if let Some(v) = a.strip_prefix("--prior=") {
                prior_arg = Some(std::path::PathBuf::from(v));
            } else if a == "--side" && i + 1 < rest.len() {
                i += 1;
                flag_values.insert(rest[i]);
                side_arg = Some(std::path::PathBuf::from(rest[i]));
            } else if let Some(v) = a.strip_prefix("--side=") {
                side_arg = Some(std::path::PathBuf::from(v));
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

        let mut bootstrap_config = match config::load(toml_arg.as_deref()) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "bootstrap: config error");
                std::process::exit(1);
            }
        };
        if let Some(path) = db_arg {
            bootstrap_config.cache_path = path;
        }

        if matches!(
            sub,
            "cache-migrate-v2"
                | "cache-verify-frozen-v1"
                | "cache-populate-activity-v2"
                | "cache-populate-payout-v2"
                | "cache-finalize-v2"
                | "cache-activate"
                | "cache-restore-prior"
                | "cache-stage-v2"
        ) {
            let now = time::OffsetDateTime::now_utc().unix_timestamp();
            let result = match sub {
                "cache-migrate-v2" => manifest_arg
                    .as_deref()
                    .ok_or_else(|| BootstrapError::Invalid {
                        message: "cache-migrate-v2 requires --manifest".to_owned(),
                    })
                    .and_then(|manifest| {
                        let _lock = pe_bootstrap::lock::CacheMutationLock::acquire(
                            &bootstrap_config.cache_path,
                        )?;
                        migrate_cache_v2(&bootstrap_config.cache_path, manifest)
                            .and_then(json_report)
                    }),
                "cache-verify-frozen-v1" => frozen_payload_arg
                    .as_deref()
                    .ok_or_else(|| BootstrapError::Invalid {
                        message: "cache-verify-frozen-v1 requires --frozen-payload".to_owned(),
                    })
                    .and_then(|reference| {
                        let _lock = pe_bootstrap::lock::CacheMutationLock::acquire(
                            &bootstrap_config.cache_path,
                        )?;
                        verify_frozen_payload_v1(&bootstrap_config.cache_path, reference, now)
                            .and_then(json_report)
                    }),
                "cache-populate-activity-v2" => {
                    async {
                        // A venue `Retry-After: 1` answer to a page burst is waited
                        // out in-line rather than ending a collection of hundreds of
                        // thousands of wallets with exit 75 (#588).
                        let fetcher = pe_source_polymarket_public::ReqwestFetcher::new(
                            reqwest::Client::new(),
                        )
                        .with_rate_limit_retry_max_secs(
                            pe_source_polymarket_public::RECONCILIATION_RATE_LIMIT_RETRY_SECS,
                        );
                        // Fresh mode (#588): no frozen reference; a newly started
                        // generation is bounded by the same settled read end the
                        // legacy poller uses, and a recorded generation keeps its end.
                        // Mode selection reads the raw flags, so a bare or
                        // malformed flag can never fall through to the other
                        // collector.
                        let flag_present = |name: &str| {
                            rest.iter()
                                .any(|a| *a == name || a.starts_with(&format!("{name}=")))
                        };
                        let bulk_root = flag_present("--bulk-root");
                        if bulk_root && (!rest.contains(&"--bulk-root") || !flag_present("--fresh-generation")) {
                            return Err(BootstrapError::Invalid { message: "--bulk-root is a bare flag requiring --fresh-generation 1 and --fixed-db (plus --prior for legacy cycles)".to_owned() });
                        }
                        if flag_present("--fresh-generation") {
                            if flag_present("--frozen-payload")
                                || flag_present("--fixed-end")
                                || flag_present("--generation")
                            {
                                return Err(BootstrapError::Invalid {
                                    message: "--fresh-generation cannot be combined with \
                                              --frozen-payload, --fixed-end, or --generation"
                                        .to_owned(),
                                });
                            }
                            let generation = match fresh_generation_arg {
                                Some(Ok(generation)) => generation,
                                Some(Err(value)) => {
                                    return Err(BootstrapError::Invalid {
                                        message: format!(
                                            "--fresh-generation requires an integer, got {value:?}"
                                        ),
                                    });
                                }
                                None => {
                                    return Err(BootstrapError::Invalid {
                                        message: "--fresh-generation requires an integer value"
                                            .to_owned(),
                                    });
                                }
                            };
                            let _lock = pe_bootstrap::lock::CacheMutationLock::acquire(
                                &bootstrap_config.cache_path,
                            )?;
                            let full_reads = match full_read_wallets_arg {
                                Some(value) if !value.is_empty() => value.split(',').map(str::to_owned).collect::<Vec<_>>(),
                                Some(_) => return Err(BootstrapError::Invalid { message: "--full-read-wallets requires comma-separated wallet addresses".to_owned() }),
                                None if flag_present("--full-read-wallets") => return Err(BootstrapError::Invalid { message: "--full-read-wallets requires a value".to_owned() }),
                                None => Vec::new(),
                            };
                            let settled_end = || time::OffsetDateTime::now_utc().unix_timestamp()
                                .checked_sub(pe_bootstrap::polymarket::ACTIVITY_SETTLE_LAG_SECS)
                                .ok_or_else(|| BootstrapError::Invalid { message: "settled activity clock underflow".to_owned() });
                            if bulk_root {
                                if generation != 1 || flag_present("--full-read-wallets") {
                                    return Err(BootstrapError::Invalid { message: "--bulk-root requires generation 1 with the complete root wallet union".to_owned() });
                                }
                                let fixed = fixed_db_arg.as_deref()
                                    .ok_or_else(|| BootstrapError::Invalid { message: "--bulk-root requires --fixed-db and staging evidence (or legacy --prior) to verify private candidate paths".to_owned() })?;
                                return populate_activity_bulk_root_v2_with_clock(
                                    &bootstrap_config.cache_path, fixed, prior_arg.as_deref(), &fetcher,
                                    &bootstrap_config.polymarket_base_url, settled_end, now,
                                ).await.and_then(activity_json_report);
                            }
                            return populate_activity_fresh_v2_with_clock(
                                &bootstrap_config.cache_path,
                                &fetcher,
                                &bootstrap_config.polymarket_base_url,
                                generation,
                                &full_reads,
                                settled_end,
                                now,
                            )
                            .await
                            .and_then(activity_json_report);
                        }
                        if flag_present("--full-read-wallets") {
                            return Err(BootstrapError::Invalid { message: "--full-read-wallets requires --fresh-generation".to_owned() });
                        }
                        let (fixed_end, generation) = fixed_end_arg.zip(generation_arg).ok_or_else(
                            || BootstrapError::Invalid {
                                message: "cache-populate-activity-v2 requires integer --fixed-end and --generation"
                                    .to_owned(),
                            },
                        )?;
                        let frozen_reference = frozen_payload_arg.as_deref().ok_or_else(|| {
                            BootstrapError::Invalid {
                                message: "cache-populate-activity-v2 requires --frozen-payload"
                                    .to_owned(),
                            }
                        })?;
                        let _lock = pe_bootstrap::lock::CacheMutationLock::acquire(
                            &bootstrap_config.cache_path,
                        )?;
                        populate_activity_v2(
                            &bootstrap_config.cache_path,
                            &fetcher,
                            &bootstrap_config.polymarket_base_url,
                            frozen_reference,
                            fixed_end,
                            generation,
                            now,
                        )
                        .await
                        .and_then(activity_json_report)
                    }
                    .await
                }
                "cache-stage-v2" => prior_arg
                    .zip(side_arg)
                    .ok_or_else(|| BootstrapError::Invalid {
                        message: "cache-stage-v2 requires --db, --prior, and --side".to_owned(),
                    })
                    .and_then(|(prior, side)| {
                        stage_cache_cycle_v2(
                            &bootstrap_config.cache_path,
                            &prior,
                            &side,
                            manifest_arg.as_deref(),
                        )
                        .and_then(json_report)
                    }),
                "cache-populate-payout-v2" => {
                    async {
                        let _lock = pe_bootstrap::lock::CacheMutationLock::acquire(
                            &bootstrap_config.cache_path,
                        )?;
                        let mut cache = WalletCache::open_existing_configured(&bootstrap_config)?;
                        pe_bootstrap::populate_clob_payout_v2(&bootstrap_config, &mut cache)
                            .await
                            .and_then(json_report)
                    }
                    .await
                }
                "cache-finalize-v2" => stage_record_arg
                    .as_deref()
                    .ok_or_else(|| BootstrapError::Invalid {
                        message: "cache-finalize-v2 requires --stage-record".to_owned(),
                    })
                    .and_then(|stage_record| {
                        let _lock = pe_bootstrap::lock::CacheMutationLock::acquire(
                            &bootstrap_config.cache_path,
                        )?;
                        finalize_cache_v2(&bootstrap_config.cache_path, stage_record, now)
                            .and_then(json_report)
                    }),
                "cache-activate" => fixed_db_arg
                    .zip(backup_arg)
                    .zip(expected_sha256_arg)
                    .ok_or_else(|| BootstrapError::Invalid {
                        message:
                            "cache-activate requires --fixed-db, --backup, and --expected-sha256"
                                .to_owned(),
                    })
                    .and_then(
                        |((fixed_path, prior_cache_backup_path), expected_side_sha256)| {
                            let loop_lock = held_loop_lock_fd_arg
                                .zip(held_loop_lock_pid_arg)
                                .map(|(fd, holder_pid)| pe_bootstrap::lock::InheritedForgeLock {
                                    fd,
                                    holder_pid,
                                });
                            let handoff = held_run_lock_fd_arg
                                .zip(held_run_lock_pid_arg)
                                .map(|(fd, holder_pid)| pe_bootstrap::lock::ForgeLockHandoff {
                                    loop_lock,
                                    run_lock: pe_bootstrap::lock::InheritedForgeLock {
                                        fd,
                                        holder_pid,
                                    },
                                });
                            let any_handoff_arg = held_loop_lock_fd_arg.is_some()
                                || held_loop_lock_pid_arg.is_some()
                                || held_run_lock_fd_arg.is_some()
                                || held_run_lock_pid_arg.is_some();
                            if any_handoff_arg && handoff.is_none() {
                                return Err(BootstrapError::Invalid {
                                    message: "cache-activate lock handoff requires both run-lock FD/PID values and either both or neither loop-lock values".to_owned(),
                                });
                            }
                            if held_loop_lock_fd_arg.is_some() != held_loop_lock_pid_arg.is_some() {
                                return Err(BootstrapError::Invalid {
                                    message: "cache-activate loop-lock handoff is incomplete".to_owned(),
                                });
                            }
                            activate_cache_v2_with_handoff(&CacheActivationRequest {
                                fixed_path,
                                side_path: bootstrap_config.cache_path.clone(),
                                prior_cache_backup_path,
                                expected_side_sha256,
                                stage_evidence_sha256: stage_evidence_sha256_arg,
                            }, handoff.as_ref())
                            .and_then(json_report)
                        },
                    ),
                "cache-restore-prior" => {
                    let prepared = fixed_db_arg
                    .zip(backup_arg)
                    .zip(displaced_backup_arg)
                    .zip(prior_sha256_arg.zip(prior_schema_arg))
                    .zip(publication_request_arg.zip(pending_pointer_arg))
                    .ok_or_else(|| BootstrapError::Invalid {
                        message: "cache-restore-prior requires --fixed-db, --backup, \
                                  --displaced-backup, --prior-sha256, --prior-schema, \
                                  --publication-request, and --pending-pointer"
                            .to_owned(),
                    })
                    .and_then(|((((fixed, backup), displaced), (sha256, schema_version)),
                                (publication_request, pending_pointer))| {
                        let base_url = std::env::var("SUPABASE_URL").map_err(|_| {
                            BootstrapError::Invalid {
                                message: "SUPABASE_URL is required for authoritative restore evidence".to_owned(),
                            }
                        })?;
                        let key = std::env::var("SUPABASE_SECRET_KEY").map_err(|_| {
                            BootstrapError::Invalid {
                                message: "SUPABASE_SECRET_KEY is required for authoritative restore evidence".to_owned(),
                            }
                        })?;
                        Ok((fixed, backup, displaced, sha256, schema_version,
                            publication_request, pending_pointer,
                            SupabasePublicationProbe::new(base_url, key)?))
                    });
                    match prepared {
                        Ok((fixed, backup, displaced, sha256, schema_version,
                            publication_request, pending_pointer, probe)) => {
                            restore_prior_cache(
                            &fixed,
                            &backup,
                            &displaced,
                            &PriorCacheBinding { sha256, schema_version },
                            &publication_request,
                            &pending_pointer,
                            &probe,
                        )
                            .await
                            .map(|()| serde_json::json!({"restored": fixed}))
                        }
                        Err(error) => Err(error),
                    }
                }
                _ => Err(BootstrapError::Internal),
            };
            match result {
                Ok(report) => {
                    println!("{report}");
                    std::process::exit(0);
                }
                Err(error) => {
                    tracing::error!(error = %error, command = sub, "cache migration command failed");
                    std::process::exit(error.exit_code());
                }
            }
        }

        // Reclamation evidence is read-only but deliberately excludes all
        // cache writers while it captures the activation gate (#544).
        if sub == "reclamation-evidence" {
            let _cache_mutation_lock =
                acquire_cache_lock_or_exit(&bootstrap_config.cache_path, sub);
            let eval_results_dir =
                reclamation_evidence::eval_results_dir_for_cache(&bootstrap_config.cache_path);
            let exit = match reclamation_evidence::capture(
                &bootstrap_config.cache_path,
                &eval_results_dir,
            ) {
                Ok(report) => match serde_json::to_string(&report) {
                    Ok(rendered) => {
                        println!("{rendered}");
                        if report.activation_ready { 0 } else { 2 }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "reclamation-evidence: encode failed");
                        1
                    }
                },
                Err(e) => {
                    tracing::error!(error = %e, "reclamation-evidence: fatal");
                    1
                }
            };
            std::process::exit(exit);
        }

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

        // Every named command below opens WalletCache read-write (including
        // `all`), so the one central guard must precede that open (#544).
        // Genuine readers return above through `open_read_only`.
        let _cache_mutation_lock = acquire_cache_lock_or_exit(&bootstrap_config.cache_path, sub);

        // These commands operate on an installed cache. Only explicit staging
        // provisions candidate copies; a rename gap must remain vacant.
        let mut cache = match WalletCache::open_existing_configured(&bootstrap_config) {
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
                    if let Err(e) = cache.reset_clob_payout_walk_v2() {
                        tracing::error!(error = %e, "resolutions: failed to reset v2 CLOB payout walk");
                        std::process::exit(1);
                    }
                    tracing::info!(
                        "resolutions: --reset-clob-cursor → deleted legacy cursor and incomplete \
                         v2 payout staging; CLOB will start from page 1"
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
                            // Neutral label: this lane now carries BOTH audit
                            // incompleteness and exhausted-transient CLOB walk
                            // failures (#534); the `error` field distinguishes them.
                            tracing::warn!(
                                error = %e,
                                exit_code,
                                "resolutions: temporary failure"
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

            // Targeted ranker-oracle mode (#536): fetch only the pass-2-emitted uncovered
            // minute windows into the isolated ranker price store. Skips the Gamma
            // start-date pass and the legacy close-anchored targeting entirely.
            "prices-history" if let Some(targets) = targets_csv.as_deref() => {
                match pe_bootstrap::prices_history::run_targeted_prices_history(
                    &bootstrap_config,
                    &mut cache,
                    targets,
                )
                .await
                {
                    Ok(r) => {
                        tracing::info!(
                            tokens = r.tokens,
                            needed_ranges = r.needed_ranges,
                            pages_complete = r.pages_complete,
                            pages_empty = r.pages_empty,
                            points_written = r.points_written,
                            transient_failures = r.transient_failures,
                            "prices-history targeted: complete"
                        );
                        // Transient page failures → partial (exit 2): durable + resumable,
                        // the uncovered remainder is re-requested on the next invocation.
                        if r.transient_failures > 0 { 2 } else { 0 }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "prices-history targeted: fatal");
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

            "recover-reclamation" => match purge::recover_pending_reclamation(&mut cache) {
                Ok(Some(report)) => {
                    tracing::info!(
                        auto_vacuum_before = report.auto_vacuum_before,
                        path = ?report.path,
                        freelist_before = report.freelist_before,
                        freelist_after = report.freelist_after,
                        page_count_before = report.page_count_before,
                        page_count_after = report.page_count_after,
                        "recover-reclamation: complete"
                    );
                    0
                }
                Ok(None) => {
                    tracing::info!("recover-reclamation: no pending marker");
                    0
                }
                Err(e) => {
                    tracing::error!(error = %e, "recover-reclamation: fatal");
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
                            "clear-infra-exclusion: exclusion cleared (infra tombstone and/or live is_infra flag)"
                        );
                        0
                    }
                    Ok(false) => {
                        tracing::error!(
                            wallet = normalized,
                            "clear-infra-exclusion: no infra tombstone or live is_infra flag for wallet"
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
        if matches!(sub, "purge" | "purge-infra") {
            let eval_results_dir =
                reclamation_evidence::eval_results_dir_for_cache(&bootstrap_config.cache_path);
            if let Err(error) = purge::append_direct_purge_status(&eval_results_dir, sub, exit) {
                tracing::warn!(error = %error, subcommand = sub, "purge status append failed");
            }
        }
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

    // No-arg → run "all" with strict=false (soft-fail default). It follows the
    // same central lock-before-open contract as the named `all` command (#544).
    let _cache_mutation_lock = acquire_cache_lock_or_exit(&bootstrap_config.cache_path, "all");
    let mut cache = match WalletCache::open_existing_configured(&bootstrap_config) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "bootstrap: cache open failed");
            std::process::exit(1);
        }
    };
    let exit = handle_all(&bootstrap_config, &mut cache, false).await;
    std::process::exit(exit);
}

fn acquire_cache_lock_or_exit(
    cache_path: &std::path::Path,
    subcommand: &str,
) -> pe_bootstrap::lock::CacheMutationLock {
    match pe_bootstrap::lock::CacheMutationLock::acquire(cache_path) {
        Ok(lock) => lock,
        Err(e) => {
            tracing::error!(
                error = %e,
                subcommand,
                "bootstrap: cache mutation lock failed"
            );
            std::process::exit(1);
        }
    }
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
        // The audit gate's typed incompleteness is a temporary condition (#523/#524).
        BootstrapError::ResolutionAuditIncomplete { .. } => BootstrapError::TEMPFAIL_EXIT_CODE,
        // Everything else delegates to the generic mapping, which sends the typed
        // temporary `TransientSource` (e.g. an exhausted-transient CLOB page walk,
        // #534) to tempfail 75 and every permanent error to 1.
        _ => error.exit_code(),
    }
}

fn activity_json_report(
    mut manifest: pe_bootstrap::cache_migration::ActivityCoverageManifestV2,
) -> Result<serde_json::Value, BootstrapError> {
    // Historical manifests still validate their embedded proofs, but reports
    // only need bounded metadata. Null means legacy proof omitted from output.
    if manifest.cursors.is_array() {
        manifest.cursors = serde_json::Value::Null;
        manifest.page_hashes.clear();
    }
    json_report(manifest)
}

fn json_report<T: serde::Serialize>(report: T) -> Result<serde_json::Value, BootstrapError> {
    serde_json::to_value(report).map_err(BootstrapError::from)
}

#[cfg(test)]
mod tests {
    use super::resolutions_error_exit_code;
    use pe_bootstrap::error::BootstrapError;

    #[test]
    fn resolutions_tempfail_covers_audit_incomplete_and_transient_source() {
        for error in [
            BootstrapError::ResolutionAuditIncomplete {
                blocked: 1,
                clipped: 0,
            },
            BootstrapError::ResolutionAuditIncomplete {
                blocked: 0,
                clipped: 1,
            },
            BootstrapError::TransientSource {
                source_name: "polymarket-clob",
                message: "fetch url: transient error (after 5 page retries)".to_owned(),
            },
        ] {
            assert_eq!(
                resolutions_error_exit_code(&error),
                BootstrapError::TEMPFAIL_EXIT_CODE
            );
        }
        // Permanent failures — fatal fetch/parse, cache — stay exit 1.
        assert_eq!(
            resolutions_error_exit_code(&BootstrapError::Clob {
                message: "fatal".to_owned(),
            }),
            1
        );
        assert_eq!(
            resolutions_error_exit_code(&BootstrapError::Cache {
                message: "disk".to_owned(),
            }),
            1
        );
    }
}
