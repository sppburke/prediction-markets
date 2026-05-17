//! `pe-bootstrap` — wallet discovery and seed watchlist generation.
//!
//! Pipeline:
//! 1. Discover wallets — load from per-contract checkpoint if available, else
//!    enumerate via Dune Analytics or Etherscan (selected by `PE_WALLET_SOURCE`).
//!    Dune path applies four quality filters at query time (see `dune::WALLET_DISCOVERY_SQL`).
//!    Etherscan: checkpoint saved after each contract; resume skips completed ones.
//! 2. Fetch trade history per wallet from the Polymarket Data API (permanent SQLite cache,
//!    incremental per run).
//! 3. Reconstruct `TraderLedger`s via `pe-trader-index`.
//! 4. Post-filter: keep wallets passing all four quality conditions (trades, win rate, recency,
//!    avg hold duration). Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
//! 5. Build a seed `Watchlist` and write it to `output_path`.
//! 6. Optionally fetch market resolution data: Gamma API (classic markets, PE_BOOTSTRAP_FETCH_RESOLUTIONS=1)
//!    + Dune on-chain `ctf_evt_conditionresolution` (all markets including financial, gated by PE_DUNE_API_KEY).
//!
//! Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" section.

pub mod backfill;
pub mod cache;
pub mod clob;
pub mod config;
pub mod delta_audit;
pub mod discovery;
pub mod dune;
pub mod error;
pub mod filter;
pub mod gamma;
pub mod migrate;
pub mod operator_audit;
pub mod pile;
pub mod polygon_ctf;
pub mod polygon_ctf_delta;
pub mod polymarket;
pub mod wallet_set;
pub mod weekly;

pub use config::BootstrapConfig;

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::stream::{self, StreamExt};
use pe_core_types::{BasisPoints, SourceTimestamp, WalletAddress};
use pe_operator_graph::OperatorIdentity;
use pe_source_onchain_polygon::{
    AlloyChainLogFetcher, BlockRange, EnumerationConfig, EtherscanFunderLookup,
    PolymarketTraderEnumeration,
    contracts::{
        ALL_EXCHANGE_CONTRACTS, ALL_ORDER_FILLED_TOPICS, CTF_EXCHANGE_V1_DEPLOY_BLOCK,
        TOPIC_ORDER_FILLED_V1, topic_to_contract_version_bit,
    },
    wallet_enumeration::SCAN_CHUNK_BLOCKS,
};
use pe_source_polymarket_public::ReqwestFetcher;
use pe_trader_index::{
    LedgerConfig, TraderLedger, Watchlist, WatchlistEntry, WatchlistTier, build_trader_ledgers,
    snapshot::TradeSnapshot,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use cache::{WalletCache, WalletUpsertRow};
use dune::DuneClient;
use error::BootstrapError;
use filter::{FilterConfig, passes_filter, win_rate_bps};
use pile::SRC_WALLET_SET_JSON;
use polymarket::PolymarketBulkFetcher;
use rust_decimal::Decimal;
use tokio::sync::Mutex;

// Upper-bound block for funder discovery — both endpoints finalized, result is time-invariant.
const FUNDER_DISCOVERY_TO_BLOCK: u64 = 80_000_000;

/// Wallet discovery backend.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WalletSource {
    /// Use Dune Analytics (legacy path; requires `PE_DUNE_API_KEY`).
    Dune,
    /// Use Polygon RPC `eth_getLogs` via alloy (requires `PE_BOOTSTRAP_POLYGON_RPC_URL`).
    /// Issue #186: migrated off Etherscan REST to escape the 100k/day free-tier
    /// quota. `"etherscan"` is accepted as a serde alias for backward compat
    /// with existing config files.
    #[default]
    #[serde(alias = "etherscan")]
    OnChain,
}

/// Delta-backfill mode (issue #176).
///
/// Selects how `backfill::run_backfill` uses the Polygon CTF on-chain `eth_getLogs`
/// scan to narrow the per-day Polymarket API surface.
///
/// - [`DeltaMode::Off`] — legacy behaviour. Every due wallet from
///   `select_backfill_due` is fetched. No on-chain scan; no audit rows.
/// - [`DeltaMode::Shadow`] — runs the on-chain scan AND the legacy full fetch on
///   every backfill run; classifies each wallet that had new trades OR appeared
///   in the delta set into `DELTA_HIT` / `DELTA_MISS` / `DELTA_EXTRA` rows in the
///   `delta_audit` table. Operators flip to [`DeltaMode::Delta`] after the audit
///   table is consistently empty of `DELTA_MISS` rows across multiple runs.
/// - [`DeltaMode::Delta`] — the on-chain scan filters the fetch set down to
///   `(full_due_set ∩ delta_set) ∪ paranoia_set`. Weekly paranoia
///   (`select_full_fetch_due`) backstops any wallet whose `last_polymarket_full_at`
///   exceeds the staleness window.
///
/// `Default` is [`DeltaMode::Shadow`] — safe-by-default for first release because
/// shadow mode is functionally a no-op for the fetch path (legacy behaviour plus
/// an audit table). Note: the delta scanner is only invoked inside
/// `backfill::run_backfill`; `lib.rs::run()` never reads this field regardless of
/// its value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeltaMode {
    /// Disable the on-chain scan entirely; legacy fetch-all-due behaviour.
    Off,
    /// Run scan + full fetch; populate `delta_audit` but do not change the fetch set.
    #[default]
    Shadow,
    /// Use the scan to filter the fetch set; weekly paranoia provides the backstop.
    Delta,
}

/// Run the full bootstrap pipeline and return the seed [`Watchlist`].
///
/// Writes the watchlist as pretty-printed JSON to `config.output_path`.
pub async fn run(config: &BootstrapConfig) -> Result<Watchlist, BootstrapError> {
    // Open the canonical SQLite cache up front — needed by both
    // `auto_migrate_legacy` (issue #181) and every later step.
    let mut cache = WalletCache::open(&config.cache_path)?;

    // Issue #181: one-shot consolidation of legacy on-disk artifacts into
    // SQLite. Detects `wallet_set.json` + `data/dune_csvs/*.csv`, ingests
    // them, persists enumeration-progress markers to `source_cursor`, then
    // deletes / archives the originals. After first successful post-deploy
    // run, this is a near-no-op (two `Path::exists()` syscalls).
    migrate::auto_migrate_legacy(config, &mut cache)?;

    // 1. Discover wallets — read enum progress from SQLite, run enumeration
    //    (Etherscan/Dune) to fill in any missing (topic, contract) pairs,
    //    then read the working wallet list back from the cache.
    let wallets: Vec<WalletAddress> = {
        let (mut completed_contracts, mut enumerated_topic_hashes) =
            migrate::load_enum_state(&cache)?;

        let total_contracts = ALL_EXCHANGE_CONTRACTS.len();
        // Issue #179: enumeration is complete only when every contract AND
        // every known `OrderFilled` topic version has been swept.
        let all_topics_done = ALL_ORDER_FILLED_TOPICS
            .iter()
            .all(|h| enumerated_topic_hashes.contains(&format!("{h}")));
        if completed_contracts.len() >= total_contracts && all_topics_done {
            tracing::info!("bootstrap: wallet discovery complete — skipping");
        } else {
            match &config.wallet_source {
                WalletSource::Dune => {
                    let api_key = config
                        .dune_api_key
                        .clone()
                        .ok_or(BootstrapError::Internal)?;
                    tracing::info!(
                        min_closed_markets = config.dune_min_closed_markets,
                        min_win_rate_pct = config.dune_min_win_rate_pct,
                        active_window_days = config.dune_active_window_days,
                        max_avg_hours_to_resolution = config.dune_max_avg_hours_to_resolution,
                        "bootstrap: querying dune for wallets"
                    );
                    let dune = DuneClient::new(api_key);
                    let found = dune
                        .discover_wallets(
                            OffsetDateTime::now_utc(),
                            config.dune_min_closed_markets,
                            config.dune_min_win_rate_pct,
                            config.dune_active_window_days,
                            config.dune_max_avg_hours_to_resolution,
                        )
                        .await?;
                    tracing::info!(count = found.len(), "bootstrap: dune returned wallets");
                    let rows: Vec<WalletUpsertRow> = found
                        .iter()
                        .map(|w| {
                            (
                                w.to_string(),
                                SRC_WALLET_SET_JSON,
                                false,
                                None,
                                None,
                                None,
                                0,
                            )
                        })
                        .collect();
                    cache.upsert_wallets_bulk(&rows)?;
                    // Mark all contracts complete so subsequent runs skip Dune.
                    completed_contracts = ALL_EXCHANGE_CONTRACTS
                        .iter()
                        .map(|c| format!("0x{c:x}"))
                        .collect();
                    // Dune SQL covers all OrderFilled events across V1 and V2
                    // implicitly; mark every known topic as enumerated so the
                    // guard short-circuits next run instead of re-querying Dune.
                    enumerated_topic_hashes = ALL_ORDER_FILLED_TOPICS
                        .iter()
                        .map(|h| format!("{h}"))
                        .collect();
                    migrate::save_enum_state(
                        &mut cache,
                        &completed_contracts,
                        &enumerated_topic_hashes,
                    )?;
                }
                WalletSource::OnChain => {
                    let rpc_url = config
                        .polygon_rpc_url
                        .clone()
                        .ok_or(BootstrapError::Internal)?;
                    let http_url: reqwest::Url = rpc_url.parse()?;
                    let provider = alloy::providers::ProviderBuilder::new().connect_http(http_url);
                    let to_block = match config.wallet_to_block {
                        Some(b) => b,
                        None => alloy::providers::Provider::get_block_number(&provider)
                            .await
                            .map_err(|e| BootstrapError::PolygonCtf {
                                message: format!("get_block_number: {e}"),
                            })?,
                    };
                    // Issue #188 Item 3: refuse to enter the topic loop with an
                    // inverted range. Without this guard, the inner `while
                    // chunk_from <= to_block` skips silently while the outer
                    // `enumerated_topic_hashes.push(...)` still fires —
                    // silently marking every topic enumerated despite zero
                    // chunks scanned.
                    if config.wallet_from_block > to_block {
                        return Err(BootstrapError::Invalid {
                            message: format!(
                                "wallet_from_block {} > to_block {}; refusing to mark enumeration done over an empty range",
                                config.wallet_from_block, to_block
                            ),
                        });
                    }
                    tracing::info!(
                        from_block = config.wallet_from_block,
                        to_block,
                        operators = config.operator_addresses.len(),
                        contracts_done = completed_contracts.len(),
                        contracts_total = total_contracts,
                        topics_done = enumerated_topic_hashes.len(),
                        topics_total = ALL_ORDER_FILLED_TOPICS.len(),
                        "bootstrap: enumerating wallets via polygon RPC"
                    );
                    let enum_config = EnumerationConfig {
                        from_block: config.wallet_from_block,
                        to_block,
                        operator_addresses: config.operator_addresses.clone(),
                    };
                    let fetcher = AlloyChainLogFetcher {
                        provider,
                        min_chunk: 1,
                    };
                    let enumerator =
                        PolymarketTraderEnumeration::with_fetcher(fetcher, enum_config);

                    // Legacy-checkpoint upgrade (one-shot): a pre-#179
                    // structured checkpoint has every contract in
                    // `completed_contracts` but an empty
                    // `enumerated_topic_hashes`. Treat that exact state as
                    // "V1 fully done" so we don't redo the V1 sweep.
                    let legacy_v1_done = enumerated_topic_hashes.is_empty()
                        && ALL_EXCHANGE_CONTRACTS
                            .iter()
                            .all(|c| completed_contracts.contains(&format!("0x{c:x}")));
                    if legacy_v1_done {
                        tracing::info!(
                            "bootstrap: legacy checkpoint upgraded; V1 marked complete, V2 pending"
                        );
                        enumerated_topic_hashes.push(format!("{TOPIC_ORDER_FILLED_V1}"));
                        migrate::save_enum_state(
                            &mut cache,
                            &completed_contracts,
                            &enumerated_topic_hashes,
                        )?;
                    }

                    // Per-topic outer loop, chunk-level persistence inner
                    // loop (issue #186): each `SCAN_CHUNK_BLOCKS`-sized window
                    // is upserted before advancing, so a crash loses at most
                    // one chunk's worth of work instead of an entire contract.
                    // The V1/V2 contract-version bit is attributed per topic
                    // and OR-merged into `polymarket_contracts_seen`.
                    //
                    // Issue #188 Item 2: per-chunk cursor (`chunk_progress`)
                    // persists `(topic, contract) → last_completed_chunk_to`
                    // after every successful chunk upsert. On mid-topic crash,
                    // resume reads the cursor and skips already-completed
                    // chunks — saves up to ~168 redundant `eth_getLogs` calls
                    // per crash on a full historical sweep.
                    let mut chunk_progress = migrate::load_chunk_progress(&cache)?;
                    for topic in &ALL_ORDER_FILLED_TOPICS {
                        let topic_hex = format!("{topic}");
                        if enumerated_topic_hashes.contains(&topic_hex) {
                            tracing::info!(
                                topic = %topic_hex,
                                "bootstrap: topic already enumerated — skipping"
                            );
                            continue;
                        }
                        // Unreachable in practice — `pe_source_onchain_polygon::contracts::tests::
                        // every_topic_in_all_order_filled_has_a_version_bit` fails CI if a topic
                        // is added to ALL_ORDER_FILLED_TOPICS without a matching bit. Surfacing
                        // `Internal` here keeps the contract explicit at the call site.
                        let contract_bit = topic_to_contract_version_bit(*topic)
                            .ok_or(BootstrapError::Internal)?;
                        for contract in &ALL_EXCHANGE_CONTRACTS {
                            let contract_hex = format!("0x{contract:x}");
                            let progress_key =
                                migrate::chunk_progress_key(&topic_hex, &contract_hex);
                            // Resume point: one past the last completed chunk, or
                            // `wallet_from_block` if no progress recorded.
                            let mut chunk_from = chunk_progress
                                .get(&progress_key)
                                .map(|last| last.saturating_add(1))
                                .unwrap_or(config.wallet_from_block)
                                .max(config.wallet_from_block);
                            if chunk_from > to_block {
                                tracing::info!(
                                    contract = %contract_hex,
                                    topic = %topic_hex,
                                    resume_from = chunk_from,
                                    to_block,
                                    "bootstrap: (topic, contract) already complete up to to_block — skipping"
                                );
                                continue;
                            }
                            while chunk_from <= to_block {
                                let chunk_to = (chunk_from + SCAN_CHUNK_BLOCKS - 1).min(to_block);
                                let found = enumerator
                                    .enumerate_chunk(*contract, *topic, chunk_from, chunk_to)
                                    .await?;
                                tracing::info!(
                                    contract = %contract_hex,
                                    topic = %topic_hex,
                                    chunk_from,
                                    chunk_to,
                                    found = found.len(),
                                    "bootstrap: chunk enumerated"
                                );
                                let rows: Vec<WalletUpsertRow> = found
                                    .iter()
                                    .map(|w| {
                                        (
                                            w.to_string(),
                                            SRC_WALLET_SET_JSON,
                                            false,
                                            None,
                                            None,
                                            None,
                                            contract_bit,
                                        )
                                    })
                                    .collect();
                                cache.upsert_wallets_bulk(&rows)?;
                                // Persist cursor AFTER successful upsert. Order
                                // matters: if upsert fails, cursor isn't
                                // advanced and the chunk is retried; if cursor
                                // save fails after a successful upsert, the
                                // chunk gets retried and OR-merge UPSERT
                                // produces an idempotent result.
                                chunk_progress.insert(progress_key.clone(), chunk_to);
                                migrate::save_chunk_progress(&mut cache, &chunk_progress)?;
                                chunk_from = chunk_to + 1;
                            }
                        }
                        // Topic is fully done. Drop the per-`(topic, contract)`
                        // entries we just finished — the outer-loop skip via
                        // `enumerated_topic_hashes.contains` short-circuits
                        // resume before the cursor is consulted, so these
                        // entries are dead weight. Keeps the JSON map bounded
                        // and avoids the documentation lie that every entry
                        // names an in-progress topic.
                        let topic_prefix = format!("{topic_hex}|");
                        chunk_progress.retain(|k, _| !k.starts_with(&topic_prefix));
                        migrate::save_chunk_progress(&mut cache, &chunk_progress)?;
                        enumerated_topic_hashes.push(topic_hex);
                        migrate::save_enum_state(
                            &mut cache,
                            &completed_contracts,
                            &enumerated_topic_hashes,
                        )?;
                    }
                }
            }
        }

        // Working wallet set for trade fetch — narrowly scoped to the
        // discovered-wallet subset via `SRC_WALLET_SET_JSON` bit. CRITICAL:
        // do NOT use `all_pile_wallet_hexes` which would expand the scope
        // to the full 2.7M-row cache (issue #181 trade-fetch scope rule).
        let hexes = cache.wallets_with_source_bit(SRC_WALLET_SET_JSON)?;
        hexes
            .iter()
            .filter_map(|h| {
                WalletAddress::from_hex(h)
                    .map_err(|e| {
                        tracing::warn!(address = %h, error = %e, "bootstrap: skipping unparseable wallet");
                    })
                    .ok()
            })
            .collect()
    };

    // 2. Fetch trade history — permanent cache (SQLite, WAL), incremental per run.
    //    On first run fetches full history. On subsequent runs fetches only trades
    //    newer than the newest cached id per wallet.
    // Short pool_idle_timeout avoids reusing connections the server has silently
    // closed (Polymarket servers enforce per-IP connection limits under load).
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    let fetcher = PolymarketBulkFetcher::new(
        config.polymarket_base_url.clone(),
        ReqwestFetcher::new(client),
    )
    .with_concurrency(config.polymarket_concurrency)
    .with_wallet_timeout(config.polymarket_wallet_timeout_secs);
    if config.skip_trade_fetch {
        tracing::warn!(
            wallets = wallets.len(),
            "bootstrap: PE_BOOTSTRAP_SKIP_TRADE_FETCH=1 — skipping Polymarket fetch; \
             cache may not reflect trades after the last full run"
        );
    } else {
        let outcome = fetcher.fetch_all(&wallets, &mut cache).await?;
        tracing::info!(
            attempted = outcome.attempted,
            failed = outcome.failed.len(),
            "bootstrap: trade fetch complete"
        );
        // Preserve prior behaviour: legacy `run()` pipeline errors on any failure.
        // The fail-soft semantics are scoped to `backfill::run_backfill` (issue #166
        // post-mortem follow-up).
        if !outcome.failed.is_empty() {
            return Err(BootstrapError::PartialFetch {
                failed_wallets: outcome.failed.len(),
            });
        }
    }

    // 2b. Fetch funder edges via Etherscan — time-invariant once block range is finalized.
    //     Per-wallet atomic commit enables resume after failure.
    //     Gated by flag + API key so the slow path is opt-in.
    if config.fetch_funder_graph {
        if let Some(api_key) = &config.etherscan_api_key {
            let pending = cache.wallets_needing_funder_lookup()?;
            let total_pending = pending.len();
            let total_cached = wallets.len().saturating_sub(total_pending);
            tracing::info!(
                cached = total_cached,
                pending = total_pending,
                "bootstrap: funder discovery starting"
            );
            let block_range = BlockRange {
                from: CTF_EXCHANGE_V1_DEPLOY_BLOCK,
                to: FUNDER_DISCOVERY_TO_BLOCK,
            };
            let lookup = EtherscanFunderLookup::new(api_key.clone());
            let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
            let failed = {
                let cache_mutex: Mutex<&mut WalletCache> = Mutex::new(&mut cache);
                let failed = Arc::new(AtomicUsize::new(0));
                let progress = Arc::new(AtomicUsize::new(0));

                stream::iter(pending.iter().copied())
                    .for_each_concurrent(config.funder_concurrency, |wallet| {
                        let cache_mutex = &cache_mutex;
                        let lookup = &lookup;
                        let failed = Arc::clone(&failed);
                        let progress = Arc::clone(&progress);
                        async move {
                            let wallet_set: HashSet<WalletAddress> =
                                std::iter::once(wallet).collect();
                            match lookup
                                .funders_of_with_timestamps(&wallet_set, block_range)
                                .await
                            {
                                Ok(funders) => {
                                    let funders_vec: Vec<(WalletAddress, i64)> =
                                        funders.into_iter().collect();
                                    let mut guard = cache_mutex.lock().await;
                                    if let Err(e) =
                                        guard.insert_funder_edges(wallet, &funders_vec, fetched_at)
                                    {
                                        tracing::error!(
                                            wallet = %wallet,
                                            error = %e,
                                            "funder discovery: cache insert failed"
                                        );
                                        failed.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                Err(e) => {
                                    tracing::error!(
                                        wallet = %wallet,
                                        error = %e,
                                        "funder discovery: fetch failed"
                                    );
                                    failed.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            let n = progress.fetch_add(1, Ordering::Relaxed) + 1;
                            if n.is_multiple_of(500) || n == total_pending {
                                tracing::info!(
                                    progress = n,
                                    total = total_pending,
                                    "bootstrap: funder discovery progress"
                                );
                            }
                        }
                    })
                    .await;

                // `cache_mutex` drops here, releasing &mut cache before the
                // total_edges query below.
                failed
            };

            let n = failed.load(Ordering::Relaxed);
            if n > 0 {
                return Err(BootstrapError::PartialFunderFetch { failed_wallets: n });
            }

            let total_edges = cache.load_funder_edges()?.len();
            tracing::info!(
                cached = total_cached,
                queried = total_pending,
                edges_total = total_edges,
                "bootstrap: funder discovery complete"
            );
        } else {
            tracing::warn!(
                "PE_BOOTSTRAP_FETCH_FUNDER_GRAPH=1 but PE_ETHERSCAN_API_KEY not set — skipping"
            );
        }
    }

    // 3. Reconstruct ledgers — per-wallet streaming from SQLite to keep peak
    //    memory bounded. Each wallet's trades are loaded, reconstructed, and
    //    dropped before the next wallet is processed.
    // audit_window_days=None → unlimited (u32::MAX sentinel passed downstream).
    // Reserved for future per-snapshot windowing; currently unused by build_trader_ledgers.
    let snapshot_at = SourceTimestamp(OffsetDateTime::now_utc());
    let audit_window_days = config.audit_window_days.unwrap_or(u32::MAX);
    let empty_operators: &[OperatorIdentity] = &[];
    let ledger_config = LedgerConfig::default();
    let mut ledgers: Vec<TraderLedger> = Vec::with_capacity(wallets.len());
    let mut total_trades: usize = 0;
    for wallet in &wallets {
        let trades = cache.trades_for(&wallet.to_string());
        if trades.is_empty() {
            continue;
        }
        total_trades += trades.len();
        let snapshot = TradeSnapshot {
            trades,
            snapshot_at: snapshot_at.clone(),
            audit_window_days,
        };
        ledgers.extend(build_trader_ledgers(
            &snapshot,
            empty_operators,
            &ledger_config,
        ));
    }
    tracing::info!(
        ledgers = ledgers.len(),
        trades = total_trades,
        "bootstrap: reconstructed ledgers"
    );

    // 4. Post-filter and 5. Build seed watchlist.
    let filter = FilterConfig {
        min_closed_trades: config.min_closed_trades,
        min_win_rate_pct: config.min_win_rate_pct,
        active_window_days: config.post_filter_active_window_days,
        max_avg_hours_to_resolution: config.post_filter_max_avg_hours_to_resolution,
    };
    let snapshot_at_for_db = snapshot_at.clone();
    let watchlist = build_seed_watchlist(ledgers, snapshot_at, &filter);
    tracing::info!(
        active = watchlist.active_count,
        incubator = watchlist.incubator_count,
        "bootstrap: watchlist built"
    );

    // Persist a leaderboard snapshot row-set: (snapshot_at_unix, wallet) for
    // every wallet that survived the post-filter. Read by the backtest's
    // walk-forward simulation to constrain its candidate pool to wallets that
    // *would have been* visible to the live system at this point in time.
    //
    // Gated on `write_snapshot` (default: false). When false, the
    // watchlist still builds and writes to the JSON output, but no row is
    // persisted to `leaderboard_snapshots` — this keeps ad-hoc retries
    // (resolutions watchdog, funder-graph reruns, dev shells) from polluting
    // the snapshot timeline with near-duplicate `now`-stamped rows. The
    // official weekly refresh path is the intended sole writer; it opts in
    // explicitly. Historical seeding via `seed_historical_snapshots`
    // (PE_SEED_AS_OF_DATES) is unaffected — that path has always written its
    // target rows and continues to do so.
    let snapshot_wallets: Vec<WalletAddress> = watchlist.entries.iter().map(|e| e.wallet).collect();
    if config.write_snapshot {
        cache.insert_snapshot(snapshot_at_for_db.0.unix_timestamp(), &snapshot_wallets)?;
        tracing::info!(
            snapshot_at = snapshot_at_for_db.0.unix_timestamp(),
            wallets = snapshot_wallets.len(),
            "bootstrap: leaderboard snapshot persisted"
        );
    } else {
        tracing::info!(
            snapshot_at = snapshot_at_for_db.0.unix_timestamp(),
            wallets = snapshot_wallets.len(),
            "bootstrap: leaderboard snapshot write skipped \
             (set PE_BOOTSTRAP_WRITE_SNAPSHOT=true to enable; \
             official weekly refresh is the intended writer)"
        );
    }

    // 6. Multi-source historical-data pipeline (issue #149). Extracted to
    //    `fetch_resolutions_and_schedules` so `pe-bootstrap backfill` (issue #166)
    //    reuses the same precision-ordered flow.
    if config.fetch_resolutions {
        let all_market_ids = cache.all_market_ids();
        fetch_resolutions_and_schedules(config, &mut cache, &all_market_ids).await?;
    }

    // Write output.
    write_watchlist(&watchlist, &config.output_path)?;

    Ok(watchlist)
}

/// Multi-source resolution + schedule pipeline (issue #149, extracted in #166).
///
/// Stage ordering puts precision sources first so `INSERT OR IGNORE` keeps the
/// most accurate `resolved_at_unix`:
///
/// - 6a. Polygon RPC CTF scan → `source='polygon'` (block timestamp).
/// - 6b. Dune `ctf_evt_conditionresolution` → `source='dune'` (block timestamp).
/// - 6c. CLOB closed-market pagination → `source='clob'` (`end_date_iso` approx).
/// - 6d. Gamma schedules → `source='gamma'` (open markets only).
/// - 6e. Gamma liquidity → only Gamma exposes liquidity (open markets).
/// - 6f. Gamma null-schedule rewrite pass (issue #137 Sub-PR 2).
///
/// `open_ids` is computed AFTER 6a/6b/6c so Gamma only fetches truly-still-open
/// markets. `market_ids` scopes the run — pass `cache.all_market_ids()` for the
/// full pipeline (run/seed) or just the wallets-newly-fetched market set for
/// targeted backfill.
///
/// `config.rebuild_resolutions = true` drops every imprecise-source row
/// (`'gamma'`, `'clob'`) up front so the precision stages can re-populate them
/// with block-timestamp accuracy. Idempotent — safe on every run.
pub async fn fetch_resolutions_and_schedules(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    market_ids: &[String],
) -> Result<(), BootstrapError> {
    if config.rebuild_resolutions {
        let deleted = cache.delete_resolutions_by_sources(&["gamma", "clob"])?;
        tracing::info!(
            deleted,
            "bootstrap: rebuild_resolutions=1 — deleted imprecise-source rows so precision stages repopulate"
        );
    }

    // 6a. Polygon RPC scan — gated on rpc_url; cursor resume avoids walking
    //     the entire chain on daily re-runs. Authoritative block timestamps.
    if let Some(rpc_url) = config.polygon_rpc_url.as_deref() {
        let from_block = cache
            .get_source_cursor(polygon_ctf::POLYGON_CTF_CURSOR_KEY)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(pe_source_onchain_polygon::contracts::CTF_DEPLOY_BLOCK);
        let inserted = polygon_ctf::scan_resolutions(
            rpc_url,
            from_block,
            None,
            config.polygon_ctf_chunk_blocks,
            cache,
        )
        .await?;
        tracing::info!(
            inserted,
            from_block,
            "bootstrap: polygon_ctf resolutions fetched"
        );
    }

    // 6b. Dune `ctf_evt_conditionresolution` — primary precision source
    //     when no Polygon RPC is configured (and gap-fill when it is).
    if let Some(api_key) = &config.dune_api_key {
        let unresolved = unresolved_market_ids(market_ids, &cache.resolved_market_ids());
        if !unresolved.is_empty() {
            let dune_resolution_client = DuneClient::new(api_key.clone());
            let unresolved_set: HashSet<String> = unresolved.into_iter().collect();
            let rows = dune_resolution_client
                .fetch_resolutions(&unresolved_set, 0, config.dune_namespace.as_deref())
                .await?;
            let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
            let mut inserted = 0usize;
            for (market_id, winner, resolved_at_unix) in rows {
                cache.insert_resolution_with_source(
                    &market_id,
                    winner,
                    resolved_at_unix,
                    fetched_at,
                    "dune",
                )?;
                inserted += 1;
            }
            tracing::info!(
                inserted,
                unresolved = unresolved_set.len(),
                "bootstrap: dune resolutions fetched"
            );
        }
    }

    // 6c. CLOB closed-market pagination.
    let clob_client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    let clob_fetcher = clob::ClobFetcher::new(
        config.clob_base_url.clone(),
        ReqwestFetcher::new(clob_client),
    );
    let (clob_schedules, clob_resolutions) = clob_fetcher.fetch_closed_markets(cache).await?;
    tracing::info!(
        clob_schedules,
        clob_resolutions,
        "bootstrap: clob closed markets fetched"
    );

    // 6d/e. Gamma schedules + liquidity — open markets only.
    let open_ids: Vec<String> = unresolved_market_ids(market_ids, &cache.resolved_market_ids());
    let gamma_client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    let gamma_fetcher = gamma::GammaFetcher::new(
        config.gamma_base_url.clone(),
        ReqwestFetcher::new(gamma_client).with_min_interval_ms(gamma::GAMMA_MIN_INTERVAL_MS),
    );
    let schedule_rows = gamma_fetcher.fetch_schedules(&open_ids, cache).await?;
    tracing::info!(
        schedule_rows,
        open_markets = open_ids.len(),
        "bootstrap: gamma schedules fetched (open markets only)"
    );
    let liquidity_rows = gamma_fetcher
        .fetch_market_liquidity(&open_ids, cache)
        .await?;
    tracing::info!(
        liquidity_rows,
        open_markets = open_ids.len(),
        "bootstrap: gamma liquidity fetched (open markets only)"
    );

    // 6f. Null-schedule rewrite pass (issue #137 Sub-PR 2).
    let null_ids = cache.null_schedule_market_ids();
    let trade_set: HashSet<String> = market_ids.iter().cloned().collect();
    let rewrite_targets: Vec<String> = null_ids.intersection(&trade_set).cloned().collect();
    if !rewrite_targets.is_empty() {
        let rewritten = gamma_fetcher
            .rewrite_null_schedules(&rewrite_targets, cache)
            .await?;
        tracing::info!(
            rewritten,
            candidates = rewrite_targets.len(),
            "bootstrap: gamma null-schedule rewrite complete"
        );
    }
    Ok(())
}

/// Seed historical leaderboard snapshots for a list of past `as_of` UTC midnights.
///
/// Runs the parameterized Dune `discover_wallets` query once per date and inserts
/// the resulting wallet set into `leaderboard_snapshots`. Idempotent: re-running
/// with overlapping dates is safe (PRIMARY KEY collision = silent skip).
///
/// Requires `dune_api_key`. The other Dune filter parameters mirror the live path
/// — they're sourced from the same config so historical snapshots use identical
/// quality filters to what the live system would have used at that time.
pub async fn seed_historical_snapshots(
    config: &BootstrapConfig,
    as_of_dates: &[OffsetDateTime],
) -> Result<usize, BootstrapError> {
    let api_key = config
        .dune_api_key
        .clone()
        .ok_or_else(|| BootstrapError::MissingEnv("PE_DUNE_API_KEY".to_owned()))?;
    let dune = DuneClient::new(api_key);
    let mut cache = WalletCache::open(&config.cache_path)?;

    let already_seeded: std::collections::HashSet<i64> =
        cache.all_snapshot_dates()?.into_iter().collect();

    let mut total_rows: usize = 0;
    for as_of in as_of_dates {
        let unix = as_of.unix_timestamp();
        if already_seeded.contains(&unix) {
            tracing::info!(
                as_of_unix = unix,
                as_of = %as_of,
                "bootstrap: snapshot already present — skipping"
            );
            continue;
        }
        tracing::info!(
            as_of_unix = unix,
            as_of = %as_of,
            "bootstrap: seeding historical snapshot via dune"
        );
        let wallets = dune
            .discover_wallets(
                *as_of,
                config.dune_min_closed_markets,
                config.dune_min_win_rate_pct,
                config.dune_active_window_days,
                config.dune_max_avg_hours_to_resolution,
            )
            .await?;
        cache.insert_snapshot(unix, &wallets)?;
        total_rows += wallets.len();
        tracing::info!(
            as_of_unix = unix,
            wallets = wallets.len(),
            "bootstrap: snapshot inserted"
        );
    }
    Ok(total_rows)
}

/// Parse `PE_SEED_AS_OF_DATES`: comma-separated `YYYY-MM-DD` UTC dates.
///
/// Returns `Ok(Vec::new())` when the variable is unset or empty (caller treats this
/// as "no seeding requested"). Whitespace around each entry is trimmed; empty
/// entries (e.g. trailing comma) are skipped.
pub fn parse_seed_as_of_env(value: &str) -> Result<Vec<OffsetDateTime>, BootstrapError> {
    let mut out = Vec::new();
    for raw in value.split(',') {
        let s = raw.trim();
        if s.is_empty() {
            continue;
        }
        let date = time::Date::parse(s, &time::format_description::well_known::Iso8601::DATE)
            .map_err(|e| BootstrapError::Parse {
                message: format!("PE_SEED_AS_OF_DATES `{s}`: {e}"),
            })?;
        out.push(date.midnight().assume_utc());
    }
    Ok(out)
}

/// Build a seed [`Watchlist`] from reconstructed ledgers using the bootstrap post-filter.
///
/// Uses win-rate basis points as the score (no historical LCB_5pct available at bootstrap).
/// All passing wallets are assigned `Active` tier; `operator_id` is always `None` since
/// operator attribution requires `source-onchain-polygon` data not available here.
pub fn build_seed_watchlist(
    ledgers: Vec<TraderLedger>,
    snapshot_at: SourceTimestamp,
    filter: &FilterConfig,
) -> Watchlist {
    let snapshot_at_unix = snapshot_at.0.unix_timestamp();
    let mut entries: Vec<WatchlistEntry> = Vec::new();

    for ledger in &ledgers {
        if !passes_filter(ledger, snapshot_at_unix, filter) {
            continue;
        }

        let total = ledger.closed_trades.len();
        let wins = ledger
            .closed_trades
            .iter()
            .filter(|t| t.realized_pnl_usd > Decimal::ZERO)
            .count();

        let win_rate = BasisPoints(win_rate_bps(wins, total));
        entries.push(WatchlistEntry {
            wallet: ledger.wallet,
            operator_id: None,
            tier: WatchlistTier::Active,
            leader_score_bps: win_rate,
            lcb_5pct_bps: BasisPoints(0),
            win_rate_bps: win_rate,
            closed_trades_in_window: u32::try_from(total).unwrap_or(u32::MAX),
            reconstruction_quality: ledger.reconstruction_quality,
        });
    }

    // Sort descending by score.
    entries.sort_by_key(|e| std::cmp::Reverse(e.leader_score_bps.0));

    let active_count = entries.len();
    Watchlist {
        entries,
        snapshot_at,
        active_count,
        incubator_count: 0,
    }
}

fn write_watchlist(watchlist: &Watchlist, path: &Path) -> Result<(), BootstrapError> {
    let json = serde_json::to_vec_pretty(watchlist)?;
    // Atomic write: tmp → rename.
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &json)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Return every market in `all` that is not present in `resolved`.
///
/// Issue #149 helper used twice in the run() pipeline: at the Dune-gate
/// (stage 6b) to short-circuit when stage 6a already resolved everything,
/// and again to compute `open_ids` for Gamma's open-market scope (stage 6d/e).
///
/// Pure data manipulation — no I/O — so the unit test below proves the
/// "skipped when nothing left to resolve" contract without spinning up a
/// real DuneClient.
pub(crate) fn unresolved_market_ids(all: &[String], resolved: &HashSet<String>) -> Vec<String> {
    all.iter()
        .filter(|id| !resolved.contains(*id))
        .cloned()
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn unresolved_market_ids_empty_when_all_resolved() {
        // Replaces what would otherwise be a scenario_dune_only_for_gaps test;
        // the Dune `if !unresolved.is_empty()` short-circuit is verified here
        // without needing to inject a DuneClient.
        let all = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let resolved: HashSet<String> = all.iter().cloned().collect();
        assert!(unresolved_market_ids(&all, &resolved).is_empty());
    }

    #[test]
    fn unresolved_market_ids_returns_complement_partial() {
        let all = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let resolved: HashSet<String> = ["b".to_owned()].into_iter().collect();
        let got = unresolved_market_ids(&all, &resolved);
        // Iteration order preserves input ordering.
        assert_eq!(got, vec!["a".to_owned(), "c".to_owned()]);
    }

    #[test]
    fn unresolved_market_ids_all_when_none_resolved() {
        let all = vec!["a".to_owned(), "b".to_owned()];
        let resolved: HashSet<String> = HashSet::new();
        assert_eq!(unresolved_market_ids(&all, &resolved), all);
    }

    #[test]
    fn unresolved_market_ids_empty_input_is_empty_output() {
        let all: Vec<String> = Vec::new();
        let resolved: HashSet<String> = HashSet::new();
        assert!(unresolved_market_ids(&all, &resolved).is_empty());
    }
}
