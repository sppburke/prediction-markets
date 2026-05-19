//! Wallet enumeration phase — `pe-bootstrap enumerate`.
//!
//! Discovers tradeable wallets via either the Dune Analytics or Polygon on-chain
//! (alloy `eth_getLogs`) path, persisting incremental chunk-level progress so a
//! crash loses at most one chunk's work. Idempotent: already-completed topics and
//! contracts are skipped.

use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::{
    AlloyChainLogFetcher, EnumerationConfig, PolymarketTraderEnumeration,
    contracts::{
        ALL_EXCHANGE_CONTRACTS, ALL_ORDER_FILLED_TOPICS, TOPIC_ORDER_FILLED_V1,
        topic_to_contract_version_bit,
    },
    wallet_enumeration::SCAN_CHUNK_BLOCKS,
};
use time::OffsetDateTime;

use crate::cache::{WalletCache, WalletUpsertRow};
use crate::config::BootstrapConfig;
use crate::dune::DuneClient;
use crate::error::BootstrapError;
use crate::lock;
use crate::migrate;
use crate::pile::SRC_WALLET_SET_JSON;

/// Result of the wallet enumeration phase.
#[derive(Debug, Default, Clone, Copy)]
pub struct EnumerateReport {
    /// Total wallets in the SRC_WALLET_SET_JSON bit-set after enumeration.
    pub wallets_discovered: usize,
    /// Total `SCAN_CHUNK_BLOCKS`-sized windows processed (OnChain path only).
    pub chunks_scanned: usize,
    /// Number of OrderFilled topics fully enumerated this run.
    pub topics_completed: usize,
    /// True when enumeration was skipped because all topics were already done.
    pub skipped: bool,
}

/// Discover wallets and persist them to `cache`.
///
/// Selects the Dune or on-chain path based on `config.wallet_source`. Acquires a
/// `CacheMutationLock` for the duration of the scan (mirrors the lock semantics in
/// the previous `lib.rs::run()` enumeration block).
///
/// # Precondition
/// `cache` must have been opened and `auto_migrate_legacy` must have already run.
pub async fn run_enumerate(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<EnumerateReport, BootstrapError> {
    use crate::WalletSource;

    let (mut completed_contracts, mut enumerated_topic_hashes) = migrate::load_enum_state(cache)?;

    let total_contracts = ALL_EXCHANGE_CONTRACTS.len();
    let all_topics_done = ALL_ORDER_FILLED_TOPICS
        .iter()
        .all(|h| enumerated_topic_hashes.contains(&format!("{h}")));

    if completed_contracts.len() >= total_contracts && all_topics_done {
        tracing::info!("enumerate: wallet discovery complete — skipping");
        let hexes = cache.wallets_with_source_bit(SRC_WALLET_SET_JSON)?;
        return Ok(EnumerateReport {
            wallets_discovered: hexes.len(),
            skipped: true,
            ..Default::default()
        });
    }

    let mut chunks_scanned: usize = 0;
    let mut topics_completed: usize = 0;

    match &config.wallet_source {
        WalletSource::Dune => {
            // Issue #193 — defensive cache-mutation lock for the Dune arm.
            let _cache_lock = lock::CacheMutationLock::acquire(&config.cache_path)?;
            let api_key = config
                .dune_api_key
                .clone()
                .ok_or(BootstrapError::Internal)?;
            tracing::info!(
                min_closed_markets = config.dune_min_closed_markets,
                min_win_rate_pct = config.dune_min_win_rate_pct,
                active_window_days = config.dune_active_window_days,
                max_avg_hours_to_resolution = config.dune_max_avg_hours_to_resolution,
                "enumerate: querying dune for wallets"
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
            tracing::info!(count = found.len(), "enumerate: dune returned wallets");
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
            completed_contracts = ALL_EXCHANGE_CONTRACTS
                .iter()
                .map(|c| format!("0x{c:x}"))
                .collect();
            enumerated_topic_hashes = ALL_ORDER_FILLED_TOPICS
                .iter()
                .map(|h| format!("{h}"))
                .collect();
            migrate::save_enum_state(cache, &completed_contracts, &enumerated_topic_hashes)?;
            topics_completed = enumerated_topic_hashes.len();
        }
        WalletSource::OnChain => {
            // Issue #191 Item 2 / #193 — defensive cache-mutation lock.
            let _cache_lock = lock::CacheMutationLock::acquire(&config.cache_path)?;
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
            // Issue #188 Item 3: refuse to enter the topic loop with an inverted range.
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
                "enumerate: enumerating wallets via polygon RPC"
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
            let enumerator = PolymarketTraderEnumeration::with_fetcher(fetcher, enum_config);

            // Legacy-checkpoint upgrade (one-shot).
            let legacy_v1_done = enumerated_topic_hashes.is_empty()
                && ALL_EXCHANGE_CONTRACTS
                    .iter()
                    .all(|c| completed_contracts.contains(&format!("0x{c:x}")));
            if legacy_v1_done {
                tracing::info!(
                    "enumerate: legacy checkpoint upgraded; V1 marked complete, V2 pending"
                );
                enumerated_topic_hashes.push(format!("{TOPIC_ORDER_FILLED_V1}"));
                migrate::save_enum_state(cache, &completed_contracts, &enumerated_topic_hashes)?;
            }

            let mut chunk_progress = migrate::load_chunk_progress(cache)?;
            for topic in &ALL_ORDER_FILLED_TOPICS {
                let topic_hex = format!("{topic}");
                if enumerated_topic_hashes.contains(&topic_hex) {
                    tracing::info!(
                        topic = %topic_hex,
                        "enumerate: topic already enumerated — skipping"
                    );
                    continue;
                }
                let contract_bit =
                    topic_to_contract_version_bit(*topic).ok_or(BootstrapError::Internal)?;
                for contract in &ALL_EXCHANGE_CONTRACTS {
                    let contract_hex = format!("0x{contract:x}");
                    let progress_key = migrate::chunk_progress_key(&topic_hex, &contract_hex);
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
                            "enumerate: (topic, contract) already complete up to to_block — skipping"
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
                            "enumerate: chunk enumerated"
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
                        chunk_progress.insert(progress_key.clone(), chunk_to);
                        migrate::save_chunk_progress(cache, &chunk_progress)?;
                        chunk_from = chunk_to + 1;
                        chunks_scanned += 1;
                    }
                }
                let topic_prefix = format!("{topic_hex}|");
                chunk_progress.retain(|k, _| !k.starts_with(&topic_prefix));
                migrate::save_chunk_progress(cache, &chunk_progress)?;
                enumerated_topic_hashes.push(topic_hex);
                migrate::save_enum_state(cache, &completed_contracts, &enumerated_topic_hashes)?;
                topics_completed += 1;
            }
        }
    }

    let hexes = cache.wallets_with_source_bit(SRC_WALLET_SET_JSON)?;
    let wallets_discovered: usize = hexes
        .iter()
        .filter(|h| WalletAddress::from_hex(h).is_ok())
        .count();

    tracing::info!(
        wallets_discovered,
        chunks_scanned,
        topics_completed,
        "enumerate: complete"
    );

    Ok(EnumerateReport {
        wallets_discovered,
        chunks_scanned,
        topics_completed,
        skipped: false,
    })
}
