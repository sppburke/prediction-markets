//! Wallet enumeration phase — `pe-bootstrap enumerate`.
//!
//! Discovers tradeable wallets via either the Dune Analytics or Polygon on-chain
//! (alloy `eth_getLogs`) path, persisting incremental chunk-level progress so a
//! crash loses at most one chunk's work. Idempotent: already-completed topics and
//! contracts are skipped.

use pe_core_types::WalletAddress;
use time::OffsetDateTime;

use crate::cache::{WalletCache, WalletUpsertRow};
use crate::chain::{ALL_EXCHANGE_CONTRACTS, ALL_ORDER_FILLED_TOPICS};
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

    // On-chain enumeration (which scanned chunks) was removed in #326 PR4; the
    // surviving Dune arm marks every topic done in one shot.
    let chunks_scanned: usize = 0;
    let topics_completed: usize;

    match &config.wallet_source {
        WalletSource::Dune => {
            // Issue #193 — defensive cache-mutation lock for the Dune arm.
            let _cache_lock = lock::CacheMutationLock::acquire(&config.cache_path)?;
            let api_key = config
                .dune_api_key
                .clone()
                .ok_or_else(|| BootstrapError::MissingEnv("PE_DUNE_API_KEY".to_owned()))?;
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
