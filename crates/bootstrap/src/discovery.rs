//! Incremental Dune wallet discovery (`pe-bootstrap discovery`, issue #166).
//!
//! Every 48h:
//!
//! 1. Upload the current `wallets.wallet_hex` pile to
//!    `<dune_namespace>.<known_wallets_dune_table>`.
//! 2. Run the discovery SQL (anti-join against the uploaded table) to find
//!    NEW makers in `polymarket_polygon.market_trades_raw` since the last
//!    cursor write — filtered by `HAVING COUNT(*) >= pile_activation_min_trades`
//!    so only sufficiently-active wallets enter the pile.
//! 3. UPSERT new rows with `source_bits |= SRC_DUNE_INCREMENTAL` and
//!    `dune_closed_markets = dune_trade_count` (so the activation rule
//!    immediately flips `is_active = 1` without waiting for DB backfill).
//! 4. Apply activation rules.
//! 5. Update the `dune_discovery_last_run` cursor.

use time::OffsetDateTime;

use crate::cache::{WalletCache, WalletUpsertRow};
use crate::config::BootstrapConfig;
use crate::dune::DuneClient;
use crate::error::BootstrapError;
use crate::pile::{self, PILE_ACTIVATION_MIN_TRADES, SRC_DUNE_INCREMENTAL};

/// `source_cursor` key for the discovery cursor.
pub const DISCOVERY_CURSOR_KEY: &str = "dune_discovery_last_run";

/// Per-run counts reported back to the caller.
#[derive(Debug, Default, Clone, Copy)]
pub struct DiscoveryReport {
    pub uploaded: usize,
    pub new_wallets: usize,
    pub activated: usize,
}

/// Run discovery against Dune.
pub async fn run_discovery(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<DiscoveryReport, BootstrapError> {
    let api_key = config.dune_api_key.clone().ok_or(BootstrapError::Dune {
        message: "discovery requires PE_DUNE_API_KEY".to_string(),
    })?;
    let namespace = config.dune_namespace.clone().ok_or(BootstrapError::Dune {
        message: "discovery requires PE_DUNE_NAMESPACE".to_string(),
    })?;

    let now_unix = OffsetDateTime::now_utc().unix_timestamp();

    // Cursor: last run cutoff, or now - discovery_lookback_days * 86400 cold-start.
    let last_run_unix = cache
        .get_source_cursor(DISCOVERY_CURSOR_KEY)
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or_else(|| now_unix - i64::from(config.discovery_lookback_days) * 86_400);

    // Upload the current pile.
    let pile = cache.all_pile_wallet_hexes()?;
    let dune = DuneClient::new(api_key);
    let rows_iter = pile.iter().map(String::as_str);
    dune.upload_table(
        &namespace,
        &config.known_wallets_dune_table,
        "wallet_hex",
        rows_iter,
    )
    .await?;
    let uploaded = pile.len();
    tracing::info!(uploaded, "discovery: pile uploaded to dune");

    // Run discovery.
    let rows = dune
        .run_discovery(
            &namespace,
            &config.known_wallets_dune_table,
            last_run_unix,
            PILE_ACTIVATION_MIN_TRADES,
        )
        .await?;
    let new_count = rows.len();

    // UPSERT new rows. `dune_closed_markets` carries the discovery trade count
    // so activation fires on the next apply_activation_rules pass.
    let upserts: Vec<WalletUpsertRow> = rows
        .into_iter()
        .map(|(hex, first_seen, trade_count)| {
            (
                hex,
                SRC_DUNE_INCREMENTAL,
                false,
                Some(first_seen),
                Some(trade_count),
                None,
            )
        })
        .collect();
    cache.upsert_wallets_bulk(&upserts)?;

    // Activate any newly-qualifying wallets.
    let activated = pile::apply_activation_rules(cache)?;

    // Advance the cursor only after successful upsert + activation.
    cache.set_source_cursor(DISCOVERY_CURSOR_KEY, &now_unix.to_string())?;

    tracing::info!(new_wallets = new_count, activated, "discovery: complete");
    Ok(DiscoveryReport {
        uploaded,
        new_wallets: new_count,
        activated,
    })
}
