//! Supabase-authoritative hot runtime configuration (#398, #544).
//!
//! `service_config` is parsed as one complete proposal. Missing, duplicate, unknown, malformed,
//! or cross-field-inconsistent rows reject the proposal without changing any applied field. The
//! only optional economic row is `kelly_fraction_override`; the financial era also permits the
//! separately owned `risk_halt_release_hash` incident row and excludes it from the economic hash.
//! Watchlist capacity remains truthful while a prepared membership transition is pending: the
//! applied snapshot keeps the old capacity until the structural writer commits the new membership.

use std::collections::{BTreeMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use pe_core_types::KellyFraction;
use pe_strategy_winner_follow::{ExecutionMode, PerTradeCap, SizingMode, WinnerFollowConfig};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::ServiceConfig;

/// Exact post-Start hot-key allowlist (#545). Every key is mandatory exactly once except
/// `kelly_fraction_override`, which may be absent.
pub const HOT_CONFIG_KEYS: [&str; 15] = [
    "active_watchlist_size",
    "mode",
    "max_fill_price",
    "min_fill_price",
    "min_resolution_horizon_secs",
    "max_resolution_horizon_secs",
    "price_impact_cap_bps",
    "flip_human_approved",
    "kelly_fraction_above_default_human_approved",
    "kelly_fraction_override",
    "per_trade_cap",
    "slippage_rate",
    "sizing_mode",
    "sizing_dollar_usd",
    "sizing_contracts",
];

pub const LEGACY_HOT_CONFIG_KEYS: [&str; 17] = [
    "active_watchlist_size",
    "mode",
    "max_fill_price",
    "min_fill_price",
    "min_resolution_horizon_secs",
    "max_resolution_horizon_secs",
    "fill_mode",
    "price_impact_cap_bps",
    "flip_human_approved",
    "kelly_fraction_above_default_human_approved",
    "polymarket_fee_rate",
    "kelly_fraction_override",
    "per_trade_cap",
    "slippage_rate",
    "sizing_mode",
    "sizing_dollar_usd",
    "sizing_contracts",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigEra {
    Legacy17,
    Financial15,
}

/// Exact database-row retirement set used by the guarded operator migration (#544).
pub const REMOVED_CONFIG_KEYS: [&str; 22] = [
    "bankroll_usd",
    "bench_overfetch",
    "demotion_cb_alpha",
    "demotion_min_trades",
    "demotion_pnl_window_secs",
    "gamma_resolution_poll_interval_secs",
    "inactivity_hard_cap_secs",
    "inactivity_threshold_secs",
    "log_retention_days",
    "maintenance_interval_secs",
    "paper_fill_haircut_bps",
    "paper_fill_slippage_bps",
    "status_interval_secs",
    "supabase_refresh_interval_secs",
    "supabase_sink_reconcile_interval_secs",
    "entry_gate_fail_closed",
    "position_page_limit",
    "position_reseed_interval_secs",
    "position_size_threshold",
    "wallet_market_history_path",
    "clob_best_ask_fallback_haircut_bps",
    "trade_poll_interval_secs",
];

pub const DEFAULT_ACTIVE_WATCHLIST_SIZE: usize = 100;
pub const MIN_ACTIVE_WATCHLIST_SIZE: usize = 1;
pub const MAX_ACTIVE_WATCHLIST_SIZE: usize = 200;
pub const MIN_PRICE_IMPACT_CAP_BPS: i32 = 1;
pub const MAX_PRICE_IMPACT_CAP_BPS: i32 = 10_000;
pub const RISK_HALT_RELEASE_HASH_KEY: &str = "risk_halt_release_hash";

/// The membership cap that has actually been published to the live watchlist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchlistCapacityEpoch {
    pub generation: u64,
    pub target: usize,
}

#[derive(Debug, Clone)]
pub struct AppliedWatchlistCapacity {
    inner: Arc<ArcSwap<WatchlistCapacityEpoch>>,
}

impl AppliedWatchlistCapacity {
    pub fn new(initial: usize) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(WatchlistCapacityEpoch {
                generation: 0,
                target: initial,
            })),
        }
    }

    pub fn load(&self) -> WatchlistCapacityEpoch {
        *self.inner.load_full()
    }

    /// Callers hold the shared structural-writer mutex while committing membership and this epoch.
    pub fn store(&self, value: WatchlistCapacityEpoch) {
        self.inner.store(Arc::new(value));
    }
}

/// One raw `service_config` row returned by PostgREST.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigRow {
    pub key: String,
    pub value: String,
    #[serde(default)]
    pub value_type: String,
}

/// One era-bound hot snapshot. It deliberately contains no revision/hash field; the one
/// canonical applied hash is process status derived from the active era's exact values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeConfig {
    pub era: ConfigEra,
    pub active_watchlist_size: usize,
    pub mode: String,
    pub max_fill_price: Decimal,
    pub min_fill_price: Decimal,
    pub min_resolution_horizon_secs: u64,
    pub max_resolution_horizon_secs: u64,
    pub price_impact_cap_bps: i32,
    pub flip_human_approved: bool,
    pub kelly_fraction_above_default_human_approved: bool,
    pub kelly_fraction_override: Option<KellyFraction>,
    pub per_trade_cap: PerTradeCap,
    pub slippage_rate: Decimal,
    pub sizing_mode: SizingMode,
    pub sizing_dollar_usd: Decimal,
    pub sizing_contracts: u64,
    /// Required pre-Start rows retained only so the Legacy17 canonical hash binds all 17 names.
    #[serde(skip_serializing_if = "Option::is_none")]
    legacy_compatibility: Option<LegacyCompatibility>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct LegacyCompatibility {
    fill_mode: String,
    polymarket_fee_rate: Decimal,
}

impl RuntimeConfig {
    /// Scenario/test baseline. Production boot never uses this as authority: it requires and
    /// validates a complete Supabase snapshot before producers start.
    pub fn from_service_config(cfg: &ServiceConfig) -> Self {
        let max_fill_price = Decimal::from_str(&cfg.max_fill_price).unwrap_or(Decimal::ZERO);
        let min_fill_price = Decimal::from_str(&cfg.min_fill_price).unwrap_or(Decimal::ZERO);
        let (sizing_dollar_usd, sizing_contracts) = match cfg.strategy.sizing_mode {
            SizingMode::Dollar { usd } => (usd, 0),
            SizingMode::Contract { contracts } => (Decimal::ZERO, contracts),
            SizingMode::Kelly => (Decimal::ZERO, 0),
        };
        Self {
            era: ConfigEra::Financial15,
            active_watchlist_size: DEFAULT_ACTIVE_WATCHLIST_SIZE,
            mode: cfg.mode.clone(),
            max_fill_price,
            min_fill_price,
            min_resolution_horizon_secs: cfg.min_resolution_horizon_secs,
            max_resolution_horizon_secs: cfg.max_resolution_horizon_secs,
            // No accepted-zero construction path remains. Production's mandatory boot snapshot
            // replaces this test baseline before any producer can start.
            price_impact_cap_bps: 100,
            flip_human_approved: cfg.strategy.flip_human_approved,
            kelly_fraction_above_default_human_approved: cfg
                .strategy
                .kelly_fraction_above_default_human_approved,
            kelly_fraction_override: cfg.strategy.kelly_fraction_override,
            per_trade_cap: cfg.strategy.per_trade_cap,
            slippage_rate: cfg.strategy.slippage_rate,
            sizing_mode: cfg.strategy.sizing_mode,
            sizing_dollar_usd,
            sizing_contracts,
            legacy_compatibility: None,
        }
    }

    pub fn winner_follow_config(&self) -> WinnerFollowConfig {
        WinnerFollowConfig {
            flip_human_approved: self.flip_human_approved,
            kelly_fraction_above_default_human_approved: self
                .kelly_fraction_above_default_human_approved,
            kelly_fraction_override: self.kelly_fraction_override,
            per_trade_cap: self.per_trade_cap,
            slippage_rate: self.slippage_rate,
            sizing_mode: self.sizing_mode,
        }
    }

    /// BLAKE3 of one canonical semantic JSON representation of the values actually applied.
    pub fn canonical_hash(&self) -> String {
        let mut body = serde_json::json!({
            "active_watchlist_size": self.active_watchlist_size,
            "mode": self.mode,
            "max_fill_price": decimal_text(self.max_fill_price),
            "min_fill_price": decimal_text(self.min_fill_price),
            "min_resolution_horizon_secs": self.min_resolution_horizon_secs,
            "max_resolution_horizon_secs": self.max_resolution_horizon_secs,
            "price_impact_cap_bps": self.price_impact_cap_bps,
            "flip_human_approved": self.flip_human_approved,
            "kelly_fraction_above_default_human_approved": self.kelly_fraction_above_default_human_approved,
            "kelly_fraction_override": self.kelly_fraction_override.map(|value| decimal_text(value.0)),
            "per_trade_cap": canonical_per_trade_cap(self.per_trade_cap),
            "slippage_rate": decimal_text(self.slippage_rate),
            "sizing_mode": canonical_sizing_mode(self.sizing_mode),
            "sizing_dollar_usd": decimal_text(self.sizing_dollar_usd),
            "sizing_contracts": self.sizing_contracts,
        });
        if let Some(legacy) = &self.legacy_compatibility {
            body["fill_mode"] = serde_json::Value::String(legacy.fill_mode.clone());
            body["polymarket_fee_rate"] =
                serde_json::Value::String(decimal_text(legacy.polymarket_fee_rate));
        }
        blake3::hash(body.to_string().as_bytes())
            .to_hex()
            .to_string()
    }
}

/// Typed reason an entire raw snapshot was rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Error)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConfigSnapshotError {
    #[error("missing mandatory service_config key '{key}'")]
    MissingKey { key: String },
    #[error("duplicate service_config key '{key}'")]
    DuplicateKey { key: String },
    #[error("unknown service_config key '{key}'")]
    UnknownKey { key: String },
    #[error("service_config key '{key}' has value_type '{actual}', expected '{expected}'")]
    WrongValueType {
        key: String,
        expected: String,
        actual: String,
    },
    #[error("service_config key '{key}' is invalid: {reason}")]
    InvalidValue { key: String, reason: String },
    #[error("service_config cross-field invariant failed: {reason}")]
    CrossField { reason: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct RejectedConfigSnapshot {
    pub error: ConfigSnapshotError,
    /// Exact raw rows that failed validation; this is not a second revision identifier.
    pub rows: Vec<ConfigRow>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeConfigStatusSnapshot {
    pub applied_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejected: Option<RejectedConfigSnapshot>,
}

/// Shared status of the sole applied runtime snapshot and the most recent rejected raw proposal.
#[derive(Clone)]
pub struct RuntimeConfigStatus {
    inner: Arc<ArcSwap<RuntimeConfigStatusSnapshot>>,
}

impl RuntimeConfigStatus {
    pub fn new(applied: &RuntimeConfig) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(RuntimeConfigStatusSnapshot {
                applied_hash: applied.canonical_hash(),
                rejected: None,
            })),
        }
    }

    pub fn snapshot(&self) -> Arc<RuntimeConfigStatusSnapshot> {
        self.inner.load_full()
    }

    pub fn record_applied(&self, applied: &RuntimeConfig) {
        self.inner.store(Arc::new(RuntimeConfigStatusSnapshot {
            applied_hash: applied.canonical_hash(),
            rejected: None,
        }));
    }

    pub fn record_rejected(&self, rows: &[ConfigRow], error: ConfigSnapshotError) {
        let applied_hash = self.inner.load().applied_hash.clone();
        self.inner.store(Arc::new(RuntimeConfigStatusSnapshot {
            applied_hash,
            rejected: Some(RejectedConfigSnapshot {
                error,
                rows: rows.to_vec(),
            }),
        }));
    }
}

/// Parse one complete proposal. `last_good` is consulted only for the guarded mode transition;
/// no proposed field is inherited from it.
pub fn parse_config(
    rows: &[ConfigRow],
    last_good: &RuntimeConfig,
    clob_creds_present: bool,
    era: ConfigEra,
) -> Result<RuntimeConfig, ConfigSnapshotError> {
    let mut map = BTreeMap::<&str, &ConfigRow>::new();
    let era_keys: &[&str] = match era {
        ConfigEra::Legacy17 => &LEGACY_HOT_CONFIG_KEYS,
        ConfigEra::Financial15 => &HOT_CONFIG_KEYS,
    };
    let mut allowlist: HashSet<&str> = era_keys.iter().copied().collect();
    if era == ConfigEra::Financial15 {
        allowlist.insert(RISK_HALT_RELEASE_HASH_KEY);
    }
    for row in rows {
        if !allowlist.contains(row.key.as_str()) {
            return Err(ConfigSnapshotError::UnknownKey {
                key: row.key.clone(),
            });
        }
        if map.insert(row.key.as_str(), row).is_some() {
            return Err(ConfigSnapshotError::DuplicateKey {
                key: row.key.clone(),
            });
        }
    }
    for key in era_keys {
        if *key != "kelly_fraction_override" && !map.contains_key(key) {
            return Err(ConfigSnapshotError::MissingKey {
                key: (*key).to_owned(),
            });
        }
    }
    for (key, row) in &map {
        let expected = expected_value_type(key);
        if row.value_type != expected {
            return Err(ConfigSnapshotError::WrongValueType {
                key: (*key).to_owned(),
                expected: expected.to_owned(),
                actual: row.value_type.clone(),
            });
        }
    }

    let active_watchlist_size = parse::<usize>(&map, "active_watchlist_size")?;
    if !(MIN_ACTIVE_WATCHLIST_SIZE..=MAX_ACTIVE_WATCHLIST_SIZE).contains(&active_watchlist_size) {
        return invalid("active_watchlist_size", "must be in 1..=200");
    }

    let mode_raw = raw(&map, "mode")?;
    let mode = validate_mode_transition(mode_raw, clob_creds_present, &last_good.mode).map_err(
        |reason| ConfigSnapshotError::InvalidValue {
            key: "mode".to_owned(),
            reason,
        },
    )?;

    let max_fill_price = parse_decimal(&map, "max_fill_price")?;
    validate_fraction_decimal("max_fill_price", max_fill_price)?;
    let min_fill_price = parse_decimal(&map, "min_fill_price")?;
    validate_fraction_decimal("min_fill_price", min_fill_price)?;
    if min_fill_price > Decimal::ZERO
        && max_fill_price > Decimal::ZERO
        && min_fill_price >= max_fill_price
    {
        return cross("enabled min_fill_price must be less than enabled max_fill_price");
    }

    let min_resolution_horizon_secs = parse(&map, "min_resolution_horizon_secs")?;
    let max_resolution_horizon_secs = parse(&map, "max_resolution_horizon_secs")?;
    if min_resolution_horizon_secs > 0
        && max_resolution_horizon_secs > 0
        && min_resolution_horizon_secs > max_resolution_horizon_secs
    {
        return cross(
            "enabled min_resolution_horizon_secs must not exceed enabled max_resolution_horizon_secs",
        );
    }

    let legacy_compatibility = match era {
        ConfigEra::Legacy17 => {
            let fill_mode = match raw(&map, "fill_mode")?
                .trim()
                .to_lowercase()
                .replace('-', "_")
                .as_str()
            {
                "clob_best_ask" | "clobbestask" => "clob_best_ask".to_owned(),
                "leader_haircut" | "leaderhaircut" => "leader_haircut".to_owned(),
                _ => return invalid("fill_mode", "must be clob_best_ask or leader_haircut"),
            };
            let polymarket_fee_rate = parse_decimal(&map, "polymarket_fee_rate")?;
            validate_fraction_decimal("polymarket_fee_rate", polymarket_fee_rate)?;
            Some(LegacyCompatibility {
                fill_mode,
                polymarket_fee_rate,
            })
        }
        ConfigEra::Financial15 => None,
    };
    let price_impact_cap_bps = parse(&map, "price_impact_cap_bps")?;
    if !(MIN_PRICE_IMPACT_CAP_BPS..=MAX_PRICE_IMPACT_CAP_BPS).contains(&price_impact_cap_bps) {
        return invalid("price_impact_cap_bps", "must be in 1..=10000");
    }

    let flip_human_approved: bool = parse(&map, "flip_human_approved")?;
    let kelly_fraction_above_default_human_approved: bool =
        parse(&map, "kelly_fraction_above_default_human_approved")?;
    let slippage_rate = parse_decimal(&map, "slippage_rate")?;
    validate_fraction_decimal("slippage_rate", slippage_rate)?;

    let kelly_fraction_override = match map.get("kelly_fraction_override") {
        None => None,
        Some(row)
            if row.value.trim().is_empty()
                || row.value.trim().eq_ignore_ascii_case("none")
                || row.value.trim().eq_ignore_ascii_case("null") =>
        {
            None
        }
        Some(row) => {
            let value = Decimal::from_str(row.value.trim()).map_err(|_| {
                ConfigSnapshotError::InvalidValue {
                    key: "kelly_fraction_override".to_owned(),
                    reason: "must be a decimal fraction".to_owned(),
                }
            })?;
            Some(
                KellyFraction::new(value).map_err(|error| ConfigSnapshotError::InvalidValue {
                    key: "kelly_fraction_override".to_owned(),
                    reason: error.to_string(),
                })?,
            )
        }
    };
    if let Some(override_value) = kelly_fraction_override {
        if override_value.0 > Decimal::new(50, 2) {
            return invalid("kelly_fraction_override", "absolute maximum is 0.50");
        }
        if override_value.0 > kelly_override_ceiling(&mode)
            && !kelly_fraction_above_default_human_approved
        {
            return cross(
                "kelly_fraction_override exceeds the mode ceiling without human approval",
            );
        }
    }

    let per_trade_cap = parse_per_trade_cap(raw(&map, "per_trade_cap")?).ok_or_else(|| {
        ConfigSnapshotError::InvalidValue {
            key: "per_trade_cap".to_owned(),
            reason: "must be mode_default, unlimited, or bps:N with N in 1..=10000".to_owned(),
        }
    })?;

    let sizing_dollar_usd = parse_decimal(&map, "sizing_dollar_usd")?;
    if sizing_dollar_usd < Decimal::ZERO {
        return invalid("sizing_dollar_usd", "must not be negative");
    }
    let sizing_contracts = parse(&map, "sizing_contracts")?;
    let sizing_mode = match raw(&map, "sizing_mode")?.trim().to_lowercase().as_str() {
        "kelly" => SizingMode::Kelly,
        "dollar" if sizing_dollar_usd > Decimal::ZERO => SizingMode::Dollar {
            usd: sizing_dollar_usd,
        },
        "dollar" => return cross("sizing_mode=dollar requires sizing_dollar_usd > 0"),
        "contract" if sizing_contracts > 0 => SizingMode::Contract {
            contracts: sizing_contracts,
        },
        "contract" => return cross("sizing_mode=contract requires sizing_contracts > 0"),
        _ => return invalid("sizing_mode", "must be kelly, dollar, or contract"),
    };

    Ok(RuntimeConfig {
        era,
        active_watchlist_size,
        mode,
        max_fill_price,
        min_fill_price,
        min_resolution_horizon_secs,
        max_resolution_horizon_secs,
        price_impact_cap_bps,
        flip_human_approved,
        kelly_fraction_above_default_human_approved,
        kelly_fraction_override,
        per_trade_cap,
        slippage_rate,
        sizing_mode,
        sizing_dollar_usd,
        sizing_contracts,
        legacy_compatibility,
    })
}

/// Production boot requires a complete valid Supabase snapshot; `ServiceConfig` supplies only the
/// guarded current-mode baseline for transition validation.
pub fn load_initial_runtime_config(
    rows: &[ConfigRow],
    cfg: &ServiceConfig,
    clob_creds_present: bool,
    era: ConfigEra,
) -> Result<RuntimeConfig, ConfigSnapshotError> {
    parse_config(
        rows,
        &RuntimeConfig::from_service_config(cfg),
        clob_creds_present,
        era,
    )
}

#[derive(Clone)]
pub struct LiveRuntimeConfig {
    inner: Arc<ArcSwap<RuntimeConfig>>,
}

impl LiveRuntimeConfig {
    pub fn new(initial: RuntimeConfig) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
        }
    }

    pub fn snapshot(&self) -> Arc<RuntimeConfig> {
        self.inner.load_full()
    }

    pub fn store(&self, config: RuntimeConfig) {
        self.inner.store(Arc::new(config));
    }
}

pub fn validate_mode_transition(
    proposed: &str,
    _clob_creds_present: bool,
    current: &str,
) -> Result<String, String> {
    match parse_execution_mode(proposed) {
        None => Err(format!("unknown mode '{proposed}'")),
        Some(mode) if is_live_mode(mode) => Err(format!(
            "ordinary mode '{proposed}' is retired; staying '{current}'"
        )),
        Some(mode) => Ok(canonical_mode_string(mode)),
    }
}

pub fn parse_execution_mode(raw: &str) -> Option<ExecutionMode> {
    match raw.trim().to_lowercase().replace('-', "_").as_str() {
        "shadow" => Some(ExecutionMode::Shadow),
        "paper" => Some(ExecutionMode::Paper),
        "live_tiny" | "livetiny" => Some(ExecutionMode::LiveTiny),
        "promoted" => Some(ExecutionMode::Promoted),
        _ => None,
    }
}

fn is_live_mode(mode: ExecutionMode) -> bool {
    matches!(mode, ExecutionMode::LiveTiny | ExecutionMode::Promoted)
}

fn canonical_mode_string(mode: ExecutionMode) -> String {
    match mode {
        ExecutionMode::Shadow => "shadow",
        ExecutionMode::Paper => "paper",
        ExecutionMode::LiveTiny => "live_tiny",
        ExecutionMode::Promoted => "promoted",
    }
    .to_owned()
}

fn expected_value_type(key: &str) -> &'static str {
    match key {
        "active_watchlist_size"
        | "min_resolution_horizon_secs"
        | "max_resolution_horizon_secs"
        | "price_impact_cap_bps"
        | "sizing_contracts" => "integer",
        "flip_human_approved" | "kelly_fraction_above_default_human_approved" => "bool",
        "max_fill_price"
        | "min_fill_price"
        | "kelly_fraction_override"
        | "slippage_rate"
        | "sizing_dollar_usd" => "decimal",
        "polymarket_fee_rate" => "decimal",
        "mode" | "fill_mode" | "per_trade_cap" | "sizing_mode" | RISK_HALT_RELEASE_HASH_KEY => {
            "text"
        }
        _ => "",
    }
}

fn raw<'a>(map: &'a BTreeMap<&str, &ConfigRow>, key: &str) -> Result<&'a str, ConfigSnapshotError> {
    map.get(key)
        .map(|row| row.value.trim())
        .ok_or_else(|| ConfigSnapshotError::MissingKey {
            key: key.to_owned(),
        })
}

fn parse<T: FromStr>(
    map: &BTreeMap<&str, &ConfigRow>,
    key: &str,
) -> Result<T, ConfigSnapshotError> {
    raw(map, key)?
        .parse::<T>()
        .map_err(|_| ConfigSnapshotError::InvalidValue {
            key: key.to_owned(),
            reason: "unparseable value".to_owned(),
        })
}

fn parse_decimal(
    map: &BTreeMap<&str, &ConfigRow>,
    key: &str,
) -> Result<Decimal, ConfigSnapshotError> {
    Decimal::from_str(raw(map, key)?).map_err(|_| ConfigSnapshotError::InvalidValue {
        key: key.to_owned(),
        reason: "unparseable decimal".to_owned(),
    })
}

fn validate_fraction_decimal(key: &str, value: Decimal) -> Result<(), ConfigSnapshotError> {
    if (Decimal::ZERO..=Decimal::ONE).contains(&value) {
        Ok(())
    } else {
        invalid(key, "must be in 0..=1")
    }
}

fn invalid<T>(key: &str, reason: &str) -> Result<T, ConfigSnapshotError> {
    Err(ConfigSnapshotError::InvalidValue {
        key: key.to_owned(),
        reason: reason.to_owned(),
    })
}

fn cross<T>(reason: &str) -> Result<T, ConfigSnapshotError> {
    Err(ConfigSnapshotError::CrossField {
        reason: reason.to_owned(),
    })
}

fn parse_per_trade_cap(raw: &str) -> Option<PerTradeCap> {
    let value = raw.trim().to_lowercase();
    match value.as_str() {
        "mode_default" | "modedefault" | "default" => Some(PerTradeCap::ModeDefault),
        "unlimited" => Some(PerTradeCap::Unlimited),
        other => {
            let bps = other.strip_prefix("bps:").unwrap_or(other).parse().ok()?;
            (1..=10_000).contains(&bps).then_some(PerTradeCap::Bps(bps))
        }
    }
}

fn kelly_override_ceiling(mode: &str) -> Decimal {
    match parse_execution_mode(mode) {
        Some(ExecutionMode::Paper | ExecutionMode::Shadow) => Decimal::new(10, 2),
        Some(ExecutionMode::LiveTiny | ExecutionMode::Promoted) => Decimal::new(25, 2),
        None => Decimal::ZERO,
    }
}

fn decimal_text(value: Decimal) -> String {
    value.normalize().to_string()
}

fn canonical_per_trade_cap(value: PerTradeCap) -> String {
    match value {
        PerTradeCap::ModeDefault => "mode_default".to_owned(),
        PerTradeCap::Unlimited => "unlimited".to_owned(),
        PerTradeCap::Bps(bps) => format!("bps:{bps}"),
    }
}

fn canonical_sizing_mode(value: SizingMode) -> &'static str {
    match value {
        SizingMode::Kelly => "kelly",
        SizingMode::Dollar { .. } => "dollar",
        SizingMode::Contract { .. } => "contract",
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn complete_rows() -> Vec<ConfigRow> {
        [
            ("active_watchlist_size", "100"),
            ("mode", "paper"),
            ("max_fill_price", "0.85"),
            ("min_fill_price", "0.15"),
            ("min_resolution_horizon_secs", "60"),
            ("max_resolution_horizon_secs", "172800"),
            ("price_impact_cap_bps", "100"),
            ("flip_human_approved", "false"),
            ("kelly_fraction_above_default_human_approved", "false"),
            ("per_trade_cap", "unlimited"),
            ("slippage_rate", "0.01"),
            ("sizing_mode", "dollar"),
            ("sizing_dollar_usd", "25"),
            ("sizing_contracts", "1"),
        ]
        .into_iter()
        .map(|(key, value)| ConfigRow {
            key: key.to_owned(),
            value: value.to_owned(),
            value_type: expected_value_type(key).to_owned(),
        })
        .collect()
    }

    fn baseline() -> RuntimeConfig {
        RuntimeConfig::from_service_config(&ServiceConfig::default())
    }

    fn set(rows: &mut [ConfigRow], key: &str, value: &str) {
        rows.iter_mut()
            .find(|row| row.key == key)
            .expect("complete row")
            .value = value.to_owned();
    }

    fn parse_rows(rows: &[ConfigRow]) -> Result<RuntimeConfig, ConfigSnapshotError> {
        parse_config(rows, &baseline(), false, ConfigEra::Financial15)
    }

    fn legacy_rows() -> Vec<ConfigRow> {
        let mut rows = complete_rows();
        rows.push(ConfigRow {
            key: "fill_mode".to_owned(),
            value: "clob_best_ask".to_owned(),
            value_type: "text".to_owned(),
        });
        rows.push(ConfigRow {
            key: "polymarket_fee_rate".to_owned(),
            value: "0.04".to_owned(),
            value_type: "decimal".to_owned(),
        });
        rows
    }

    fn schema_seed_rows() -> Vec<ConfigRow> {
        let sql = include_str!("../../../scripts/supabase_schema.sql");
        let mut rows = Vec::new();
        let mut in_seed = false;
        for line in sql.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("insert into service_config") {
                in_seed = true;
                continue;
            }
            if in_seed && trimmed.starts_with("on conflict") {
                break;
            }
            if in_seed && trimmed.starts_with("('") {
                let parts: Vec<_> = trimmed.split('\'').collect();
                rows.push(ConfigRow {
                    key: parts[1].to_owned(),
                    value: parts[3].to_owned(),
                    value_type: parts[5].to_owned(),
                });
            }
        }
        rows
    }

    fn quoted_keys(sql: &str) -> HashSet<&str> {
        sql.split('\'')
            .enumerate()
            .filter_map(|(index, value)| (index % 2 == 1).then_some(value))
            .filter(|value| {
                HOT_CONFIG_KEYS.contains(value)
                    || LEGACY_HOT_CONFIG_KEYS.contains(value)
                    || REMOVED_CONFIG_KEYS.contains(value)
                    || *value == "risk_halt_release_hash"
            })
            .collect()
    }

    #[test]
    fn exact_allowlist_and_removed_set() {
        assert_eq!(HOT_CONFIG_KEYS.len(), 15);
        assert_eq!(LEGACY_HOT_CONFIG_KEYS.len(), 17);
        assert_eq!(REMOVED_CONFIG_KEYS.len(), 22);
        assert_eq!(
            HOT_CONFIG_KEYS.into_iter().collect::<HashSet<_>>().len(),
            HOT_CONFIG_KEYS.len()
        );
        assert_eq!(
            REMOVED_CONFIG_KEYS
                .into_iter()
                .collect::<HashSet<_>>()
                .len(),
            REMOVED_CONFIG_KEYS.len()
        );
        assert!(
            HOT_CONFIG_KEYS
                .into_iter()
                .all(|key| !REMOVED_CONFIG_KEYS.contains(&key))
        );
    }

    #[test]
    fn guarded_migration_checks_legacy_preflight_then_deletes_only_two_rows() {
        let sql = include_str!("../../../scripts/migrate_service_config_545.sql");
        assert!(sql.contains("\\set ON_ERROR_STOP on"));
        let guard = sql
            .split("delete from service_config")
            .next()
            .expect("migration unknown-key guard");
        let expected_guard: HashSet<_> = LEGACY_HOT_CONFIG_KEYS
            .into_iter()
            .chain(["risk_halt_release_hash"])
            .collect();
        assert_eq!(quoted_keys(guard), expected_guard);

        let delete = sql
            .split("delete from service_config")
            .nth(1)
            .expect("migration delete");
        assert_eq!(
            quoted_keys(delete),
            ["fill_mode", "polymarket_fee_rate"].into_iter().collect()
        );
    }

    #[test]
    fn complete_snapshot_parses_and_optional_override_may_be_absent() {
        let parsed = parse_rows(&complete_rows()).unwrap();
        assert_eq!(parsed.active_watchlist_size, 100);
        assert_eq!(parsed.price_impact_cap_bps, 100);
        assert_eq!(parsed.per_trade_cap, PerTradeCap::Unlimited);
        assert_eq!(parsed.sizing_mode, SizingMode::Dollar { usd: dec!(25) });
        assert_eq!(parsed.kelly_fraction_override, None);
        assert_eq!(parsed.era, ConfigEra::Financial15);
    }

    #[test]
    fn era_contract_requires_legacy_rows_before_start_and_rejects_them_after_start() {
        let legacy = legacy_rows();
        let parsed = parse_config(&legacy, &baseline(), false, ConfigEra::Legacy17).unwrap();
        assert_eq!(parsed.era, ConfigEra::Legacy17);
        assert!(parse_config(&legacy, &baseline(), false, ConfigEra::Financial15).is_err());
        assert!(parse_config(&complete_rows(), &baseline(), false, ConfigEra::Legacy17).is_err());
    }

    #[test]
    fn financial_era_permits_incident_release_row_without_changing_economic_hash() {
        let rows = complete_rows();
        let baseline = parse_rows(&rows).unwrap();
        let mut with_release = rows;
        with_release.push(ConfigRow {
            key: RISK_HALT_RELEASE_HASH_KEY.to_owned(),
            value: "release-proof".to_owned(),
            value_type: "text".to_owned(),
        });
        let parsed = parse_rows(&with_release).unwrap();
        assert_eq!(parsed.canonical_hash(), baseline.canonical_hash());
        assert!(parse_config(&with_release, &baseline, false, ConfigEra::Legacy17).is_err());
    }

    #[test]
    fn new_install_schema_seed_is_a_valid_compiled_snapshot() {
        let rows = schema_seed_rows();
        let parsed = load_initial_runtime_config(
            &rows,
            &ServiceConfig::default(),
            false,
            ConfigEra::Financial15,
        )
        .unwrap();
        assert_eq!(rows.len(), 14);
        assert_eq!(parsed.active_watchlist_size, 100);
        assert_eq!(parsed.price_impact_cap_bps, 100);
        assert_eq!(parsed.sizing_mode, SizingMode::Dollar { usd: dec!(25) });
        assert_eq!(parsed.per_trade_cap, PerTradeCap::ModeDefault);
        assert_eq!(parsed.kelly_fraction_override, None);
    }

    #[test]
    fn boot_rejects_missing_or_invalid_mandatory_cap() {
        assert!(matches!(
            load_initial_runtime_config(
                &[],
                &ServiceConfig::default(),
                false,
                ConfigEra::Financial15
            ),
            Err(ConfigSnapshotError::MissingKey { key }) if key == "active_watchlist_size"
        ));
        let mut missing = complete_rows();
        missing.retain(|row| row.key != "price_impact_cap_bps");
        assert!(matches!(
            load_initial_runtime_config(
                &missing,
                &ServiceConfig::default(),
                false,
                ConfigEra::Financial15
            ),
            Err(ConfigSnapshotError::MissingKey { key }) if key == "price_impact_cap_bps"
        ));
        let mut invalid_cap = complete_rows();
        set(&mut invalid_cap, "price_impact_cap_bps", "0");
        assert!(
            load_initial_runtime_config(
                &invalid_cap,
                &ServiceConfig::default(),
                false,
                ConfigEra::Financial15
            )
            .is_err()
        );
    }

    #[test]
    fn every_snapshot_shape_violation_rejects_whole_proposal() {
        let rows = complete_rows();
        assert!(matches!(
            parse_rows(&rows[1..]),
            Err(ConfigSnapshotError::MissingKey { .. })
        ));

        let mut duplicate = rows.clone();
        duplicate.push(rows[0].clone());
        assert!(matches!(
            parse_rows(&duplicate),
            Err(ConfigSnapshotError::DuplicateKey { .. })
        ));

        let mut unknown = rows.clone();
        unknown.push(ConfigRow {
            key: "surprise".to_owned(),
            value: "1".to_owned(),
            value_type: "integer".to_owned(),
        });
        assert!(matches!(
            parse_rows(&unknown),
            Err(ConfigSnapshotError::UnknownKey { .. })
        ));

        let mut wrong_type = rows.clone();
        wrong_type[0].value_type = "text".to_owned();
        assert!(matches!(
            parse_rows(&wrong_type),
            Err(ConfigSnapshotError::WrongValueType { .. })
        ));

        let mut invalid_value = rows;
        set(&mut invalid_value, "price_impact_cap_bps", "0");
        assert!(matches!(
            parse_rows(&invalid_value),
            Err(ConfigSnapshotError::InvalidValue { .. })
        ));
    }

    #[test]
    fn ordered_bounds_and_zero_disable_contracts() {
        let mut reversed_fill = complete_rows();
        set(&mut reversed_fill, "min_fill_price", "0.90");
        set(&mut reversed_fill, "max_fill_price", "0.80");
        assert!(matches!(
            parse_rows(&reversed_fill),
            Err(ConfigSnapshotError::CrossField { .. })
        ));

        let mut reversed_horizon = complete_rows();
        set(&mut reversed_horizon, "min_resolution_horizon_secs", "90");
        set(&mut reversed_horizon, "max_resolution_horizon_secs", "80");
        assert!(matches!(
            parse_rows(&reversed_horizon),
            Err(ConfigSnapshotError::CrossField { .. })
        ));

        for key in [
            "min_fill_price",
            "max_fill_price",
            "min_resolution_horizon_secs",
            "max_resolution_horizon_secs",
        ] {
            let mut disabled = complete_rows();
            set(&mut disabled, key, "0");
            assert!(parse_rows(&disabled).is_ok(), "zero disables {key}");
        }
    }

    #[test]
    fn every_hot_field_rule_rejects_its_invalid_value() {
        for (key, value) in [
            ("active_watchlist_size", "201"),
            ("mode", "live_tiny"),
            ("max_fill_price", "1.01"),
            ("min_fill_price", "-0.01"),
            ("min_resolution_horizon_secs", "-1"),
            ("max_resolution_horizon_secs", "never"),
            ("price_impact_cap_bps", "10001"),
            ("flip_human_approved", "yes"),
            ("kelly_fraction_above_default_human_approved", "yes"),
            ("per_trade_cap", "bps:0"),
            ("slippage_rate", "-0.01"),
            ("sizing_mode", "shares"),
            ("sizing_dollar_usd", "-1"),
            ("sizing_contracts", "-1"),
        ] {
            let mut rows = complete_rows();
            set(&mut rows, key, value);
            assert!(parse_rows(&rows).is_err(), "{key}={value} must reject");
        }

        let mut bad_override = complete_rows();
        bad_override.push(ConfigRow {
            key: "kelly_fraction_override".to_owned(),
            value: "garbage".to_owned(),
            value_type: "decimal".to_owned(),
        });
        assert!(parse_rows(&bad_override).is_err());
    }

    #[test]
    fn sizing_dependencies_and_approval_ceiling_are_atomic() {
        assert_eq!(kelly_override_ceiling("paper"), dec!(0.10));
        assert_eq!(kelly_override_ceiling("shadow"), dec!(0.10));
        assert_eq!(kelly_override_ceiling("live_tiny"), dec!(0.25));
        assert_eq!(kelly_override_ceiling("promoted"), dec!(0.25));

        let mut dollar_zero = complete_rows();
        set(&mut dollar_zero, "sizing_dollar_usd", "0");
        assert!(matches!(
            parse_rows(&dollar_zero),
            Err(ConfigSnapshotError::CrossField { .. })
        ));

        let mut contract_zero = complete_rows();
        set(&mut contract_zero, "sizing_mode", "contract");
        set(&mut contract_zero, "sizing_contracts", "0");
        assert!(matches!(
            parse_rows(&contract_zero),
            Err(ConfigSnapshotError::CrossField { .. })
        ));

        let mut unapproved = complete_rows();
        unapproved.push(ConfigRow {
            key: "kelly_fraction_override".to_owned(),
            value: "0.20".to_owned(),
            value_type: "decimal".to_owned(),
        });
        assert!(matches!(
            parse_rows(&unapproved),
            Err(ConfigSnapshotError::CrossField { .. })
        ));
        set(
            &mut unapproved,
            "kelly_fraction_above_default_human_approved",
            "true",
        );
        assert!(parse_rows(&unapproved).is_ok());
        unapproved
            .iter_mut()
            .find(|row| row.key == "kelly_fraction_override")
            .expect("override")
            .value = "0.51".to_owned();
        assert!(matches!(
            parse_rows(&unapproved),
            Err(ConfigSnapshotError::InvalidValue { .. })
        ));
    }

    #[test]
    fn canonical_hash_normalizes_decimal_spelling_and_capacity_is_applied_value() {
        let one = parse_rows(&complete_rows()).unwrap();
        let mut alternate = complete_rows();
        set(&mut alternate, "max_fill_price", "0.8500");
        let two = parse_rows(&alternate).unwrap();
        assert_eq!(one.canonical_hash(), two.canonical_hash());

        let mut pending = two;
        pending.active_watchlist_size = 150;
        assert_ne!(one.canonical_hash(), pending.canonical_hash());
    }

    #[test]
    fn status_preserves_applied_hash_and_reports_rejected_raw_rows_separately() {
        let applied = parse_rows(&complete_rows()).unwrap();
        let status = RuntimeConfigStatus::new(&applied);
        let before = status.snapshot().applied_hash.clone();
        let mut rejected = complete_rows();
        set(&mut rejected, "price_impact_cap_bps", "0");
        let error = parse_rows(&rejected).unwrap_err();
        status.record_rejected(&rejected, error.clone());
        let snapshot = status.snapshot();
        assert_eq!(snapshot.applied_hash, before);
        assert_eq!(snapshot.rejected.as_ref().unwrap().error, error);
        assert_eq!(snapshot.rejected.as_ref().unwrap().rows, rejected);
    }
}
