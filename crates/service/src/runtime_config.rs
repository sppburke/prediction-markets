//! Supabase-authoritative runtime configuration (issue #398 WS1).
//!
//! The `service_config` KV table is authoritative for every non-secret, runtime-mutable knob.
//! pe-service polls it into a [`LiveRuntimeConfig`] `ArcSwap` snapshot (the poller lands in a
//! later PR) and rebuilds the strategy config per event. Secrets, paths, bind, channel caps,
//! `supabase_sink_enabled`, and `supabase_authoritative` stay env/boot-frozen — this module
//! never carries them.
//!
//! Precedence is **KV > env > compiled**: [`load_initial_runtime_config`] seeds a
//! [`RuntimeConfig`] from the boot [`ServiceConfig`] (env over compiled, already merged by
//! figment) and overlays valid KV rows. [`parse_config`] keeps the **last-known-good** value
//! per field when a KV cell is absent or unparseable, so a Supabase outage or a single bad
//! admin edit never reverts a field to its serde default.
//!
//! ## Born carrying `flat_usd_per_trade` (WS1 ↔ WS2 handoff)
//! `WinnerFollowConfig.sizing_mode` does not exist until WS2; WS1's `RuntimeConfig` carries the
//! existing `flat_usd_per_trade`, and WS2 migrates `RuntimeConfig`/`parse_config`/
//! [`RuntimeConfig::winner_follow_config`] to the `sizing_mode` keys in lockstep.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use pe_core_types::KellyFraction;
use pe_strategy_winner_follow::{ExecutionMode, PerTradeCap, WinnerFollowConfig};
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::warn;

use crate::config::ServiceConfig;

/// One `service_config` row as returned by PostgREST (`select=key,value,value_type`).
#[derive(Debug, Clone, Deserialize)]
pub struct ConfigRow {
    pub key: String,
    pub value: String,
    /// Drives the admin panel's typed input (`bool` | `integer` | `decimal` | `text`); the
    /// typed parse here keys off the field's static type, not this column.
    #[serde(default)]
    pub value_type: String,
}

/// The full non-secret, runtime-mutable configuration snapshot.
///
/// Service-level money knobs (`bankroll_usd`, `max_fill_price`, `demotion_cb_alpha`) keep the
/// `String` shape of [`ServiceConfig`] so existing consumers parse them unchanged; the strategy
/// knobs keep their [`WinnerFollowConfig`] types so [`RuntimeConfig::winner_follow_config`] can
/// reconstruct a complete config with no lossy round-trip.
#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeConfig {
    // ── Service runtime knobs ────────────────────────────────────────────────
    /// Trading mode (`paper` | `shadow` | `live_tiny` | `promoted`). Transitions to a live mode
    /// are guarded by [`validate_mode_transition`].
    pub mode: String,
    /// Configured starting-capital **baseline** (dashboard denominator); never writes the
    /// running bankroll (no re-credit invariant, true by construction — there is no writer).
    pub bankroll_usd: String,
    pub max_fill_price: String,
    pub demotion_cb_alpha: String,
    pub min_resolution_horizon_secs: u64,
    pub max_resolution_horizon_secs: u64,
    pub entry_gate_fail_closed: bool,
    pub trade_poll_interval_secs: u64,
    pub position_reseed_interval_secs: u64,
    pub position_page_limit: u32,
    pub position_size_threshold: u32,
    pub paper_fill_haircut_bps: u32,
    pub paper_fill_slippage_bps: u32,
    pub status_interval_secs: u64,
    pub log_retention_days: usize,
    pub gamma_resolution_poll_interval_secs: u64,
    pub supabase_refresh_interval_secs: u64,
    pub supabase_sink_reconcile_interval_secs: u64,
    pub maintenance_interval_secs: u64,
    pub inactivity_threshold_secs: u64,
    pub inactivity_hard_cap_secs: u64,
    pub bench_overfetch: usize,
    pub demotion_min_trades: usize,
    /// Price-impact gate cap in basis points; `0` disables it. Carried here in WS1 (no consumer
    /// yet); the WS2 gate reads it. Not seeded yet, so it falls through to this default.
    pub price_impact_cap_bps: i32,

    // ── Strategy (WinnerFollowConfig-derived) ────────────────────────────────
    pub flip_human_approved: bool,
    pub kelly_fraction_above_default_human_approved: bool,
    pub polymarket_fee_rate: Decimal,
    pub kelly_fraction_override: Option<KellyFraction>,
    pub per_trade_cap: PerTradeCap,
    pub slippage_rate: Decimal,
    pub flat_usd_per_trade: Option<Decimal>,
}

impl RuntimeConfig {
    /// Build the boot snapshot from the merged [`ServiceConfig`] (env over compiled). Strategy
    /// fields are copied from `cfg.strategy`; `price_impact_cap_bps` has no `ServiceConfig`
    /// field yet, so it defaults to `0` (disabled).
    pub fn from_service_config(cfg: &ServiceConfig) -> Self {
        Self {
            mode: cfg.mode.clone(),
            bankroll_usd: cfg.bankroll_usd.clone(),
            max_fill_price: cfg.max_fill_price.clone(),
            demotion_cb_alpha: cfg.demotion_cb_alpha.clone(),
            min_resolution_horizon_secs: cfg.min_resolution_horizon_secs,
            max_resolution_horizon_secs: cfg.max_resolution_horizon_secs,
            entry_gate_fail_closed: cfg.entry_gate_fail_closed,
            trade_poll_interval_secs: cfg.trade_poll_interval_secs,
            position_reseed_interval_secs: cfg.position_reseed_interval_secs,
            position_page_limit: cfg.position_page_limit,
            position_size_threshold: cfg.position_size_threshold,
            paper_fill_haircut_bps: cfg.paper_fill_haircut_bps,
            paper_fill_slippage_bps: cfg.paper_fill_slippage_bps,
            status_interval_secs: cfg.status_interval_secs,
            log_retention_days: cfg.log_retention_days,
            gamma_resolution_poll_interval_secs: cfg.gamma_resolution_poll_interval_secs,
            supabase_refresh_interval_secs: cfg.supabase_refresh_interval_secs,
            supabase_sink_reconcile_interval_secs: cfg.supabase_sink_reconcile_interval_secs,
            maintenance_interval_secs: cfg.maintenance_interval_secs,
            inactivity_threshold_secs: cfg.inactivity_threshold_secs,
            inactivity_hard_cap_secs: cfg.inactivity_hard_cap_secs,
            bench_overfetch: cfg.bench_overfetch,
            demotion_min_trades: cfg.demotion_min_trades,
            price_impact_cap_bps: 0,
            flip_human_approved: cfg.strategy.flip_human_approved,
            kelly_fraction_above_default_human_approved: cfg
                .strategy
                .kelly_fraction_above_default_human_approved,
            polymarket_fee_rate: cfg.strategy.polymarket_fee_rate,
            kelly_fraction_override: cfg.strategy.kelly_fraction_override,
            per_trade_cap: cfg.strategy.per_trade_cap,
            slippage_rate: cfg.strategy.slippage_rate,
            flat_usd_per_trade: cfg.strategy.flat_usd_per_trade,
        }
    }

    /// Reconstruct a **complete** [`WinnerFollowConfig`] from the snapshot. The explicit struct
    /// literal makes an omitted field a compile error; the reconstruction-fidelity test guards
    /// against a mis-mapping (#398 round-5 Blocking).
    pub fn winner_follow_config(&self) -> WinnerFollowConfig {
        WinnerFollowConfig {
            flip_human_approved: self.flip_human_approved,
            kelly_fraction_above_default_human_approved: self
                .kelly_fraction_above_default_human_approved,
            polymarket_fee_rate: self.polymarket_fee_rate,
            kelly_fraction_override: self.kelly_fraction_override,
            per_trade_cap: self.per_trade_cap,
            slippage_rate: self.slippage_rate,
            flat_usd_per_trade: self.flat_usd_per_trade,
        }
    }
}

/// Parse `proposed` against a guard-hard mode-transition policy.
///
/// Returns `Ok(canonical_mode)` to apply, or `Err(reason)` to refuse (the caller keeps
/// `current` and logs the reason). A live mode (`live_tiny` | `promoted`) is refused unless CLOB
/// credentials are present; an unknown string is refused.
pub fn validate_mode_transition(
    proposed: &str,
    clob_creds_present: bool,
    current: &str,
) -> Result<String, String> {
    match parse_execution_mode(proposed) {
        None => Err(format!("unknown mode '{proposed}'")),
        Some(m) if is_live_mode(m) && !clob_creds_present => Err(format!(
            "mode '{proposed}' requires CLOB credentials (absent); staying '{current}'"
        )),
        Some(m) => Ok(canonical_mode_string(m)),
    }
}

/// Overlay valid `rows` onto `last_good`, keeping the last-known-good value per field on an
/// absent or unparseable cell. Re-enforces the kelly-override invariant the boot guard used to
/// own (step 8 removes that guard) and the mode-transition guard.
pub fn parse_config(
    rows: &[ConfigRow],
    last_good: &RuntimeConfig,
    clob_creds_present: bool,
) -> RuntimeConfig {
    let map: HashMap<&str, &str> = rows
        .iter()
        .map(|r| (r.key.as_str(), r.value.trim()))
        .collect();
    let mut out = last_good.clone();

    // Plain typed fields (FromStr): bad parse -> keep last-known-good.
    apply_parsed(
        &map,
        "min_resolution_horizon_secs",
        &mut out.min_resolution_horizon_secs,
    );
    apply_parsed(
        &map,
        "max_resolution_horizon_secs",
        &mut out.max_resolution_horizon_secs,
    );
    apply_parsed(
        &map,
        "entry_gate_fail_closed",
        &mut out.entry_gate_fail_closed,
    );
    apply_parsed(
        &map,
        "trade_poll_interval_secs",
        &mut out.trade_poll_interval_secs,
    );
    apply_parsed(
        &map,
        "position_reseed_interval_secs",
        &mut out.position_reseed_interval_secs,
    );
    apply_parsed(&map, "position_page_limit", &mut out.position_page_limit);
    apply_parsed(
        &map,
        "position_size_threshold",
        &mut out.position_size_threshold,
    );
    apply_parsed(
        &map,
        "paper_fill_haircut_bps",
        &mut out.paper_fill_haircut_bps,
    );
    apply_parsed(
        &map,
        "paper_fill_slippage_bps",
        &mut out.paper_fill_slippage_bps,
    );
    apply_parsed(&map, "status_interval_secs", &mut out.status_interval_secs);
    apply_parsed(&map, "log_retention_days", &mut out.log_retention_days);
    apply_parsed(
        &map,
        "gamma_resolution_poll_interval_secs",
        &mut out.gamma_resolution_poll_interval_secs,
    );
    apply_parsed(
        &map,
        "supabase_refresh_interval_secs",
        &mut out.supabase_refresh_interval_secs,
    );
    apply_parsed(
        &map,
        "supabase_sink_reconcile_interval_secs",
        &mut out.supabase_sink_reconcile_interval_secs,
    );
    apply_parsed(
        &map,
        "maintenance_interval_secs",
        &mut out.maintenance_interval_secs,
    );
    apply_parsed(
        &map,
        "inactivity_threshold_secs",
        &mut out.inactivity_threshold_secs,
    );
    apply_parsed(
        &map,
        "inactivity_hard_cap_secs",
        &mut out.inactivity_hard_cap_secs,
    );
    apply_parsed(&map, "bench_overfetch", &mut out.bench_overfetch);
    apply_parsed(&map, "demotion_min_trades", &mut out.demotion_min_trades);
    apply_parsed(&map, "price_impact_cap_bps", &mut out.price_impact_cap_bps);
    apply_parsed(&map, "polymarket_fee_rate", &mut out.polymarket_fee_rate);
    apply_parsed(&map, "slippage_rate", &mut out.slippage_rate);
    apply_parsed(&map, "flip_human_approved", &mut out.flip_human_approved);
    apply_parsed(
        &map,
        "kelly_fraction_above_default_human_approved",
        &mut out.kelly_fraction_above_default_human_approved,
    );

    // Decimal-valued strings: validate as Decimal, store the string shape consumers expect.
    apply_decimal_string(&map, "bankroll_usd", &mut out.bankroll_usd);
    apply_decimal_string(&map, "max_fill_price", &mut out.max_fill_price);
    apply_decimal_string(&map, "demotion_cb_alpha", &mut out.demotion_cb_alpha);

    // Optional decimal: empty/none/null -> None.
    apply_optional_decimal(&map, "flat_usd_per_trade", &mut out.flat_usd_per_trade);

    // Per-trade cap enum.
    if let Some(raw) = map.get("per_trade_cap") {
        match parse_per_trade_cap(raw) {
            Some(c) => out.per_trade_cap = c,
            None => warn!(
                key = "per_trade_cap",
                value = %raw,
                "service_config: unparseable per_trade_cap; keeping last-known-good"
            ),
        }
    }

    // Mode transition: guard-hard.
    if let Some(raw) = map.get("mode") {
        match validate_mode_transition(raw, clob_creds_present, &last_good.mode) {
            Ok(accepted) => out.mode = accepted,
            Err(reason) => warn!(%reason, "service_config: mode transition refused"),
        }
    }

    // Kelly-override invariant (relocated from the boot guard): an override above the proposed
    // mode's ceiling needs the approval flag, else reject the field and keep last-known-good.
    let mut proposed_override = last_good.kelly_fraction_override;
    if let Some(raw) = map.get("kelly_fraction_override") {
        let t = raw.trim();
        if t.is_empty() || t.eq_ignore_ascii_case("none") || t.eq_ignore_ascii_case("null") {
            proposed_override = None;
        } else {
            match Decimal::from_str(t)
                .ok()
                .and_then(|d| KellyFraction::new(d).ok())
            {
                Some(kf) => proposed_override = Some(kf),
                None => warn!(
                    key = "kelly_fraction_override",
                    value = %raw,
                    "service_config: kelly_fraction_override is not a fraction in [0,1]; keeping last-known-good"
                ),
            }
        }
    }
    out.kelly_fraction_override = match proposed_override {
        Some(kf)
            if kf.0 > kelly_override_ceiling(&out.mode)
                && !out.kelly_fraction_above_default_human_approved =>
        {
            warn!(
                override_value = %kf.0,
                mode = %out.mode,
                "service_config: kelly_fraction_override exceeds the mode ceiling without \
                 kelly_fraction_above_default_human_approved; rejecting (last-known-good)"
            );
            last_good.kelly_fraction_override
        }
        other => other,
    };

    out
}

/// Build the boot snapshot then overlay KV rows: precedence KV > env > compiled.
pub fn load_initial_runtime_config(
    rows: &[ConfigRow],
    cfg: &ServiceConfig,
    clob_creds_present: bool,
) -> RuntimeConfig {
    let boot = RuntimeConfig::from_service_config(cfg);
    parse_config(rows, &boot, clob_creds_present)
}

/// `ArcSwap` holder for the live runtime config, mirroring `live_watchlist::LiveWatchlist`.
/// Cloning shares one cell, so every consumer observes the same swaps; the poller takes one
/// `snapshot()` per use and the single writer `store()`s a fresh config.
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

    /// Wait-free load of the current snapshot. Take exactly one per event so all reads in that
    /// event observe a single generation.
    pub fn snapshot(&self) -> Arc<RuntimeConfig> {
        self.inner.load_full()
    }

    /// Publish a new snapshot (single-writer; the poller serializes against the watchlist tick
    /// via the shared writer lock).
    pub fn store(&self, cfg: RuntimeConfig) {
        self.inner.store(Arc::new(cfg));
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn apply_parsed<T: FromStr>(map: &HashMap<&str, &str>, key: &str, slot: &mut T) {
    if let Some(raw) = map.get(key) {
        match raw.parse::<T>() {
            Ok(v) => *slot = v,
            Err(_) => warn!(
                key,
                value = %raw,
                "service_config: unparseable value; keeping last-known-good"
            ),
        }
    }
}

fn apply_decimal_string(map: &HashMap<&str, &str>, key: &str, slot: &mut String) {
    if let Some(raw) = map.get(key) {
        if Decimal::from_str(raw).is_ok() {
            *slot = (*raw).to_string();
        } else {
            warn!(
                key,
                value = %raw,
                "service_config: non-decimal value; keeping last-known-good"
            );
        }
    }
}

fn apply_optional_decimal(map: &HashMap<&str, &str>, key: &str, slot: &mut Option<Decimal>) {
    if let Some(raw) = map.get(key) {
        if raw.is_empty() || raw.eq_ignore_ascii_case("none") || raw.eq_ignore_ascii_case("null") {
            *slot = None;
        } else if let Ok(d) = Decimal::from_str(raw) {
            *slot = Some(d);
        } else {
            warn!(
                key,
                value = %raw,
                "service_config: non-decimal value; keeping last-known-good"
            );
        }
    }
}

/// Parse a `per_trade_cap` KV cell: `mode_default` | `unlimited` | `bps:N` | a bare integer `N`.
fn parse_per_trade_cap(raw: &str) -> Option<PerTradeCap> {
    let t = raw.trim().to_lowercase();
    match t.as_str() {
        "mode_default" | "modedefault" | "default" => Some(PerTradeCap::ModeDefault),
        "unlimited" => Some(PerTradeCap::Unlimited),
        other => other
            .strip_prefix("bps:")
            .unwrap_or(other)
            .parse::<i32>()
            .ok()
            .map(PerTradeCap::Bps),
    }
}

/// The kelly-override approval ceiling per mode. Mirrors the boot guard at `main.rs` exactly
/// (Shadow/Paper/LiveTiny → 0.25, Promoted → 0.50) — these are approval ceilings, NOT the
/// strategy's per-mode defaults (0.10/0.10/0.25/0.25). An unknown mode is treated conservatively.
fn kelly_override_ceiling(mode: &str) -> Decimal {
    match parse_execution_mode(mode) {
        Some(ExecutionMode::Promoted) => Decimal::new(50, 2),
        _ => Decimal::new(25, 2),
    }
}

/// `service_config.mode` string → [`ExecutionMode`], mirroring `main.rs::parse_mode`.
fn parse_execution_mode(s: &str) -> Option<ExecutionMode> {
    match s.trim().to_lowercase().replace('-', "_").as_str() {
        "shadow" => Some(ExecutionMode::Shadow),
        "paper" => Some(ExecutionMode::Paper),
        "live_tiny" | "livetiny" => Some(ExecutionMode::LiveTiny),
        "promoted" => Some(ExecutionMode::Promoted),
        _ => None,
    }
}

fn is_live_mode(m: ExecutionMode) -> bool {
    matches!(m, ExecutionMode::LiveTiny | ExecutionMode::Promoted)
}

fn canonical_mode_string(m: ExecutionMode) -> String {
    match m {
        ExecutionMode::Shadow => "shadow",
        ExecutionMode::Paper => "paper",
        ExecutionMode::LiveTiny => "live_tiny",
        ExecutionMode::Promoted => "promoted",
    }
    .to_string()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn row(key: &str, value: &str, value_type: &str) -> ConfigRow {
        ConfigRow {
            key: key.to_string(),
            value: value.to_string(),
            value_type: value_type.to_string(),
        }
    }

    /// Parse the committed `service_config` seed (key, value) into rows, mirroring the schema's
    /// single-quote layout (values carry no apostrophes; value at split index 3, type at 5).
    fn seed_rows() -> Vec<ConfigRow> {
        let manifest = env!("CARGO_MANIFEST_DIR");
        let sql = std::fs::read_to_string(format!("{manifest}/../../scripts/supabase_schema.sql"))
            .unwrap();
        let mut rows = Vec::new();
        let mut in_block = false;
        for line in sql.lines() {
            let t = line.trim();
            if t.starts_with("insert into service_config") {
                in_block = true;
                continue;
            }
            if in_block {
                if t.starts_with("on conflict") {
                    break;
                }
                if t.starts_with("('") {
                    let parts: Vec<&str> = t.split('\'').collect();
                    if parts.len() >= 6 {
                        rows.push(row(parts[1], parts[3], parts[5]));
                    }
                }
            }
        }
        rows
    }

    #[test]
    fn winner_follow_config_round_trips_defaults() {
        // from_service_config -> winner_follow_config reproduces the boot strategy (defaults).
        let cfg = ServiceConfig::default();
        assert_eq!(
            RuntimeConfig::from_service_config(&cfg).winner_follow_config(),
            cfg.strategy
        );
    }

    #[test]
    fn winner_follow_config_round_trips_every_nondefault_field() {
        // #398 round-5 reconstruction fidelity: with EVERY strategy field non-default, the
        // rebuild must reproduce it field-for-field — a mis-mapped field would mismatch here.
        let strat = WinnerFollowConfig {
            flip_human_approved: true,
            kelly_fraction_above_default_human_approved: true,
            polymarket_fee_rate: dec!(0.07),
            kelly_fraction_override: Some(KellyFraction::new(dec!(0.40)).unwrap()),
            per_trade_cap: PerTradeCap::Bps(42),
            slippage_rate: dec!(0.03),
            flat_usd_per_trade: Some(dec!(99)),
        };
        let cfg = ServiceConfig {
            strategy: strat.clone(),
            ..Default::default()
        };
        assert_eq!(
            RuntimeConfig::from_service_config(&cfg).winner_follow_config(),
            strat
        );
    }

    #[test]
    fn seed_reconstructs_boot_strategy() {
        // The committed seed, parsed through load_initial, reconstructs the live boot strategy:
        // flat $25 (never Kelly), fee 0.04, slippage 0.01, approvals off; per_trade_cap and
        // kelly_fraction_override are not seeded, so they fall through to the compiled defaults.
        let rc = load_initial_runtime_config(&seed_rows(), &ServiceConfig::default(), false);
        let expected = WinnerFollowConfig {
            flip_human_approved: false,
            kelly_fraction_above_default_human_approved: false,
            polymarket_fee_rate: dec!(0.04),
            kelly_fraction_override: None,
            per_trade_cap: PerTradeCap::ModeDefault,
            slippage_rate: dec!(0.01),
            flat_usd_per_trade: Some(dec!(25)),
        };
        assert_eq!(rc.winner_follow_config(), expected);
        // Service-level knobs reconstruct too (spot-check the risk-engine gate inputs).
        assert_eq!(rc.mode, "paper");
        assert_eq!(rc.max_fill_price, "0.85");
        assert_eq!(rc.bankroll_usd, "10000");
        assert_eq!(rc.min_resolution_horizon_secs, 60);
    }

    #[test]
    fn empty_rows_keep_last_known_good() {
        // Supabase outage (no rows) -> the snapshot is unchanged, never a default revert.
        let boot = RuntimeConfig::from_service_config(&ServiceConfig::default());
        assert_eq!(parse_config(&[], &boot, false), boot);
    }

    #[test]
    fn unparseable_field_keeps_last_known_good() {
        let boot = RuntimeConfig::from_service_config(&ServiceConfig::default());
        let rows = vec![
            row("trade_poll_interval_secs", "not_a_number", "integer"),
            row("max_fill_price", "not_a_decimal", "decimal"),
            row("paper_fill_haircut_bps", "777", "integer"), // a valid one still applies
        ];
        let out = parse_config(&rows, &boot, false);
        assert_eq!(out.trade_poll_interval_secs, boot.trade_poll_interval_secs);
        assert_eq!(out.max_fill_price, boot.max_fill_price);
        assert_eq!(out.paper_fill_haircut_bps, 777);
    }

    #[test]
    fn kelly_override_rejected_without_approval_but_accepted_with() {
        let boot = RuntimeConfig::from_service_config(&ServiceConfig::default()); // mode=paper, ceiling 0.25
        // 0.40 > 0.25 ceiling, no approval -> rejected, keep last-known-good (None).
        let rejected = parse_config(
            &[row("kelly_fraction_override", "0.40", "decimal")],
            &boot,
            false,
        );
        assert_eq!(rejected.kelly_fraction_override, None);
        // Same override WITH the approval flag in the same poll -> accepted.
        let accepted = parse_config(
            &[
                row(
                    "kelly_fraction_above_default_human_approved",
                    "true",
                    "bool",
                ),
                row("kelly_fraction_override", "0.40", "decimal"),
            ],
            &boot,
            false,
        );
        assert_eq!(
            accepted.kelly_fraction_override,
            Some(KellyFraction::new(dec!(0.40)).unwrap())
        );
        // An override at/below the ceiling is accepted without the flag.
        let ok = parse_config(
            &[row("kelly_fraction_override", "0.20", "decimal")],
            &boot,
            false,
        );
        assert_eq!(
            ok.kelly_fraction_override,
            Some(KellyFraction::new(dec!(0.20)).unwrap())
        );
    }

    #[test]
    fn mode_transition_guard() {
        // paper -> live_tiny is refused without CLOB creds, accepted with.
        assert!(validate_mode_transition("live_tiny", false, "paper").is_err());
        assert_eq!(
            validate_mode_transition("live_tiny", true, "paper").unwrap(),
            "live_tiny"
        );
        // non-live transitions and canonicalization always succeed; unknown is refused.
        assert_eq!(
            validate_mode_transition("Shadow", false, "paper").unwrap(),
            "shadow"
        );
        assert!(validate_mode_transition("bogus", true, "paper").is_err());

        // parse_config applies the guard: a refused mode keeps the current mode.
        let boot = RuntimeConfig::from_service_config(&ServiceConfig::default());
        let refused = parse_config(&[row("mode", "live_tiny", "text")], &boot, false);
        assert_eq!(refused.mode, "paper");
        let accepted = parse_config(&[row("mode", "live_tiny", "text")], &boot, true);
        assert_eq!(accepted.mode, "live_tiny");
    }

    #[test]
    fn kv_overrides_boot_for_a_valid_edit() {
        // A valid admin edit wins over the boot/env value (KV > env).
        let boot = RuntimeConfig::from_service_config(&ServiceConfig::default());
        let out = parse_config(&[row("max_fill_price", "0.90", "decimal")], &boot, false);
        assert_eq!(out.max_fill_price, "0.90");
    }
}
