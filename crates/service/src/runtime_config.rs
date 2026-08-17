//! Supabase-authoritative runtime configuration (issue #398 WS1).
//!
//! The `service_config` KV table is authoritative for every non-secret, runtime-mutable knob.
//! pe-service polls it (`config_poller`) into a [`LiveRuntimeConfig`] `ArcSwap` snapshot and the
//! orchestrator rebuilds the strategy/mode/gate knobs per event. A watchlist-capacity edit is
//! committed to this snapshot only after its admission preparation and atomic membership swap
//! succeed. Secrets, paths, bind, channel caps, `supabase_sink_enabled`, and
//! `supabase_authoritative` stay env/boot-frozen — this module never carries them.
//!
//! Precedence is **KV > env > compiled**: [`load_initial_runtime_config`] seeds a
//! [`RuntimeConfig`] from the boot [`ServiceConfig`] (env over compiled, already merged by
//! figment) and overlays valid KV rows. [`parse_config`] keeps the **last-known-good** value
//! per field when a KV cell is absent or unparseable, so a Supabase outage or a single bad
//! admin edit never reverts a field to its serde default.
//!
//! ## Sizing mode (WS2)
//! `WinnerFollowConfig.sizing_mode` is stored as three flat `service_config` keys —
//! `sizing_mode` (`kelly` | `dollar` | `contract`), `sizing_dollar_usd`, `sizing_contracts` —
//! reassembled into the [`SizingMode`] enum here; the `[strategy]` TOML boot default uses the
//! enum's `kind`/`value` serde form. (This replaced the WS1-era `flat_usd_per_trade` in lockstep.)

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use pe_core_types::KellyFraction;
use pe_strategy_winner_follow::{ExecutionMode, PerTradeCap, SizingMode, WinnerFollowConfig};
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::warn;

use crate::config::ServiceConfig;

/// Default number of top-ranked wallets followed by `pe-service`.
///
/// Supabase `service_config.active_watchlist_size` is authoritative when present; this value is
/// the boot/last-known-good fallback when the row is absent or the initial fetch fails.
pub const DEFAULT_ACTIVE_WATCHLIST_SIZE: usize = 100;

/// Smallest accepted runtime watchlist target. Zero is rejected rather than silently stopping
/// every copy decision through an operator typo.
pub const MIN_ACTIVE_WATCHLIST_SIZE: usize = 1;

/// Largest accepted runtime watchlist target. The rank pipeline publishes a top-200 bench, so a
/// larger value cannot be satisfied without changing that upstream contract first.
pub const MAX_ACTIVE_WATCHLIST_SIZE: usize = 200;

/// Smallest accepted `price_impact_cap_bps` edit (#508 Phase A). `0` is rejected — the sole
/// policy size limit cannot be switched off by edit; gate-off exists only as the compiled
/// boot default.
pub const MIN_PRICE_IMPACT_CAP_BPS: i32 = 1;

/// Largest accepted `price_impact_cap_bps` edit: 10_000 bps (100 % of best ask) is the
/// explicit "effectively off" value.
pub const MAX_PRICE_IMPACT_CAP_BPS: i32 = 10_000;

/// The membership cap that has actually been published to the live watchlist.
///
/// This is deliberately separate from a pending Supabase request and from the broader runtime
/// config snapshot. Structural writers consult it while holding their shared mutex, which lets a
/// stale maintenance plan detect that a newer capacity transition already won the race.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchlistCapacityEpoch {
    /// Monotonic generation assigned when Supabase requests a different target.
    pub generation: u64,
    /// Requested/applied top-N cap for this generation.
    pub target: usize,
}

#[derive(Debug, Clone)]
pub struct AppliedWatchlistCapacity {
    inner: Arc<ArcSwap<WatchlistCapacityEpoch>>,
}

impl AppliedWatchlistCapacity {
    /// Seed the applied cap from the boot-time Supabase config used to build the initial set.
    pub fn new(initial: usize) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(WatchlistCapacityEpoch {
                generation: 0,
                target: initial,
            })),
        }
    }

    /// Read the last epoch committed in the structural-writer critical section.
    pub fn load(&self) -> WatchlistCapacityEpoch {
        *self.inner.load_full()
    }

    /// Commit a successful capacity transition.
    ///
    /// Callers must hold the live watchlist's structural-writer mutex while publishing both the
    /// membership generation and this value.
    pub fn store(&self, value: WatchlistCapacityEpoch) {
        self.inner.store(Arc::new(value));
    }
}

/// Paper fill-price mode (#486): the fresh CLOB best-ask, or the boot-frozen leader-price
/// haircut. Stored as the `fill_mode` `service_config` string; an unknown value keeps the
/// last-known-good (never a silent revert). `ClobBestAsk` is the compiled default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FillMode {
    /// A paper BUY fills at the fresh copy-time CLOB best-ask (sizing/band-gates key off it).
    #[default]
    ClobBestAsk,
    /// The pre-#486 boot-frozen leader-price haircut fill (rollback / test-pin mode).
    LeaderHaircut,
}

impl FillMode {
    /// Parse a `fill_mode` string; `None` on an unknown value (caller keeps last-known-good).
    pub fn parse(s: &str) -> Option<FillMode> {
        match s.trim().to_lowercase().replace('-', "_").as_str() {
            "clob_best_ask" | "clobbestask" => Some(FillMode::ClobBestAsk),
            "leader_haircut" | "leaderhaircut" => Some(FillMode::LeaderHaircut),
            _ => None,
        }
    }
}

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
    /// Maximum number of top-ranked wallets actively followed. Supabase-only (no TOML/env
    /// surface), hot-reloaded by the config poller and bounded to the published ranking bench.
    pub active_watchlist_size: usize,
    // ── Service runtime knobs ────────────────────────────────────────────────
    /// Trading mode (`paper` | `shadow` | `live_tiny` | `promoted`). Transitions to a live mode
    /// are guarded by [`validate_mode_transition`].
    pub mode: String,
    /// Mirror of the boot `PE_BANKROLL_USD` paper baseline (#516): parsed into every
    /// runtime snapshot but consumed by nothing — the BOOT value seeds a fresh book
    /// (docs/34) and is the `/paper/pnl` denominator. Editing the KV row re-credits
    /// nothing and has no live effect.
    pub bankroll_usd: String,
    pub max_fill_price: String,
    /// Run28 entry-band lower bound on the current price (`"0"` disables) — the copy-time
    /// twin of the backtest `min_signal_price` floor (#468 selection↔deployment parity).
    pub min_fill_price: String,
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
    /// Paper fill-price mode (#486): `ClobBestAsk` (best-ask BUY fill) or `LeaderHaircut`
    /// (boot-frozen haircut). Consumed by the orchestrator's per-event `resolve_fill_price`.
    pub fill_mode: FillMode,
    /// Fallback BUY haircut (bps) when a `clob_best_ask` fill has no usable ask (#486).
    pub clob_best_ask_fallback_haircut_bps: u32,
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
    pub demotion_pnl_window_secs: u64,
    /// Price-impact gate cap in basis points; `0` disables it (fail-open). Consumed by the
    /// orchestrator's per-event `/book` gate (#398 WS2) and seeded in `service_config` at `0`.
    pub price_impact_cap_bps: i32,

    // ── Strategy (WinnerFollowConfig-derived) ────────────────────────────────
    pub flip_human_approved: bool,
    pub kelly_fraction_above_default_human_approved: bool,
    pub polymarket_fee_rate: Decimal,
    pub kelly_fraction_override: Option<KellyFraction>,
    pub per_trade_cap: PerTradeCap,
    pub slippage_rate: Decimal,
    pub sizing_mode: SizingMode,
}

impl RuntimeConfig {
    /// Build the boot snapshot from the merged [`ServiceConfig`] (env over compiled). Strategy
    /// fields are copied from `cfg.strategy`; `price_impact_cap_bps` has no `ServiceConfig`
    /// field yet, so it defaults to `0` (disabled).
    pub fn from_service_config(cfg: &ServiceConfig) -> Self {
        Self {
            active_watchlist_size: DEFAULT_ACTIVE_WATCHLIST_SIZE,
            mode: cfg.mode.clone(),
            bankroll_usd: cfg.bankroll_usd.clone(),
            max_fill_price: cfg.max_fill_price.clone(),
            min_fill_price: cfg.min_fill_price.clone(),
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
            fill_mode: FillMode::parse(&cfg.fill_mode).unwrap_or_else(|| {
                warn!(value = %cfg.fill_mode, "boot fill_mode unknown; using clob_best_ask");
                FillMode::default()
            }),
            clob_best_ask_fallback_haircut_bps: cfg.clob_best_ask_fallback_haircut_bps,
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
            demotion_pnl_window_secs: cfg.demotion_pnl_window_secs,
            price_impact_cap_bps: 0,
            flip_human_approved: cfg.strategy.flip_human_approved,
            kelly_fraction_above_default_human_approved: cfg
                .strategy
                .kelly_fraction_above_default_human_approved,
            polymarket_fee_rate: cfg.strategy.polymarket_fee_rate,
            kelly_fraction_override: cfg.strategy.kelly_fraction_override,
            // #508 Phase A outage posture, compiled (round-5 Blocking fix): the boot snapshot
            // pins `ModeDefault` regardless of TOML `[strategy]`/`PE_` env, exactly like the
            // `price_impact_cap_bps: 0` hardcode above. A config-fetch outage therefore
            // provably boots the 25 bps clamp + gate off; `per_trade_cap=unlimited` can take
            // effect only through the KV row on a successful poll.
            per_trade_cap: PerTradeCap::ModeDefault,
            slippage_rate: cfg.strategy.slippage_rate,
            sizing_mode: cfg.strategy.sizing_mode,
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
            sizing_mode: self.sizing_mode,
        }
    }
}

/// Parse `proposed` against a guard-hard mode-transition policy.
///
/// Returns `Ok(canonical_mode)` to apply, or `Err(reason)` to refuse (the caller keeps
/// `current` and logs the reason). Ordinary live modes are retired and always refused; an unknown
/// string is refused. `clob_creds_present` remains in the signature while callers migrate, but it
/// cannot authorize a transition.
pub fn validate_mode_transition(
    proposed: &str,
    _clob_creds_present: bool,
    current: &str,
) -> Result<String, String> {
    match parse_execution_mode(proposed) {
        None => Err(format!("unknown mode '{proposed}'")),
        Some(m) if is_live_mode(m) => Err(format!(
            "ordinary mode '{proposed}' is retired; staying '{current}'"
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

    // A syntactically valid 0 or value beyond the published bench is still invalid. Never clamp
    // an operator edit silently; retain the last-known-good target and warn.
    if let Some(raw) = map.get("active_watchlist_size") {
        match raw.parse::<usize>() {
            Ok(value)
                if (MIN_ACTIVE_WATCHLIST_SIZE..=MAX_ACTIVE_WATCHLIST_SIZE).contains(&value) =>
            {
                out.active_watchlist_size = value;
            }
            _ => warn!(
                key = "active_watchlist_size",
                value = %raw,
                min = MIN_ACTIVE_WATCHLIST_SIZE,
                max = MAX_ACTIVE_WATCHLIST_SIZE,
                "service_config: watchlist size is invalid; keeping last-known-good"
            ),
        }
    }

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
    apply_parsed(
        &map,
        "clob_best_ask_fallback_haircut_bps",
        &mut out.clob_best_ask_fallback_haircut_bps,
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
    apply_parsed(
        &map,
        "demotion_pnl_window_secs",
        &mut out.demotion_pnl_window_secs,
    );
    // Price-impact cap (#508 Phase A): the sole policy size limit. Valid range 1..=10_000
    // inclusive — `0` and out-of-range values are REJECTED (last-known-good retained), so
    // the limit cannot be switched off by an edit; "effectively off" is an explicit 10_000.
    // Gate-off exists only as the compiled boot default (the committed `'0'` seed is
    // rejected here, leaving the compiled 0 until the first in-range edit — e.g. the #508
    // A3 cutover UPDATE to 100).
    if let Some(raw) = map.get("price_impact_cap_bps") {
        match raw.parse::<i32>() {
            Ok(v) if (MIN_PRICE_IMPACT_CAP_BPS..=MAX_PRICE_IMPACT_CAP_BPS).contains(&v) => {
                out.price_impact_cap_bps = v;
            }
            _ => warn!(
                key = "price_impact_cap_bps",
                value = %raw,
                min = MIN_PRICE_IMPACT_CAP_BPS,
                max = MAX_PRICE_IMPACT_CAP_BPS,
                "service_config: price_impact_cap_bps outside 1..=10000; keeping last-known-good"
            ),
        }
    }
    apply_parsed(&map, "polymarket_fee_rate", &mut out.polymarket_fee_rate);
    apply_parsed(&map, "slippage_rate", &mut out.slippage_rate);
    // Approval flags are admin-mutable via service_config (#398 Decision #2 — reverses the old
    // "signed config change only" rule, in lockstep with the doc updates in _GLOSSARY / 19- /
    // CLAUDE.md). kelly_fraction_above_default_human_approved gates the override ceiling below, so
    // it is applied BEFORE that check.
    apply_parsed(&map, "flip_human_approved", &mut out.flip_human_approved);
    apply_parsed(
        &map,
        "kelly_fraction_above_default_human_approved",
        &mut out.kelly_fraction_above_default_human_approved,
    );

    // Decimal-valued strings: validate as Decimal, store the string shape consumers expect.
    apply_decimal_string(&map, "bankroll_usd", &mut out.bankroll_usd);
    apply_decimal_string(&map, "max_fill_price", &mut out.max_fill_price);
    apply_decimal_string(&map, "min_fill_price", &mut out.min_fill_price);
    apply_decimal_string(&map, "demotion_cb_alpha", &mut out.demotion_cb_alpha);

    // Sizing mode: reassemble the three flat KV keys into SizingMode (#398 WS2). An absent
    // `sizing_mode` key or an invalid/missing param keeps the last-known-good mode.
    out.sizing_mode = parse_sizing_mode(&map, last_good.sizing_mode);

    // Paper fill mode (#486): an unknown value keeps the last-known-good (never a silent revert
    // to a compiled default — a bad admin edit must not flip a paper BUY off the best-ask basis).
    if let Some(raw) = map.get("fill_mode") {
        match FillMode::parse(raw) {
            Some(m) => out.fill_mode = m,
            None => warn!(
                key = "fill_mode",
                value = %raw,
                "service_config: unknown fill_mode; keeping last-known-good"
            ),
        }
    }

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

    // Kelly-override invariant (relocated from the boot guard): an override above the mode's
    // ceiling needs the approval flag. Enforced on the RESULT — a KV edit OR a value carried
    // from last-known-good (e.g. after a mode change lowers the ceiling) — so an above-ceiling
    // override without approval can never take effect. A parse failure or absent cell keeps
    // last-known-good (then re-validated here); rejection drops to None, so mode-default Kelly
    // applies rather than silently preserving a now-disallowed elevated value.
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
                 kelly_fraction_above_default_human_approved; rejecting (override cleared)"
            );
            None
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

    /// Publish a new snapshot. The config coordinator is the sole production writer; structural
    /// watchlist serialization is handled separately by the capacity request/applier path.
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

/// Reassemble the three flat sizing KV keys into [`SizingMode`] (#398 WS2). An absent
/// `sizing_mode` key, an unknown kind, or a missing/invalid param for the chosen kind keeps
/// `last_good` — never a silent revert to the serde default.
fn parse_sizing_mode(map: &HashMap<&str, &str>, last_good: SizingMode) -> SizingMode {
    let Some(kind) = map.get("sizing_mode") else {
        return last_good;
    };
    match kind.trim().to_lowercase().as_str() {
        "kelly" => SizingMode::Kelly,
        "dollar" => match map
            .get("sizing_dollar_usd")
            .and_then(|v| Decimal::from_str(v.trim()).ok())
        {
            Some(usd) => SizingMode::Dollar { usd },
            None => {
                warn!(
                    "service_config: sizing_mode=dollar but sizing_dollar_usd is missing/invalid; keeping last-known-good"
                );
                last_good
            }
        },
        "contract" => match map
            .get("sizing_contracts")
            .and_then(|v| v.trim().parse::<u64>().ok())
        {
            Some(contracts) => SizingMode::Contract { contracts },
            None => {
                warn!(
                    "service_config: sizing_mode=contract but sizing_contracts is missing/invalid; keeping last-known-good"
                );
                last_good
            }
        },
        other => {
            warn!(value = %other, "service_config: unknown sizing_mode; keeping last-known-good");
            last_good
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

/// `service_config.mode` string → [`ExecutionMode`], mirroring `main.rs::parse_mode`. Public so
/// the orchestrator can parse the per-event snapshot's `mode` string into an `ExecutionMode`.
pub fn parse_execution_mode(s: &str) -> Option<ExecutionMode> {
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
        // Exception (#508 Phase A): `per_trade_cap` is PINNED to `ModeDefault` at boot (the
        // compiled outage posture), so its TOML/env value never reaches the runtime config —
        // covered by `boot_pins_per_trade_cap_regardless_of_toml_env` below.
        let strat = WinnerFollowConfig {
            flip_human_approved: true,
            kelly_fraction_above_default_human_approved: true,
            polymarket_fee_rate: dec!(0.07),
            kelly_fraction_override: Some(KellyFraction::new(dec!(0.40)).unwrap()),
            per_trade_cap: PerTradeCap::Bps(42),
            slippage_rate: dec!(0.03),
            sizing_mode: SizingMode::Dollar { usd: dec!(99) },
        };
        let cfg = ServiceConfig {
            strategy: strat.clone(),
            ..Default::default()
        };
        let expected = WinnerFollowConfig {
            per_trade_cap: PerTradeCap::ModeDefault, // pinned boot posture (#508)
            ..strat
        };
        assert_eq!(
            RuntimeConfig::from_service_config(&cfg).winner_follow_config(),
            expected
        );
    }

    #[test]
    fn boot_pins_per_trade_cap_regardless_of_toml_env() {
        // #508 round-5 Blocking regression: with a TOML/env `per_trade_cap = unlimited`
        // override and ZERO config rows (a config-fetch outage), boot must still land on
        // `ModeDefault` — never "unlimited with the gate off" (no policy size limit at all).
        let cfg = ServiceConfig {
            strategy: WinnerFollowConfig {
                per_trade_cap: PerTradeCap::Unlimited,
                ..WinnerFollowConfig::default()
            },
            ..Default::default()
        };
        let rc = load_initial_runtime_config(&[], &cfg, false);
        assert_eq!(rc.per_trade_cap, PerTradeCap::ModeDefault);
        assert_eq!(rc.price_impact_cap_bps, 0, "gate off until the first poll");
        // The KV row remains the sole path to `unlimited` (a successful poll).
        let polled = parse_config(&[row("per_trade_cap", "unlimited", "text")], &rc, false);
        assert_eq!(polled.per_trade_cap, PerTradeCap::Unlimited);
    }

    #[test]
    fn price_impact_cap_rejects_zero_and_out_of_range() {
        // #508 Phase A: valid range 1..=10_000; `0` (the committed seed) and >10_000 are
        // rejected with the last-known-good retained — the sole policy size limit cannot be
        // switched off by edit.
        let boot = RuntimeConfig::from_service_config(&ServiceConfig::default());
        assert_eq!(boot.price_impact_cap_bps, 0); // compiled gate-off default
        for invalid in ["0", "-5", "10001", "nope"] {
            let out = parse_config(
                &[row("price_impact_cap_bps", invalid, "integer")],
                &boot,
                false,
            );
            assert_eq!(
                out.price_impact_cap_bps, 0,
                "invalid edit {invalid} must retain last-known-good"
            );
        }
        let last = RuntimeConfig {
            price_impact_cap_bps: 100,
            ..boot.clone()
        };
        let out = parse_config(&[row("price_impact_cap_bps", "0", "integer")], &last, false);
        assert_eq!(
            out.price_impact_cap_bps, 100,
            "a 0 edit cannot disable the gate"
        );
        for (valid, want) in [("1", 1), ("100", 100), ("10000", 10_000)] {
            let out = parse_config(
                &[row("price_impact_cap_bps", valid, "integer")],
                &boot,
                false,
            );
            assert_eq!(out.price_impact_cap_bps, want);
        }
    }

    #[test]
    fn seed_reconstructs_boot_strategy() {
        // The committed seed, parsed through load_initial, reconstructs the live boot strategy:
        // sizing dollar-$25 (never Kelly), fee 0.04, slippage 0.01, approvals off. per_trade_cap
        // IS seeded (`mode_default`, #508 Phase A) and must reconstruct the pinned boot posture;
        // kelly_fraction_override is not seeded and falls through to the compiled default.
        let rc = load_initial_runtime_config(&seed_rows(), &ServiceConfig::default(), false);
        let expected = WinnerFollowConfig {
            flip_human_approved: false,
            kelly_fraction_above_default_human_approved: false,
            polymarket_fee_rate: dec!(0.04),
            kelly_fraction_override: None,
            per_trade_cap: PerTradeCap::ModeDefault,
            slippage_rate: dec!(0.01),
            sizing_mode: SizingMode::Dollar { usd: dec!(25) },
        };
        assert_eq!(rc.winner_follow_config(), expected);
        // Service-level knobs reconstruct too (spot-check the risk-engine gate inputs).
        assert_eq!(rc.mode, "paper");
        assert_eq!(rc.active_watchlist_size, 100);
        assert_eq!(rc.max_fill_price, "0.85");
        assert_eq!(rc.min_fill_price, "0.15"); // run28 band floor (2026-07-03 cutover)
        assert_eq!(rc.bankroll_usd, "10000");
        assert_eq!(rc.min_resolution_horizon_secs, 60);
        assert_eq!(rc.max_resolution_horizon_secs, 172_800); // run28 TTR ceiling
        assert_eq!(rc.demotion_pnl_window_secs, 2_592_000); // #473, now seeded
        // The committed '0' seed is REJECTED by the 1..=10_000 validator (#508 Phase A);
        // boot retains the compiled gate-off default — the intended safe posture.
        assert_eq!(rc.price_impact_cap_bps, 0);
        // #486: paper fills record the CLOB best-ask; the fallback haircut is 1%.
        assert_eq!(rc.fill_mode, FillMode::ClobBestAsk);
        assert_eq!(rc.clob_best_ask_fallback_haircut_bps, 100);
    }

    #[test]
    fn fill_mode_kv_reassembly_and_last_known_good() {
        let boot = RuntimeConfig::from_service_config(&ServiceConfig::default());
        assert_eq!(boot.fill_mode, FillMode::ClobBestAsk); // #486 compiled default
        // A valid admin edit switches the mode.
        let hc = parse_config(&[row("fill_mode", "leader_haircut", "text")], &boot, false);
        assert_eq!(hc.fill_mode, FillMode::LeaderHaircut);
        // An unknown value keeps the last-known-good (never a silent revert to a compiled default).
        let last = RuntimeConfig {
            fill_mode: FillMode::LeaderHaircut,
            ..boot.clone()
        };
        assert_eq!(
            parse_config(&[row("fill_mode", "bogus", "text")], &last, false).fill_mode,
            FillMode::LeaderHaircut
        );
        // The fallback haircut overlays as a plain integer; an unparseable fill_mode in the same
        // poll still applies the valid integer and keeps the last-known-good fill_mode.
        let bps = parse_config(
            &[
                row("clob_best_ask_fallback_haircut_bps", "250", "integer"),
                row("fill_mode", "not_a_mode", "text"),
            ],
            &boot,
            false,
        );
        assert_eq!(bps.clob_best_ask_fallback_haircut_bps, 250);
        assert_eq!(bps.fill_mode, FillMode::ClobBestAsk);
    }

    #[test]
    fn sizing_mode_kv_reassembly_and_last_known_good() {
        let boot = RuntimeConfig::from_service_config(&ServiceConfig::default());
        assert_eq!(boot.sizing_mode, SizingMode::Kelly); // compiled default
        // dollar: kind + sizing_dollar_usd
        let dollar = parse_config(
            &[
                row("sizing_mode", "dollar", "text"),
                row("sizing_dollar_usd", "25", "decimal"),
            ],
            &boot,
            false,
        );
        assert_eq!(dollar.sizing_mode, SizingMode::Dollar { usd: dec!(25) });
        // contract: kind + sizing_contracts
        let contract = parse_config(
            &[
                row("sizing_mode", "contract", "text"),
                row("sizing_contracts", "7", "integer"),
            ],
            &boot,
            false,
        );
        assert_eq!(contract.sizing_mode, SizingMode::Contract { contracts: 7 });
        // A `dollar` edit missing its param, or an unknown kind, keeps the last-known-good mode.
        let last = RuntimeConfig {
            sizing_mode: SizingMode::Contract { contracts: 3 },
            ..boot.clone()
        };
        assert_eq!(
            parse_config(&[row("sizing_mode", "dollar", "text")], &last, false).sizing_mode,
            SizingMode::Contract { contracts: 3 }
        );
        assert_eq!(
            parse_config(&[row("sizing_mode", "bogus", "text")], &last, false).sizing_mode,
            SizingMode::Contract { contracts: 3 }
        );
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
    fn active_watchlist_size_accepts_bounds_and_rejects_invalid_values() {
        let boot = RuntimeConfig::from_service_config(&ServiceConfig::default());
        assert_eq!(boot.active_watchlist_size, DEFAULT_ACTIVE_WATCHLIST_SIZE);

        for value in [MIN_ACTIVE_WATCHLIST_SIZE, 100, MAX_ACTIVE_WATCHLIST_SIZE] {
            let parsed = parse_config(
                &[row("active_watchlist_size", &value.to_string(), "integer")],
                &boot,
                false,
            );
            assert_eq!(parsed.active_watchlist_size, value);
        }

        let last = RuntimeConfig {
            active_watchlist_size: 77,
            ..boot
        };
        for invalid in ["0", "201", "-1", "not-a-number"] {
            let parsed = parse_config(
                &[row("active_watchlist_size", invalid, "integer")],
                &last,
                false,
            );
            assert_eq!(
                parsed.active_watchlist_size, 77,
                "invalid value {invalid} must retain last-known-good"
            );
        }
    }

    #[test]
    fn kelly_override_rejected_without_approval_but_accepted_with() {
        let boot = RuntimeConfig::from_service_config(&ServiceConfig::default()); // mode=paper, ceiling 0.25
        // 0.40 > 0.25 ceiling, approval false -> rejected -> override cleared (None).
        let rejected = parse_config(
            &[row("kelly_fraction_override", "0.40", "decimal")],
            &boot,
            false,
        );
        assert_eq!(rejected.kelly_fraction_override, None);
        // With the approval flag already true at the baseline, the same above-ceiling override is
        // accepted. (Setting the flag via KV in the same poll is covered by
        // `approval_flags_are_kv_mutable`.)
        let approved_boot = RuntimeConfig {
            kelly_fraction_above_default_human_approved: true,
            ..boot.clone()
        };
        let accepted = parse_config(
            &[row("kelly_fraction_override", "0.40", "decimal")],
            &approved_boot,
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
    fn carried_above_ceiling_override_is_dropped_not_preserved() {
        // Regression guard: an above-ceiling override already in last-known-good (e.g. carried
        // from a higher-ceiling mode, or set when approval was true) must be DROPPED to None when
        // it no longer satisfies the invariant — never silently preserved (a risk-control bypass).
        let elevated = RuntimeConfig {
            kelly_fraction_override: Some(KellyFraction::new(dec!(0.40)).unwrap()),
            kelly_fraction_above_default_human_approved: false,
            ..RuntimeConfig::from_service_config(&ServiceConfig::default()) // mode=paper, ceiling 0.25
        };
        // No override edit in this poll; the carried 0.40 violates the paper ceiling sans approval.
        let out = parse_config(&[], &elevated, false);
        assert_eq!(out.kelly_fraction_override, None);
    }

    #[test]
    fn approval_flags_are_kv_mutable() {
        // #398 Decision #2: approval flags are admin-mutable via service_config. The flag is
        // applied before the override-ceiling check, so setting it in the SAME poll admits an
        // above-ceiling override.
        let boot = RuntimeConfig::from_service_config(&ServiceConfig::default());
        assert!(!boot.flip_human_approved);
        let out = parse_config(
            &[
                row("flip_human_approved", "true", "bool"),
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
        assert!(out.flip_human_approved);
        assert!(out.kelly_fraction_above_default_human_approved);
        assert_eq!(
            out.kelly_fraction_override,
            Some(KellyFraction::new(dec!(0.40)).unwrap())
        );
    }

    #[test]
    fn mode_transition_guard() {
        // Ordinary live modes are refused regardless of credential presence.
        assert!(validate_mode_transition("live_tiny", false, "paper").is_err());
        assert!(validate_mode_transition("live_tiny", true, "paper").is_err());
        assert!(validate_mode_transition("promoted", true, "paper").is_err());
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
        let still_refused = parse_config(&[row("mode", "live_tiny", "text")], &boot, true);
        assert_eq!(still_refused.mode, "paper");
    }

    #[test]
    fn kv_overrides_boot_for_a_valid_edit() {
        // A valid admin edit wins over the boot/env value (KV > env).
        let boot = RuntimeConfig::from_service_config(&ServiceConfig::default());
        let out = parse_config(&[row("max_fill_price", "0.90", "decimal")], &boot, false);
        assert_eq!(out.max_fill_price, "0.90");
    }
}
