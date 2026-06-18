//! Wallet pile orchestration (issue #166).
//!
//! Free functions over `&mut WalletCache` that implement the activation rule
//! and the staleness queries used by the `backfill` and `weekly` subcommands.
//!
//! Bit definitions for `wallets.source_bits`:
//!
//! | Bit | Mask | Source |
//! |---:|---:|---|
//! | 0 | 0b0000001 | `wallet_set.json` |
//! | 1 | 0b0000010 | `trades` table (DB-resident) |
//! | 2 | 0b0000100 | _(removed #335 — was Dune CSV; gap kept, persisted)_ |
//! | 3 | 0b0001000 | _(removed #335 — was Dune incremental; gap kept, persisted)_ |
//! | 4 | 0b0010000 | Polymarket leaderboard |
//! | 5 | 0b0100000 | Radion |
//! | 6 | 0b1000000 | 502-gap |
//! | 7 | 0b10000000 | datadash.xyz cohorts (#365) |
//!
//! Canonical `wallet_hex` form: `"0x" + 40 lowercase hex chars` (matches
//! `WalletAddress::Display`). All callers must normalise before inserting.

use crate::cache::WalletCache;
use crate::error::BootstrapError;

/// Minimum trade-count threshold for activation. Documented in
/// `docs/_GLOSSARY.md` "Bootstrap defaults" as `pile_activation_min_trades`.
pub const PILE_ACTIVATION_MIN_TRADES: i64 = 100;

/// Default staleness windows.
pub const BACKFILL_STALENESS_SECS: i64 = 86_400; // 1 day
pub const WEEKLY_STALENESS_SECS: i64 = 7 * 86_400; // 7 days

// Source bit masks (kept as `i64` so they line up with the SQLite column type).
pub const SRC_WALLET_SET_JSON: i64 = 0b0000001;
pub const SRC_TRADES: i64 = 0b0000010;
// bit2 (0b0000100) and bit3 (0b0001000) were SRC_DUNE_CSV / SRC_DUNE_INCREMENTAL,
// removed in #335. The gap is intentional — `source_bits` is persisted in the
// live `wallet_cache.db`; do not renumber the surviving bits.
pub const SRC_LEADERBOARD: i64 = 0b0010000;
pub const SRC_RADION: i64 = 0b0100000;
pub const SRC_GAP502: i64 = 0b1000000;
/// datadash.xyz cohort discovery (issue #365). Bypasses the activation gate like
/// the other curation-list sources.
pub const SRC_DATADASH: i64 = 0b10000000;

/// Apply the activation rule. Sticky 0→1; `is_infra = 0` gates every branch.
///
/// Returns the number of newly-activated wallets.
pub fn apply_activation_rules(cache: &mut WalletCache) -> Result<usize, BootstrapError> {
    cache.apply_activation_rules(PILE_ACTIVATION_MIN_TRADES)
}

/// Select active wallets due for Polymarket backfill (1-day staleness).
///
/// `limit = 0` returns every due wallet (no cap). Used by the daily timer.
pub fn select_backfill_due(
    cache: &WalletCache,
    now_unix: i64,
    limit: usize,
) -> Result<Vec<String>, BootstrapError> {
    cache.select_backfill_due(now_unix, BACKFILL_STALENESS_SECS, limit)
}

/// Select active wallets due for weekly funder refresh (7-day staleness).
pub fn select_weekly_due(
    cache: &WalletCache,
    now_unix: i64,
    limit: usize,
) -> Result<Vec<String>, BootstrapError> {
    cache.select_weekly_due(now_unix, WEEKLY_STALENESS_SECS, limit)
}

/// Select active wallets due for a Polymarket *full-fetch* (paranoia backstop
/// for the delta-backfill flow, issue #176).
///
/// Unlike [`select_backfill_due`] (1-day staleness, every due wallet enters
/// the fetch set), this picks wallets whose `last_polymarket_full_at` is NULL
/// or older than `staleness_secs` ago. The canonical config window is 7 days
/// so no wallet stays "delta-only" for more than a week even if the on-chain
/// scanner misses it.
pub fn select_full_fetch_due(
    cache: &WalletCache,
    now_unix: i64,
    staleness_secs: i64,
) -> Result<Vec<String>, BootstrapError> {
    cache.wallets_due_for_full_fetch(now_unix, staleness_secs)
}

/// Update `last_polymarket_fetch_at` for a wallet.
pub fn update_last_polymarket_fetch(
    cache: &mut WalletCache,
    wallet_hex: &str,
    now_unix: i64,
) -> Result<(), BootstrapError> {
    cache.update_last_polymarket_fetch(wallet_hex, now_unix)
}

/// Update `last_funder_fetch_at` for a wallet.
pub fn update_last_funder_fetch(
    cache: &mut WalletCache,
    wallet_hex: &str,
    now_unix: i64,
) -> Result<(), BootstrapError> {
    cache.update_last_funder_fetch(wallet_hex, now_unix)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmp_cache() -> (TempDir, WalletCache) {
        let dir = TempDir::new().unwrap();
        let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        (dir, cache)
    }

    fn hex(i: u8) -> String {
        format!("0x{:040x}", i)
    }

    // ── Activation rule ──────────────────────────────────────────────────────

    #[test]
    fn activation_by_trade_count_only_after_threshold() {
        let (_dir, mut cache) = tmp_cache();
        // Insert wallet via UPSERT and bump trade_count via the helper. Below
        // threshold = NOT activated.
        cache
            .upsert_wallet(&hex(1), SRC_TRADES, false, None, None, None)
            .unwrap();
        cache.conn_for_test_set_trade_count(&hex(1), PILE_ACTIVATION_MIN_TRADES - 1);
        let activated = apply_activation_rules(&mut cache).unwrap();
        assert_eq!(activated, 0);

        // Equal to threshold = activated.
        cache.conn_for_test_set_trade_count(&hex(1), PILE_ACTIVATION_MIN_TRADES);
        let activated = apply_activation_rules(&mut cache).unwrap();
        assert_eq!(activated, 1);
    }

    #[test]
    fn activation_by_dune_closed_markets_before_db_backfill() {
        let (_dir, mut cache) = tmp_cache();
        cache
            .upsert_wallet(&hex(2), SRC_WALLET_SET_JSON, false, None, Some(150), None)
            .unwrap();
        // trade_count is 0 (no DB trades yet) but dune_closed_markets satisfies.
        let activated = apply_activation_rules(&mut cache).unwrap();
        assert_eq!(activated, 1);
    }

    #[test]
    fn activation_leaderboard_membership_bypasses_count_gate() {
        let (_dir, mut cache) = tmp_cache();
        cache
            .upsert_wallet(&hex(3), SRC_LEADERBOARD, false, None, None, None)
            .unwrap();
        let activated = apply_activation_rules(&mut cache).unwrap();
        assert_eq!(activated, 1);
    }

    #[test]
    fn activation_blocked_by_infra_even_with_curation_membership() {
        let (_dir, mut cache) = tmp_cache();
        cache
            .upsert_wallet(&hex(4), SRC_LEADERBOARD, true, None, None, None)
            .unwrap();
        let activated = apply_activation_rules(&mut cache).unwrap();
        assert_eq!(activated, 0, "infra wallets must never activate");
    }

    #[test]
    fn activation_is_sticky_zero_to_one_only() {
        let (_dir, mut cache) = tmp_cache();
        cache
            .upsert_wallet(&hex(5), SRC_RADION, false, None, None, None)
            .unwrap();
        let first = apply_activation_rules(&mut cache).unwrap();
        assert_eq!(first, 1);
        // Re-running activates nobody new — sticky.
        let second = apply_activation_rules(&mut cache).unwrap();
        assert_eq!(second, 0);
    }

    // ── source_bits accumulation ─────────────────────────────────────────────

    #[test]
    fn upsert_accumulates_source_bits_across_calls() {
        let (_dir, mut cache) = tmp_cache();
        cache
            .upsert_wallet(&hex(6), SRC_WALLET_SET_JSON, false, None, None, None)
            .unwrap();
        cache
            .upsert_wallet(&hex(6), SRC_TRADES, false, None, None, None)
            .unwrap();
        cache
            .upsert_wallet(&hex(6), SRC_LEADERBOARD, false, None, None, None)
            .unwrap();
        let bits = cache.conn_for_test_source_bits(&hex(6));
        assert_eq!(
            bits,
            SRC_WALLET_SET_JSON | SRC_TRADES | SRC_LEADERBOARD,
            "expected bits 0b0010011, got 0b{:07b}",
            bits
        );
    }

    #[test]
    fn upsert_is_infra_is_sticky() {
        let (_dir, mut cache) = tmp_cache();
        cache
            .upsert_wallet(&hex(7), SRC_WALLET_SET_JSON, true, None, None, None)
            .unwrap();
        cache
            .upsert_wallet(&hex(7), SRC_LEADERBOARD, false, None, None, None)
            .unwrap();
        let infra = cache.conn_for_test_is_infra(&hex(7));
        assert!(
            infra,
            "is_infra must stay 1 after re-upsert with infra=false"
        );
    }

    // ── select_backfill_due ──────────────────────────────────────────────────

    #[test]
    fn select_backfill_due_picks_only_active_with_stale_timestamps() {
        let (_dir, mut cache) = tmp_cache();
        // Active, NULL timestamp.
        cache
            .upsert_wallet(&hex(10), SRC_LEADERBOARD, false, None, None, None)
            .unwrap();
        // Active, fresh timestamp (now - 1h).
        cache
            .upsert_wallet(&hex(11), SRC_LEADERBOARD, false, None, None, None)
            .unwrap();
        // Inactive (nothing activates it).
        cache
            .upsert_wallet(&hex(12), 0, false, None, None, None)
            .unwrap();
        apply_activation_rules(&mut cache).unwrap();

        let now: i64 = 1_700_000_000;
        cache
            .update_last_polymarket_fetch(&hex(11), now - 3600)
            .unwrap();

        let due = select_backfill_due(&cache, now, 0).unwrap();
        assert_eq!(
            due,
            vec![hex(10)],
            "fresh wallet must NOT be due; inactive must NOT be due"
        );
    }

    #[test]
    fn select_backfill_due_orders_by_quality_within_null_bucket() {
        let (_dir, mut cache) = tmp_cache();
        // Two active wallets with NULL last_polymarket_fetch_at; different quality.
        cache
            .upsert_wallet(&hex(20), SRC_LEADERBOARD, false, None, Some(50), Some(9500))
            .unwrap();
        cache
            .upsert_wallet(&hex(21), SRC_LEADERBOARD, false, None, Some(50), Some(9900))
            .unwrap();
        apply_activation_rules(&mut cache).unwrap();

        let due = select_backfill_due(&cache, 1_700_000_000, 0).unwrap();
        assert_eq!(
            due,
            vec![hex(21), hex(20)],
            "higher win_rate_bps must come first"
        );
    }

    #[test]
    fn select_backfill_due_zero_limit_returns_all() {
        let (_dir, mut cache) = tmp_cache();
        for i in 30..40 {
            cache
                .upsert_wallet(&hex(i), SRC_LEADERBOARD, false, None, None, None)
                .unwrap();
        }
        apply_activation_rules(&mut cache).unwrap();
        let due = select_backfill_due(&cache, 1_700_000_000, 0).unwrap();
        assert_eq!(due.len(), 10);
    }

    #[test]
    fn select_backfill_due_positive_limit_caps() {
        let (_dir, mut cache) = tmp_cache();
        for i in 40..50 {
            cache
                .upsert_wallet(&hex(i), SRC_LEADERBOARD, false, None, None, None)
                .unwrap();
        }
        apply_activation_rules(&mut cache).unwrap();
        let due = select_backfill_due(&cache, 1_700_000_000, 3).unwrap();
        assert_eq!(due.len(), 3);
    }

    // ── select_weekly_due ────────────────────────────────────────────────────

    #[test]
    fn select_weekly_due_uses_seven_day_window() {
        let (_dir, mut cache) = tmp_cache();
        cache
            .upsert_wallet(&hex(50), SRC_LEADERBOARD, false, None, None, None)
            .unwrap();
        apply_activation_rules(&mut cache).unwrap();

        let now: i64 = 1_700_000_000;
        // Stamp 3 days ago — NOT due yet (weekly = 7d).
        cache
            .update_last_funder_fetch(&hex(50), now - 3 * 86_400)
            .unwrap();
        let due = select_weekly_due(&cache, now, 0).unwrap();
        assert!(due.is_empty(), "3-day-old funder fetch is fresh for weekly");

        // Stamp 8 days ago — due.
        cache
            .update_last_funder_fetch(&hex(50), now - 8 * 86_400)
            .unwrap();
        let due = select_weekly_due(&cache, now, 0).unwrap();
        assert_eq!(due, vec![hex(50)]);
    }
}
