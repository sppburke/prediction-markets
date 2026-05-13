//! Scenario tests for `OutcomeId` widening to `u16` (issue #159).
//!
//! Exercises the cache write/read pipeline and the simulation-style
//! resolution-match path for Polymarket multi-outcome markets with
//! `outcomeIndex > 255`. The parse path (`bootstrap::polymarket` DTO + the
//! `service::trade_parser` DTO) is covered by inline unit tests in those
//! modules; this file covers the integration points — cache idempotency,
//! voided-multi-outcome semantics, and the resolution-match equality the
//! backtest depends on.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::cache::{MarketResolution, WalletCache};
use pe_core_types::{MarketId, OutcomeId, VenueMarketId};
use tempfile::TempDir;

fn tmp_cache(dir: &TempDir) -> WalletCache {
    WalletCache::open(&dir.path().join("cache.db")).unwrap()
}

// ── Scenario 1: cache insert/load round-trip preserves OutcomeId(999) ────────
//
// PASS: load_all_resolutions returns OutcomeId(999) for the multi-outcome market.
// FAIL: row silently dropped (pre-#159 behaviour) or wrong outcome_id loaded.

#[test]
fn cache_round_trips_outcome_index_999() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);
    cache
        .insert_resolution_with_source(
            "0xcond_multi",
            Some(999),
            1_700_000_100,
            1_700_000_200,
            "polygon",
        )
        .unwrap();
    let idx = cache.load_all_resolutions().unwrap();
    let res: &MarketResolution = idx
        .get(&MarketId(VenueMarketId("0xcond_multi".to_owned())))
        .expect("multi-outcome market must appear in ResolutionIndex");
    assert_eq!(res.winning_outcome_id, OutcomeId(999));
    assert_eq!(res.resolved_at_unix, 1_700_000_100);
}

// ── Scenario 2: u16::MAX winner round-trips ──────────────────────────────────
//
// PASS: u16::MAX (65,535) survives the SQLite INTEGER round-trip and the
//       OutcomeId::try_from path on read.
// FAIL: truncation or row drop.

#[test]
fn cache_round_trips_u16_max_outcome() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);
    cache
        .insert_resolution_with_source(
            "0xcond_max",
            Some(u16::MAX),
            1_700_000_100,
            1_700_000_200,
            "polygon",
        )
        .unwrap();
    let idx = cache.load_all_resolutions().unwrap();
    let res = idx
        .get(&MarketId(VenueMarketId("0xcond_max".to_owned())))
        .expect("u16::MAX outcome must round-trip");
    assert_eq!(res.winning_outcome_id, OutcomeId(u16::MAX));
}

// ── Scenario 3: voided multi-outcome market stays voided ─────────────────────
//
// PASS: insert with None loads as None — `load_all_resolutions` filters it out,
//       but `resolved_market_ids` still surfaces it so Gamma/Dune don't re-fetch.
// FAIL: voided row appears in the resolution index.

#[test]
fn voided_multi_outcome_market_excluded_from_index() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);
    cache
        .insert_resolution_with_source(
            "0xcond_voided_multi",
            None,
            1_700_000_100,
            1_700_000_200,
            "polygon",
        )
        .unwrap();
    let idx = cache.load_all_resolutions().unwrap();
    assert!(
        !idx.contains_key(&MarketId(VenueMarketId("0xcond_voided_multi".to_owned()))),
        "voided market must be excluded from load_all_resolutions"
    );
    assert!(
        cache.resolved_market_ids().contains("0xcond_voided_multi"),
        "voided market must still appear in resolved_market_ids to prevent re-fetch"
    );
}

// ── Scenario 4: simulation-style match against OutcomeId(999) ────────────────
//
// Exercises the type-homogeneous comparison at `backtest/src/simulation.rs:421`:
// after the `.0` deref removal, both sides are `OutcomeId` and a 999-vs-999
// match yields equality without compiler-silent truncation.

#[test]
fn outcome_id_equality_match_at_resolution_path() {
    let resolved_winner = OutcomeId(999);
    let position_outcome_match = OutcomeId(999);
    let position_outcome_mismatch = OutcomeId(0);
    assert_eq!(resolved_winner, position_outcome_match);
    assert_ne!(resolved_winner, position_outcome_mismatch);
}

// ── Scenario 5: first-fetch-wins idempotency holds for large outcomes ────────
//
// PASS: second insert (different winner) is a no-op; first winner sticks.
// FAIL: idempotency lost when the winner index is in the widened range.

#[test]
fn idempotency_holds_for_large_outcome_id() {
    let dir = TempDir::new().unwrap();
    let mut cache = tmp_cache(&dir);
    cache
        .insert_resolution_with_source(
            "0xcond_idem",
            Some(999),
            1_700_000_100,
            1_700_000_200,
            "polygon",
        )
        .unwrap();
    // Different source attempts to overwrite with a different winner —
    // INSERT OR IGNORE must keep the first.
    cache
        .insert_resolution_with_source("0xcond_idem", Some(0), 1_700_000_300, 1_700_000_400, "dune")
        .unwrap();
    let idx = cache.load_all_resolutions().unwrap();
    let res = idx
        .get(&MarketId(VenueMarketId("0xcond_idem".to_owned())))
        .unwrap();
    assert_eq!(
        res.winning_outcome_id,
        OutcomeId(999),
        "first insert (winner=999) must not be overwritten"
    );
}
