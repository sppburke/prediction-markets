#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scenario: datadash.xyz cohort discovery against the real captured contract
//! (issue #365).
//!
//! Loads the committed fixtures captured live from `api.datadash.xyz` on
//! 2026-06-17 (`tests/fixtures/datadash_list_cohorts.json` +
//! `datadash_list_cohort_wallets.json`), deserializes them through the *production*
//! parsers (`parse_list_cohorts` / `parse_list_cohort_wallets`) so any drift in
//! the reverse-engineered shape — `cohorts[]`, `numWallets`-as-string,
//! `addresses[]` — fails loudly here, then runs the full `run_datadash_discovery`
//! sweep through a `FixtureCohortFetcher` against a temp cache. Deterministic; no
//! network.

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::datadash_discovery::{
    FixtureCohortFetcher, parse_list_cohort_wallets, parse_list_cohorts, run_datadash_discovery,
};
use tempfile::TempDir;

const LIST_COHORTS: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/datadash_list_cohorts.json"
));
const LIST_WALLETS: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/datadash_list_cohort_wallets.json"
));

/// The default exclusion config (mirrors `BootstrapConfig` defaults).
const EXCLUDE_ID: &str = "07NQHFRAGB6HV";
const EXCLUDE_TITLE: &str = "Polymarket Twitter/X Linked Traders";

fn tmp_cache() -> (TempDir, WalletCache) {
    let dir = TempDir::new().unwrap();
    let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    (dir, cache)
}

// PASS: the real captured contract parses (9 cohorts; the 102,871-wallet cohort's
//       numWallets-as-string round-trips), the exact-id/title exclusion drops only
//       the `Polymarket Twitter/X Linked Traders` cohort (8 kept), and the 7 real
//       addresses are ingested + activated.
// FAIL: contract shape drift (parse error / wrong count), the excluded cohort
//       leaks in, or wallets are not activated.
#[tokio::test]
async fn real_contract_exclusion_end_to_end() {
    let (_d, mut cache) = tmp_cache();

    // Production parsers — pin the live shape.
    let cohorts = parse_list_cohorts(LIST_COHORTS).unwrap();
    let real_addrs = parse_list_cohort_wallets(LIST_WALLETS).unwrap();
    assert_eq!(cohorts.len(), 9, "captured ListCohorts has 9 cohorts");
    assert_eq!(
        real_addrs.len(),
        7,
        "captured Followed Wallets cohort has 7 addresses"
    );
    let huge = cohorts
        .iter()
        .find(|c| c.id == EXCLUDE_ID)
        .expect("excluded cohort present in fixture");
    assert_eq!(huge.num_wallets, 102_871, "numWallets-as-string parsed");

    // Every cohort returns the same 7 real addresses; the excluded one is never
    // fetched (dropped before fetch), so the result must still be exactly 7.
    let wallets: HashMap<String, Vec<String>> = cohorts
        .iter()
        .map(|c| (c.id.clone(), real_addrs.clone()))
        .collect();
    let fetcher = FixtureCohortFetcher::new(cohorts, wallets);

    let ex_ids = vec![EXCLUDE_ID.to_owned()];
    let ex_titles = vec![EXCLUDE_TITLE.to_owned()];
    let report = run_datadash_discovery(&fetcher, &ex_ids, &ex_titles, 10_000, &mut cache)
        .await
        .unwrap();

    let active = cache.active_wallet_count().unwrap();
    let pass = report.cohorts_listed == 9
        && report.cohorts_kept == 8
        && report.unique_wallets == 7
        && report.activated == 7
        && active == 7;
    println!(
        "{}: real_contract_exclusion_end_to_end \
         (listed={}, kept={}, unique={}, activated={}, active_count={})",
        if pass { "PASS" } else { "FAIL" },
        report.cohorts_listed,
        report.cohorts_kept,
        report.unique_wallets,
        report.activated,
        active,
    );
    assert!(
        pass,
        "expected listed=9, kept=8, unique=7, activated=7, active=7; got {report:?}, active={active}"
    );
}

// PASS: with the exclusion lists EMPTY, the 102,871-wallet cohort is dropped
//       purely by the magnitude cap (10,000) before its (distinct) wallet is
//       fetched, so that distinct wallet never reaches the cache.
// FAIL: the over-cap cohort is fetched and its distinct wallet is ingested.
#[tokio::test]
async fn magnitude_cap_drops_huge_cohort_on_real_data() {
    let (_d, mut cache) = tmp_cache();

    let cohorts = parse_list_cohorts(LIST_COHORTS).unwrap();
    let real_addrs = parse_list_cohort_wallets(LIST_WALLETS).unwrap();

    // The over-cap cohort gets a DISTINCT address; if the cap fails to drop it,
    // this address would leak into the cache and bump `unique_wallets` to 8.
    let cap_canary = format!("0x{:040x}", 0xDEADu32);
    let wallets: HashMap<String, Vec<String>> = cohorts
        .iter()
        .map(|c| {
            if c.id == EXCLUDE_ID {
                (c.id.clone(), vec![cap_canary.clone()])
            } else {
                (c.id.clone(), real_addrs.clone())
            }
        })
        .collect();
    let fetcher = FixtureCohortFetcher::new(cohorts, wallets);

    // Empty exclusion lists — only the magnitude cap can drop the huge cohort.
    let report = run_datadash_discovery(&fetcher, &[], &[], 10_000, &mut cache)
        .await
        .unwrap();

    let ingested = cache
        .wallets_with_source_bit(pe_bootstrap::pile::SRC_DATADASH)
        .unwrap();
    let canary_leaked = ingested.contains(&cap_canary);
    let pass = report.cohorts_kept == 8 && report.unique_wallets == 7 && !canary_leaked;
    println!(
        "{}: magnitude_cap_drops_huge_cohort_on_real_data \
         (kept={}, unique={}, canary_leaked={})",
        if pass { "PASS" } else { "FAIL" },
        report.cohorts_kept,
        report.unique_wallets,
        canary_leaked,
    );
    assert!(
        pass,
        "expected kept=8, unique=7, no canary leak; got {report:?}, canary_leaked={canary_leaked}"
    );
}
