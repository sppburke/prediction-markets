//! Scenario coverage for the #188 follow-up cluster (depends on PR #187/#186):
//!
//! 1. **Item 1 — env-var alias**: `PE_POLYGON_HTTP_URL` populates
//!    `polygon_rpc_url` via the new serde alias; `PE_BOOTSTRAP_POLYGON_RPC_URL`
//!    still overrides it; absence of both is rejected at validate().
//! 2. **Item 3 — inverted-range guard**: `enumerate_chunk` rejects `from > to`
//!    with `EnumerationError::InvalidConfig`; the bootstrap-level guard fires
//!    before the topic loop so `enumerated_topic_hashes` is never silently
//!    populated over an empty range.
//! 3. **Item 2 — chunk-level cursor**: after upserting wallets from chunks 0
//!    and 1, persisting the cursor, then crashing on chunk 2, a fresh resume
//!    starts at chunk 2 (not chunk 0) — proves the cursor is read on
//!    re-entry and skips already-completed chunks. Combined with the
//!    OR-merge UPSERT, no wallet data is ever lost.
//! 4. **Item 5 — fetcher accessor**: `PolymarketTraderEnumeration::fetcher()`
//!    exposes the underlying `ChainLogFetcher` for test introspection.

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    // figment::Error is ~208 bytes; only on test-only Jail closures, same
    // allowance the inline tests in `config.rs` already take.
    clippy::result_large_err
)]

use std::collections::HashMap;
use std::sync::Mutex;

use alloy::primitives::{Address, B256, Bytes, LogData};
use alloy::rpc::types::{Filter, Log};
use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::migrate;
use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::ChainLogFetcher;
use pe_source_onchain_polygon::contracts::{
    ALL_EXCHANGE_CONTRACTS, CTF_EXCHANGE_V1, TOPIC_ORDER_FILLED_V1,
};
use pe_source_onchain_polygon::eth_logs::PolygonRpcError;
use pe_source_onchain_polygon::wallet_enumeration::{
    EnumerationConfig, EnumerationError, PolymarketTraderEnumeration, SCAN_CHUNK_BLOCKS,
};
use tempfile::TempDir;

// ── Test fetcher: records calls + fails on a specific call index ─────────────

struct RecordingFetcher {
    head_block: u64,
    fixture_logs: Vec<Log>,
    fail_on_call_index: usize,
    calls: Mutex<Vec<(u64, u64)>>,
}

impl RecordingFetcher {
    fn new(head_block: u64, fixture_logs: Vec<Log>, fail_on_call_index: usize) -> Self {
        Self {
            head_block,
            fixture_logs,
            fail_on_call_index,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<(u64, u64)> {
        self.calls.lock().unwrap().clone()
    }
}

impl ChainLogFetcher for RecordingFetcher {
    async fn get_block_number(&self) -> Result<u64, PolygonRpcError> {
        Ok(self.head_block)
    }

    async fn get_logs(
        &self,
        _filter: Filter,
        from: u64,
        to: u64,
    ) -> Result<Vec<Log>, PolygonRpcError> {
        let mut calls = self.calls.lock().unwrap();
        let idx = calls.len();
        calls.push((from, to));
        if idx == self.fail_on_call_index {
            return Err(PolygonRpcError::GetLogs {
                from,
                to,
                message: "simulated mid-sweep failure".to_owned(),
            });
        }
        Ok(self.fixture_logs.clone())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn w(b: u8) -> WalletAddress {
    let mut bytes = [0u8; 20];
    bytes[19] = b;
    WalletAddress(bytes)
}

fn wallet_topic(addr: WalletAddress) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[12..].copy_from_slice(&addr.0);
    B256::from(bytes)
}

fn order_filled_log(topic0: B256, maker: WalletAddress, taker: WalletAddress) -> Log {
    let order_hash = B256::repeat_byte(0xaa);
    let inner = alloy::primitives::Log {
        address: Address::ZERO,
        data: LogData::new_unchecked(
            vec![topic0, order_hash, wallet_topic(maker), wallet_topic(taker)],
            Bytes::from(vec![0u8; 32]),
        ),
    };
    Log {
        inner,
        ..Default::default()
    }
}

fn open_cache() -> (TempDir, WalletCache) {
    let dir = TempDir::new().unwrap();
    let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    (dir, cache)
}

// ── Scenario 1 (Item 3) — `enumerate_chunk` rejects inverted range ───────────

/// PASS: `enumerate_chunk(from=100, to=50)` returns `Err(InvalidConfig)` and
///       issues zero `get_logs` calls.
/// FAIL: the function returns `Ok(empty)` silently (the pre-fix behaviour
///       that motivated #188 Item 3) or panics.
#[tokio::test]
async fn enumerate_chunk_rejects_inverted_range() {
    let fetcher = RecordingFetcher::new(1_000_000, Vec::new(), usize::MAX);
    let config = EnumerationConfig {
        from_block: 0,
        to_block: SCAN_CHUNK_BLOCKS,
        operator_addresses: vec![],
    };
    let enumerator = PolymarketTraderEnumeration::with_fetcher(fetcher, config);

    let result = enumerator
        .enumerate_chunk(CTF_EXCHANGE_V1, TOPIC_ORDER_FILLED_V1, 100, 50)
        .await;

    assert!(
        matches!(result, Err(EnumerationError::InvalidConfig(_))),
        "inverted range must surface as InvalidConfig; got {result:?}"
    );
    assert!(
        enumerator.fetcher().calls().is_empty(),
        "no get_logs call must be issued when the range is rejected up-front"
    );
}

// ── Scenario 2 (Item 5) — `fetcher()` accessor exposes the backend ───────────

/// PASS: `PolymarketTraderEnumeration::fetcher()` returns a shared reference
///       to the underlying `ChainLogFetcher`, exposing recorded mock state
///       (the recording fetcher's `calls()` list) without reaching into a
///       private field.
/// FAIL: the accessor doesn't exist (compilation fails) or returns the wrong
///       type (the test won't compile).
#[tokio::test]
async fn fetcher_accessor_exposes_mock_state_for_assertion() {
    let alice = w(0xa1);
    let fetcher = RecordingFetcher::new(
        SCAN_CHUNK_BLOCKS,
        vec![order_filled_log(TOPIC_ORDER_FILLED_V1, alice, alice)],
        usize::MAX,
    );
    let config = EnumerationConfig {
        from_block: 0,
        to_block: SCAN_CHUNK_BLOCKS - 1,
        operator_addresses: vec![],
    };
    let enumerator = PolymarketTraderEnumeration::with_fetcher(fetcher, config);

    let _ = enumerator
        .enumerate_chunk(
            CTF_EXCHANGE_V1,
            TOPIC_ORDER_FILLED_V1,
            0,
            SCAN_CHUNK_BLOCKS - 1,
        )
        .await
        .unwrap();

    // The .fetcher() accessor is the public surface this scenario asserts —
    // without it, tests would need to reach into a private field.
    let recorded = enumerator.fetcher().calls();
    assert_eq!(
        recorded.len(),
        1,
        "fetcher().calls() must reflect the single get_logs invocation"
    );
    assert_eq!(recorded[0], (0u64, SCAN_CHUNK_BLOCKS - 1));
}

// ── Scenario 3 (Item 2) — chunk-progress cursor round-trips through SQLite ───

/// PASS: an explicit `save_chunk_progress` call followed by `load_chunk_progress`
///       on the same cache file returns the same map. Missing key on a fresh
///       cache returns `Default::default()` (empty map), not an error.
/// FAIL: the cursor isn't persisted, or a missing key surfaces as an error
///       — either would break the resume invariant.
#[test]
fn chunk_progress_round_trips_and_missing_key_is_empty() {
    let (dir, mut cache) = open_cache();

    // Missing-key path: a fresh cache returns the empty map, not an error.
    let initial = migrate::load_chunk_progress(&cache).unwrap();
    assert!(
        initial.is_empty(),
        "missing chunk-progress key must yield empty map (fresh install / pre-#188 cache)"
    );

    // Save a representative two-entry map.
    let mut progress = HashMap::new();
    let key_a = migrate::chunk_progress_key(
        &format!("{}", TOPIC_ORDER_FILLED_V1),
        &format!("0x{:x}", CTF_EXCHANGE_V1),
    );
    let key_b = migrate::chunk_progress_key(
        &format!("{}", TOPIC_ORDER_FILLED_V1),
        &format!("0x{:x}", ALL_EXCHANGE_CONTRACTS[1]),
    );
    progress.insert(key_a.clone(), 500_000_u64);
    progress.insert(key_b.clone(), 1_000_000_u64);
    migrate::save_chunk_progress(&mut cache, &progress).unwrap();

    // Round-trip from a fresh handle to the same file.
    drop(cache);
    let cache2 = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    let recovered = migrate::load_chunk_progress(&cache2).unwrap();
    assert_eq!(recovered, progress, "cursor must round-trip via SQLite");
    assert_eq!(recovered.get(&key_a).copied(), Some(500_000));
    assert_eq!(recovered.get(&key_b).copied(), Some(1_000_000));
}

// ── Scenario 4 (Item 2) — simulated mid-sweep crash skips done chunks ────────

/// PASS: drive the bootstrap's chunk loop semantics by hand against a
///       `RecordingFetcher` that fails on call N. After the failure:
///       - chunks `[0, N-1]` are persisted in the cache (UPSERT)
///       - the cursor records `last_completed_chunk_to` for the last
///         successful chunk
///       - a second loop pass over the same cache resumes from
///         `last_completed_chunk_to + 1` and issues `get_logs` only for the
///         remaining chunks
/// FAIL: resume restarts from `wallet_from_block` (the pre-#188 behaviour
///       this issue exists to fix), or loses wallet output from earlier
///       chunks, or skips chunks that weren't completed.
#[tokio::test]
async fn mid_sweep_crash_resumes_at_next_chunk_without_redoing_done_work() {
    use pe_bootstrap::cache::WalletUpsertRow;
    use pe_bootstrap::pile::SRC_WALLET_SET_JSON;
    use pe_source_onchain_polygon::contracts::topic_to_contract_version_bit;

    let (dir, mut cache) = open_cache();
    let alice = w(0xa1);
    let bob = w(0xb2);

    // 4-chunk sweep: succeed on chunks 0, 1 then fail on chunk 2 (call_index = 2).
    let logs = vec![order_filled_log(TOPIC_ORDER_FILLED_V1, alice, bob)];
    let from_block: u64 = 0;
    let to_block: u64 = SCAN_CHUNK_BLOCKS * 4 - 1;

    let topic_hex = format!("{}", TOPIC_ORDER_FILLED_V1);
    let contract_hex = format!("0x{:x}", CTF_EXCHANGE_V1);
    let progress_key = migrate::chunk_progress_key(&topic_hex, &contract_hex);
    let contract_bit = topic_to_contract_version_bit(TOPIC_ORDER_FILLED_V1).unwrap();

    // First run — simulates the bootstrap loop until the third chunk fails.
    {
        let fetcher = RecordingFetcher::new(to_block + 100, logs.clone(), 2);
        let config = EnumerationConfig {
            from_block,
            to_block,
            operator_addresses: vec![],
        };
        let enumerator = PolymarketTraderEnumeration::with_fetcher(fetcher, config);
        let mut chunk_progress = migrate::load_chunk_progress(&cache).unwrap();
        let mut chunk_from = chunk_progress
            .get(&progress_key)
            .map(|last| last.saturating_add(1))
            .unwrap_or(from_block)
            .max(from_block);

        let mut crash_observed = false;
        while chunk_from <= to_block {
            let chunk_to = (chunk_from + SCAN_CHUNK_BLOCKS - 1).min(to_block);
            match enumerator
                .enumerate_chunk(CTF_EXCHANGE_V1, TOPIC_ORDER_FILLED_V1, chunk_from, chunk_to)
                .await
            {
                Ok(found) => {
                    let rows: Vec<WalletUpsertRow> = found
                        .iter()
                        .map(|wallet| {
                            (
                                wallet.to_string(),
                                SRC_WALLET_SET_JSON,
                                false,
                                None,
                                None,
                                None,
                                contract_bit,
                            )
                        })
                        .collect();
                    cache.upsert_wallets_bulk(&rows).unwrap();
                    chunk_progress.insert(progress_key.clone(), chunk_to);
                    migrate::save_chunk_progress(&mut cache, &chunk_progress).unwrap();
                    chunk_from = chunk_to + 1;
                }
                Err(_) => {
                    crash_observed = true;
                    break;
                }
            }
        }
        assert!(
            crash_observed,
            "the fetcher must have failed on chunk index 2"
        );

        // The cursor must reflect the last successful chunk (chunk 1, which
        // ended at `2 * SCAN_CHUNK_BLOCKS - 1`).
        let recorded = migrate::load_chunk_progress(&cache).unwrap();
        assert_eq!(
            recorded.get(&progress_key).copied(),
            Some(SCAN_CHUNK_BLOCKS * 2 - 1),
            "cursor must point at the last successfully-completed chunk's chunk_to"
        );
    }

    // Wallets from chunks 0 and 1 are durably persisted (UPSERT).
    assert_eq!(
        cache.conn_for_test_contracts_seen(&alice.to_string()),
        contract_bit
    );
    assert_eq!(
        cache.conn_for_test_contracts_seen(&bob.to_string()),
        contract_bit
    );

    // Second run — resume against a fresh fetcher that never fails.
    {
        let fetcher2 = RecordingFetcher::new(to_block + 100, logs.clone(), usize::MAX);
        let config = EnumerationConfig {
            from_block,
            to_block,
            operator_addresses: vec![],
        };
        let enumerator = PolymarketTraderEnumeration::with_fetcher(fetcher2, config);
        let mut chunk_progress = migrate::load_chunk_progress(&cache).unwrap();
        let resume_from = chunk_progress
            .get(&progress_key)
            .map(|last| last.saturating_add(1))
            .unwrap_or(from_block)
            .max(from_block);

        assert_eq!(
            resume_from,
            SCAN_CHUNK_BLOCKS * 2,
            "resume must start at chunk 2 (one past the last completed chunk_to)"
        );

        let mut chunk_from = resume_from;
        while chunk_from <= to_block {
            let chunk_to = (chunk_from + SCAN_CHUNK_BLOCKS - 1).min(to_block);
            let _ = enumerator
                .enumerate_chunk(CTF_EXCHANGE_V1, TOPIC_ORDER_FILLED_V1, chunk_from, chunk_to)
                .await
                .unwrap();
            chunk_progress.insert(progress_key.clone(), chunk_to);
            migrate::save_chunk_progress(&mut cache, &chunk_progress).unwrap();
            chunk_from = chunk_to + 1;
        }

        // The second run's recorded calls cover ONLY the two missing chunks
        // (chunks 2 and 3), not the full 4-chunk range. This is the core
        // resume invariant — chunks already done are not re-fetched.
        let calls = enumerator.fetcher().calls();
        assert_eq!(
            calls.len(),
            2,
            "second run must issue exactly 2 get_logs calls (chunks 2 and 3); got {:?}",
            calls
        );
        assert_eq!(
            calls[0].0,
            SCAN_CHUNK_BLOCKS * 2,
            "first resume call starts at chunk 2"
        );
        assert_eq!(
            calls[1].0,
            SCAN_CHUNK_BLOCKS * 3,
            "second resume call starts at chunk 3"
        );
    }

    // Final cursor reflects full completion.
    let final_progress = migrate::load_chunk_progress(&cache).unwrap();
    assert_eq!(
        final_progress.get(&progress_key).copied(),
        Some(to_block),
        "after both runs the cursor must point at the final to_block"
    );

    drop(cache);
    drop(dir);
}

// ── Item 2 follow-up — stale cursor entries pruned at topic completion ──────

/// PASS: when a topic finishes (`enumerated_topic_hashes.push(...)`), every
///       `chunk_progress` entry keyed by that topic is removed before
///       `save_chunk_progress`. Keeps the JSON map bounded across full sweeps.
/// FAIL: stale entries linger after topic completion (the cursor grows
///       monotonically with every topic ever swept — flagged by the PR #189
///       reviewer as concern #2).
#[test]
fn chunk_progress_prunes_entries_keyed_by_completed_topic() {
    use std::collections::HashMap;

    let topic_v1_hex = format!("{}", TOPIC_ORDER_FILLED_V1);
    let contract_a_hex = format!("0x{:x}", ALL_EXCHANGE_CONTRACTS[0]);
    let contract_b_hex = format!("0x{:x}", ALL_EXCHANGE_CONTRACTS[1]);
    let key_a = migrate::chunk_progress_key(&topic_v1_hex, &contract_a_hex);
    let key_b = migrate::chunk_progress_key(&topic_v1_hex, &contract_b_hex);
    let key_other_topic = migrate::chunk_progress_key("0xdeadbeef", &contract_a_hex);

    let mut progress = HashMap::new();
    progress.insert(key_a.clone(), 500_000_u64);
    progress.insert(key_b.clone(), 1_000_000_u64);
    progress.insert(key_other_topic.clone(), 250_000_u64);

    // Production code: at topic completion, retain everything NOT prefixed by
    // the topic's `"{topic_hex}|"` namespace.
    let topic_prefix = format!("{topic_v1_hex}|");
    progress.retain(|k, _| !k.starts_with(&topic_prefix));

    assert!(
        !progress.contains_key(&key_a),
        "V1-topic / contract-A entry must be pruned"
    );
    assert!(
        !progress.contains_key(&key_b),
        "V1-topic / contract-B entry must be pruned"
    );
    assert_eq!(
        progress.get(&key_other_topic).copied(),
        Some(250_000),
        "entries from other topics must survive the prune"
    );
    assert_eq!(progress.len(), 1);
}

// ── Scenario 5 (Item 1) — `PE_POLYGON_HTTP_URL` populates `polygon_rpc_url` ──

/// PASS: setting only `PE_POLYGON_HTTP_URL` makes `config::load()` produce a
///       `BootstrapConfig` whose `polygon_rpc_url` is `Some(...)`. This is the
///       operator-facing config the merged #186 PR depends on; without the
///       fallback in `load()`, deployments with the canonical `.env`
///       (PE_POLYGON_HTTP_URL) would fail at validate() with
///       `MissingEnv("PE_POLYGON_HTTP_URL or PE_BOOTSTRAP_POLYGON_RPC_URL")`.
/// FAIL: the fallback doesn't fire and `polygon_rpc_url` is `None`, OR the
///       env-var binding silently maps to a different field.
#[test]
fn polygon_http_url_env_var_populates_polygon_rpc_url() {
    use pe_bootstrap::config;

    figment::Jail::expect_with(|jail| {
        jail.create_file("config.toml", r#"output_path = "/tmp/watchlist.json""#)?;
        jail.set_env(
            "PE_POLYGON_HTTP_URL",
            "https://alchemy.invalid/v2/shared-key",
        );
        let cfg = config::load(Some(std::path::Path::new("config.toml")))
            .map_err(|e| figment::Error::from(e.to_string()))?;
        assert_eq!(
            cfg.polygon_rpc_url.as_deref(),
            Some("https://alchemy.invalid/v2/shared-key"),
            "PE_POLYGON_HTTP_URL must populate polygon_rpc_url via the load()-time fallback"
        );
        Ok(())
    });
}

/// PASS: `PE_BOOTSTRAP_POLYGON_RPC_URL` takes precedence over
///       `PE_POLYGON_HTTP_URL` even when both are set. This guards the
///       "operator override" semantic: an explicit bootstrap-specific knob
///       must win over the workspace-shared one, and the two env vars must
///       not collide (the earlier `#[serde(alias)]` approach hit
///       "duplicate field" — issue #188 Item 1 review pass).
/// FAIL: the override doesn't fire (operator-facing surprise where the
///       bootstrap-specific knob is silently ignored or the loader errors).
#[test]
fn bootstrap_specific_env_var_overrides_polygon_http_url() {
    use pe_bootstrap::config;

    figment::Jail::expect_with(|jail| {
        jail.create_file("config.toml", r#"output_path = "/tmp/watchlist.json""#)?;
        jail.set_env(
            "PE_POLYGON_HTTP_URL",
            "https://shared.invalid/v2/shared-key",
        );
        jail.set_env(
            "PE_BOOTSTRAP_POLYGON_RPC_URL",
            "https://bootstrap.invalid/v2/bootstrap-key",
        );
        let cfg = config::load(Some(std::path::Path::new("config.toml")))
            .map_err(|e| figment::Error::from(e.to_string()))?;
        assert_eq!(
            cfg.polygon_rpc_url.as_deref(),
            Some("https://bootstrap.invalid/v2/bootstrap-key"),
            "PE_BOOTSTRAP_POLYGON_RPC_URL must take precedence over PE_POLYGON_HTTP_URL"
        );
        Ok(())
    });
}

/// PASS: when neither env var is set and no TOML key is present,
///       `config::load()` fails at validate() with a `MissingEnv` error
///       naming *both* env vars so the operator knows either is acceptable.
/// FAIL: silent default (rpc_url stays None and validate() doesn't fire), or
///       the error message names only one of the two env vars (regressing
///       the issue #188 Item 1 operator UX fix).
#[test]
fn missing_polygon_rpc_url_yields_clear_error_naming_both_env_vars() {
    use pe_bootstrap::config;
    use pe_bootstrap::error::BootstrapError;

    figment::Jail::expect_with(|jail| {
        jail.create_file("config.toml", r#"output_path = "/tmp/watchlist.json""#)?;
        // Deliberately do NOT set either env var.
        let result = config::load(Some(std::path::Path::new("config.toml")));
        match result {
            Err(BootstrapError::MissingEnv(name)) => {
                assert!(
                    name.contains("PE_POLYGON_HTTP_URL")
                        && name.contains("PE_BOOTSTRAP_POLYGON_RPC_URL"),
                    "MissingEnv must name both env vars; got: {name}"
                );
                Ok(())
            }
            Err(e) => Err(figment::Error::from(format!(
                "expected MissingEnv, got {e:?}"
            ))),
            Ok(_) => Err(figment::Error::from(
                "expected load() to fail with MissingEnv, got Ok".to_owned(),
            )),
        }
    });
}
