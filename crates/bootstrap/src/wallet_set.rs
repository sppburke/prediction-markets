//! Wallet set cache — persists the enumerated wallet address list to disk.
//!
//! Atomic write: serialize to `<path>.tmp`, then `rename` to `<path>`.
//! No TTL; delete the file to force re-enumeration.
//!
//! Two on-disk formats are understood:
//!
//! **Current (checkpoint) format** — written by this module:
//! ```json
//! {
//!   "completed_contracts": ["0xabc..."],
//!   "wallets": ["0x111...", ...],
//!   "enumerated_topic_hashes": ["0xd0a0...", "0xd543..."]
//! }
//! ```
//!
//! The `enumerated_topic_hashes` field (issue #179) carries `#[serde(default)]`
//! so pre-#179 checkpoints (no field) load with an empty `Vec`; callers treat
//! that exact state as "legacy V1-only complete; V2 enumeration pending" and
//! kick off an additive sweep.
//!
//! **Legacy format** — written by pre-checkpoint bootstrap runs (PR #68):
//! ```json
//! ["0xabc...", "0xdef...", ...]
//! ```
//! `load_state` returns `None` for legacy files so the caller can call
//! `load` (bare-array path) and upgrade to the current format.

use std::path::Path;

use pe_core_types::WalletAddress;
use serde::{Deserialize, Serialize};

use crate::error::BootstrapError;

/// Per-contract checkpoint state persisted between bootstrap runs.
///
/// Enumeration proceeds (topic, contract) pair by pair (issue #179); after
/// each pair the state is saved atomically. On startup, topics already in
/// `enumerated_topic_hashes` are skipped entirely.
///
/// `completed_contracts` is preserved for backward-compat detection of
/// legacy (pre-#179) checkpoints — see `lib.rs` for the
/// "legacy V1-only done" upgrade path. New runs do NOT append to it.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct WalletSetState {
    /// Lowercase-hex contract addresses whose enumeration is complete (legacy
    /// V1-only signal; superseded by `enumerated_topic_hashes` for new runs).
    pub completed_contracts: Vec<String>,
    /// Accumulated wallet addresses (hex) — union across every completed
    /// (topic, contract) pair. Load-time dedup happens in the caller.
    pub wallets: Vec<String>,
    /// Lowercase-hex `OrderFilled` topic0 hashes (B256 Display format —
    /// includes the `0x` prefix) that have been fully enumerated across
    /// `ALL_EXCHANGE_CONTRACTS`. Issue #179.
    ///
    /// `#[serde(default)]` makes pre-#179 JSON files load with an empty
    /// `Vec`; the bootstrap orchestrator treats that exact state combined
    /// with a full `completed_contracts` set as "V1 done, V2 pending".
    #[serde(default)]
    pub enumerated_topic_hashes: Vec<String>,
}

/// Load the checkpoint state from `path`.
///
/// Returns `None` if the file does not exist or is in the legacy bare-array
/// format (the caller should fall back to [`load`] + upgrade in that case).
pub fn load_state(path: &Path) -> Result<Option<WalletSetState>, BootstrapError> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path)?;
    // If parsing as WalletSetState fails (e.g. legacy bare-array), return None
    // so the caller can try the legacy path.
    match serde_json::from_slice::<WalletSetState>(&bytes) {
        Ok(s) => Ok(Some(s)),
        Err(_) => Ok(None),
    }
}

/// Atomically write the checkpoint state to `path`.
pub fn save_state(path: &Path, state: &WalletSetState) -> Result<(), BootstrapError> {
    let json = serde_json::to_vec_pretty(state)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &json)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Load the wallet set from `path` (legacy bare-array format).
///
/// Returns `None` if the file does not exist, triggering a fresh enumeration
/// by the caller.
pub fn load(path: &Path) -> Result<Option<Vec<WalletAddress>>, BootstrapError> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path)?;
    let hexes: Vec<String> = serde_json::from_slice(&bytes).map_err(|e| BootstrapError::Cache {
        message: format!("parse wallet set at {}: {e}", path.display()),
    })?;
    let wallets = hexes
        .iter()
        .filter_map(|h| {
            WalletAddress::from_hex(h)
                .map_err(|e| {
                    tracing::warn!(address = %h, error = %e, "wallet set: skipping unparseable address");
                })
                .ok()
        })
        .collect();
    Ok(Some(wallets))
}

/// Atomically write the wallet set to `path` (legacy bare-array format).
///
/// Retained for callers that only need the flat address list.
pub fn save(path: &Path, wallets: &[WalletAddress]) -> Result<(), BootstrapError> {
    let hexes: Vec<String> = wallets.iter().map(|w| w.to_string()).collect();
    let json = serde_json::to_vec_pretty(&hexes)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &json)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn addr(hex: &str) -> WalletAddress {
        WalletAddress::from_hex(hex).unwrap()
    }

    // ── load / save (legacy) ────────────────────────────────────────────────

    #[test]
    fn load_nonexistent_returns_none() {
        let dir = TempDir::new().unwrap();
        let result = load(&dir.path().join("wallets.json")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn round_trip_preserves_all_addresses() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wallets.json");
        let wallets = vec![
            addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        ];
        save(&path, &wallets).unwrap();
        let loaded = load(&path).unwrap().unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded.contains(&addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")));
        assert!(loaded.contains(&addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")));
    }

    #[test]
    fn empty_set_round_trips() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wallets.json");
        save(&path, &[]).unwrap();
        let loaded = load(&path).unwrap().unwrap();
        assert!(loaded.is_empty());
    }

    // ── load_state / save_state ─────────────────────────────────────────────

    #[test]
    fn load_state_nonexistent_returns_none() {
        let dir = TempDir::new().unwrap();
        let result = load_state(&dir.path().join("wallets.json")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn state_round_trip_preserves_contracts_and_wallets() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wallets.json");
        let state = WalletSetState {
            completed_contracts: vec!["0xabc".to_owned(), "0xdef".to_owned()],
            wallets: vec![
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            ],
            enumerated_topic_hashes: vec![
                "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6".to_owned(),
            ],
        };
        save_state(&path, &state).unwrap();
        let loaded = load_state(&path).unwrap().unwrap();
        assert_eq!(loaded.completed_contracts, state.completed_contracts);
        assert_eq!(loaded.wallets, state.wallets);
        assert_eq!(
            loaded.enumerated_topic_hashes,
            state.enumerated_topic_hashes
        );
    }

    #[test]
    fn state_default_is_empty() {
        let s = WalletSetState::default();
        assert!(s.completed_contracts.is_empty());
        assert!(s.wallets.is_empty());
        assert!(s.enumerated_topic_hashes.is_empty());
    }

    /// Pre-#179 JSON has no `enumerated_topic_hashes` field. Verify
    /// `#[serde(default)]` makes it load with an empty `Vec` (the
    /// "legacy V1-only done" sentinel) rather than failing.
    #[test]
    fn state_loads_pre_179_json_with_empty_topics() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wallets.json");
        // Exact shape the old binary wrote — no `enumerated_topic_hashes` key.
        let legacy_json = br#"{"completed_contracts":["0xabc"],"wallets":["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]}"#;
        std::fs::write(&path, legacy_json).unwrap();
        let loaded = load_state(&path).unwrap().unwrap();
        assert_eq!(loaded.completed_contracts, vec!["0xabc"]);
        assert_eq!(loaded.wallets.len(), 1);
        assert!(
            loaded.enumerated_topic_hashes.is_empty(),
            "pre-#179 JSON must load with empty enumerated_topic_hashes"
        );
    }

    #[test]
    fn load_state_returns_none_for_legacy_bare_array() {
        // A file written by pre-checkpoint bootstrap (PR #68) is a bare JSON array.
        // load_state must return None so the caller can fall back to load().
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wallets.json");
        let legacy: Vec<&str> = vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"];
        std::fs::write(&path, serde_json::to_vec(&legacy).unwrap()).unwrap();

        let result = load_state(&path).unwrap();
        assert!(
            result.is_none(),
            "load_state should return None for legacy bare-array format"
        );
    }

    #[test]
    fn load_legacy_bare_array_succeeds() {
        // Verify the fallback path: load() still parses the old format.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wallets.json");
        let legacy = vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"];
        std::fs::write(&path, serde_json::to_vec(&legacy).unwrap()).unwrap();

        let wallets = load(&path).unwrap().unwrap();
        assert_eq!(wallets.len(), 1);
        assert_eq!(
            wallets[0],
            addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
    }

    #[test]
    fn save_state_is_atomic_tmp_rename() {
        // Verify the tmp file does not linger after a successful save.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wallets.json");
        let state = WalletSetState::default();
        save_state(&path, &state).unwrap();
        assert!(path.exists(), "final file must exist after save_state");
        assert!(
            !path.with_extension("json.tmp").exists(),
            "tmp file must be removed after rename"
        );
    }
}
