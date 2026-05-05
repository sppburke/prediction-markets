//! Load and merge an optional bootstrap seed watchlist with the leaderboard result.

use std::collections::HashSet;
use std::io::ErrorKind;
use std::path::Path;

use anyhow::Context;
use pe_trader_index::{Watchlist, WatchlistTier};
use tracing::warn;

/// Load a seed [`Watchlist`] from `path`.
///
/// Returns `Ok(Some(_))` when the file exists and parses, `Ok(None)` when
/// `path` is empty or the file is absent (warns), and `Err` when the file
/// exists but cannot be read or parsed.
pub fn load_seed_watchlist(path: &str) -> anyhow::Result<Option<Watchlist>> {
    if path.is_empty() {
        return Ok(None);
    }
    match std::fs::read(Path::new(path)) {
        Ok(bytes) => {
            let wl =
                serde_json::from_slice::<Watchlist>(&bytes).context("parse seed watchlist JSON")?;
            Ok(Some(wl))
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            warn!(
                path = %path,
                "seed_watchlist_path set but file not found; using leaderboard only"
            );
            Ok(None)
        }
        Err(e) => Err(e).context("read seed_watchlist_path"),
    }
}

/// Merge `leaderboard` and an optional `seed` watchlist into a single
/// [`Watchlist`]. Leaderboard entries appear first (order preserved);
/// seed-only entries are appended in their original order. Duplicates
/// (by wallet address) are dropped from the seed — the leaderboard entry wins.
///
/// The returned [`Watchlist`] carries full [`pe_trader_index::WatchlistEntry`]
/// metadata for every wallet, so the Orchestrator can look up tier, scores,
/// and operator identity for seed-only wallets.
pub fn merge_watchlist(leaderboard: &Watchlist, seed: Option<&Watchlist>) -> Watchlist {
    let capacity = leaderboard.entries.len() + seed.map_or(0, |s| s.entries.len());
    let mut seen = HashSet::with_capacity(capacity);
    let mut entries = Vec::with_capacity(capacity);

    for entry in leaderboard
        .entries
        .iter()
        .chain(seed.into_iter().flat_map(|s| s.entries.iter()))
    {
        if seen.insert(entry.wallet) {
            entries.push(entry.clone());
        }
    }

    let active_count = entries
        .iter()
        .filter(|e| e.tier == WatchlistTier::Active)
        .count();
    let incubator_count = entries
        .iter()
        .filter(|e| e.tier == WatchlistTier::Incubator)
        .count();

    Watchlist {
        entries,
        snapshot_at: leaderboard.snapshot_at.clone(),
        active_count,
        incubator_count,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_core_types::{BasisPoints, ReconstructionQuality, SourceTimestamp, WalletAddress};
    use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};

    fn wallet(n: u8) -> WalletAddress {
        let mut bytes = [0u8; 20];
        bytes[19] = n;
        WalletAddress(bytes)
    }

    fn entry(w: WalletAddress) -> WatchlistEntry {
        WatchlistEntry {
            wallet: w,
            operator_id: None,
            tier: WatchlistTier::Active,
            leader_score_bps: BasisPoints(0),
            lcb_5pct_bps: BasisPoints(0),
            closed_trades_in_window: 0,
            reconstruction_quality: ReconstructionQuality::new(0).unwrap(),
        }
    }

    fn wl(wallets: &[WalletAddress]) -> Watchlist {
        let entries: Vec<_> = wallets.iter().copied().map(entry).collect();
        let n = entries.len();
        Watchlist {
            entries,
            snapshot_at: SourceTimestamp(time::OffsetDateTime::UNIX_EPOCH),
            active_count: n,
            incubator_count: 0,
        }
    }

    #[test]
    fn merge_no_seed() {
        let a = wallet(1);
        let result = merge_watchlist(&wl(&[a]), None);
        assert_eq!(
            result.entries.iter().map(|e| e.wallet).collect::<Vec<_>>(),
            vec![a]
        );
    }

    #[test]
    fn merge_disjoint() {
        let (a, b) = (wallet(1), wallet(2));
        let result = merge_watchlist(&wl(&[a]), Some(&wl(&[b])));
        assert_eq!(
            result.entries.iter().map(|e| e.wallet).collect::<Vec<_>>(),
            vec![a, b]
        );
    }

    #[test]
    fn merge_overlap_leaderboard_wins() {
        let (a, b) = (wallet(1), wallet(2));
        // b in both — leaderboard slot wins; appears exactly once
        let result = merge_watchlist(&wl(&[a, b]), Some(&wl(&[b])));
        assert_eq!(
            result.entries.iter().map(|e| e.wallet).collect::<Vec<_>>(),
            vec![a, b]
        );
    }

    #[test]
    fn merge_counts_recomputed() {
        let (a, b) = (wallet(1), wallet(2));
        let lb = wl(&[a]);
        let mut seed_wl = wl(&[b]);
        seed_wl.entries[0].tier = WatchlistTier::Incubator;
        seed_wl.active_count = 0;
        seed_wl.incubator_count = 1;
        let result = merge_watchlist(&lb, Some(&seed_wl));
        assert_eq!(result.active_count, 1);
        assert_eq!(result.incubator_count, 1);
    }

    #[test]
    fn load_empty_path_returns_none() {
        assert!(load_seed_watchlist("").unwrap().is_none());
    }

    #[test]
    fn load_missing_file_returns_none() {
        assert!(
            load_seed_watchlist("/tmp/pe-seed-no-such-file-xzy123.json")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn load_malformed_json_errors() {
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(f.path(), b"not json").unwrap();
        assert!(load_seed_watchlist(f.path().to_str().unwrap()).is_err());
    }
}
