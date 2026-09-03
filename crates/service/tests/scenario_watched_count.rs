//! Deterministic scenarios for the serialized watchlist projection (#544).
#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashSet;
use std::sync::Mutex;

use pe_core_types::{
    BasisPoints, ReconstructionQuality, SourceTimestamp, SourceTradeId, WalletAddress,
};
use pe_paper_state::{
    ActivityBucketCommit, ActivityDispositionRecord, PaperStateDb, WalletFenceRecord,
};
use pe_service::live_watchlist::{LiveWatchlist, projection_dirty_channel};
use pe_service::supabase_refresh::{
    ProjectionApply, ProjectionEntry, ProjectionError, RuntimeToken, WatchlistProjectionStatus,
    WatchlistProjector, effective_projection_entries, project_once,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use tempfile::TempDir;
use time::OffsetDateTime;

#[derive(Default)]
struct FakeState {
    token: String,
    rows: Vec<ProjectionEntry>,
    writes: usize,
}

#[derive(Default)]
struct FakeProjector {
    state: Mutex<FakeState>,
}

impl FakeProjector {
    fn seeded(token: &str) -> Self {
        Self {
            state: Mutex::new(FakeState {
                token: token.to_owned(),
                ..FakeState::default()
            }),
        }
    }

    fn external_advance(&self, token: &str) {
        self.state.lock().unwrap().token = token.to_owned();
    }

    fn rows(&self) -> Vec<ProjectionEntry> {
        self.state.lock().unwrap().rows.clone()
    }

    fn writes(&self) -> usize {
        self.state.lock().unwrap().writes
    }
}

impl WatchlistProjector for FakeProjector {
    async fn load_token(&self) -> Result<RuntimeToken, ProjectionError> {
        let state = self.state.lock().unwrap();
        Ok(RuntimeToken {
            token: state.token.clone(),
            count: state.rows.len(),
        })
    }

    async fn replace(
        &self,
        expected_token: &str,
        entries: &[ProjectionEntry],
    ) -> Result<ProjectionApply, ProjectionError> {
        let mut state = self.state.lock().unwrap();
        if state.token != expected_token {
            return Err(ProjectionError::Conflict);
        }
        state.writes = state.writes.saturating_add(1);
        state.rows = entries.to_vec();
        state.token = format!("token-{}", state.writes);
        Ok(ProjectionApply {
            new_token: state.token.clone(),
            count: state.rows.len(),
        })
    }
}

fn entry(seed: u8, score: i32) -> WatchlistEntry {
    WatchlistEntry {
        wallet: WalletAddress::from_hex(&format!("0x{seed:040x}")).unwrap(),
        tier: WatchlistTier::Active,
        leader_score_bps: BasisPoints(score),
        lcb_5pct_bps: BasisPoints(0),
        win_rate_bps: BasisPoints(0),
        closed_trades_in_window: 0,
        reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
    }
}

fn watchlist(entries: Vec<WatchlistEntry>) -> Watchlist {
    let total = entries.len();
    Watchlist {
        entries,
        snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
        active_count: total,
        incubator_count: 0,
    }
}

fn paper(dir: &TempDir) -> PaperStateDb {
    PaperStateDb::open(&dir.path().join("paper-state.db")).unwrap()
}

fn fence(paper: &PaperStateDb, wallet: WalletAddress) {
    let source_trade_id = SourceTradeId(format!("g2:{}", "a".repeat(64)));
    paper
        .commit_activity_bucket(&ActivityBucketCommit {
            wallet,
            source_epoch: 1,
            dispositions: vec![ActivityDispositionRecord {
                source_trade_id: source_trade_id.clone(),
                transaction_hash: format!("0x{}", "b".repeat(64)),
                wallet,
                source_epoch: 1,
                semantic_revision: "test".to_owned(),
                activity_type: "CONVERSION".to_owned(),
                disposition: "conversion_ambiguous".to_owned(),
                no_copy: None,
                proof_json: "{}".to_owned(),
            }],
            leader_positions: Vec::new(),
            gate_results: Vec::new(),
            history_effects: Vec::new(),
            history_status: None,
            pending: Vec::new(),
            fence: Some(WalletFenceRecord {
                wallet,
                source_trade_id,
                cause: "conversion_ambiguous".to_owned(),
                proof_json: "{}".to_owned(),
                fenced_at_unix: 1,
            }),
            reanchor: None,
            advance_cursor: true,
        })
        .unwrap();
}

#[tokio::test]
async fn score_only_refresh_persists_scores_and_rank_order() {
    let dir = TempDir::new().unwrap();
    let paper = paper(&dir);
    let (dirty, mut dirty_rx) = projection_dirty_channel();
    let live =
        LiveWatchlist::new_with_projection(watchlist(vec![entry(1, 100), entry(2, 200)]), dirty);
    let projector = FakeProjector::seeded("boot");
    let status = WatchlistProjectionStatus::default();
    let mut token = None;

    project_once(&live, &paper, &projector, &mut token, &status)
        .await
        .unwrap();
    live.apply_refresh(&watchlist(vec![entry(1, 900), entry(2, 300)]));
    dirty_rx.changed().await.unwrap();
    project_once(&live, &paper, &projector, &mut token, &status)
        .await
        .unwrap();

    assert_eq!(
        projector
            .rows()
            .iter()
            .map(|row| (row.wallet_hex.clone(), row.rank, row.leader_score_bps))
            .collect::<Vec<_>>(),
        vec![
            (entry(1, 0).wallet.to_string(), 1, 900),
            (entry(2, 0).wallet.to_string(), 2, 300),
        ]
    );
    let applied = status.snapshot().applied.clone().expect("applied status");
    assert_eq!((applied.token, applied.count), ("token-2".to_owned(), 2));
}

#[tokio::test]
async fn stale_writer_loses_without_writes_then_recovers_from_newest_snapshot() {
    let dir = TempDir::new().unwrap();
    let paper = paper(&dir);
    let live = LiveWatchlist::new(watchlist(vec![entry(1, 100)]));
    let projector = FakeProjector::seeded("boot");
    let status = WatchlistProjectionStatus::default();
    let mut token = None;
    project_once(&live, &paper, &projector, &mut token, &status)
        .await
        .unwrap();
    let writes_before_conflict = projector.writes();

    live.replace(&HashSet::from([entry(1, 0).wallet]), &[entry(2, 800)], 1);
    projector.external_advance("external");
    assert!(matches!(
        project_once(&live, &paper, &projector, &mut token, &status).await,
        Err(ProjectionError::Conflict)
    ));
    assert_eq!(projector.writes(), writes_before_conflict);
    assert!(token.is_none(), "conflict must discard the stale token");

    project_once(&live, &paper, &projector, &mut token, &status)
        .await
        .unwrap();
    assert_eq!(
        projector.rows()[0].wallet_hex,
        entry(2, 0).wallet.to_string()
    );
}

#[tokio::test]
async fn durable_fences_are_excluded_and_empty_is_a_valid_projection() {
    let dir = TempDir::new().unwrap();
    let paper = paper(&dir);
    let wallet = entry(1, 100).wallet;
    let live = LiveWatchlist::new(watchlist(vec![entry(1, 100)]));
    fence(&paper, wallet);
    assert!(
        effective_projection_entries(&live, &paper)
            .unwrap()
            .is_empty()
    );

    let projector = FakeProjector::seeded("boot");
    let status = WatchlistProjectionStatus::default();
    let mut token = None;
    project_once(&live, &paper, &projector, &mut token, &status)
        .await
        .unwrap();
    assert!(projector.rows().is_empty());
    assert_eq!(status.snapshot().applied.as_ref().unwrap().count, 0);
}

#[tokio::test]
async fn dirty_signal_coalesces_multiple_mutations_to_the_newest_generation() {
    let (dirty, mut dirty_rx) = projection_dirty_channel();
    let live = LiveWatchlist::new_with_projection(watchlist(vec![entry(1, 100)]), dirty);
    live.apply_refresh(&watchlist(vec![entry(1, 200)]));
    live.apply_refresh(&watchlist(vec![entry(1, 300)]));
    dirty_rx.changed().await.unwrap();
    assert_eq!(*dirty_rx.borrow_and_update(), 2);
    assert_eq!(live.snapshot().entries[0].leader_score_bps.0, 300);
}
