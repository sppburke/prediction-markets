//! Causal current-position validation for watchlist admission (#544).
//!
//! Venue positions own absolute balances at each proved anchor; ordered
//! activity owns exact causal effects after that anchor.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
use pe_copy_signal_engine::{PositionSnapshot, PositionState, SignalConfig};
use pe_core_types::{
    MarketId, MarketOutcomeId, OutcomeId, PolymarketTokenId, ReceivedAt, ReconstructionQuality,
    ShareAmount, SourceId, SourceTimestamp, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::{ContentType, EnvelopeIn};
use pe_paper_state::{
    MarketHistoryRecord, NoCopyDisposition, PaperStateDb, WalletFenceRecord,
    WalletHistoryStatusRecord,
};
use pe_position_ledger::LedgerEffect;
use pe_position_ledger::PositionLedger;
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    ACTIVITY_PARSER_VERSION, ACTIVITY_SCHEMA_VERSION, ActivityAssetMapping, ActivityParseError,
    ActivityReadError, ActivityValidationError, CompleteActivityRead, CompletePositionsRead,
    PositionClassification, PositionReadError, ReconciliationFetcher, fetch_complete_activity,
    fetch_complete_positions,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::asset_identity::{AssetIdentityResolver, BootSourceLog, IdentityProvenance};
use crate::bucket_commit::{
    AnchorInstallError, BucketCommitEngine, BucketDecisionContext, IdentityOverride,
};
use crate::orchestrator_control::{AdmissionLedgerCapture, OrchestratorControl};
use crate::trade_poller::ACTIVITY_POLL_SOURCE_ID;

#[cfg(feature = "scenario")]
type BracketStepHook = Arc<dyn Fn(WalletAddress, usize, &mut BucketCommitEngine) + Send + Sync>;

#[cfg(feature = "scenario")]
type AnchorInstallHook = Arc<dyn Fn(&[AnchorInstall]) + Send + Sync>;

pub const BRACKET_CONCURRENCY: usize = 4;

/// Routine refresh yields unseen post-anchor activity to ordinary reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationPurpose {
    CatchUp,
    RoutineRefresh { cutoff: i64 },
}

/// A venue-authoritative balance snapshot waiting for the single-owner install.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchorInstall {
    pub fresh_history: Vec<MarketHistoryRecord>,
    pub expected_fence: Option<WalletFenceRecord>,
    /// Runtime acceptance completes history in the anchor transaction; direct boot leaves this absent.
    pub history_status: Option<WalletHistoryStatusRecord>,
    pub wallet: WalletAddress,
    pub balances: Vec<(MarketId, OutcomeId, ShareAmount)>,
    pub cutoff: i64,
    pub proof: AnchorProof,
    pub expected: AnchorExpectation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchorProof {
    pub positions_proof_hash: String,
    pub activity_bounds_json: String,
    pub source_log_generation: String,
    pub document: String,
    pub recorded_at_unix: i64,
}

/// Whether an accepted anchor retained the original full-history request for
/// each bracket walk. Split pages can have narrower bounds and are not substitutes
/// for the original page-zero evidence. Older proof shapes require revalidation.
#[must_use]
pub fn anchor_proves_full_history(document: &str) -> bool {
    #[derive(Deserialize)]
    struct Proof {
        activity_walks: [Walk; 3],
    }
    #[derive(Deserialize)]
    struct Walk {
        fixed_end: i64,
        pages: Vec<Page>,
    }
    #[derive(Deserialize)]
    struct Page {
        offset: u32,
        bounds: Option<pe_source_polymarket_public::ActivityRequestBounds>,
    }

    serde_json::from_str::<Proof>(document).is_ok_and(|proof| {
        proof.activity_walks.iter().all(|walk| {
            walk.pages.iter().any(|page| {
                page.offset == 0
                    && page.bounds.is_some_and(|bounds| {
                        bounds.start == Some(0) && bounds.end == walk.fixed_end
                    })
            })
        })
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchorExpectation {
    pub ledger_hash: String,
    pub cursor: Option<i64>,
    pub anchor_seq: Option<i64>,
    pub coverage_generation: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum CausalPositionError {
    #[error("activity catch-up for {wallet}: {source}")]
    Activity {
        wallet: WalletAddress,
        source: pe_source_polymarket_public::ActivityReadError,
    },
    #[error("positions read for {wallet}: {source}")]
    Positions {
        wallet: WalletAddress,
        source: pe_source_polymarket_public::PositionReadError,
    },
    #[error("asset identity resolution for {wallet}: {source}")]
    Identity {
        wallet: WalletAddress,
        source: SourceError,
    },
    #[error("activity bucket proof encoding failed: {0}")]
    ProofEncoding(#[from] serde_json::Error),
    #[error("activity bucket commit failed for {wallet}: {message}")]
    BucketCommit {
        wallet: WalletAddress,
        message: String,
    },
    #[error("wallet {wallet} became durably fenced during validation")]
    Fenced { wallet: WalletAddress },
    #[error("unsafe attributable activity prevents fence recovery for {wallet}")]
    UnsafeRecovery { wallet: WalletAddress },
    #[error("activity changed between bracket steps for {wallet}")]
    InterveningActivity { wallet: WalletAddress },
    #[error("current-position semantic proofs changed for {wallet}")]
    PositionRevision { wallet: WalletAddress },
    #[error("activity ledger changed between bracket steps for {wallet}")]
    LedgerRevision { wallet: WalletAddress },
    #[error("activity fixed end moved backwards for {wallet}: previous {previous}, next {next}")]
    NonMonotonicActivityBounds {
        wallet: WalletAddress,
        previous: i64,
        next: i64,
    },
    #[error("orchestrator control channel closed during position validation")]
    ControlClosed,
    #[error("orchestrator dropped a position-validation acknowledgement")]
    AcknowledgementClosed,
    #[error("ledger proof contains multiple assets for {condition_id} outcome {outcome}")]
    DuplicateOutcome { condition_id: String, outcome: u16 },
    #[error("ledger proof component is too long")]
    ProofComponentTooLong,
    #[error("paper-state position proof install failed: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
    #[error("position anchor install failed: {0}")]
    AnchorInstall(#[from] AnchorInstallError),
    #[error("reconstruction quality invariant failed")]
    ReconstructionQuality,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    WalletTransient,
    WalletPersistent,
    Shared,
}

fn classify_source(error: &SourceError) -> FailureClass {
    match error {
        SourceError::Transient { .. }
        | SourceError::RateLimited { .. }
        | SourceError::Fatal { .. } => FailureClass::Shared,
    }
}

fn classify_activity_read(error: &pe_source_polymarket_public::ActivityReadError) -> FailureClass {
    use pe_source_polymarket_public::ActivityReadError;
    use pe_source_polymarket_public::activity::{
        ActivityAggregationError, ActivityIdentityError, ActivityParseError,
        ActivityValidationError,
    };
    match error {
        ActivityReadError::Fetch { source, .. } => classify_source(source),
        ActivityReadError::Parse(ActivityParseError::InvalidRow { source, .. }) => match source {
            ActivityValidationError::InvalidActivityType { .. }
            | ActivityValidationError::EmptyField { .. }
            | ActivityValidationError::InvalidSide { .. }
            | ActivityValidationError::InvalidPrice { .. }
            | ActivityValidationError::InvalidShareAmount { .. }
            | ActivityValidationError::InvalidCollateralAmount { .. }
            | ActivityValidationError::InvalidConditionOutcomeMapping
            | ActivityValidationError::InvalidTimestampValue { .. }
            | ActivityValidationError::LegacyProjectionOutOfRange => FailureClass::WalletPersistent,
            ActivityValidationError::Identity
            | ActivityValidationError::MissingField { .. }
            | ActivityValidationError::InvalidTimestamp(_) => FailureClass::Shared,
        },
        ActivityReadError::Parse(
            ActivityParseError::Json { .. } | ActivityParseError::WindowInvalidated(_),
        ) => FailureClass::Shared,
        ActivityReadError::Aggregate(source) => match source {
            ActivityAggregationError::Identity(inner) => match inner {
                ActivityIdentityError::ComponentTooLong
                | ActivityIdentityError::ComponentMismatch { .. } => FailureClass::Shared,
            },
            ActivityAggregationError::CausalAmbiguity { .. }
            | ActivityAggregationError::MixedComboState { .. }
            | ActivityAggregationError::EmptyGroup { .. }
            | ActivityAggregationError::AmountOverflow { .. }
            | ActivityAggregationError::PriceWeightedSumOverflow { .. }
            | ActivityAggregationError::ZeroShareSum { .. }
            | ActivityAggregationError::InvalidWeightedPrice { .. } => FailureClass::Shared,
        },
        ActivityReadError::Identity(source) => match source {
            ActivityIdentityError::ComponentTooLong
            | ActivityIdentityError::ComponentMismatch { .. } => FailureClass::Shared,
        },
        ActivityReadError::SaturatedTerminalSecond { .. } => FailureClass::WalletPersistent,
        ActivityReadError::InvalidOffset { .. }
        | ActivityReadError::RowOutsideBounds { .. }
        | ActivityReadError::InvalidSplit { .. }
        | ActivityReadError::CanonicalPage(_)
        | ActivityReadError::RowCountOverflow
        | ActivityReadError::PageTooLarge { .. } => FailureClass::Shared,
    }
}

fn classify_position_read(error: &pe_source_polymarket_public::PositionReadError) -> FailureClass {
    use pe_source_polymarket_public::PositionReadError;
    match error {
        PositionReadError::Fetch { source, .. } => classify_source(source),
        PositionReadError::InvalidAmount { .. }
        | PositionReadError::MissingActivityMapping { .. }
        | PositionReadError::ConflictingActivityMapping { .. }
        | PositionReadError::MixedActivityClassification { .. }
        | PositionReadError::ConflictingOutcomeMapping { .. }
        | PositionReadError::DuplicateAsset { .. }
        | PositionReadError::SaturatedTerminalPage { .. } => FailureClass::WalletPersistent,
        PositionReadError::MetadataUnresolved { .. }
        | PositionReadError::ProofComponentTooLong
        | PositionReadError::PageTooLarge { .. }
        | PositionReadError::Json(_)
        | PositionReadError::MissingField { .. }
        | PositionReadError::InvalidWallet { .. }
        | PositionReadError::WalletMismatch { .. }
        | PositionReadError::CanonicalPage(_)
        | PositionReadError::RowCountOverflow => FailureClass::Shared,
    }
}

impl CausalPositionError {
    pub fn class(&self) -> FailureClass {
        match self {
            Self::Activity { source, .. } => classify_activity_read(source),
            Self::Positions { source, .. } => classify_position_read(source),
            Self::Identity {
                source: SourceError::Transient { .. },
                ..
            } => FailureClass::WalletTransient,
            Self::Identity { source, .. } => classify_source(source),
            Self::UnsafeRecovery { .. } | Self::Fenced { .. } | Self::DuplicateOutcome { .. } => {
                FailureClass::WalletPersistent
            }
            Self::InterveningActivity { .. }
            | Self::PositionRevision { .. }
            | Self::LedgerRevision { .. } => FailureClass::WalletTransient,
            Self::AnchorInstall(error) => error.class(),
            Self::ProofEncoding(_)
            | Self::BucketCommit { .. }
            | Self::NonMonotonicActivityBounds { .. }
            | Self::ControlClosed
            | Self::AcknowledgementClosed
            | Self::ProofComponentTooLong
            | Self::PaperState(_)
            | Self::ReconstructionQuality => FailureClass::Shared,
        }
    }

    pub fn kind(&self) -> &'static str {
        use pe_source_polymarket_public::activity::{ActivityParseError, ActivityValidationError};
        use pe_source_polymarket_public::{ActivityReadError, PositionReadError};
        match self {
            Self::Activity {
                source:
                    ActivityReadError::Fetch {
                        source: SourceError::Transient { .. },
                        ..
                    },
                ..
            }
            | Self::Positions {
                source:
                    PositionReadError::Fetch {
                        source: SourceError::Transient { .. },
                        ..
                    },
                ..
            }
            | Self::Identity {
                source: SourceError::Transient { .. },
                ..
            } => "source.transient",
            Self::Activity {
                source:
                    ActivityReadError::Fetch {
                        source: SourceError::RateLimited { .. },
                        ..
                    },
                ..
            }
            | Self::Positions {
                source:
                    PositionReadError::Fetch {
                        source: SourceError::RateLimited { .. },
                        ..
                    },
                ..
            }
            | Self::Identity {
                source: SourceError::RateLimited { .. },
                ..
            } => "source.rate_limited",
            Self::Activity {
                source:
                    ActivityReadError::Fetch {
                        source: SourceError::Fatal { .. },
                        ..
                    },
                ..
            }
            | Self::Positions {
                source:
                    PositionReadError::Fetch {
                        source: SourceError::Fatal { .. },
                        ..
                    },
                ..
            }
            | Self::Identity {
                source: SourceError::Fatal { .. },
                ..
            } => "source.fatal",
            Self::Activity {
                source: ActivityReadError::Parse(ActivityParseError::InvalidRow { source, .. }),
                ..
            } => match source {
                ActivityValidationError::InvalidActivityType { .. } => {
                    "activity.invalid_activity_type"
                }
                ActivityValidationError::Identity => "activity.identity",
                ActivityValidationError::MissingField { .. } => "activity.missing_field",
                ActivityValidationError::EmptyField { .. } => "activity.empty_field",
                ActivityValidationError::InvalidSide { .. } => "activity.invalid_side",
                ActivityValidationError::InvalidPrice { .. } => "activity.invalid_price",
                ActivityValidationError::InvalidShareAmount { .. } => {
                    "activity.invalid_share_amount"
                }
                ActivityValidationError::InvalidCollateralAmount { .. } => {
                    "activity.invalid_collateral_amount"
                }
                ActivityValidationError::InvalidConditionOutcomeMapping => {
                    "activity.invalid_condition_outcome_mapping"
                }
                ActivityValidationError::InvalidTimestamp(_) => "activity.invalid_timestamp",
                ActivityValidationError::InvalidTimestampValue { .. } => {
                    "activity.invalid_timestamp_value"
                }
                ActivityValidationError::LegacyProjectionOutOfRange => {
                    "activity.legacy_projection_out_of_range"
                }
            },
            Self::Activity {
                source: ActivityReadError::Parse(ActivityParseError::Json { .. }),
                ..
            } => "activity.json",
            Self::Activity {
                source: ActivityReadError::Parse(ActivityParseError::WindowInvalidated(_)),
                ..
            } => "activity.window_invalidated",
            Self::Activity {
                source: ActivityReadError::Aggregate(_),
                ..
            } => "activity.aggregate",
            Self::Activity {
                source: ActivityReadError::Identity(_),
                ..
            } => "activity.identity",
            Self::Activity {
                source: ActivityReadError::InvalidOffset { .. },
                ..
            } => "activity.invalid_offset",
            Self::Activity {
                source: ActivityReadError::RowOutsideBounds { .. },
                ..
            } => "activity.row_outside_bounds",
            Self::Activity {
                source: ActivityReadError::InvalidSplit { .. },
                ..
            } => "activity.invalid_split",
            Self::Activity {
                source: ActivityReadError::SaturatedTerminalSecond { .. },
                ..
            } => "activity.saturated_terminal_second",
            Self::Activity {
                source: ActivityReadError::CanonicalPage(_),
                ..
            } => "activity.canonical_page",
            Self::Activity {
                source: ActivityReadError::RowCountOverflow,
                ..
            } => "activity.row_count_overflow",
            Self::Activity {
                source: ActivityReadError::PageTooLarge { .. },
                ..
            } => "activity.page_too_large",
            Self::Positions {
                source: PositionReadError::Json(_),
                ..
            } => "positions.json",
            Self::Positions {
                source: PositionReadError::MissingField { .. },
                ..
            } => "positions.missing_field",
            Self::Positions {
                source: PositionReadError::InvalidWallet { .. },
                ..
            } => "positions.invalid_wallet",
            Self::Positions {
                source: PositionReadError::WalletMismatch { .. },
                ..
            } => "positions.wallet_mismatch",
            Self::Positions {
                source: PositionReadError::InvalidAmount { .. },
                ..
            } => "positions.invalid_amount",
            Self::Positions {
                source: PositionReadError::MissingActivityMapping { .. },
                ..
            } => "positions.missing_activity_mapping",
            Self::Positions {
                source: PositionReadError::ConflictingActivityMapping { .. },
                ..
            } => "positions.conflicting_activity_mapping",
            Self::Positions {
                source: PositionReadError::MixedActivityClassification { .. },
                ..
            } => "positions.mixed_activity_classification",
            Self::Positions {
                source: PositionReadError::MetadataUnresolved { .. },
                ..
            } => "positions.metadata_unresolved",
            Self::Positions {
                source: PositionReadError::ConflictingOutcomeMapping { .. },
                ..
            } => "positions.conflicting_outcome_mapping",
            Self::Positions {
                source: PositionReadError::DuplicateAsset { .. },
                ..
            } => "positions.duplicate_asset",
            Self::Positions {
                source: PositionReadError::SaturatedTerminalPage { .. },
                ..
            } => "positions.saturated_terminal_page",
            Self::Positions {
                source: PositionReadError::CanonicalPage(_),
                ..
            } => "positions.canonical_page",
            Self::Positions {
                source: PositionReadError::ProofComponentTooLong,
                ..
            } => "positions.proof_component_too_long",
            Self::Positions {
                source: PositionReadError::RowCountOverflow,
                ..
            } => "positions.row_count_overflow",
            Self::Positions {
                source: PositionReadError::PageTooLarge { .. },
                ..
            } => "positions.page_too_large",
            Self::ProofEncoding(_) => "validation.proof_encoding",
            Self::BucketCommit { .. } => "validation.bucket_commit",
            Self::Fenced { .. } => "validation.fenced",
            Self::UnsafeRecovery { .. } => "validation.unsafe_recovery",
            Self::InterveningActivity { .. } => "validation.intervening_activity",
            Self::PositionRevision { .. } => "validation.position_revision",
            Self::LedgerRevision { .. } => "validation.ledger_revision",
            Self::NonMonotonicActivityBounds { .. } => "validation.nonmonotonic_activity_bounds",
            Self::ControlClosed => "validation.control_closed",
            Self::AcknowledgementClosed => "validation.acknowledgement_closed",
            Self::DuplicateOutcome { .. } => "validation.duplicate_outcome",
            Self::ProofComponentTooLong => "validation.proof_component_too_long",
            Self::PaperState(_) => "validation.paper_state",
            Self::AnchorInstall(error) => error.kind(),
            Self::ReconstructionQuality => "validation.reconstruction_quality",
        }
    }
}

pub struct ValidationOutcomes {
    pub started_prefix: usize,
    pub(crate) completed_at: HashMap<WalletAddress, Instant>,
    pub accepted: Vec<AnchorInstall>,
    pub deferred: Vec<(WalletAddress, CausalPositionError)>,
    pub shared: Option<CausalPositionError>,
}

pub struct DirectValidationOutcome {
    pub(crate) completed_at: HashMap<WalletAddress, Instant>,
    pub accepted: Vec<AnchorInstall>,
    pub deferred: Vec<(WalletAddress, CausalPositionError)>,
}

impl DirectValidationOutcome {
    pub fn failure_completed_at(&self, wallet: &WalletAddress) -> Option<Instant> {
        self.completed_at.get(wallet).copied()
    }
}

/// Exact per-wallet source outcomes that retry without fencing or failing boot.
///
/// A venue history the activity row parser refuses (issue #594: rehearsal attempt 9 aborted the
/// whole boot on one wallet's `TRADE` row with price 3.1968021978) is that wallet's problem. A
/// `validate_direct` walk that encounters the row leaves the wallet out of the accepted set
/// (unvalidated for runtime admission; a wallet reused on a fresh anchor is not re-read at boot),
/// and a live wallet's periodic refresh yields `Deferred`, retried whenever the refresh rotation
/// selects it while it keeps the anchor it already holds (issue #597). Only the observed shape is
/// deferred: malformed pages, window invalidations, other row validations, aggregation and
/// identity failures stay fatal.
#[must_use]
pub fn is_deferred_causal_position_error(error: &CausalPositionError) -> bool {
    match error {
        CausalPositionError::PositionRevision { .. }
        | CausalPositionError::InterveningActivity { .. } => true,
        CausalPositionError::Activity {
            source: ActivityReadError::SaturatedTerminalSecond { .. },
            ..
        } => true,
        CausalPositionError::Activity {
            source:
                ActivityReadError::Parse(ActivityParseError::InvalidRow {
                    source: ActivityValidationError::InvalidPrice { .. },
                    ..
                }),
            ..
        } => true,
        CausalPositionError::Activity {
            source: ActivityReadError::Fetch { source, .. },
            ..
        }
        | CausalPositionError::Positions {
            source: PositionReadError::Fetch { source, .. },
            ..
        }
        | CausalPositionError::Identity { source, .. } => matches!(
            source,
            SourceError::Transient { .. } | SourceError::RateLimited { .. }
        ),
        CausalPositionError::Positions { source, .. } => matches!(
            source,
            PositionReadError::MissingActivityMapping { .. }
                | PositionReadError::ConflictingActivityMapping { .. }
                | PositionReadError::MixedActivityClassification { .. }
                | PositionReadError::MetadataUnresolved { .. }
                | PositionReadError::ConflictingOutcomeMapping { .. }
                | PositionReadError::DuplicateAsset { .. }
                | PositionReadError::SaturatedTerminalPage { .. }
        ),
        _ => false,
    }
}

/// Shared real/fake-source bracket implementation.
#[derive(Clone)]
pub struct CausalPositionValidator {
    fetcher: Arc<dyn ReconciliationFetcher>,
    asset_identity: Arc<AssetIdentityResolver>,
    base_url: Arc<str>,
    source_log_generation: Arc<str>,
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
    #[cfg(feature = "scenario")]
    step_hook: Option<BracketStepHook>,
    #[cfg(feature = "scenario")]
    install_hook: Option<AnchorInstallHook>,
}

/// What the anchor proof keeps from a complete activity read once its rows have
/// been committed: the fixed end and the page evidence. The rows themselves are
/// already recorded per group, and holding three full histories per wallet was
/// the boot's memory peak (#555 activation).
struct ActivityEvidence {
    fixed_end: i64,
    pages: Vec<pe_source_polymarket_public::ReconciliationPageEvidence>,
}

struct PreparedActivity {
    mapping: ActivityAssetMapping,
    identity_overrides: HashMap<SourceTradeId, IdentityOverride>,
    identity_unresolved: HashSet<SourceTradeId>,
    no_copy_dispositions: HashMap<SourceTradeId, NoCopyDisposition>,
    unresolved_assets: BTreeMap<String, String>,
    metadata_reads: BTreeMap<PolymarketTokenId, IdentityProvenance>,
}

impl From<CompleteActivityRead> for ActivityEvidence {
    fn from(read: CompleteActivityRead) -> Self {
        Self {
            fixed_end: read.fixed_end,
            pages: read.pages,
        }
    }
}

impl CausalPositionValidator {
    pub fn new(
        fetcher: Arc<dyn ReconciliationFetcher>,
        base_url: impl Into<Arc<str>>,
        source_log_generation: impl Into<Arc<str>>,
        asset_identity: Arc<AssetIdentityResolver>,
    ) -> Self {
        Self {
            fetcher,
            asset_identity,
            base_url: base_url.into(),
            source_log_generation: source_log_generation.into(),
            now: Arc::new(|| time::OffsetDateTime::now_utc().unix_timestamp()),
            #[cfg(feature = "scenario")]
            step_hook: None,
            #[cfg(feature = "scenario")]
            install_hook: None,
        }
    }

    /// Boot-migration form: every fetched activity/positions page is appended
    /// and synchronized through the normal source-log owner before parsing and
    /// application to the side database (#544).
    pub fn new_recording(
        fetcher: Arc<dyn ReconciliationFetcher>,
        base_url: impl Into<Arc<str>>,
        source_log_generation: impl Into<Arc<str>>,
        source_log: BootSourceLog,
        asset_identity: Arc<AssetIdentityResolver>,
    ) -> Self {
        let recording = DurableRecordingFetcher {
            inner: fetcher,
            sink: source_log,
        };
        Self::new(
            Arc::new(recording),
            base_url,
            source_log_generation,
            asset_identity,
        )
    }

    /// Deterministic bracket clock for hermetic unit and scenario tests.
    #[cfg(any(test, feature = "scenario"))]
    #[must_use]
    pub fn with_clock(mut self, now: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.now = now;
        self
    }

    /// Inject a deterministic mutation between bracket steps in scenario tests.
    #[cfg(feature = "scenario")]
    #[must_use]
    pub fn with_step_hook(mut self, hook: BracketStepHook) -> Self {
        self.step_hook = Some(hook);
        self
    }

    /// Observe the one direct-install batch in deterministic scenario tests.
    #[cfg(feature = "scenario")]
    #[must_use]
    pub fn with_install_hook(mut self, hook: AnchorInstallHook) -> Self {
        self.install_hook = Some(hook);
        self
    }

    /// Validate every wallet through the live orchestrator owner. The caller
    /// sends all returned acceptances in one final installation command.
    pub async fn validate_via_control(
        &self,
        wallets: &[WalletAddress],
        control_tx: &mpsc::Sender<OrchestratorControl>,
        paper_state: &PaperStateDb,
        purpose: ValidationPurpose,
        deadline: Option<Instant>,
    ) -> ValidationOutcomes {
        let mut running = futures::stream::FuturesUnordered::new();
        let mut started_prefix = 0;
        let mut completed = Vec::new();
        loop {
            while running.len() < BRACKET_CONCURRENCY
                && started_prefix < wallets.len()
                && deadline.is_none_or(|end| Instant::now() < end)
            {
                let index = started_prefix;
                let wallet = wallets[index];
                started_prefix += 1;
                running.push(async move {
                    let started = Instant::now();
                    let result = self.validate_control_with_retry(wallet, control_tx, paper_state, purpose, deadline).await;
                    tracing::info!(wallet = %wallet, elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                        outcome = result.as_ref().map_or_else(|error| error.kind(), |_| "accepted"), "wallet bracket completed");
                    (index, result, Instant::now())
                });
            }
            let Some(result) = running.next().await else {
                break;
            };
            completed.push(result);
        }
        completed.sort_by_key(|(index, _, _)| *index);
        let mut outcomes = ValidationOutcomes {
            started_prefix,
            completed_at: HashMap::new(),
            accepted: Vec::with_capacity(completed.len()),
            deferred: Vec::new(),
            shared: None,
        };
        for (index, result, completed_at) in completed {
            outcomes.completed_at.insert(wallets[index], completed_at);
            match result {
                Ok(install) => outcomes.accepted.push(install),
                Err(error) if error.class() == FailureClass::Shared => {
                    if outcomes.shared.is_none() {
                        outcomes.shared = Some(error);
                    }
                }
                Err(error) => outcomes.deferred.push((wallets[index], error)),
            }
        }
        outcomes
    }

    /// Boot-time form used before producers start. The same bucket engine is
    /// moved into the orchestrator after the atomic proof installation.
    pub async fn validate_direct(
        &self,
        wallets: &[WalletAddress],
        engine: &mut BucketCommitEngine,
        paper_state: &PaperStateDb,
    ) -> Result<Vec<AnchorInstall>, CausalPositionError> {
        Ok(self
            .validate_direct_with_deferrals(wallets, engine, paper_state)
            .await?
            .accepted)
    }

    /// Boot form that also retains wallet failure classes for the first maintenance batch.
    pub async fn validate_direct_with_deferrals(
        &self,
        wallets: &[WalletAddress],
        engine: &mut BucketCommitEngine,
        paper_state: &PaperStateDb,
    ) -> Result<DirectValidationOutcome, CausalPositionError> {
        let engine = Arc::new(tokio::sync::Mutex::new(engine));
        let mut completed = futures::stream::iter(wallets.iter().copied().enumerate())
            .map(|(index, wallet)| {
                let engine = Arc::clone(&engine);
                async move {
                    let started = Instant::now();
                    let outcome = self.validate_direct_with_retry(wallet, &engine, paper_state).await;
                    let completed = Instant::now();
                    tracing::info!(%wallet, elapsed_ms = u64::try_from(completed.duration_since(started).as_millis()).unwrap_or(u64::MAX), outcome = outcome.as_ref().map_or_else(|error| error.kind(), |_| "accepted"), "wallet bracket completed");
                    (index, wallet, outcome, completed)
                }
            })
            .buffer_unordered(BRACKET_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        completed.sort_by_key(|(index, _, _, _)| *index);
        let mut accepted = Vec::with_capacity(completed.len());
        let mut deferred = Vec::new();
        let mut completed_at = HashMap::new();
        for (_, wallet, result, terminal) in completed {
            completed_at.insert(wallet, terminal);
            match result {
                Ok(acceptance) => accepted.push(acceptance),
                // A newly durable fence is deterministic quarantine (the
                // commit already recorded it; the next boot's pre-bracket
                // filter would exclude the wallet anyway). A retryable
                // outcome — positions revised between reads, activity
                // intervening mid-bracket, or an incomplete source read —
                // leaves the wallet unvalidated: it stays history-incomplete,
                // is filtered after the bracket, and re-enters only through
                // the serialized runtime admission preparer, which owns the
                // retries (both observed live in the #544 activation
                // rehearsals as activation deadlocks). Infrastructure errors
                // and a post-loop ledger revision still fail the boot.
                Err(CausalPositionError::Fenced { wallet }) => {
                    tracing::warn!(
                        wallet = %wallet,
                        "boot bracket: wallet durably fenced; excluded from the boot universe"
                    );
                    deferred.push((wallet, CausalPositionError::Fenced { wallet }));
                }
                Err(error) if is_deferred_causal_position_error(&error) => {
                    tracing::warn!(
                        wallet = %wallet,
                        outcome = %error,
                        "boot bracket: wallet left unvalidated for runtime admission"
                    );
                    deferred.push((wallet, error));
                }
                Err(error) => return Err(error),
            }
        }
        #[cfg(feature = "scenario")]
        if let Some(hook) = &self.install_hook {
            hook(&accepted);
        }
        engine.lock().await.install_anchors(&accepted)?;
        // The accepted bracket IS the plan's history-validating reconciliation:
        // promote each wallet's SEEDED (conservative) history row to complete.
        // Unseeded wallets stay incomplete and are filtered fail-closed (#544
        // activation fix — without this the first v2 boot can never publish).
        let wallets: Vec<WalletAddress> = accepted.iter().map(|install| install.wallet).collect();
        paper_state.mark_seeded_history_validated(
            &wallets,
            "{\"source\":\"causal_position_bracket_v2\"}",
            time::OffsetDateTime::now_utc().unix_timestamp(),
        )?;
        Ok(DirectValidationOutcome {
            accepted,
            deferred,
            completed_at,
        })
    }

    async fn validate_control_with_retry(
        &self,
        wallet: WalletAddress,
        control_tx: &mpsc::Sender<OrchestratorControl>,
        paper_state: &PaperStateDb,
        purpose: ValidationPurpose,
        deadline: Option<Instant>,
    ) -> Result<AnchorInstall, CausalPositionError> {
        let mut ordinary_reconciliation_needed = false;
        let first = self
            .validate_one_control(
                wallet,
                control_tx,
                paper_state,
                purpose,
                &mut ordinary_reconciliation_needed,
            )
            .await;
        if ordinary_reconciliation_needed {
            return first;
        }
        if first.as_ref().is_err_and(is_bounded_retry_error)
            && deadline.is_none_or(|end| Instant::now() < end)
        {
            if let Some(fence) = paper_state.wallet_fence(&wallet)?
                && !recoverable_fence(paper_state, &fence)?
            {
                return Err(CausalPositionError::Fenced { wallet });
            }
            return self
                .validate_one_control(
                    wallet,
                    control_tx,
                    paper_state,
                    purpose,
                    &mut ordinary_reconciliation_needed,
                )
                .await;
        }
        first
    }

    async fn validate_direct_with_retry(
        &self,
        wallet: WalletAddress,
        engine: &Arc<tokio::sync::Mutex<&mut BucketCommitEngine>>,
        paper_state: &PaperStateDb,
    ) -> Result<AnchorInstall, CausalPositionError> {
        let first = self.validate_one_direct(wallet, engine, paper_state).await;
        if first.as_ref().is_err_and(is_bounded_retry_error) {
            if paper_state.is_wallet_fenced(&wallet)? {
                return Err(CausalPositionError::Fenced { wallet });
            }
            return self.validate_one_direct(wallet, engine, paper_state).await;
        }
        first
    }

    async fn validate_one_control(
        &self,
        wallet: WalletAddress,
        control_tx: &mpsc::Sender<OrchestratorControl>,
        paper_state: &PaperStateDb,
        purpose: ValidationPurpose,
        ordinary_reconciliation_needed: &mut bool,
    ) -> Result<AnchorInstall, CausalPositionError> {
        let expected_fence = paper_state.wallet_fence(&wallet)?;
        if let Some(fence) = &expected_fence
            && !recoverable_fence(paper_state, fence)?
        {
            return Err(CausalPositionError::Fenced { wallet });
        }
        let mut fresh_history = Vec::new();
        let mut metadata_reads = BTreeMap::new();
        let mut unresolved_assets = BTreeMap::new();
        let first_activity = self.activity(wallet).await?;
        self.preflight_control_read(
            wallet,
            &first_activity,
            paper_state,
            purpose,
            ordinary_reconciliation_needed,
        )?;
        let first_prepared = self.prepare_activity(wallet, &first_activity).await?;
        if expected_fence.is_some() {
            fresh_history.extend(recovery_fresh_history(
                wallet,
                &first_activity,
                &first_prepared,
            )?);
        }
        metadata_reads.extend(first_prepared.metadata_reads.clone());
        unresolved_assets.extend(first_prepared.unresolved_assets.clone());
        self.commit_control(wallet, &first_activity, &first_prepared, control_tx, false)
            .await?;
        let first_ledger = capture_control(wallet, control_tx).await?;
        let first_positions =
            retain_missing_mapping(self.positions(wallet, &first_prepared.mapping).await)?;
        let first_activity = ActivityEvidence::from(first_activity);

        let second_activity = self.activity(wallet).await?;
        self.preflight_control_read(
            wallet,
            &second_activity,
            paper_state,
            purpose,
            ordinary_reconciliation_needed,
        )?;
        let second_prepared = self.prepare_activity(wallet, &second_activity).await?;
        if expected_fence.is_some() {
            fresh_history.extend(recovery_fresh_history(
                wallet,
                &second_activity,
                &second_prepared,
            )?);
        }
        resolve_missing_mapping(wallet, &first_positions, &second_prepared.mapping)?;
        metadata_reads.extend(second_prepared.metadata_reads.clone());
        unresolved_assets.extend(second_prepared.unresolved_assets.clone());
        if self
            .commit_control(wallet, &second_activity, &second_prepared, control_tx, true)
            .await?
        {
            return Err(CausalPositionError::InterveningActivity { wallet });
        }
        let second_ledger = capture_control(wallet, control_tx).await?;
        let second_positions =
            retain_missing_mapping(self.positions(wallet, &second_prepared.mapping).await)?;
        let second_activity = ActivityEvidence::from(second_activity);

        let final_activity = self.activity(wallet).await?;
        self.preflight_control_read(
            wallet,
            &final_activity,
            paper_state,
            purpose,
            ordinary_reconciliation_needed,
        )?;
        let final_prepared = self.prepare_activity(wallet, &final_activity).await?;
        if expected_fence.is_some() {
            fresh_history.extend(recovery_fresh_history(
                wallet,
                &final_activity,
                &final_prepared,
            )?);
        }
        resolve_missing_mapping(wallet, &second_positions, &final_prepared.mapping)?;
        unresolved_assets.extend(final_prepared.unresolved_assets.clone());
        metadata_reads.extend(final_prepared.metadata_reads.clone());
        if self
            .commit_control(wallet, &final_activity, &final_prepared, control_tx, true)
            .await?
        {
            return Err(CausalPositionError::InterveningActivity { wallet });
        }
        let final_ledger = capture_control(wallet, control_tx).await?;
        let final_activity = ActivityEvidence::from(final_activity);
        let mut install = self.finish(
            wallet,
            [&first_activity, &second_activity, &final_activity],
            [&first_ledger, &second_ledger, &final_ledger],
            first_positions
                .as_ref()
                .map_err(|_| CausalPositionError::InterveningActivity { wallet })?,
            second_positions
                .as_ref()
                .map_err(|_| CausalPositionError::InterveningActivity { wallet })?,
            metadata_reads.into_values().collect(),
        )?;
        log_unresolved_activity_assets(wallet, &unresolved_assets);
        if let Some(fence) = &expected_fence {
            if fence_epoch(fence).is_none_or(|epoch| install.cutoff <= epoch) {
                return Err(CausalPositionError::Fenced { wallet });
            }
            fresh_history.retain(|history| history.first_epoch <= install.cutoff);
            install.fresh_history = fresh_history;
            install.expected_fence = expected_fence;
        }
        install.history_status = Some(WalletHistoryStatusRecord {
            wallet,
            complete: true,
            proof_json: install.proof.document.clone(),
            updated_at_unix: install.proof.recorded_at_unix,
        });
        Ok(install)
    }

    async fn validate_one_direct(
        &self,
        wallet: WalletAddress,
        engine: &Arc<tokio::sync::Mutex<&mut BucketCommitEngine>>,
        paper_state: &PaperStateDb,
    ) -> Result<AnchorInstall, CausalPositionError> {
        let mut metadata_reads = BTreeMap::new();
        let mut unresolved_assets = BTreeMap::new();
        let first_activity = self.activity(wallet).await?;
        let first_prepared = self.prepare_activity(wallet, &first_activity).await?;
        metadata_reads.extend(first_prepared.metadata_reads.clone());
        unresolved_assets.extend(first_prepared.unresolved_assets.clone());
        let first_ledger = {
            let mut engine = engine.lock().await;
            if commit_direct(
                wallet,
                &first_activity,
                &first_prepared,
                &mut engine,
                false,
                &self.source_log_generation,
            )? {
                return Err(CausalPositionError::InterveningActivity { wallet });
            }
            let captured = ledger_capture(engine.ledger(), paper_state, wallet)?;
            #[cfg(feature = "scenario")]
            self.run_step_hook(wallet, 1, &mut engine);
            captured
        };
        let first_positions =
            retain_missing_mapping(self.positions(wallet, &first_prepared.mapping).await)?;
        let first_activity = ActivityEvidence::from(first_activity);
        #[cfg(feature = "scenario")]
        {
            let mut engine = engine.lock().await;
            self.run_step_hook(wallet, 2, &mut engine);
        }

        let second_activity = self.activity(wallet).await?;
        let second_prepared = self.prepare_activity(wallet, &second_activity).await?;
        resolve_missing_mapping(wallet, &first_positions, &second_prepared.mapping)?;
        metadata_reads.extend(second_prepared.metadata_reads.clone());
        unresolved_assets.extend(second_prepared.unresolved_assets.clone());
        let second_ledger = {
            let mut engine = engine.lock().await;
            if commit_direct(
                wallet,
                &second_activity,
                &second_prepared,
                &mut engine,
                true,
                &self.source_log_generation,
            )? {
                return Err(CausalPositionError::InterveningActivity { wallet });
            }
            let captured = ledger_capture(engine.ledger(), paper_state, wallet)?;
            #[cfg(feature = "scenario")]
            self.run_step_hook(wallet, 3, &mut engine);
            captured
        };
        let second_positions =
            retain_missing_mapping(self.positions(wallet, &second_prepared.mapping).await)?;
        let second_activity = ActivityEvidence::from(second_activity);
        #[cfg(feature = "scenario")]
        {
            let mut engine = engine.lock().await;
            self.run_step_hook(wallet, 4, &mut engine);
        }

        let final_activity = self.activity(wallet).await?;
        let final_prepared = self.prepare_activity(wallet, &final_activity).await?;
        resolve_missing_mapping(wallet, &second_positions, &final_prepared.mapping)?;
        unresolved_assets.extend(final_prepared.unresolved_assets.clone());
        metadata_reads.extend(final_prepared.metadata_reads.clone());
        let final_ledger = {
            let mut engine = engine.lock().await;
            if commit_direct(
                wallet,
                &final_activity,
                &final_prepared,
                &mut engine,
                true,
                &self.source_log_generation,
            )? {
                return Err(CausalPositionError::InterveningActivity { wallet });
            }
            ledger_capture(engine.ledger(), paper_state, wallet)?
        };
        let final_activity = ActivityEvidence::from(final_activity);
        let install = self.finish(
            wallet,
            [&first_activity, &second_activity, &final_activity],
            [&first_ledger, &second_ledger, &final_ledger],
            first_positions
                .as_ref()
                .map_err(|_| CausalPositionError::InterveningActivity { wallet })?,
            second_positions
                .as_ref()
                .map_err(|_| CausalPositionError::InterveningActivity { wallet })?,
            metadata_reads.into_values().collect(),
        )?;
        log_unresolved_activity_assets(wallet, &unresolved_assets);
        #[cfg(feature = "scenario")]
        {
            let mut engine = engine.lock().await;
            self.run_step_hook(wallet, 5, &mut engine);
        }
        Ok(install)
    }

    #[cfg(feature = "scenario")]
    fn run_step_hook(&self, wallet: WalletAddress, step: usize, engine: &mut BucketCommitEngine) {
        if let Some(hook) = &self.step_hook {
            hook(wallet, step, engine);
        }
    }

    async fn activity(
        &self,
        wallet: WalletAddress,
    ) -> Result<CompleteActivityRead, CausalPositionError> {
        fetch_complete_activity(
            self.fetcher.as_ref(),
            &self.base_url,
            wallet,
            Some(0),
            (self.now)(),
        )
        .await
        .map_err(|source| CausalPositionError::Activity { wallet, source })
    }

    async fn prepare_activity(
        &self,
        wallet: WalletAddress,
        activity: &CompleteActivityRead,
    ) -> Result<PreparedActivity, CausalPositionError> {
        let mut mapping = activity
            .asset_mapping()
            .map_err(|source| CausalPositionError::Positions { wallet, source })?;
        let tokens = mapping.tokens().cloned().collect::<Vec<_>>();
        let resolved = self
            .asset_identity
            .resolve(tokens.clone())
            .await
            .map_err(|source| CausalPositionError::Identity { wallet, source })?;
        for (asset, identity) in &resolved.verified {
            mapping
                .apply_verified(asset, identity)
                .map_err(|source| CausalPositionError::Positions { wallet, source })?;
        }
        for asset in &tokens {
            if mapping.classification(asset).is_none() {
                let source = PositionReadError::MixedActivityClassification {
                    asset: asset.0.clone(),
                };
                return Err(CausalPositionError::Positions { wallet, source });
            }
            if resolved.verified.contains_key(asset) && !resolved.provenance.contains_key(asset) {
                return Err(CausalPositionError::Identity {
                    wallet,
                    source: SourceError::Fatal {
                        message: format!(
                            "verified token {} has no durable metadata provenance",
                            asset.0
                        ),
                    },
                });
            }
        }

        let mut identity_overrides = HashMap::new();
        let unresolved_assets = tokens
            .iter()
            .filter(|asset| !resolved.verified.contains_key(*asset))
            .map(|asset| {
                (
                    asset.0.clone(),
                    resolved.unverified.get(asset).cloned().unwrap_or_else(|| {
                        "token absent from open and closed Gamma metadata".to_owned()
                    }),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut identity_unresolved = HashSet::new();
        let mut no_copy_dispositions = HashMap::new();
        for bucket in activity
            .buckets()
            .map_err(|source| CausalPositionError::Activity { wallet, source })?
        {
            for aggregate in bucket {
                let components = aggregate.group_id.components();
                let Some(asset) = &components.asset else {
                    continue;
                };
                let Some(identity) = resolved.verified.get(asset) else {
                    let source_trade_id = aggregate.group_id.key().clone();
                    identity_unresolved.insert(source_trade_id.clone());
                    no_copy_dispositions.insert(
                        source_trade_id,
                        NoCopyDisposition {
                            provenance: "rest_poll".to_owned(),
                            age_secs: activity
                                .fixed_end
                                .saturating_sub(aggregate.source_time.0.unix_timestamp()),
                            reason: "identity_unresolved".to_owned(),
                            recorded_at_unix: activity.fixed_end,
                        },
                    );
                    continue;
                };
                if components.condition_id.as_ref() != Some(&identity.condition_id)
                    || components.outcome != Some(identity.outcome)
                {
                    identity_overrides.insert(
                        aggregate.group_id.key().clone(),
                        IdentityOverride {
                            verified: MarketOutcomeId::new(
                                MarketId(VenueMarketId(identity.condition_id.0.clone())),
                                identity.outcome,
                            ),
                            evidence_hash: identity.evidence_hash.clone(),
                        },
                    );
                }
            }
        }
        Ok(PreparedActivity {
            mapping,
            identity_overrides,
            identity_unresolved,
            no_copy_dispositions,
            unresolved_assets,
            metadata_reads: resolved.provenance,
        })
    }

    async fn positions(
        &self,
        wallet: WalletAddress,
        mapping: &pe_source_polymarket_public::ActivityAssetMapping,
    ) -> Result<CompletePositionsRead, CausalPositionError> {
        fetch_complete_positions(self.fetcher.as_ref(), &self.base_url, wallet, mapping)
            .await
            .map_err(|source| CausalPositionError::Positions { wallet, source })
    }

    fn preflight_control_read(
        &self,
        wallet: WalletAddress,
        activity: &CompleteActivityRead,
        paper_state: &PaperStateDb,
        purpose: ValidationPurpose,
        ordinary_reconciliation_needed: &mut bool,
    ) -> Result<(), CausalPositionError> {
        if let ValidationPurpose::RoutineRefresh { cutoff } = purpose {
            for bucket in activity
                .buckets()
                .map_err(|source| CausalPositionError::Activity { wallet, source })?
            {
                for aggregate in bucket {
                    if aggregate.source_time.0.unix_timestamp() > cutoff
                        && paper_state
                            .activity_group_state(aggregate.group_id.key())?
                            .is_none()
                    {
                        *ordinary_reconciliation_needed = true;
                        return Err(CausalPositionError::InterveningActivity { wallet });
                    }
                }
            }
        }
        Ok(())
    }

    async fn commit_control(
        &self,
        wallet: WalletAddress,
        activity: &CompleteActivityRead,
        prepared: &PreparedActivity,
        control_tx: &mpsc::Sender<OrchestratorControl>,
        count_change: bool,
    ) -> Result<bool, CausalPositionError> {
        let mut changed = false;
        let buckets = activity
            .buckets()
            .map_err(|source| CausalPositionError::Activity { wallet, source })?;
        let context = Arc::new(bracket_context(
            activity,
            &self.source_log_generation,
            prepared,
        )?);
        for bucket in buckets {
            let (committed, acknowledgement) = oneshot::channel();
            control_tx
                .send(OrchestratorControl::CommitActivityBucket {
                    aggregates: bucket,
                    context: Arc::clone(&context),
                    committed,
                })
                .await
                .map_err(|_| CausalPositionError::ControlClosed)?;
            let result = acknowledgement
                .await
                .map_err(|_| CausalPositionError::AcknowledgementClosed)?
                .map_err(|message| CausalPositionError::BucketCommit { wallet, message })?;
            if result.retained_revision {
                return Err(CausalPositionError::InterveningActivity { wallet });
            }
            if result.newly_fenced.is_some() {
                return Err(CausalPositionError::Fenced { wallet });
            }
            changed |= count_change && !result.already_committed;
        }
        Ok(changed)
    }

    fn finish(
        &self,
        wallet: WalletAddress,
        activities: [&ActivityEvidence; 3],
        ledgers: [&AdmissionLedgerCapture; 3],
        first_positions: &CompletePositionsRead,
        second_positions: &CompletePositionsRead,
        metadata_reads: Vec<IdentityProvenance>,
    ) -> Result<AnchorInstall, CausalPositionError> {
        for pair in activities.windows(2) {
            if pair[1].fixed_end < pair[0].fixed_end {
                return Err(CausalPositionError::NonMonotonicActivityBounds {
                    wallet,
                    previous: pair[0].fixed_end,
                    next: pair[1].fixed_end,
                });
            }
        }
        if !first_positions.semantically_equal(second_positions) {
            return Err(CausalPositionError::PositionRevision { wallet });
        }
        if ledgers[0].hash != ledgers[1].hash || ledgers[1].hash != ledgers[2].hash {
            return Err(CausalPositionError::LedgerRevision { wallet });
        }
        let coverage_matches = |left: &AdmissionLedgerCapture, right: &AdmissionLedgerCapture| {
            left.cursor == right.cursor
                && left.anchor_seq == right.anchor_seq
                && left.coverage_generation == right.coverage_generation
        };
        if !coverage_matches(ledgers[0], ledgers[1]) || !coverage_matches(ledgers[1], ledgers[2]) {
            return Err(CausalPositionError::InterveningActivity { wallet });
        }
        let balances = ordinary_position_balances(first_positions)?;

        let activity_bounds = activities
            .iter()
            .map(|activity| {
                json!({
                    "fixed_end": activity.fixed_end,
                    "pages": activity.pages,
                })
            })
            .collect::<Vec<_>>();
        let proof = json!({
            "version": 1,
            "wallet": wallet,
            "source_id": ACTIVITY_POLL_SOURCE_ID,
            "positions_semantic_hash": first_positions.semantic_hash(),
            "expected_ledger_hash": ledgers[2].hash,
            "source_log_generation": self.source_log_generation,
            "activity_walks": activity_bounds,
            "metadata_reads": metadata_reads,
            "positions_reads": [
                {
                    "semantic_hash": first_positions.semantic_hash(),
                    "pages": first_positions.pages,
                },
                {
                    "semantic_hash": second_positions.semantic_hash(),
                    "pages": second_positions.pages,
                },
            ],
        });
        Ok(AnchorInstall {
            fresh_history: Vec::new(),
            expected_fence: None,
            history_status: None,
            wallet,
            balances,
            cutoff: activities[1].fixed_end,
            proof: AnchorProof {
                positions_proof_hash: first_positions.semantic_hash().to_owned(),
                activity_bounds_json: serde_json::to_string(&activity_bounds)?,
                source_log_generation: self.source_log_generation.to_string(),
                document: serde_json::to_string(&proof)?,
                recorded_at_unix: (self.now)(),
            },
            expected: AnchorExpectation {
                ledger_hash: ledgers[2].hash.clone(),
                cursor: ledgers[2].cursor,
                anchor_seq: ledgers[2].anchor_seq,
                coverage_generation: ledgers[2].coverage_generation,
            },
        })
    }
}

// Only an absent activity mapping is held across the next walk. Every other position-read
// failure retains its existing immediate classification.
fn retain_missing_mapping(
    read: Result<CompletePositionsRead, CausalPositionError>,
) -> Result<Result<CompletePositionsRead, CausalPositionError>, CausalPositionError> {
    match read {
        Err(
            error @ CausalPositionError::Positions {
                source:
                    pe_source_polymarket_public::PositionReadError::MissingActivityMapping { .. },
                ..
            },
        ) => Ok(Err(error)),
        Err(error) => Err(error),
        Ok(positions) => Ok(Ok(positions)),
    }
}

fn resolve_missing_mapping(
    wallet: WalletAddress,
    read: &Result<CompletePositionsRead, CausalPositionError>,
    next_mapping: &pe_source_polymarket_public::ActivityAssetMapping,
) -> Result<(), CausalPositionError> {
    if let Err(CausalPositionError::Positions {
        source: pe_source_polymarket_public::PositionReadError::MissingActivityMapping { asset },
        ..
    }) = read
    {
        if next_mapping
            .identity(&pe_core_types::PolymarketTokenId(asset.clone()))
            .is_some()
        {
            return Err(CausalPositionError::InterveningActivity { wallet });
        }
        return Err(CausalPositionError::Positions {
            wallet,
            source: pe_source_polymarket_public::PositionReadError::MissingActivityMapping {
                asset: asset.clone(),
            },
        });
    }
    Ok(())
}

pub(crate) fn fence_epoch(fence: &WalletFenceRecord) -> Option<i64> {
    serde_json::from_str::<serde_json::Value>(&fence.proof_json)
        .ok()?
        .get("bucket_epoch")?
        .as_i64()
}

pub fn recoverable_fence(
    paper: &PaperStateDb,
    fence: &WalletFenceRecord,
) -> Result<bool, pe_paper_state::PaperStateError> {
    let Some(epoch) = fence_epoch(fence) else {
        return Ok(false);
    };
    Ok(match fence.cause.as_str() {
        "order_dependent_equal_second"
        | "position_underflow"
        | "position_overflow"
        | "late_group_after_bucket_commit" => true,
        "revised_applied_aggregate" => paper.revised_fence_trigger_disposed(fence, epoch)?,
        _ => false,
    })
}

/// Only fence-only failures bypass batch-local persistent parking.
pub(crate) fn recoverable_fence_failure(
    paper: &PaperStateDb,
    wallet: &WalletAddress,
    kind: &str,
) -> bool {
    matches!(
        kind,
        "fence.active" | "validation.fenced" | "anchor.fenced" | "publication.fenced"
    ) && paper
        .wallet_fence(wallet)
        .ok()
        .flatten()
        .is_some_and(|fence| recoverable_fence(paper, &fence).unwrap_or(false))
}

pub(crate) fn unrecoverable_fenced_wallets(
    paper: &PaperStateDb,
) -> Result<HashSet<WalletAddress>, pe_paper_state::PaperStateError> {
    let mut wallets = HashSet::new();
    for fence in paper.wallet_fences()? {
        if !recoverable_fence(paper, &fence)? {
            wallets.insert(fence.wallet);
        }
    }
    Ok(wallets)
}

fn recovery_fresh_history(
    wallet: WalletAddress,
    read: &CompleteActivityRead,
    prepared: &PreparedActivity,
) -> Result<Vec<MarketHistoryRecord>, CausalPositionError> {
    let context = bracket_context(read, "", prepared)?;
    let mut history = Vec::new();
    for bucket in read
        .buckets()
        .map_err(|source| CausalPositionError::Activity { wallet, source })?
    {
        for aggregate in bucket {
            let mutation = crate::bucket_commit::recordable_mutation(&aggregate, &context);
            match mutation.effect.effective() {
                LedgerEffect::Conversion | LedgerEffect::UnknownEffect => {
                    return Err(CausalPositionError::UnsafeRecovery { wallet });
                }
                LedgerEffect::Trade {
                    market_id,
                    side: pe_core_types::Side::Buy,
                    ..
                } => history.push(MarketHistoryRecord {
                    wallet,
                    market_id: market_id.clone(),
                    first_epoch: aggregate.source_time.0.unix_timestamp(),
                    source_trade_id: aggregate.group_id.key().clone(),
                }),
                _ => {}
            }
        }
    }
    Ok(history)
}

fn is_bounded_retry_error(error: &CausalPositionError) -> bool {
    matches!(
        error,
        CausalPositionError::InterveningActivity { .. }
            | CausalPositionError::PositionRevision { .. }
    )
}

fn log_unresolved_activity_assets(
    wallet: WalletAddress,
    unresolved_assets: &BTreeMap<String, String>,
) {
    if !unresolved_assets.is_empty() {
        tracing::warn!(
            wallet = %wallet,
            raw_only_assets = ?unresolved_assets,
            "position bracket: activity assets recorded raw-only"
        );
    }
}

struct DurableRecordingFetcher {
    inner: Arc<dyn ReconciliationFetcher>,
    sink: BootSourceLog,
}

impl ReconciliationFetcher for DurableRecordingFetcher {
    fn fetch<'a>(
        &'a self,
        url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
        Box::pin(async move {
            let payload = self.inner.fetch(url).await?;
            let received_at = time::OffsetDateTime::now_utc();
            let envelope = EnvelopeIn {
                source_id: SourceId(ACTIVITY_POLL_SOURCE_ID.to_owned()),
                schema_version: ACTIVITY_SCHEMA_VERSION,
                parser_version: ACTIVITY_PARSER_VERSION,
                observed_at: SourceTimestamp(received_at),
                received_at: ReceivedAt(received_at),
                content_type: ContentType::Json,
                payload: payload.clone(),
            };
            self.sink
                .lock()
                .await
                .append_durable(envelope)
                .map_err(|error| SourceError::Fatal {
                    message: format!("source-log append failed: {error}"),
                })?;
            Ok(payload)
        })
    }
}

fn bracket_context(
    activity: &CompleteActivityRead,
    source_log_generation: &str,
    prepared: &PreparedActivity,
) -> Result<BucketDecisionContext, CausalPositionError> {
    let reconstruction_quality =
        ReconstructionQuality::new(100).map_err(|_| CausalPositionError::ReconstructionQuality)?;
    Ok(BucketDecisionContext {
        applied_configuration: crate::runtime_config::RuntimeConfig::from_service_config(
            &crate::config::ServiceConfig::default(),
        ),
        decision_inputs_json: serde_json::to_string(&json!({
            "fixed_end": activity.fixed_end,
            "pages": activity.pages,
            "source_log_generation": source_log_generation,
        }))?,
        page_occurrences: Vec::new(),
        observed_source_receipts: HashMap::new(),
        reconstruction_quality,
        read_commitment: None,

        signal_config: SignalConfig::default(),
        copy_eligible: false,
        bracket_commit: true,
        recorded_at_unix: time::OffsetDateTime::now_utc().unix_timestamp(),
        observation_provenance: HashMap::new(),
        no_copy_dispositions: prepared.no_copy_dispositions.clone(),
        identity_overrides: prepared.identity_overrides.clone(),
        identity_unresolved: prepared.identity_unresolved.clone(),
        history_status: None,
    })
}

fn commit_direct(
    wallet: WalletAddress,
    activity: &CompleteActivityRead,
    prepared: &PreparedActivity,
    engine: &mut BucketCommitEngine,
    count_change: bool,
    source_log_generation: &str,
) -> Result<bool, CausalPositionError> {
    enum BatchOutcome {
        Complete { changed: bool },
        NewlyFenced,
    }

    let buckets = activity
        .buckets()
        .map_err(|source| CausalPositionError::Activity { wallet, source })?;
    let context = bracket_context(activity, source_log_generation, prepared)?;
    let outcome = engine
        .commit_batch(|engine| {
            let mut changed = false;
            for bucket in buckets {
                let result = engine.commit(
                    // Bracket catch-up is never copy-eligible (`copy_eligible: false`
                    // above), so no pending decision is created and the frozen basis is
                    // inert; a zero basis keeps that invariant explicit (#544).
                    bucket,
                    &context,
                    crate::bucket_commit::FrozenDecisionBasis {
                        win_rate_p: pe_core_types::Probability::ZERO,
                        bankroll: rust_decimal::Decimal::ZERO,
                    },
                )?;
                if result.retained_revision {
                    return Ok(BatchOutcome::Complete { changed: true });
                }
                if result.newly_fenced.is_some() {
                    return Ok(BatchOutcome::NewlyFenced);
                }
                changed |= count_change && !result.already_committed;
            }
            Ok(BatchOutcome::Complete { changed })
        })
        .map_err(|error| CausalPositionError::BucketCommit {
            wallet,
            message: error.to_string(),
        })?;
    match outcome {
        BatchOutcome::Complete { changed } => Ok(changed),
        BatchOutcome::NewlyFenced => {
            // The fence bucket and every preceding bucket are durable now.
            Err(CausalPositionError::Fenced { wallet })
        }
    }
}

/// Canonical exact ledger proof for one wallet.
pub fn ledger_capture(
    ledger: &PositionLedger,
    paper_state: &PaperStateDb,
    wallet: WalletAddress,
) -> Result<AdmissionLedgerCapture, CausalPositionError> {
    let mut all = BTreeMap::<(String, u16), (ShareAmount, ShareAmount)>::new();
    if let Some(snapshot) = ledger.position(&wallet) {
        for (key, state) in &snapshot.positions {
            all.insert(
                (key.market().to_string(), key.outcome().0),
                (state.long_contracts, state.short_contracts),
            );
        }
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"prediction-edge/admission-ledger/v1\0");
    hash_part(&mut hasher, wallet.to_string().as_bytes())?;
    for ((condition_id, outcome), (long, short)) in all {
        hash_part(&mut hasher, condition_id.as_bytes())?;
        hash_part(&mut hasher, &outcome.to_be_bytes())?;
        hash_part(&mut hasher, &long.atomic().to_be_bytes())?;
        hash_part(&mut hasher, &short.atomic().to_be_bytes())?;
    }
    let coverage = paper_state.wallet_coverage(&wallet)?;
    Ok(AdmissionLedgerCapture {
        wallet,
        hash: hasher.finalize().to_hex().to_string(),
        cursor: paper_state.cursor(&wallet)?,
        anchor_seq: coverage.anchor_seq,
        coverage_generation: coverage.coverage_generation,
    })
}

fn ordinary_position_balances(
    read: &CompletePositionsRead,
) -> Result<Vec<(MarketId, OutcomeId, ShareAmount)>, CausalPositionError> {
    let mut balances = BTreeMap::new();
    for position in &read.positions {
        if position.classification == PositionClassification::Combo
            || position.size == ShareAmount::ZERO
        {
            continue;
        }
        let key = (position.condition_id.0.clone(), position.outcome.0);
        if balances.insert(key.clone(), position.size).is_some() {
            return Err(CausalPositionError::DuplicateOutcome {
                condition_id: key.0,
                outcome: key.1,
            });
        }
    }
    Ok(balances
        .into_iter()
        .map(|((condition_id, outcome), amount)| {
            (
                MarketId(VenueMarketId(condition_id)),
                OutcomeId(outcome),
                amount,
            )
        })
        .collect())
}

fn hash_part(hasher: &mut blake3::Hasher, value: &[u8]) -> Result<(), CausalPositionError> {
    let len = u64::try_from(value.len()).map_err(|_| CausalPositionError::ProofComponentTooLong)?;
    hasher.update(&len.to_be_bytes());
    hasher.update(value);
    Ok(())
}

async fn capture_control(
    wallet: WalletAddress,
    control_tx: &mpsc::Sender<OrchestratorControl>,
) -> Result<AdmissionLedgerCapture, CausalPositionError> {
    let (captured, acknowledgement) = oneshot::channel();
    control_tx
        .send(OrchestratorControl::CaptureAdmissionLedger { wallet, captured })
        .await
        .map_err(|_| CausalPositionError::ControlClosed)?;
    acknowledgement
        .await
        .map_err(|_| CausalPositionError::AcknowledgementClosed)?
        .map_err(|message| CausalPositionError::BucketCommit { wallet, message })
}

// Retained only for the isolated stopped canary, whose authenticated posture
// needs strict `outcomeIndex` parsing but does not mutate the ordinary ledger.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CanaryRawPosition {
    condition_id: String,
    outcome_index: Option<u16>,
    size: Decimal,
}

#[derive(Debug, thiserror::Error)]
pub enum PositionParseError {
    #[error("failed to deserialize positions response: {0}")]
    Json(#[from] serde_json::Error),
    #[error("position omitted outcomeIndex")]
    MissingOutcomeIndex,
    #[error("position has invalid exact size {value}: {reason}")]
    InvalidAmount { value: Decimal, reason: String },
}

pub fn parse_positions_strict(
    bytes: &[u8],
    wallet: WalletAddress,
) -> Result<PositionSnapshot, PositionParseError> {
    let raw: Vec<CanaryRawPosition> = serde_json::from_slice(bytes)?;
    let mut positions = HashMap::new();
    for item in raw {
        let outcome = item
            .outcome_index
            .ok_or(PositionParseError::MissingOutcomeIndex)?;
        let amount = ShareAmount::from_decimal_exact(item.size).map_err(|error| {
            PositionParseError::InvalidAmount {
                value: item.size,
                reason: error.to_string(),
            }
        })?;
        if amount == ShareAmount::ZERO {
            continue;
        }
        positions.insert(
            MarketOutcomeId::new(
                MarketId(VenueMarketId(item.condition_id)),
                OutcomeId(outcome),
            ),
            PositionState {
                long_contracts: amount,
                short_contracts: ShareAmount::ZERO,
            },
        );
    }
    Ok(PositionSnapshot { wallet, positions })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::source_event_sink::SourceEventSink;
    use pe_source_polymarket_public::activity::{ActivityParseError, ActivityValidationError};
    use pe_source_polymarket_public::{
        ActivityParseContext, ActivityReadError, ActivityTransport, PositionPartition,
        PositionReadError, aggregate_activity_rows, parse_activity_response,
    };

    #[test]
    fn admission_failure_classes_follow_typed_origins() {
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let source = |index| match index {
            0 => SourceError::Transient {
                message: "exhausted".to_owned(),
            },
            1 => SourceError::RateLimited {
                retry_after_secs: 1,
            },
            2 => SourceError::Fatal {
                message: "opaque".to_owned(),
            },
            _ => unreachable!(),
        };
        for (index, identity_class) in [
            (0, FailureClass::WalletTransient),
            (1, FailureClass::Shared),
            (2, FailureClass::Shared),
        ] {
            assert_eq!(
                CausalPositionError::Activity {
                    wallet,
                    source: ActivityReadError::Fetch {
                        url: String::new(),
                        source: source(index)
                    }
                }
                .class(),
                FailureClass::Shared
            );
            assert_eq!(
                CausalPositionError::Positions {
                    wallet,
                    source: PositionReadError::Fetch {
                        url: String::new(),
                        source: source(index)
                    }
                }
                .class(),
                FailureClass::Shared
            );
            assert_eq!(
                CausalPositionError::Identity {
                    wallet,
                    source: source(index)
                }
                .class(),
                identity_class
            );
        }
        assert_eq!(
            classify_position_read(&PositionReadError::MissingActivityMapping {
                asset: "a".to_owned()
            }),
            FailureClass::WalletPersistent
        );
        assert_eq!(
            classify_position_read(&PositionReadError::MetadataUnresolved {
                asset: "a".to_owned(),
                reason: String::new()
            }),
            FailureClass::Shared
        );
        assert_eq!(
            classify_position_read(&PositionReadError::PageTooLarge {
                row_count: 2,
                limit: 1
            }),
            FailureClass::Shared
        );
        assert_eq!(
            classify_position_read(&PositionReadError::SaturatedTerminalPage {
                partition: PositionPartition::Redeemable,
                offset: 0
            }),
            FailureClass::WalletPersistent
        );
        assert_eq!(
            classify_position_read(&PositionReadError::ProofComponentTooLong),
            FailureClass::Shared
        );
        assert_eq!(
            classify_activity_read(&ActivityReadError::InvalidSplit {
                start: None,
                end: 1,
                boundary: 1
            }),
            FailureClass::Shared
        );
        assert_eq!(
            classify_activity_read(&ActivityReadError::RowOutsideBounds {
                timestamp: 1,
                start: None,
                end: 0
            }),
            FailureClass::Shared
        );
        assert_eq!(
            classify_activity_read(&ActivityReadError::PageTooLarge {
                row_count: 2,
                limit: 1
            }),
            FailureClass::Shared
        );
        assert_eq!(
            classify_activity_read(&ActivityReadError::SaturatedTerminalSecond {
                end: 1,
                offset: 0
            }),
            FailureClass::WalletPersistent
        );
        assert_eq!(
            classify_activity_read(&ActivityReadError::Parse(ActivityParseError::InvalidRow {
                row_index: 0,
                source: ActivityValidationError::InvalidPrice {
                    value: rust_decimal::Decimal::ONE,
                    reason: String::new()
                }
            })),
            FailureClass::WalletPersistent
        );
        assert_eq!(
            classify_activity_read(&ActivityReadError::Parse(ActivityParseError::InvalidRow {
                row_index: 0,
                source: ActivityValidationError::MissingField { field: "price" }
            })),
            FailureClass::Shared
        );
        assert_eq!(
            classify_activity_read(&ActivityReadError::Parse(ActivityParseError::Json {
                message: String::new()
            })),
            FailureClass::Shared
        );
        assert_eq!(
            classify_position_read(&PositionReadError::WalletMismatch {
                row_index: 0,
                requested_wallet: wallet,
                payload_wallet: WalletAddress::from_hex(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                )
                .unwrap()
            }),
            FailureClass::Shared
        );
        assert_eq!(
            CausalPositionError::NonMonotonicActivityBounds {
                wallet,
                previous: 1,
                next: 0
            }
            .class(),
            FailureClass::Shared
        );
        assert_eq!(
            CausalPositionError::ProofComponentTooLong.class(),
            FailureClass::Shared
        );
        assert_eq!(
            crate::bucket_commit::AnchorInstallError::CutoffRegression {
                wallet,
                stored: 2,
                candidate: 1
            }
            .class(),
            FailureClass::WalletTransient
        );
        assert_eq!(
            crate::bucket_commit::AnchorInstallError::LedgerHashChanged { wallet }.class(),
            FailureClass::WalletTransient
        );
        assert_eq!(
            crate::bucket_commit::AnchorInstallError::Durability("failed".to_owned()).class(),
            FailureClass::Shared
        );
        use crate::watchlist_maintenance::{
            MembershipApplyError, PublishError, WalletPublishCause,
        };
        for (cause, class) in [
            (
                WalletPublishCause::FencedAdmission,
                FailureClass::WalletPersistent,
            ),
            (
                WalletPublishCause::IncompleteHistory,
                FailureClass::WalletPersistent,
            ),
            (
                WalletPublishCause::UnvalidatedPosition,
                FailureClass::WalletTransient,
            ),
            (
                WalletPublishCause::ProofChanged,
                FailureClass::WalletTransient,
            ),
        ] {
            assert_eq!(
                MembershipApplyError::Publication(PublishError::Wallet { wallet, cause }).class(),
                class
            );
        }
        assert_eq!(
            crate::watchlist_admission::AdmissionError::ControlClosed.class(),
            FailureClass::Shared
        );
    }

    struct EmptyFetcher;

    impl ReconciliationFetcher for EmptyFetcher {
        fn fetch<'a>(
            &'a self,
            _url: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
            Box::pin(async { Ok(b"[]".to_vec()) })
        }
    }

    #[tokio::test]
    async fn recorder_append_failure_is_wrapped_as_a_fatal_source_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = SourceEventSink::open(dir.path().join("source.log")).unwrap();
        sink.fail_next_append();
        let fetcher = DurableRecordingFetcher {
            inner: Arc::new(EmptyFetcher),
            sink: Arc::new(tokio::sync::Mutex::new(sink)),
        };

        let error = fetcher
            .fetch("https://api.example.com/activity")
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            SourceError::Fatal { message }
                if message.contains("source-log append failed: I/O error: injected append failure")
        ));
    }

    #[tokio::test]
    async fn control_commit_shares_one_context_across_read_buckets() {
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let observed_at = time::OffsetDateTime::from_unix_timestamp(100).unwrap();
        let row = |epoch: i64, asset: &str, market: &str, tx: &str| {
            json!({
                "proxyWallet": wallet,
                "timestamp": epoch,
                "conditionId": market,
                "type": "TRADE",
                "size": "1",
                "usdcSize": "0.5",
                "transactionHash": tx,
                "price": "0.5",
                "asset": asset,
                "side": "BUY",
                "outcomeIndex": 0,
                "outcome": "Yes",
                "isCombo": false
            })
        };
        let parsed = parse_activity_response(
            &serde_json::to_vec(&vec![
                row(1, "asset-first", "market-first", "0xfirst"),
                row(2, "asset-second", "market-second", "0xsecond"),
            ])
            .unwrap(),
            wallet,
            &ActivityParseContext {
                source_id: SourceId("fixture".to_owned()),
                observed_at: SourceTimestamp(observed_at),
                received_at: ReceivedAt(observed_at),
                transport: ActivityTransport::Rest,
            },
        )
        .unwrap();
        let activity = CompleteActivityRead {
            requested_wallet: wallet,
            fixed_end: 100,
            rows: parsed.rows,
            pages: Vec::new(),
        };
        let prepared = PreparedActivity {
            mapping: activity.asset_mapping().unwrap(),
            identity_overrides: HashMap::new(),
            identity_unresolved: HashSet::new(),
            no_copy_dispositions: HashMap::new(),
            unresolved_assets: BTreeMap::new(),
            metadata_reads: BTreeMap::new(),
        };
        let dir = tempfile::tempdir().unwrap();
        let fetcher: Arc<dyn ReconciliationFetcher> = Arc::new(EmptyFetcher);
        let source_log = Arc::new(tokio::sync::Mutex::new(
            SourceEventSink::open(dir.path().join("source.log")).unwrap(),
        ));
        let resolver = Arc::new(AssetIdentityResolver::new(
            Arc::clone(&fetcher),
            "https://gamma.example.com".to_owned(),
            10,
            source_log,
        ));
        let validator = CausalPositionValidator::new(
            fetcher,
            "https://data.example.com",
            "fixture-generation",
            resolver,
        );
        let (control_tx, mut control_rx) = mpsc::channel(2);
        let actor = tokio::spawn(async move {
            let mut first_context = None;
            for source_epoch in [1, 2] {
                let Some(OrchestratorControl::CommitActivityBucket {
                    context, committed, ..
                }) = control_rx.recv().await
                else {
                    return false;
                };
                if let Some(first_context) = &first_context
                    && !Arc::ptr_eq(first_context, &context)
                {
                    return false;
                }
                first_context.get_or_insert_with(|| Arc::clone(&context));
                committed
                    .send(Ok(crate::bucket_commit::BucketCommitResult {
                        retained_revision: false,
                        wallet,
                        source_epoch,
                        dispositions: BTreeMap::new(),
                        pending: Vec::new(),
                        newly_fenced: None,
                        already_committed: false,
                    }))
                    .unwrap();
            }
            true
        });

        assert!(
            validator
                .commit_control(wallet, &activity, &prepared, &control_tx, true)
                .await
                .unwrap()
        );
        assert!(actor.await.unwrap());
    }

    #[test]
    fn direct_batch_surfaces_fence_after_committing_its_prefix() {
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let observed_at = time::OffsetDateTime::from_unix_timestamp(100).unwrap();
        let row = |epoch: i64, activity_type: &str, asset: &str, market: &str, tx: &str| {
            json!({
                "proxyWallet": wallet,
                "timestamp": epoch,
                "conditionId": market,
                "type": activity_type,
                "size": "1",
                "usdcSize": "0.5",
                "transactionHash": tx,
                "price": "0.5",
                "asset": asset,
                "side": "BUY",
                "outcomeIndex": 0,
                "outcome": "Yes",
                "isCombo": false
            })
        };
        let parsed = parse_activity_response(
            &serde_json::to_vec(&vec![
                row(1, "TRADE", "asset-first", "market-first", "0xfirst"),
                row(2, "REDEEM", "asset-fence", "market-fence", "0xfence"),
                row(3, "TRADE", "asset-later", "market-later", "0xlater"),
            ])
            .unwrap(),
            wallet,
            &ActivityParseContext {
                source_id: SourceId("fixture".to_owned()),
                observed_at: SourceTimestamp(observed_at),
                received_at: ReceivedAt(observed_at),
                transport: ActivityTransport::Rest,
            },
        )
        .unwrap();
        let aggregates = aggregate_activity_rows(&parsed.rows).unwrap();
        let first_id = aggregates[0].group_id.key().clone();
        let fence_id = aggregates[1].group_id.key().clone();
        let later_id = aggregates[2].group_id.key().clone();
        let activity = CompleteActivityRead {
            requested_wallet: wallet,
            fixed_end: 100,
            rows: parsed.rows,
            pages: Vec::new(),
        };
        let prepared = PreparedActivity {
            mapping: activity.asset_mapping().unwrap(),
            identity_overrides: HashMap::new(),
            identity_unresolved: HashSet::new(),
            no_copy_dispositions: HashMap::new(),
            unresolved_assets: BTreeMap::new(),
            metadata_reads: BTreeMap::new(),
        };
        let dir = tempfile::tempdir().unwrap();
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let mut engine =
            BucketCommitEngine::load(Arc::clone(&paper), PositionLedger::new()).unwrap();
        paper.set_cursor(&wallet, 0).unwrap();
        let captured = ledger_capture(engine.ledger(), &paper, wallet).unwrap();
        engine
            .install_anchors(&[AnchorInstall {
                fresh_history: Vec::new(),
                expected_fence: None,
                history_status: None,
                wallet,
                balances: Vec::new(),
                cutoff: 0,
                proof: AnchorProof {
                    positions_proof_hash: "fixture".to_owned(),
                    activity_bounds_json: "[]".to_owned(),
                    source_log_generation: "fixture-generation".to_owned(),
                    document: "{}".to_owned(),
                    recorded_at_unix: 0,
                },
                expected: AnchorExpectation {
                    ledger_hash: captured.hash,
                    cursor: captured.cursor,
                    anchor_seq: captured.anchor_seq,
                    coverage_generation: captured.coverage_generation,
                },
            }])
            .unwrap();

        let error = commit_direct(
            wallet,
            &activity,
            &prepared,
            &mut engine,
            false,
            "fixture-generation",
        )
        .unwrap_err();

        assert!(matches!(error, CausalPositionError::Fenced { .. }));
        assert!(paper.activity_group_state(&first_id).unwrap().is_some());
        assert!(paper.activity_group_state(&fence_id).unwrap().is_some());
        assert!(paper.activity_group_state(&later_id).unwrap().is_none());
        assert!(paper.is_wallet_fenced(&wallet).unwrap());
        assert!(engine.is_fenced(&wallet));
        assert_eq!(
            engine
                .ledger()
                .position(&wallet)
                .and_then(|snapshot| {
                    snapshot.positions.get(&MarketOutcomeId::new(
                        MarketId(VenueMarketId("market-first".to_owned())),
                        OutcomeId(0),
                    ))
                })
                .map(|position| position.long_contracts.atomic()),
            Some(1_000_000)
        );
    }
    #[test]
    fn paper_service_rollout_identity_transient_is_wallet_scoped_and_rate_limit_shared() {
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        for (source, expected) in [
            (
                SourceError::Transient {
                    message: "Gamma unavailable".to_owned(),
                },
                FailureClass::WalletTransient,
            ),
            (
                SourceError::RateLimited {
                    retry_after_secs: 1,
                },
                FailureClass::Shared,
            ),
            (
                SourceError::Fatal {
                    message: "durability unavailable".to_owned(),
                },
                FailureClass::Shared,
            ),
        ] {
            assert_eq!(
                CausalPositionError::Identity { wallet, source }.class(),
                expected
            );
        }
    }
}
