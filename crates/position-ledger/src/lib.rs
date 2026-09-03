//! Pure deterministic position accumulator.
//!
//! [`PositionLedger`] is a stateful in-memory accumulator; it has no I/O and produces
//! deterministic output given the same ordered input stream.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use pe_copy_signal_engine::{IncomingTrade, PositionSnapshot, PositionState};
use pe_core_types::{
    MarketId, MarketOutcomeId, OutcomeId, Price, ShareAmount, Side, SourceTimestamp, SourceTradeId,
    VenueMarketId, WalletAddress,
};
use pe_source_polymarket_public::{ActivityAggregate, ActivityType};

/// One exact, reconciled position effect derived from a complete activity group (#544).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerMutation {
    pub source_trade_id: SourceTradeId,
    pub transaction_hash: String,
    pub wallet: WalletAddress,
    pub source_time: SourceTimestamp,
    pub effect: LedgerEffect,
}

/// Venue-metadata proof that rebinds a stamped activity identity (#555).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityCorrection {
    pub stamped: MarketOutcomeId,
    pub verified: MarketOutcomeId,
    pub evidence_hash: String,
}

/// Supported ledger effects. Non-mutating rows are retained so a complete
/// bucket can prove every group reached a durable disposition (#544).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerEffect {
    Trade {
        market_id: MarketId,
        outcome_id: OutcomeId,
        side: Side,
        amount: ShareAmount,
        price: Price,
    },
    Split {
        market_id: MarketId,
        amount: ShareAmount,
    },
    Merge {
        market_id: MarketId,
        amount: ShareAmount,
    },
    Redeem {
        market_id: MarketId,
        outcome_id: OutcomeId,
        amount: ShareAmount,
    },
    RequiresAnchor,
    Conversion,
    RawOnly,
    UnknownEffect,
    /// An effect whose identity was rebound after aggregation while retaining
    /// the source-stamped identity as replay evidence.
    Corrected {
        effect: Box<LedgerEffect>,
        correction: IdentityCorrection,
    },
}

/// Typed failures while decoding a persisted versioned [`LedgerEffect`] document.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LedgerEffectDocumentError {
    #[error("unsupported ledger effect document version {version}")]
    UnknownVersion { version: u64 },
    #[error("malformed ledger effect document: {message}")]
    Malformed { message: String },
}

const LEDGER_EFFECT_DOCUMENT_VERSION_V1: u64 = 1;
const LEDGER_EFFECT_DOCUMENT_VERSION_V2: u64 = 2;
const LEDGER_EFFECT_DOCUMENT_VERSION_V3: u64 = 3;

/// One four-decimal `/positions` quantum. Per EVIDENCE_FIXFWD.md F1,
/// redeem residuals `1..=99` clamp while a residual of 100 fences.
pub const REDEEM_RESIDUAL_LIMIT_ATOMIC: u64 = 100;

/// The result of applying one ledger effect, including any bounded redeem
/// residual needed to prove production/replay parity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedEffect {
    pub effect: LedgerEffect,
    pub clamped_residual: Option<u64>,
}

/// Versioned persisted form of a [`LedgerEffect`] (issue #555): the exact
/// normalized mutation stored beside each activity-group disposition so a
/// wallet ledger replays from anchors plus groups with zero network.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LedgerEffectDocument {
    version: u64,
    effect: LedgerEffectDto,
    #[serde(skip_serializing_if = "Option::is_none")]
    clamped_residual_atomic: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    correction: Option<IdentityCorrection>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum LedgerEffectDto {
    Trade {
        market: MarketId,
        outcome: OutcomeId,
        side: Side,
        price: Price,
        amount: ShareAmount,
    },
    Split {
        market: MarketId,
        amount: ShareAmount,
    },
    Merge {
        market: MarketId,
        amount: ShareAmount,
    },
    Redeem {
        market: MarketId,
        outcome: OutcomeId,
        amount: ShareAmount,
    },
    RequiresAnchor,
    Conversion,
    RawOnly,
    UnknownEffect,
}

impl From<&LedgerEffect> for LedgerEffectDto {
    fn from(effect: &LedgerEffect) -> Self {
        match effect {
            LedgerEffect::Trade {
                market_id,
                outcome_id,
                side,
                amount,
                price,
            } => Self::Trade {
                market: market_id.clone(),
                outcome: *outcome_id,
                side: *side,
                price: *price,
                amount: *amount,
            },
            LedgerEffect::Split { market_id, amount } => Self::Split {
                market: market_id.clone(),
                amount: *amount,
            },
            LedgerEffect::Merge { market_id, amount } => Self::Merge {
                market: market_id.clone(),
                amount: *amount,
            },
            LedgerEffect::Redeem {
                market_id,
                outcome_id,
                amount,
            } => Self::Redeem {
                market: market_id.clone(),
                outcome: *outcome_id,
                amount: *amount,
            },
            LedgerEffect::RequiresAnchor => Self::RequiresAnchor,
            LedgerEffect::Conversion => Self::Conversion,
            LedgerEffect::RawOnly => Self::RawOnly,
            LedgerEffect::UnknownEffect => Self::UnknownEffect,
            LedgerEffect::Corrected { effect, .. } => Self::from(effect.as_ref()),
        }
    }
}

impl From<LedgerEffectDto> for LedgerEffect {
    fn from(dto: LedgerEffectDto) -> Self {
        match dto {
            LedgerEffectDto::Trade {
                market,
                outcome,
                side,
                price,
                amount,
            } => Self::Trade {
                market_id: market,
                outcome_id: outcome,
                side,
                amount,
                price,
            },
            LedgerEffectDto::Split { market, amount } => Self::Split {
                market_id: market,
                amount,
            },
            LedgerEffectDto::Merge { market, amount } => Self::Merge {
                market_id: market,
                amount,
            },
            LedgerEffectDto::Redeem {
                market,
                outcome,
                amount,
            } => Self::Redeem {
                market_id: market,
                outcome_id: outcome,
                amount,
            },
            LedgerEffectDto::RequiresAnchor => Self::RequiresAnchor,
            LedgerEffectDto::Conversion => Self::Conversion,
            LedgerEffectDto::RawOnly => Self::RawOnly,
            LedgerEffectDto::UnknownEffect => Self::UnknownEffect,
        }
    }
}

impl LedgerEffect {
    /// Return correction evidence parsed from or destined for a version-two
    /// effect document.
    #[must_use]
    pub const fn correction(&self) -> Option<&IdentityCorrection> {
        match self {
            Self::Corrected { correction, .. } => Some(correction),
            Self::Trade { .. }
            | Self::Split { .. }
            | Self::Merge { .. }
            | Self::Redeem { .. }
            | Self::RequiresAnchor
            | Self::Conversion
            | Self::RawOnly
            | Self::UnknownEffect => None,
        }
    }

    /// Return the authoritative effect to use for classification and replay.
    /// A corrected effect already contains the verified market/outcome.
    #[must_use]
    pub fn effective(&self) -> &Self {
        match self {
            Self::Corrected { effect, .. } => effect.effective(),
            Self::Trade { .. }
            | Self::Split { .. }
            | Self::Merge { .. }
            | Self::Redeem { .. }
            | Self::RequiresAnchor
            | Self::Conversion
            | Self::RawOnly
            | Self::UnknownEffect => self,
        }
    }

    fn identity(&self) -> Option<MarketOutcomeId> {
        match self.effective() {
            Self::Trade {
                market_id,
                outcome_id,
                ..
            }
            | Self::Redeem {
                market_id,
                outcome_id,
                ..
            } => Some(MarketOutcomeId::new(market_id.clone(), *outcome_id)),
            Self::Split { .. }
            | Self::Merge { .. }
            | Self::RequiresAnchor
            | Self::Conversion
            | Self::RawOnly
            | Self::UnknownEffect => None,
            Self::Corrected { .. } => None,
        }
    }

    fn into_effective(self) -> Self {
        match self {
            Self::Corrected { effect, .. } => effect.into_effective(),
            effect => effect,
        }
    }

    fn with_identity(self, verified: &MarketOutcomeId) -> Self {
        match self {
            Self::Trade {
                side,
                amount,
                price,
                ..
            } => Self::Trade {
                market_id: verified.market().clone(),
                outcome_id: verified.outcome(),
                side,
                amount,
                price,
            },
            Self::Redeem { amount, .. } => Self::Redeem {
                market_id: verified.market().clone(),
                outcome_id: verified.outcome(),
                amount,
            },
            effect => effect,
        }
    }

    /// Canonical JSON document persisted with an activity-group disposition.
    /// Field order is the declaration order above, so the encoding is
    /// deterministic for equal effects.
    pub fn to_document(&self) -> Result<String, LedgerEffectDocumentError> {
        AppliedEffect {
            effect: self.clone(),
            clamped_residual: None,
        }
        .to_document()
    }

    /// Decode a versioned effect document; unknown versions and any missing,
    /// extra, or malformed field are typed failures.
    pub fn from_document(document: &str) -> Result<Self, LedgerEffectDocumentError> {
        Ok(AppliedEffect::from_document(document)?.effect)
    }
}

impl AppliedEffect {
    /// Canonical JSON document for an applied outcome. Version three is used
    /// only for a redeem whose bounded residual was clamped.
    pub fn to_document(&self) -> Result<String, LedgerEffectDocumentError> {
        validate_clamped_residual(&self.effect, self.clamped_residual)?;
        // Encoding through `Value` sorts object keys, so equal effects always
        // produce byte-identical documents.
        let correction = self.effect.correction().cloned();
        let version = if self.clamped_residual.is_some() {
            LEDGER_EFFECT_DOCUMENT_VERSION_V3
        } else if correction.is_some() {
            LEDGER_EFFECT_DOCUMENT_VERSION_V2
        } else {
            LEDGER_EFFECT_DOCUMENT_VERSION_V1
        };
        let value = serde_json::to_value(LedgerEffectDocument {
            version,
            effect: self.effect.effective().into(),
            clamped_residual_atomic: self.clamped_residual,
            correction,
        })
        .map_err(malformed)?;
        serde_json::to_string(&value).map_err(malformed)
    }

    /// Decode a versioned effect document together with its expected redeem
    /// residual. Version-one and version-two documents yield no residual.
    pub fn from_document(document: &str) -> Result<Self, LedgerEffectDocumentError> {
        let value: serde_json::Value = serde_json::from_str(document).map_err(malformed)?;
        let decoded: LedgerEffectDocument =
            serde_json::from_value(value.clone()).map_err(malformed)?;
        let (effect, clamped_residual) = match (
            decoded.version,
            decoded.correction,
            decoded.clamped_residual_atomic,
        ) {
            (LEDGER_EFFECT_DOCUMENT_VERSION_V1, None, None) => (decoded.effect.into(), None),
            (LEDGER_EFFECT_DOCUMENT_VERSION_V1, Some(_), _) => {
                return Err(LedgerEffectDocumentError::Malformed {
                    message: "version 1 ledger effect document contains a correction".to_owned(),
                });
            }
            (LEDGER_EFFECT_DOCUMENT_VERSION_V1, None, Some(_)) => {
                return Err(LedgerEffectDocumentError::Malformed {
                    message: "version 1 ledger effect document contains a clamped residual"
                        .to_owned(),
                });
            }
            (LEDGER_EFFECT_DOCUMENT_VERSION_V2, Some(correction), None) => {
                (corrected_effect(decoded.effect.into(), correction)?, None)
            }
            (LEDGER_EFFECT_DOCUMENT_VERSION_V2, None, _) => {
                return Err(LedgerEffectDocumentError::Malformed {
                    message: "version 2 ledger effect document is missing its correction"
                        .to_owned(),
                });
            }
            (LEDGER_EFFECT_DOCUMENT_VERSION_V2, Some(_), Some(_)) => {
                return Err(LedgerEffectDocumentError::Malformed {
                    message: "version 2 ledger effect document contains a clamped residual"
                        .to_owned(),
                });
            }
            (LEDGER_EFFECT_DOCUMENT_VERSION_V3, correction, Some(residual)) => {
                let effect = match correction {
                    Some(correction) => corrected_effect(decoded.effect.into(), correction)?,
                    None => decoded.effect.into(),
                };
                validate_clamped_residual(&effect, Some(residual))?;
                (effect, Some(residual))
            }
            (LEDGER_EFFECT_DOCUMENT_VERSION_V3, _, None) => {
                return Err(LedgerEffectDocumentError::Malformed {
                    message: "version 3 ledger effect document is missing its clamped residual"
                        .to_owned(),
                });
            }
            (version, _, _) => {
                return Err(LedgerEffectDocumentError::UnknownVersion { version });
            }
        };
        let applied = Self {
            effect,
            clamped_residual,
        };
        // Exact decoding: the input must be the canonical encoding of what it
        // decoded to, which rejects extra fields on any variant.
        if serde_json::to_string(&value).map_err(malformed)? != applied.to_document()? {
            return Err(LedgerEffectDocumentError::Malformed {
                message: "document is not the canonical encoding of its effect".to_owned(),
            });
        }
        Ok(applied)
    }
}

fn corrected_effect(
    effect: LedgerEffect,
    correction: IdentityCorrection,
) -> Result<LedgerEffect, LedgerEffectDocumentError> {
    if correction.stamped == correction.verified {
        return Err(LedgerEffectDocumentError::Malformed {
            message: "ledger identity correction does not change identity".to_owned(),
        });
    }
    if effect.identity().as_ref() != Some(&correction.verified) {
        return Err(LedgerEffectDocumentError::Malformed {
            message: "corrected effect identity does not match verified identity".to_owned(),
        });
    }
    Ok(LedgerEffect::Corrected {
        effect: Box::new(effect),
        correction,
    })
}

fn validate_clamped_residual(
    effect: &LedgerEffect,
    clamped_residual: Option<u64>,
) -> Result<(), LedgerEffectDocumentError> {
    let Some(residual) = clamped_residual else {
        return Ok(());
    };
    if residual == 0 || residual >= REDEEM_RESIDUAL_LIMIT_ATOMIC {
        return Err(LedgerEffectDocumentError::Malformed {
            message: "clamped redeem residual is outside 1..=99 atomic".to_owned(),
        });
    }
    if !matches!(effect.effective(), LedgerEffect::Redeem { .. }) {
        return Err(LedgerEffectDocumentError::Malformed {
            message: "clamped residual belongs only to a redeem effect".to_owned(),
        });
    }
    Ok(())
}

fn malformed(error: impl std::fmt::Display) -> LedgerEffectDocumentError {
    LedgerEffectDocumentError::Malformed {
        message: error.to_string(),
    }
}

impl LedgerMutation {
    /// Map a source-owned reconciled aggregate into the ledger domain.
    pub fn from_activity(aggregate: &ActivityAggregate) -> Result<Self, LedgerError> {
        aggregate
            .group_id
            .verify_components()
            .map_err(|_| LedgerError::InvalidMapping {
                source_trade_id: aggregate.group_id.key().clone(),
            })?;
        let components = aggregate.group_id.components();
        let source_trade_id = aggregate.group_id.key().clone();
        let market = || {
            components
                .condition_id
                .as_ref()
                .map(|condition| MarketId(VenueMarketId(condition.0.clone())))
                .ok_or_else(|| LedgerError::InvalidMapping {
                    source_trade_id: source_trade_id.clone(),
                })
        };
        let effect = match &components.activity_type {
            ActivityType::Conversion => LedgerEffect::Conversion,
            ActivityType::Unknown(_) => LedgerEffect::UnknownEffect,
            ActivityType::Reward
            | ActivityType::Deposit
            | ActivityType::Withdrawal
            | ActivityType::Yield
            | ActivityType::MakerRebate
            | ActivityType::TakerRebate
            | ActivityType::ReferralReward => LedgerEffect::RawOnly,
            _ if aggregate.is_combo => LedgerEffect::RawOnly,
            ActivityType::Redeem if aggregate.share_sum == ShareAmount::ZERO => {
                LedgerEffect::RequiresAnchor
            }
            // Other zero-share position-changing rows are arithmetically zero.
            // `Unknown` stays fenced above regardless of size because its effect
            // cannot be trusted as zero.
            _ if aggregate.share_sum == ShareAmount::ZERO => LedgerEffect::RawOnly,
            _ => match &components.activity_type {
                ActivityType::Trade => LedgerEffect::Trade {
                    market_id: market()?,
                    outcome_id: components
                        .outcome
                        .ok_or_else(|| LedgerError::InvalidMapping {
                            source_trade_id: source_trade_id.clone(),
                        })?,
                    side: components.side.ok_or_else(|| LedgerError::InvalidMapping {
                        source_trade_id: source_trade_id.clone(),
                    })?,
                    amount: aggregate.share_sum,
                    price: aggregate.volume_weighted_price().map_err(|_| {
                        LedgerError::InvalidMapping {
                            source_trade_id: source_trade_id.clone(),
                        }
                    })?,
                },
                ActivityType::Split => LedgerEffect::Split {
                    market_id: market()?,
                    amount: aggregate.share_sum,
                },
                ActivityType::Merge => LedgerEffect::Merge {
                    market_id: market()?,
                    amount: aggregate.share_sum,
                },
                ActivityType::Redeem => match components.outcome {
                    Some(outcome_id) => LedgerEffect::Redeem {
                        market_id: market()?,
                        outcome_id,
                        amount: aggregate.share_sum,
                    },
                    None => LedgerEffect::RequiresAnchor,
                },
                ActivityType::Conversion
                | ActivityType::Unknown(_)
                | ActivityType::Reward
                | ActivityType::Deposit
                | ActivityType::Withdrawal
                | ActivityType::Yield
                | ActivityType::MakerRebate
                | ActivityType::TakerRebate
                | ActivityType::ReferralReward => LedgerEffect::RawOnly,
            },
        };
        Ok(Self {
            source_trade_id,
            transaction_hash: components.transaction_hash.clone(),
            wallet: components.wallet,
            source_time: aggregate.source_time.clone(),
            effect,
        })
    }

    /// Rebind a single-outcome trade or redemption to venue-verified identity.
    /// The reconciled source-group key and all other mutation evidence remain
    /// unchanged. Effects without a single market/outcome identity are left
    /// unchanged.
    #[must_use]
    pub fn with_verified_identity(
        mut self,
        verified: MarketOutcomeId,
        evidence_hash: String,
    ) -> Self {
        let Some(current) = self.effect.identity() else {
            return self;
        };
        if current == verified {
            return self;
        }
        let stamped = self
            .effect
            .correction()
            .map_or_else(|| current.clone(), |correction| correction.stamped.clone());
        let effect = self.effect.into_effective().with_identity(&verified);
        self.effect = LedgerEffect::Corrected {
            effect: Box::new(effect),
            correction: IdentityCorrection {
                stamped,
                verified,
                evidence_hash,
            },
        };
        self
    }

    #[must_use]
    pub fn touched_keys(&self) -> Vec<MarketOutcomeId> {
        match self.effect.effective() {
            LedgerEffect::Trade {
                market_id,
                outcome_id,
                ..
            }
            | LedgerEffect::Redeem {
                market_id,
                outcome_id,
                ..
            } => vec![MarketOutcomeId::new(market_id.clone(), *outcome_id)],
            LedgerEffect::Split { market_id, .. } | LedgerEffect::Merge { market_id, .. } => vec![
                MarketOutcomeId::new(market_id.clone(), OutcomeId(0)),
                MarketOutcomeId::new(market_id.clone(), OutcomeId(1)),
            ],
            LedgerEffect::RequiresAnchor
            | LedgerEffect::Conversion
            | LedgerEffect::RawOnly
            | LedgerEffect::UnknownEffect => Vec::new(),
            LedgerEffect::Corrected { .. } => Vec::new(),
        }
    }
}

/// Defined causes that require the orchestrator to durably fence one wallet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalletFenceCause {
    RevisedAggregate,
    LateEqualSecondGroup,
    InvalidMapping,
    Underflow,
    Overflow,
    Conversion,
    UnknownEffect,
    OrderDependentEqualSecond,
}

impl WalletFenceCause {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RevisedAggregate => "revised_applied_aggregate",
            Self::LateEqualSecondGroup => "late_group_after_bucket_commit",
            Self::InvalidMapping => "invalid_mapping",
            Self::Underflow => "position_underflow",
            Self::Overflow => "position_overflow",
            Self::Conversion => "conversion_unknown_conditions",
            Self::UnknownEffect => "unknown_activity_effect",
            Self::OrderDependentEqualSecond => "order_dependent_equal_second",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LedgerError {
    #[error("activity group {source_trade_id} has an invalid position mapping")]
    InvalidMapping { source_trade_id: SourceTradeId },
    #[error("activity group {source_trade_id} would underflow leader position")]
    Underflow { source_trade_id: SourceTradeId },
    #[error("activity group {source_trade_id} would overflow leader position")]
    Overflow { source_trade_id: SourceTradeId },
    #[error("activity group {source_trade_id} is a conversion with unknown affected conditions")]
    Conversion { source_trade_id: SourceTradeId },
    #[error("activity group {source_trade_id} has an unknown position effect")]
    UnknownEffect { source_trade_id: SourceTradeId },
}

impl LedgerError {
    #[must_use]
    pub const fn fence_cause(&self) -> WalletFenceCause {
        match self {
            Self::InvalidMapping { .. } => WalletFenceCause::InvalidMapping,
            Self::Underflow { .. } => WalletFenceCause::Underflow,
            Self::Overflow { .. } => WalletFenceCause::Overflow,
            Self::Conversion { .. } => WalletFenceCause::Conversion,
            Self::UnknownEffect { .. } => WalletFenceCause::UnknownEffect,
        }
    }
}

// ── PositionLedger ────────────────────────────────────────────────────────────

/// Accumulates [`IncomingTrade`] events into per-wallet position snapshots.
///
/// Maintains net long/short exposure per `(wallet, market, outcome)`. Trades are
/// applied in arrival order; no settlement or expiry logic is included here.
///
/// # Precondition
///
/// `position()` returns the state after all trades ingested so far. Calling it
/// before any trade has been ingested returns `None` for every wallet — this is
/// the correct sentinel, not an error.
#[derive(Clone)]
pub struct PositionLedger {
    snapshots: HashMap<WalletAddress, PositionSnapshot>,
}

impl PositionLedger {
    pub fn new() -> Self {
        Self {
            snapshots: HashMap::new(),
        }
    }

    /// Rehydrate the ledger from previously persisted per-wallet snapshots (the
    /// `leader_positions` mirror in `paper-state`), so that classification after a
    /// restart sees each leader's existing position rather than treating the first
    /// post-restart trade as a fresh Entry. Reuses the existing `PositionSnapshot`
    /// type; no trades are replayed.
    pub fn from_snapshots(snapshots: HashMap<WalletAddress, PositionSnapshot>) -> Self {
        Self { snapshots }
    }

    /// Apply one trade to the ledger, updating net exposure for the wallet.
    ///
    /// Buying reduces short contracts first (covering), then adds to long.
    /// Selling reduces long contracts first (trimming), then adds to short.
    pub fn ingest(&mut self, trade: &IncomingTrade) -> Result<(), LedgerError> {
        let mutation = LedgerMutation {
            source_trade_id: trade.source_trade_id.clone(),
            transaction_hash: trade
                .transaction_hash
                .clone()
                .unwrap_or_else(|| trade.source_trade_id.0.clone()),
            wallet: trade.wallet,
            source_time: SourceTimestamp(trade.observed_at),
            effect: LedgerEffect::Trade {
                market_id: trade.market_id.clone(),
                outcome_id: trade.outcome_id,
                side: trade.side,
                amount: trade.contracts,
                price: trade.price,
            },
        };
        self.apply(&mutation)
    }

    /// Apply a complete reconciled group with checked exact arithmetic.
    pub fn apply(&mut self, mutation: &LedgerMutation) -> Result<(), LedgerError> {
        let mut candidate = self.clone();
        let mut closed_by_residual = HashSet::new();
        candidate.apply_in_place(mutation, &mut closed_by_residual)?;
        *self = candidate;
        Ok(())
    }

    fn apply_in_place(
        &mut self,
        mutation: &LedgerMutation,
        closed_by_residual: &mut HashSet<(MarketId, OutcomeId)>,
    ) -> Result<AppliedEffect, LedgerError> {
        let clamped_residual = match mutation.effect.effective() {
            LedgerEffect::Trade {
                market_id,
                outcome_id,
                side,
                amount,
                ..
            } => {
                if *side == Side::Sell
                    && *amount != ShareAmount::ZERO
                    && closed_by_residual.contains(&(market_id.clone(), *outcome_id))
                {
                    return Err(LedgerError::Underflow {
                        source_trade_id: mutation.source_trade_id.clone(),
                    });
                }
                let state = self.state_mut(mutation.wallet, market_id, *outcome_id);
                apply_trade(state, *side, *amount, &mutation.source_trade_id)?;
                None
            }
            LedgerEffect::Split { market_id, amount } => {
                for outcome in [OutcomeId(0), OutcomeId(1)] {
                    let state = self.state_mut(mutation.wallet, market_id, outcome);
                    state.long_contracts =
                        state.long_contracts.checked_add(*amount).map_err(|_| {
                            LedgerError::Overflow {
                                source_trade_id: mutation.source_trade_id.clone(),
                            }
                        })?;
                }
                None
            }
            LedgerEffect::Merge { market_id, amount } => {
                self.checked_remove_pair(mutation, market_id, *amount, closed_by_residual)?;
                None
            }
            LedgerEffect::Redeem {
                market_id,
                outcome_id,
                amount,
            } => self.checked_remove_redeem(
                mutation,
                market_id,
                *outcome_id,
                *amount,
                closed_by_residual,
            )?,
            LedgerEffect::Conversion => {
                return Err(LedgerError::Conversion {
                    source_trade_id: mutation.source_trade_id.clone(),
                });
            }
            LedgerEffect::RequiresAnchor | LedgerEffect::RawOnly => None,
            LedgerEffect::UnknownEffect => {
                return Err(LedgerError::UnknownEffect {
                    source_trade_id: mutation.source_trade_id.clone(),
                });
            }
            LedgerEffect::Corrected { .. } => None,
        };
        Ok(AppliedEffect {
            effect: mutation.effect.clone(),
            clamped_residual,
        })
    }

    /// Apply every group atomically. Any invalid mapping/effect/arithmetic leaves
    /// the original ledger byte-equivalent (#544).
    pub fn apply_all_or_none(
        &mut self,
        mutations: &[LedgerMutation],
    ) -> Result<Vec<AppliedEffect>, LedgerError> {
        let mut candidate = self.clone();
        let mut closed_by_residual = HashSet::new();
        let mut applied = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            applied.push(candidate.apply_in_place(mutation, &mut closed_by_residual)?);
        }
        *self = candidate;
        Ok(applied)
    }

    fn state_mut(
        &mut self,
        wallet: WalletAddress,
        market_id: &MarketId,
        outcome_id: OutcomeId,
    ) -> &mut PositionState {
        let snapshot = self
            .snapshots
            .entry(wallet)
            .or_insert_with(|| PositionSnapshot {
                wallet,
                positions: HashMap::new(),
            });
        snapshot
            .positions
            .entry(MarketOutcomeId::new(market_id.clone(), outcome_id))
            .or_default()
    }

    fn checked_remove_pair(
        &mut self,
        mutation: &LedgerMutation,
        market_id: &MarketId,
        amount: ShareAmount,
        closed_by_residual: &HashSet<(MarketId, OutcomeId)>,
    ) -> Result<(), LedgerError> {
        if amount != ShareAmount::ZERO
            && [OutcomeId(0), OutcomeId(1)]
                .into_iter()
                .any(|outcome| closed_by_residual.contains(&(market_id.clone(), outcome)))
        {
            return Err(LedgerError::Underflow {
                source_trade_id: mutation.source_trade_id.clone(),
            });
        }
        let mut candidate = self.clone();
        candidate.checked_remove_strict(mutation, market_id, OutcomeId(0), amount)?;
        candidate.checked_remove_strict(mutation, market_id, OutcomeId(1), amount)?;
        *self = candidate;
        Ok(())
    }

    fn checked_remove_strict(
        &mut self,
        mutation: &LedgerMutation,
        market_id: &MarketId,
        outcome_id: OutcomeId,
        amount: ShareAmount,
    ) -> Result<(), LedgerError> {
        let state = self.state_mut(mutation.wallet, market_id, outcome_id);
        state.long_contracts =
            state
                .long_contracts
                .checked_sub(amount)
                .map_err(|_| LedgerError::Underflow {
                    source_trade_id: mutation.source_trade_id.clone(),
                })?;
        Ok(())
    }

    fn checked_remove_redeem(
        &mut self,
        mutation: &LedgerMutation,
        market_id: &MarketId,
        outcome_id: OutcomeId,
        amount: ShareAmount,
        closed_by_residual: &mut HashSet<(MarketId, OutcomeId)>,
    ) -> Result<Option<u64>, LedgerError> {
        let key = (market_id.clone(), outcome_id);
        if amount != ShareAmount::ZERO && closed_by_residual.contains(&key) {
            return Err(LedgerError::Underflow {
                source_trade_id: mutation.source_trade_id.clone(),
            });
        }
        let state = self.state_mut(mutation.wallet, market_id, outcome_id);
        if amount <= state.long_contracts {
            state.long_contracts =
                state
                    .long_contracts
                    .checked_sub(amount)
                    .map_err(|_| LedgerError::Underflow {
                        source_trade_id: mutation.source_trade_id.clone(),
                    })?;
            return Ok(None);
        }
        let residual = amount
            .checked_sub(state.long_contracts)
            .map_err(|_| LedgerError::Underflow {
                source_trade_id: mutation.source_trade_id.clone(),
            })?
            .atomic();
        if residual >= REDEEM_RESIDUAL_LIMIT_ATOMIC {
            return Err(LedgerError::Underflow {
                source_trade_id: mutation.source_trade_id.clone(),
            });
        }
        state.long_contracts = ShareAmount::ZERO;
        closed_by_residual.insert(key);
        Ok(Some(residual))
    }

    /// Restore the exact pre-trade state for one `(wallet, market-outcome)` — the
    /// inverse of a single `ingest` whose pre-trade `(long, short)` was captured by the
    /// caller (#511 pre-frame rollback: an abandoned-unseen admission must leave the
    /// ledger byte-identical so redelivery classifies identically). `prev = None` means
    /// the trade created the entry — remove it.
    pub fn restore(
        &mut self,
        wallet: WalletAddress,
        key: &MarketOutcomeId,
        prev: Option<(ShareAmount, ShareAmount)>,
    ) {
        let Some(snap) = self.snapshots.get_mut(&wallet) else {
            return;
        };
        match prev {
            Some((long_contracts, short_contracts)) => {
                let state = snap.positions.entry(key.clone()).or_default();
                state.long_contracts = long_contracts;
                state.short_contracts = short_contracts;
            }
            None => {
                snap.positions.remove(key);
            }
        }
    }

    /// Replace one wallet's complete authoritative position snapshot.
    pub fn replace_wallet_snapshot(
        &mut self,
        wallet: WalletAddress,
        positions: HashMap<MarketOutcomeId, PositionState>,
    ) {
        let candidate = PositionSnapshot { wallet, positions };
        self.snapshots.insert(wallet, candidate);
    }

    /// Return the current position snapshot for a wallet, or `None` if the wallet
    /// has never been observed.
    pub fn position(&self, wallet: &WalletAddress) -> Option<&PositionSnapshot> {
        self.snapshots.get(wallet)
    }

    /// Read-only state export for replay/isolated canary reconstruction.
    #[must_use]
    pub fn snapshots(&self) -> &HashMap<WalletAddress, PositionSnapshot> {
        &self.snapshots
    }

    /// Required for replay reconciliation; deferred to Phase 1.
    pub fn rewind_to(&mut self, _ts: SourceTimestamp) {}
}

fn apply_trade(
    state: &mut PositionState,
    side: Side,
    amount: ShareAmount,
    source_trade_id: &SourceTradeId,
) -> Result<(), LedgerError> {
    match side {
        Side::Buy => {
            let covered = state.short_contracts.min(amount);
            state.short_contracts =
                state
                    .short_contracts
                    .checked_sub(covered)
                    .map_err(|_| LedgerError::Underflow {
                        source_trade_id: source_trade_id.clone(),
                    })?;
            let remainder = amount
                .checked_sub(covered)
                .map_err(|_| LedgerError::Underflow {
                    source_trade_id: source_trade_id.clone(),
                })?;
            state.long_contracts =
                state
                    .long_contracts
                    .checked_add(remainder)
                    .map_err(|_| LedgerError::Overflow {
                        source_trade_id: source_trade_id.clone(),
                    })?;
        }
        Side::Sell => {
            let trimmed = state.long_contracts.min(amount);
            state.long_contracts =
                state
                    .long_contracts
                    .checked_sub(trimmed)
                    .map_err(|_| LedgerError::Underflow {
                        source_trade_id: source_trade_id.clone(),
                    })?;
            let remainder = amount
                .checked_sub(trimmed)
                .map_err(|_| LedgerError::Underflow {
                    source_trade_id: source_trade_id.clone(),
                })?;
            state.short_contracts = state.short_contracts.checked_add(remainder).map_err(|_| {
                LedgerError::Overflow {
                    source_trade_id: source_trade_id.clone(),
                }
            })?;
        }
    }
    Ok(())
}

impl Default for PositionLedger {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::HashMap;

    use pe_copy_signal_engine::TradeProvenance;
    use pe_core_types::{OutcomeId, Price, ReceivedAt, SourceId, SourceTradeId};
    use pe_source_polymarket_public::{
        ActivityParseContext, ActivityTransport, parse_activity_response,
    };
    use rust_decimal_macros::dec;
    use serde_json::{Value, json};
    use time::OffsetDateTime;

    use super::*;

    fn wallet(hex: &str) -> WalletAddress {
        serde_json::from_str(&format!("\"{hex}\"")).unwrap()
    }

    fn market() -> pe_core_types::MarketId {
        use pe_core_types::VenueMarketId;
        pe_core_types::MarketId(VenueMarketId("0xmarket1".to_string()))
    }

    fn trade(w: WalletAddress, side: Side, contracts: u64, ts_unix: i64) -> IncomingTrade {
        let ts = OffsetDateTime::from_unix_timestamp(ts_unix).unwrap();
        IncomingTrade {
            wallet: w,
            market_id: market(),
            outcome_id: OutcomeId(0),
            side,
            price: Price(dec!(0.5)),
            contracts: pe_core_types::ShareAmount::from_whole(contracts).unwrap(),
            observed_at: ts,
            received_at: ts,
            source_trade_id: SourceTradeId("t1".to_string()),
            transaction_hash: None,
            provenance: TradeProvenance::RestPoll,
        }
    }

    fn activity_aggregate(
        activity_type: &str,
        size: &str,
        outcome: Option<u16>,
        is_combo: bool,
    ) -> ActivityAggregate {
        let outcome_label = outcome.map(|value| if value == 0 { "Yes" } else { "No" });
        let row = json!({
            "proxyWallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "timestamp": 1_788_000_000_i64,
            "conditionId": "0xmarket1",
            "type": activity_type,
            "size": size,
            "usdcSize": "0.000000",
            "transactionHash": format!("0x{activity_type}-{size}"),
            "price": "0.5",
            "asset": "asset-0",
            "side": "BUY",
            "outcomeIndex": outcome.unwrap_or(999),
            "outcome": outcome_label.unwrap_or(""),
            "isCombo": is_combo,
        });
        let context = ActivityParseContext {
            source_id: SourceId("polymarket-data-api".to_owned()),
            observed_at: SourceTimestamp(
                OffsetDateTime::from_unix_timestamp(1_788_000_010).unwrap(),
            ),
            received_at: ReceivedAt(OffsetDateTime::from_unix_timestamp(1_788_000_011).unwrap()),
            transport: ActivityTransport::Rest,
        };
        let raw = serde_json::to_vec(&[row]).unwrap();
        let window = parse_activity_response(
            &raw,
            wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            &context,
        )
        .unwrap();
        let mut aggregates = window.aggregates().unwrap();
        assert_eq!(aggregates.len(), 1);
        aggregates.remove(0)
    }

    #[test]
    fn position_starts_empty() {
        let ledger = PositionLedger::new();
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert!(ledger.position(&w).is_none());
    }

    #[test]
    fn buy_opens_long() {
        let mut ledger = PositionLedger::new();
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        ledger.ingest(&trade(w, Side::Buy, 10, 1_000)).unwrap();
        let snap = ledger.position(&w).unwrap();
        let key = MarketOutcomeId::new(market(), OutcomeId(0));
        let state = snap.positions[&key];
        assert_eq!(state.long_contracts, ShareAmount::from_whole(10).unwrap());
        assert_eq!(state.short_contracts, ShareAmount::ZERO);
    }

    #[test]
    fn sell_covers_long() {
        let mut ledger = PositionLedger::new();
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        ledger.ingest(&trade(w, Side::Buy, 10, 1_000)).unwrap();
        ledger.ingest(&trade(w, Side::Sell, 3, 1_001)).unwrap();
        let snap = ledger.position(&w).unwrap();
        let key = MarketOutcomeId::new(market(), OutcomeId(0));
        let state = snap.positions[&key];
        assert_eq!(state.long_contracts, ShareAmount::from_whole(7).unwrap());
        assert_eq!(state.short_contracts, ShareAmount::ZERO);
    }

    #[test]
    fn sell_flips_to_short() {
        let mut ledger = PositionLedger::new();
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        ledger.ingest(&trade(w, Side::Buy, 10, 1_000)).unwrap();
        ledger.ingest(&trade(w, Side::Sell, 13, 1_001)).unwrap();
        let snap = ledger.position(&w).unwrap();
        let key = MarketOutcomeId::new(market(), OutcomeId(0));
        let state = snap.positions[&key];
        assert_eq!(state.long_contracts, ShareAmount::ZERO);
        assert_eq!(state.short_contracts, ShareAmount::from_whole(3).unwrap());
    }

    #[test]
    fn rewind_to_is_noop() {
        let mut ledger = PositionLedger::new();
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        ledger.ingest(&trade(w, Side::Buy, 10, 1_000)).unwrap();
        let ts = SourceTimestamp(OffsetDateTime::from_unix_timestamp(900).unwrap());
        ledger.rewind_to(ts); // must not panic or mutate
        assert!(ledger.position(&w).is_some());
    }

    #[test]
    fn split_overflow_on_second_outcome_leaves_first_outcome_unchanged() {
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let first = MarketOutcomeId::new(market(), OutcomeId(0));
        let second = MarketOutcomeId::new(market(), OutcomeId(1));
        let original_first = ShareAmount::from_atomic(7);
        let mut positions = HashMap::new();
        positions.insert(
            first.clone(),
            PositionState {
                long_contracts: original_first,
                short_contracts: ShareAmount::ZERO,
            },
        );
        positions.insert(
            second,
            PositionState {
                long_contracts: ShareAmount::from_atomic(u64::MAX),
                short_contracts: ShareAmount::ZERO,
            },
        );
        let mut ledger = PositionLedger::from_snapshots(HashMap::from([(
            w,
            PositionSnapshot {
                wallet: w,
                positions,
            },
        )]));
        let mutation = LedgerMutation {
            source_trade_id: SourceTradeId("g2:test".to_owned()),
            transaction_hash: "0xtest".to_owned(),
            wallet: w,
            source_time: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
            effect: LedgerEffect::Split {
                market_id: market(),
                amount: ShareAmount::from_atomic(1),
            },
        };

        assert!(matches!(
            ledger.apply(&mutation),
            Err(LedgerError::Overflow { .. })
        ));
        assert_eq!(
            ledger.position(&w).unwrap().positions[&first].long_contracts,
            original_first
        );
    }

    fn ledger_with_longs(w: WalletAddress, long0: u64, long1: u64) -> PositionLedger {
        let mut positions = HashMap::new();
        for (outcome, long) in [(OutcomeId(0), long0), (OutcomeId(1), long1)] {
            positions.insert(
                MarketOutcomeId::new(market(), outcome),
                PositionState {
                    long_contracts: ShareAmount::from_atomic(long),
                    short_contracts: ShareAmount::ZERO,
                },
            );
        }
        PositionLedger::from_snapshots(HashMap::from([(
            w,
            PositionSnapshot {
                wallet: w,
                positions,
            },
        )]))
    }

    #[test]
    fn unexpressible_redeems_require_an_anchor() {
        let zero = LedgerMutation::from_activity(&activity_aggregate(
            "REDEEM",
            "0.000000",
            Some(0),
            false,
        ))
        .unwrap();
        assert_eq!(zero.effect, LedgerEffect::RequiresAnchor);

        let outcome_less =
            LedgerMutation::from_activity(&activity_aggregate("REDEEM", "1.250000", None, false))
                .unwrap();
        assert_eq!(outcome_less.effect, LedgerEffect::RequiresAnchor);
    }

    #[test]
    fn stamped_non_zero_redeem_keeps_exact_arithmetic_and_clamps_small_residual() {
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let mutation = LedgerMutation::from_activity(&activity_aggregate(
            "REDEEM",
            "0.000100",
            Some(0),
            false,
        ))
        .unwrap();
        assert_eq!(
            mutation.effect,
            LedgerEffect::Redeem {
                market_id: market(),
                outcome_id: OutcomeId(0),
                amount: ShareAmount::from_atomic(100),
            }
        );

        let mut ledger = ledger_with_longs(w, 500, 20);
        ledger.apply(&mutation).unwrap();
        let snapshot = ledger.position(&w).unwrap();
        assert_eq!(
            snapshot.positions[&MarketOutcomeId::new(market(), OutcomeId(0))].long_contracts,
            ShareAmount::from_atomic(400)
        );
        assert_eq!(
            snapshot.positions[&MarketOutcomeId::new(market(), OutcomeId(1))].long_contracts,
            ShareAmount::from_atomic(20)
        );

        let mut underfunded = ledger_with_longs(w, 50, 20);
        underfunded.apply(&mutation).unwrap();
        assert_eq!(
            underfunded.position(&w).unwrap().positions
                [&MarketOutcomeId::new(market(), OutcomeId(0))]
                .long_contracts,
            ShareAmount::ZERO
        );
    }

    fn redeem_mutation(w: WalletAddress, id: &str, amount: u64) -> LedgerMutation {
        LedgerMutation {
            source_trade_id: SourceTradeId(id.to_owned()),
            transaction_hash: format!("0x{id}"),
            wallet: w,
            source_time: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
            effect: LedgerEffect::Redeem {
                market_id: market(),
                outcome_id: OutcomeId(0),
                amount: ShareAmount::from_atomic(amount),
            },
        }
    }

    #[test]
    fn incident_redeem_residuals_clamp_and_emit_version_three_documents() {
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let fixtures = [
            (1_369_800, 1_369_862, 62),
            (442_157_700, 442_157_776, 76),
            (20_338_900, 20_338_982, 82),
            (6_410_200, 6_410_254, 54),
            (33_322_200, 33_322_220, 20),
            (704_934_500, 704_934_540, 40),
            (65_086_600, 65_086_665, 65),
        ];

        for (ordinal, (balance, amount, residual)) in fixtures.into_iter().enumerate() {
            let mut ledger = ledger_with_longs(w, balance, 0);
            let mutation = redeem_mutation(w, &format!("g2:residual-{ordinal}"), amount);
            let applied = ledger.apply_all_or_none(&[mutation]).unwrap();
            assert_eq!(applied.len(), 1);
            assert_eq!(applied[0].clamped_residual, Some(residual));
            assert_eq!(
                ledger.position(&w).unwrap().positions
                    [&MarketOutcomeId::new(market(), OutcomeId(0))]
                    .long_contracts,
                ShareAmount::ZERO
            );
            let document = applied[0].to_document().unwrap();
            let value: Value = serde_json::from_str(&document).unwrap();
            assert_eq!(value["version"], 3);
            assert_eq!(value["clamped_residual_atomic"], residual);
            assert_eq!(AppliedEffect::from_document(&document).unwrap(), applied[0]);
        }
    }

    #[test]
    fn redeem_residual_limit_and_merge_remain_strict() {
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        for residual in [REDEEM_RESIDUAL_LIMIT_ATOMIC, 200] {
            let mut ledger = ledger_with_longs(w, 1_000, 1_000);
            let before = ledger.clone();
            let mutation = redeem_mutation(w, "g2:strict-redeem", 1_000 + residual);
            assert!(matches!(
                ledger.apply_all_or_none(&[mutation]),
                Err(LedgerError::Underflow { .. })
            ));
            assert_eq!(ledger.snapshots(), before.snapshots());
        }

        let mut ledger = ledger_with_longs(w, 1_000, 1_000);
        let before = ledger.clone();
        let merge = LedgerMutation {
            source_trade_id: SourceTradeId("g2:strict-merge".to_owned()),
            transaction_hash: "0xstrict-merge".to_owned(),
            wallet: w,
            source_time: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
            effect: LedgerEffect::Merge {
                market_id: market(),
                amount: ShareAmount::from_atomic(1_001),
            },
        };
        assert!(matches!(
            ledger.apply_all_or_none(&[merge]),
            Err(LedgerError::Underflow { .. })
        ));
        assert_eq!(ledger.snapshots(), before.snapshots());
    }

    #[test]
    fn redeem_residual_tolerance_cannot_stack_within_a_batch() {
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        for amounts in [[190, 20], [20, 190]] {
            let mut ledger = ledger_with_longs(w, 100, 0);
            let before = ledger.clone();
            let mutations = amounts.map(|amount| redeem_mutation(w, "g2:stack", amount));
            assert!(matches!(
                ledger.apply_all_or_none(&mutations),
                Err(LedgerError::Underflow { .. })
            ));
            assert_eq!(ledger.snapshots(), before.snapshots());
        }
    }

    #[test]
    fn zero_non_redeem_effects_and_combo_redeem_stay_raw_only() {
        for activity_type in ["TRADE", "SPLIT", "MERGE"] {
            let mutation = LedgerMutation::from_activity(&activity_aggregate(
                activity_type,
                "0.000000",
                Some(0),
                false,
            ))
            .unwrap();
            assert_eq!(mutation.effect, LedgerEffect::RawOnly);
        }

        let combo =
            LedgerMutation::from_activity(&activity_aggregate("REDEEM", "1.000000", None, true))
                .unwrap();
        assert_eq!(combo.effect, LedgerEffect::RawOnly);

        let conversion = LedgerMutation::from_activity(&activity_aggregate(
            "CONVERSION",
            "0.000000",
            None,
            false,
        ))
        .unwrap();
        assert_eq!(conversion.effect, LedgerEffect::Conversion);

        let unknown =
            LedgerMutation::from_activity(&activity_aggregate("NEW_TYPE", "0.000000", None, false))
                .unwrap();
        assert_eq!(unknown.effect, LedgerEffect::UnknownEffect);
    }

    #[test]
    fn requires_anchor_is_non_mutating_and_touches_no_keys() {
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let mut ledger = ledger_with_longs(w, 500, 20);
        let before = ledger.clone();
        let mutation = LedgerMutation {
            source_trade_id: SourceTradeId("g2:requires-anchor".to_owned()),
            transaction_hash: "0xtest".to_owned(),
            wallet: w,
            source_time: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
            effect: LedgerEffect::RequiresAnchor,
        };

        ledger.apply(&mutation).unwrap();
        assert!(mutation.touched_keys().is_empty());
        assert_eq!(ledger.snapshots(), before.snapshots());
    }

    fn sorted_position_rows(
        positions: &HashMap<MarketOutcomeId, PositionState>,
    ) -> Vec<(String, u16, u64, u64)> {
        let mut rows = positions
            .iter()
            .map(|(key, state)| {
                (
                    key.market().to_string(),
                    key.outcome().0,
                    state.long_contracts.atomic(),
                    state.short_contracts.atomic(),
                )
            })
            .collect::<Vec<_>>();
        rows.sort();
        rows
    }

    #[test]
    fn replace_wallet_snapshot_removes_absent_keys_and_preserves_other_wallets() {
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let other = wallet("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let mut ledger = ledger_with_longs(w, 500, 20);
        let other_trade = trade(other, Side::Buy, 3, 1_000);
        ledger.ingest(&other_trade).unwrap();
        let other_before = ledger.position(&other).cloned();

        let retained = MarketOutcomeId::new(market(), OutcomeId(1));
        let added = MarketOutcomeId::new(
            MarketId(VenueMarketId("0xmarket2".to_owned())),
            OutcomeId(0),
        );
        let replacement = HashMap::from([
            (
                retained.clone(),
                PositionState {
                    long_contracts: ShareAmount::from_atomic(42),
                    short_contracts: ShareAmount::from_atomic(3),
                },
            ),
            (
                added.clone(),
                PositionState {
                    long_contracts: ShareAmount::from_atomic(7),
                    short_contracts: ShareAmount::ZERO,
                },
            ),
        ]);
        ledger.replace_wallet_snapshot(w, replacement);

        let snapshot = ledger.position(&w).unwrap();
        assert_eq!(snapshot.positions.len(), 2);
        assert!(
            !snapshot
                .positions
                .contains_key(&MarketOutcomeId::new(market(), OutcomeId(0)))
        );
        assert_eq!(
            snapshot.positions[&retained].long_contracts,
            ShareAmount::from_atomic(42)
        );
        assert!(snapshot.positions.contains_key(&added));
        assert_eq!(ledger.position(&other), other_before.as_ref());
    }

    #[test]
    fn replacing_the_same_wallet_snapshot_twice_equals_once_for_generated_inputs() {
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        for mask in 0_u16..16 {
            let mut anchored = HashMap::new();
            for ordinal in 0_u16..4 {
                if mask & (1 << ordinal) != 0 {
                    anchored.insert(
                        MarketOutcomeId::new(
                            MarketId(VenueMarketId(format!("0xmarket-{ordinal}"))),
                            OutcomeId(ordinal % 2),
                        ),
                        PositionState {
                            long_contracts: ShareAmount::from_atomic(u64::from(ordinal) + 1),
                            short_contracts: ShareAmount::from_atomic(u64::from(mask)),
                        },
                    );
                }
            }
            let mut expected = anchored
                .iter()
                .map(|(key, state)| {
                    (
                        key.market().to_string(),
                        key.outcome().0,
                        state.long_contracts.atomic(),
                        state.short_contracts.atomic(),
                    )
                })
                .collect::<Vec<_>>();
            expected.sort();
            let mut once = PositionLedger::new();
            once.replace_wallet_snapshot(w, anchored.clone());
            let mut twice = once.clone();
            twice.replace_wallet_snapshot(w, anchored.clone());

            assert_eq!(once.snapshots(), twice.snapshots());
            assert_eq!(
                sorted_position_rows(&once.position(&w).unwrap().positions),
                expected
            );
        }
    }

    #[test]
    fn effect_documents_round_trip_every_variant_in_canonical_json() {
        let effects = [
            LedgerEffect::Trade {
                market_id: market(),
                outcome_id: OutcomeId(7),
                side: Side::Buy,
                amount: ShareAmount::from_atomic(1_234_567),
                price: Price(dec!(0.5000)),
            },
            LedgerEffect::Split {
                market_id: market(),
                amount: ShareAmount::from_atomic(2_000_001),
            },
            LedgerEffect::Merge {
                market_id: market(),
                amount: ShareAmount::from_atomic(3_000_002),
            },
            LedgerEffect::Redeem {
                market_id: market(),
                outcome_id: OutcomeId(1),
                amount: ShareAmount::from_atomic(4_000_003),
            },
            LedgerEffect::RequiresAnchor,
            LedgerEffect::RawOnly,
            LedgerEffect::Conversion,
            LedgerEffect::UnknownEffect,
        ];

        for effect in effects {
            let document = effect.to_document().unwrap();
            let value: Value = serde_json::from_str(&document).unwrap();
            assert_eq!(serde_json::to_string(&value).unwrap(), document);
            assert_eq!(LedgerEffect::from_document(&document).unwrap(), effect);
        }

        let document = LedgerEffect::Trade {
            market_id: market(),
            outcome_id: OutcomeId(7),
            side: Side::Buy,
            amount: ShareAmount::from_atomic(1_234_567),
            price: Price(dec!(0.5000)),
        }
        .to_document()
        .unwrap();
        let value: Value = serde_json::from_str(&document).unwrap();
        assert_eq!(value["version"], 1);
        assert_eq!(value["effect"]["kind"], "trade");
        assert_eq!(value["effect"]["outcome"], 7);
        assert_eq!(value["effect"]["side"], "Buy");
        assert!(value["effect"]["amount"].is_string() || value["effect"]["amount"].is_number());
        assert_eq!(
            LedgerEffect::from_document(&document).unwrap(),
            LedgerEffect::Trade {
                market_id: market(),
                outcome_id: OutcomeId(7),
                side: Side::Buy,
                amount: ShareAmount::from_atomic(1_234_567),
                price: Price(dec!(0.5000)),
            }
        );
    }

    #[test]
    fn uncorrected_v1_document_bytes_remain_frozen() {
        let effect = LedgerEffect::Trade {
            market_id: market(),
            outcome_id: OutcomeId(7),
            side: Side::Buy,
            amount: ShareAmount::from_atomic(1_234_567),
            price: Price(dec!(0.5000)),
        };

        assert_eq!(
            effect.to_document().unwrap(),
            r#"{"effect":{"amount":1234567,"kind":"trade","market":"0xmarket1","outcome":7,"price":"0.5000","side":"Buy"},"version":1}"#
        );
    }

    #[test]
    fn corrected_mutation_round_trips_v2_and_applies_only_to_verified_identity() {
        let original =
            LedgerMutation::from_activity(&activity_aggregate("TRADE", "1.250000", Some(0), false))
                .unwrap();
        let source_trade_id = original.source_trade_id.clone();
        let stamped = MarketOutcomeId::new(market(), OutcomeId(0));
        let verified = MarketOutcomeId::new(
            MarketId(VenueMarketId("0xverified-market".to_owned())),
            OutcomeId(1),
        );
        let expected_correction = IdentityCorrection {
            stamped: stamped.clone(),
            verified: verified.clone(),
            evidence_hash: "metadata-page-hash".to_owned(),
        };
        let corrected = original
            .with_verified_identity(verified.clone(), expected_correction.evidence_hash.clone());

        assert_eq!(corrected.source_trade_id, source_trade_id);
        assert_eq!(corrected.effect.correction(), Some(&expected_correction));
        assert_eq!(corrected.touched_keys(), vec![verified.clone()]);

        let document = corrected.effect.to_document().unwrap();
        let value: Value = serde_json::from_str(&document).unwrap();
        assert_eq!(value["version"], 2);
        assert_eq!(value["effect"]["market"], "0xverified-market");
        assert_eq!(value["effect"]["outcome"], 1);
        assert_eq!(value["correction"]["stamped"]["market"], "0xmarket1");
        assert_eq!(value["correction"]["stamped"]["outcome"], 0);
        assert_eq!(
            value["correction"]["verified"]["market"],
            "0xverified-market"
        );
        assert_eq!(value["correction"]["verified"]["outcome"], 1);

        let replayed_effect = LedgerEffect::from_document(&document).unwrap();
        assert_eq!(replayed_effect, corrected.effect);
        assert_eq!(replayed_effect.correction(), Some(&expected_correction));

        let corrected_redeem = AppliedEffect {
            effect: LedgerEffect::Corrected {
                effect: Box::new(LedgerEffect::Redeem {
                    market_id: verified.market().clone(),
                    outcome_id: verified.outcome(),
                    amount: ShareAmount::from_atomic(1_000),
                }),
                correction: expected_correction.clone(),
            },
            clamped_residual: Some(42),
        };
        let document = corrected_redeem.to_document().unwrap();
        let value: Value = serde_json::from_str(&document).unwrap();
        assert_eq!(value["version"], 3);
        assert_eq!(value["clamped_residual_atomic"], 42);
        assert_eq!(
            value["correction"]["verified"]["market"],
            "0xverified-market"
        );
        assert_eq!(
            AppliedEffect::from_document(&document).unwrap(),
            corrected_redeem
        );

        let mut replayed = corrected.clone();
        replayed.effect = replayed_effect;
        let mut ledger = PositionLedger::new();
        ledger.apply(&replayed).unwrap();
        let snapshot = ledger.position(&replayed.wallet).unwrap();
        assert!(!snapshot.positions.contains_key(&stamped));
        assert_eq!(
            snapshot.positions[&verified].long_contracts,
            ShareAmount::from_atomic(1_250_000)
        );
    }

    #[test]
    fn equal_verified_identity_is_the_identity_function() {
        let mutation =
            LedgerMutation::from_activity(&activity_aggregate("TRADE", "1.250000", Some(0), false))
                .unwrap();
        let verified = MarketOutcomeId::new(market(), OutcomeId(0));

        assert_eq!(
            mutation
                .clone()
                .with_verified_identity(verified, "unused-evidence".to_owned()),
            mutation
        );
    }

    #[test]
    fn effect_document_unknown_version_is_typed() {
        let document = json!({"effect": {"kind": "raw_only"}, "version": 4});
        let error = LedgerEffect::from_document(&document.to_string()).unwrap_err();
        assert_eq!(
            error,
            LedgerEffectDocumentError::UnknownVersion { version: 4 }
        );
    }

    #[test]
    fn effect_document_malformed_inputs_are_typed() {
        for document in [
            "not-json",
            r#"{"effect":{"kind":"trade"},"version":1}"#,
            r#"{"effect":{"kind":"raw_only","market":"extra"},"version":1}"#,
            r#"{"effect":{"kind":"raw_only"},"version":"1"}"#,
            r#"{"effect":{"amount":1,"kind":"redeem","market":"0xmarket1","outcome":0},"version":3}"#,
            r#"{"clamped_residual_atomic":0,"effect":{"amount":1,"kind":"redeem","market":"0xmarket1","outcome":0},"version":3}"#,
            r#"{"clamped_residual_atomic":100,"effect":{"amount":1,"kind":"redeem","market":"0xmarket1","outcome":0},"version":3}"#,
            r#"{"clamped_residual_atomic":1,"effect":{"kind":"raw_only"},"version":3}"#,
        ] {
            assert!(matches!(
                LedgerEffect::from_document(document),
                Err(LedgerEffectDocumentError::Malformed { .. })
            ));
        }
    }

    #[test]
    fn effect_document_malformed_corrections_are_typed() {
        for document in [
            r#"{"effect":{"amount":1,"kind":"trade","market":"0xverified","outcome":1,"price":"0.5","side":"Buy"},"version":2}"#,
            r#"{"correction":{"evidence_hash":7,"stamped":{"market":"0xstamped","outcome":0},"verified":{"market":"0xverified","outcome":1}},"effect":{"amount":1,"kind":"trade","market":"0xverified","outcome":1,"price":"0.5","side":"Buy"},"version":2}"#,
            r#"{"correction":{"evidence_hash":"hash","stamped":{"market":"0xverified","outcome":1},"verified":{"market":"0xverified","outcome":1}},"effect":{"amount":1,"kind":"trade","market":"0xverified","outcome":1,"price":"0.5","side":"Buy"},"version":2}"#,
            r#"{"correction":{"evidence_hash":"hash","stamped":{"market":"0xstamped","outcome":0},"verified":{"market":"0xverified","outcome":1}},"effect":{"amount":1,"kind":"trade","market":"0xother","outcome":1,"price":"0.5","side":"Buy"},"version":2}"#,
        ] {
            assert!(matches!(
                LedgerEffect::from_document(document),
                Err(LedgerEffectDocumentError::Malformed { .. })
            ));
        }
    }
}
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::arithmetic_side_effects
)]
mod restore_tests {
    use super::*;
    use pe_copy_signal_engine::TradeProvenance;
    use pe_core_types::{MarketId, OutcomeId, VenueMarketId};

    #[test]
    fn restore_is_the_exact_inverse_of_one_ingest() {
        let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let key = MarketOutcomeId::new(MarketId(VenueMarketId("0xm".into())), OutcomeId(0));
        let mut ledger = PositionLedger::new();
        let trade = |contracts: u64, side: Side| IncomingTrade {
            wallet,
            market_id: MarketId(VenueMarketId("0xm".into())),
            outcome_id: OutcomeId(0),
            side,
            price: pe_core_types::Price(rust_decimal::Decimal::ONE),
            contracts: pe_core_types::ShareAmount::from_whole(contracts).unwrap(),
            observed_at: time::OffsetDateTime::UNIX_EPOCH,
            received_at: time::OffsetDateTime::UNIX_EPOCH,
            source_trade_id: pe_core_types::SourceTradeId("t".into()),
            transaction_hash: None,
            provenance: TradeProvenance::RestPoll,
        };
        // Entry created by the trade → restore(None) removes it entirely.
        ledger.ingest(&trade(10, Side::Buy)).unwrap();
        ledger.restore(wallet, &key, None);
        assert!(
            !ledger
                .position(&wallet)
                .unwrap()
                .positions
                .contains_key(&key)
        );
        // Existing position: capture, mutate via a partially-covering BUY, restore exactly.
        ledger.ingest(&trade(4, Side::Sell)).unwrap(); // short 4
        let prev = ledger
            .position(&wallet)
            .unwrap()
            .positions
            .get(&key)
            .map(|st| (st.long_contracts, st.short_contracts));
        assert_eq!(
            prev,
            Some((ShareAmount::ZERO, ShareAmount::from_whole(4).unwrap()))
        );
        ledger.ingest(&trade(10, Side::Buy)).unwrap(); // covers 4, long 6 — NOT trivially invertible
        ledger.restore(wallet, &key, prev);
        let st = ledger
            .position(&wallet)
            .unwrap()
            .positions
            .get(&key)
            .cloned()
            .unwrap();
        assert_eq!(
            (st.long_contracts, st.short_contracts),
            (ShareAmount::ZERO, ShareAmount::from_whole(4).unwrap())
        );
    }
}
