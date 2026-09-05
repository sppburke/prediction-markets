//! Canonical prepared-order economics shared by paper, live, and replay (#545).

use pe_core_types::{
    CollateralAmount, KellyFraction, PolymarketConditionId, PolymarketTokenId, Price, Probability,
    ShareAmount, Side,
};
use pe_event_log::AppendReceipt;
use pe_risk_engine::{RiskBlock, RiskSnapshot};
use pe_venue_polymarket::CompactFeeSchedule;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::live_journal::{
    LadderPlanAudit, LiveAdmissionArtifactAudit, LiveJournalError, hash_serializable,
};

pub const ECONOMIC_PREPARED_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketSelection {
    pub condition_id: PolymarketConditionId,
    pub outcome_index: u8,
    pub token_id: PolymarketTokenId,
    pub side: Side,
    pub market_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode", deny_unknown_fields)]
pub enum SizingModeAudit {
    Kelly {
        fraction: KellyFraction,
        probability: Probability,
    },
    Dollar {
        usd: Decimal,
    },
    Contract {
        contracts: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SizingAudit {
    pub mode: SizingModeAudit,
    pub budget: CollateralAmount,
    pub principal: CollateralAmount,
    pub minimum_shares: ShareAmount,
    pub expected_shares: ShareAmount,
    pub expected_vwap: Price,
    pub all_in_price: Price,
    pub slippage_rate: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeAudit {
    pub schedule: CompactFeeSchedule,
    pub expected_fee: CollateralAmount,
    pub reserve: CollateralAmount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "decision", deny_unknown_fields)]
pub enum RiskDecisionAudit {
    Approved,
    Blocked { reason: RiskBlock },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskAudit {
    pub snapshot: RiskSnapshot,
    pub decision: RiskDecisionAudit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BalanceAudit {
    pub cash_before: CollateralAmount,
    pub worst_case_debit: CollateralAmount,
    pub price_impact_cap_bps: i32,
    pub chase_ceiling: Price,
    pub band_floor: Price,
    pub band_ceiling_exclusive: Price,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationEvidence {
    pub source_receipt: AppendReceipt,
    pub complete_bound_receipt: AppendReceipt,
    pub observed_unix_ms: i64,
    pub provenance: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EconomicPrepared {
    pub version: u16,
    pub market: MarketSelection,
    pub admission: LiveAdmissionArtifactAudit,
    pub ladder: LadderPlanAudit,
    pub book_receipt: AppendReceipt,
    pub observation: Option<ObservationEvidence>,
    pub sizing: SizingAudit,
    pub fee: FeeAudit,
    pub risk: RiskAudit,
    pub balance: BalanceAudit,
    pub applied_configuration_hash: String,
}

impl EconomicPrepared {
    /// Deterministic BLAKE3 over the domain, version, and canonical JSON record.
    pub fn core_hash(&self) -> Result<String, LiveJournalError> {
        hash_serializable(&("prediction-edge/economic-prepared", self.version, self))
    }

    pub fn all_in_debit(&self) -> Result<CollateralAmount, pe_core_types::Error> {
        self.sizing.principal.checked_add(self.fee.expected_fee)
    }

    pub fn worst_case_all_in_debit(&self) -> Result<CollateralAmount, pe_core_types::Error> {
        self.sizing.principal.checked_add(self.fee.reserve)
    }
}
