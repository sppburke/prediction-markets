//! Canonical prepared-order economics shared by paper, live, and replay (#545).

use pe_core_types::{
    CollateralAmount, KellyFraction, PolymarketConditionId, PolymarketTokenId, Price, Probability,
    ShareAmount, Side,
};
use pe_event_log::AppendReceipt;
use pe_risk_engine::{RiskBlock, RiskSnapshot};
use pe_venue_polymarket::{CompactFeeSchedule, FeeError, LadderPlan, fee_reserve, taker_fee};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::LiveAdmissionArtifact;
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
    /// Source-log receipts of every current-price observation the snapshot consumed (sorted by sequence, deduplicated).
    pub price_receipts: Vec<AppendReceipt>,
    /// The clock the snapshot windows (intraday/rolling/latency hours) were evaluated at.
    pub evaluated_at_unix_ms: i64,
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

/// Inputs to the one economic composition shared by paper and live (#545). Every field is
/// evidence the caller already holds; the composer adds no I/O and reads no clocks.
pub struct EconomicInputs<'a> {
    pub market: MarketSelection,
    /// Admission artifact returned by `LiveAdmissionBuilder`.
    pub admission: &'a LiveAdmissionArtifact,
    /// The exact collateral-path plan selected by `plan_sized_buy`.
    pub plan: &'a LadderPlan,
    /// Receipt of the exact `/book` body walked by `plan`.
    pub book_receipt: AppendReceipt,
    pub observation: Option<ObservationEvidence>,
    pub sizing_mode: SizingModeAudit,
    pub budget: CollateralAmount,
    pub slippage_rate: Decimal,
    pub risk: RiskAudit,
    pub cash_before: CollateralAmount,
    pub price_impact_cap_bps: i32,
    pub chase_ceiling: Price,
    pub band_floor: Price,
    pub band_ceiling_exclusive: Price,
    pub applied_configuration_hash: String,
}

#[derive(Debug, thiserror::Error)]
pub enum EconomicError {
    #[error("fee calculation failed: {0}")]
    Fee(#[from] FeeError),
    #[error("exact economic arithmetic failed: {0}")]
    Arithmetic(#[from] pe_core_types::Error),
    #[error("the ladder audit has no valid expected VWAP")]
    InvalidLadderAudit,
    #[error("the book source receipt is unbound")]
    MissingBookReceipt,
}

impl EconomicPrepared {
    /// Compose the canonical economic record exactly once from already-acquired evidence.
    pub fn compose(inputs: EconomicInputs<'_>) -> Result<Self, EconomicError> {
        if inputs.book_receipt.this_hash == blake3::Hash::from_bytes([0; 32]) {
            return Err(EconomicError::MissingBookReceipt);
        }

        let ladder = LadderPlanAudit::new(inputs.plan);
        let expected_shares = ladder.expected_shares()?;
        let expected_vwap = ladder
            .expected_vwap()
            .ok_or(EconomicError::InvalidLadderAudit)?;
        let expected_spend = ladder.expected_spend()?;
        if expected_shares < inputs.plan.shares || expected_spend > inputs.plan.worst_case_debit {
            return Err(EconomicError::InvalidLadderAudit);
        }

        let schedule = inputs.admission.fee_schedule;
        let expected_fee = taker_fee(schedule, inputs.plan.shares, inputs.plan.limit_price)?;
        let reserve = fee_reserve(
            schedule,
            inputs.plan.worst_case_debit,
            inputs.plan.shares,
            inputs.plan.best_ask,
            inputs.plan.limit_price,
        )?;
        let expected_debit = inputs.plan.worst_case_debit.checked_add(expected_fee)?;
        let slippage_multiplier = Decimal::ONE.checked_add(inputs.slippage_rate).ok_or(
            pe_core_types::Error::OutOfRange {
                field: "EconomicInputs.slippage_rate",
            },
        )?;
        let all_in_price = Price::new(
            expected_debit
                .to_decimal()
                .checked_div(inputs.plan.shares.to_decimal())
                .and_then(|price| price.checked_mul(slippage_multiplier))
                .ok_or(pe_core_types::Error::OutOfRange {
                    field: "EconomicPrepared.all_in_price",
                })?,
        )?;
        let worst_case_debit = inputs.plan.worst_case_debit.checked_add(reserve)?;

        Ok(Self {
            version: ECONOMIC_PREPARED_VERSION,
            market: inputs.market,
            admission: LiveAdmissionArtifactAudit::new(
                &inputs.admission.market,
                &inputs.admission.settlement,
                schedule,
                inputs.admission.receipts,
            ),
            ladder,
            book_receipt: inputs.book_receipt,
            observation: inputs.observation,
            sizing: SizingAudit {
                mode: inputs.sizing_mode,
                budget: inputs.budget,
                principal: inputs.plan.worst_case_debit,
                minimum_shares: inputs.plan.shares,
                expected_shares,
                expected_vwap,
                all_in_price,
                slippage_rate: inputs.slippage_rate,
            },
            fee: FeeAudit {
                schedule,
                expected_fee,
                reserve,
            },
            risk: inputs.risk,
            balance: BalanceAudit {
                cash_before: inputs.cash_before,
                worst_case_debit,
                price_impact_cap_bps: inputs.price_impact_cap_bps,
                chase_ceiling: inputs.chase_ceiling,
                band_floor: inputs.band_floor,
                band_ceiling_exclusive: inputs.band_ceiling_exclusive,
            },
            applied_configuration_hash: inputs.applied_configuration_hash,
        })
    }

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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use pe_core_types::{BasisPoints, EventSeq};
    use pe_resolver_card::{
        VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
    };
    use pe_risk_engine::RiskSnapshot;
    use pe_source_polymarket_public::{
        LIVE_MARKET_PARSER_VERSION, LIVE_MARKET_SCHEMA_VERSION, LiveMarketEvidence,
    };
    use pe_venue_polymarket::AskLevel;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{AdmissionReceipts, LiveAdmissionArtifact};

    fn receipt(byte: u8) -> AppendReceipt {
        AppendReceipt {
            sequence: EventSeq(u64::from(byte)),
            this_hash: blake3::Hash::from_bytes([byte; 32]),
        }
    }

    fn admission() -> LiveAdmissionArtifact {
        let now = 1_800_000_000;
        LiveAdmissionArtifact {
            market: LiveMarketEvidence {
                condition_id: PolymarketConditionId("condition".to_owned()),
                ordered_outcome_token_ids: [
                    PolymarketTokenId("11".to_owned()),
                    PolymarketTokenId("22".to_owned()),
                ],
                neg_risk: false,
                minimum_tick_size: Price::new(dec!(0.01)).unwrap(),
                minimum_order_size: ShareAmount::from_atomic(1_000_000),
                scheduled_end_unix: Some(now + 3_600),
                observed_at_unix: now,
                schema_version: LIVE_MARKET_SCHEMA_VERSION,
                parser_version: LIVE_MARKET_PARSER_VERSION,
                freshness_window_secs: 60,
            },
            settlement: VenueSettlementRecord {
                schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
                condition_id: PolymarketConditionId("condition".to_owned()),
                status: VenueResolutionStatus::Unresolved,
                raw_evidence_hash: blake3::hash(b"settlement").to_hex().to_string(),
                source_timestamp_unix: Some(now),
                observed_at_unix: now,
                parser_version: 1,
                freshness_window_secs: 60,
            },
            fee_schedule: CompactFeeSchedule::Taker { rate: dec!(0.04) },
            receipts: AdmissionReceipts {
                gamma: receipt(1),
                clob_long: receipt(2),
                clob_compact: receipt(3),
            },
        }
    }

    fn risk() -> RiskAudit {
        RiskAudit {
            snapshot: RiskSnapshot {
                leader_exposure_bps: BasisPoints::ZERO,
                market_exposure_bps: BasisPoints::ZERO,
                family_exposure_bps: BasisPoints::ZERO,
                total_copy_exposure_bps: BasisPoints::ZERO,
                intraday_pnl_bps: BasisPoints::ZERO,
                rolling_7d_pnl_bps: BasisPoints::ZERO,
                absolute_pnl_bps: BasisPoints::ZERO,
                copy_latency_kill_switch_active: false,
                proposed_trade_bps: BasisPoints(50),
                per_trade_cap_bps: 100,
                concentration_caps: None,
            },
            decision: RiskDecisionAudit::Approved,
            price_receipts: vec![receipt(5)],
            evaluated_at_unix_ms: 1_700_000_000_000,
        }
    }

    fn plan() -> LadderPlan {
        LadderPlan {
            used_asks: vec![
                AskLevel {
                    price: Price::new(dec!(0.49)).unwrap(),
                    shares: ShareAmount::from_decimal_exact(dec!(2)).unwrap(),
                },
                AskLevel {
                    price: Price::new(dec!(0.50)).unwrap(),
                    shares: ShareAmount::from_decimal_exact(dec!(3.02)).unwrap(),
                },
            ],
            best_ask: Price::new(dec!(0.49)).unwrap(),
            limit_price: Price::new(dec!(0.50)).unwrap(),
            shares: ShareAmount::from_decimal_exact(dec!(5)).unwrap(),
            worst_case_debit: CollateralAmount::from_decimal_exact(dec!(2.50)).unwrap(),
        }
    }

    fn inputs<'a>(
        admission: &'a LiveAdmissionArtifact,
        plan: &'a LadderPlan,
    ) -> EconomicInputs<'a> {
        EconomicInputs {
            market: MarketSelection {
                condition_id: admission.market.condition_id.clone(),
                outcome_index: 0,
                token_id: admission.market.ordered_outcome_token_ids[0].clone(),
                side: Side::Buy,
                market_id: "condition".to_owned(),
            },
            admission,
            plan,
            book_receipt: receipt(4),
            observation: None,
            sizing_mode: SizingModeAudit::Contract { contracts: 5 },
            budget: CollateralAmount::from_decimal_exact(dec!(3)).unwrap(),
            slippage_rate: dec!(0.01),
            risk: risk(),
            cash_before: CollateralAmount::from_decimal_exact(dec!(10)).unwrap(),
            price_impact_cap_bps: 100,
            chase_ceiling: Price::new(dec!(0.50)).unwrap(),
            band_floor: Price::new(dec!(0.15)).unwrap(),
            band_ceiling_exclusive: Price::new(dec!(0.85)).unwrap(),
            applied_configuration_hash: "config".to_owned(),
        }
    }

    #[test]
    fn composer_derives_improved_quantity_and_signed_price_fee_once() {
        let admission = admission();
        let plan = plan();
        let prepared = EconomicPrepared::compose(inputs(&admission, &plan)).unwrap();

        assert_eq!(prepared.sizing.minimum_shares.to_decimal(), dec!(5));
        assert_eq!(prepared.sizing.expected_shares.to_decimal(), dec!(5.02));
        assert_eq!(prepared.sizing.principal.to_decimal(), dec!(2.5));
        assert_eq!(prepared.fee.expected_fee.to_decimal(), dec!(0.05));
        assert_eq!(prepared.sizing.all_in_price.0, dec!(0.5151));
        assert_eq!(prepared.book_receipt, receipt(4));
        assert_eq!(prepared.admission.receipts.gamma, receipt(1));
        assert!(prepared.fee.reserve >= prepared.fee.expected_fee);
    }

    #[test]
    fn composer_rejects_an_unbound_book_receipt() {
        let admission = admission();
        let plan = plan();
        let mut inputs = inputs(&admission, &plan);
        inputs.book_receipt = AppendReceipt {
            sequence: EventSeq(0),
            this_hash: blake3::Hash::from_bytes([0; 32]),
        };
        assert!(matches!(
            EconomicPrepared::compose(inputs),
            Err(EconomicError::MissingBookReceipt)
        ));
    }

    #[test]
    fn risk_replay_inputs_are_bound_by_the_core_hash() {
        let admission = admission();
        let plan = plan();
        let prepared = EconomicPrepared::compose(inputs(&admission, &plan)).unwrap();
        let mut changed_receipt = prepared.clone();
        changed_receipt.risk.price_receipts = vec![receipt(6)];
        let mut changed_clock = prepared.clone();
        changed_clock.risk.evaluated_at_unix_ms += 1;

        let hash = prepared.core_hash().unwrap();
        assert_ne!(changed_receipt.core_hash().unwrap(), hash);
        assert_ne!(changed_clock.core_hash().unwrap(), hash);
    }
}
