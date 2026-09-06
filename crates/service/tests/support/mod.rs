//! Test-only schema-one wire fixtures.
//!
//! Production keeps the legacy decoder private. Read-compatibility scenarios independently encode
//! the historical bytes instead of importing the decoder's Rust DTOs.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use pe_copy_signal_engine::{IncomingTrade, SignalConfig};
use pe_core_types::{
    EventSeq, Price, ReceivedAt, ReconstructionQuality, Side, SourceId, SourceTimestamp,
};
use pe_event_log::AppendReceipt;
use pe_service::bucket_commit::{BucketDecisionContext, PageOccurrence};
use pe_service::config::ServiceConfig;
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::runtime_config::RuntimeConfig;
use pe_source_polymarket_public::{
    ActivityParseContext, ActivityTransport, parse_activity_response,
};
use pe_venue_core::OrderIntent;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum LegacyFillSource {
    ClobBestAsk,
    Fallback,
    #[default]
    LeaderHaircut,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegacyPaperFill {
    pub intent: OrderIntent,
    pub simulated_fill_price: Price,
    pub simulated_at: SourceTimestamp,
    #[serde(default)]
    pub fill_source: LegacyFillSource,
}

pub fn page_occurrence() -> PageOccurrence {
    PageOccurrence {
        request_url: "scenario://activity".to_owned(),
        raw_hash: blake3::hash(b"scenario activity page").to_hex().to_string(),
        receipt: AppendReceipt {
            sequence: EventSeq(1),
            this_hash: blake3::hash(b"scenario source receipt"),
        },
    }
}

pub fn install_empty_anchor(
    paper_state: &pe_paper_state::PaperStateDb,
    wallet: pe_core_types::WalletAddress,
    cutoff_unix: i64,
) {
    if !paper_state.position_anchors(&wallet).unwrap().is_empty()
        || paper_state
            .leader_positions()
            .unwrap()
            .iter()
            .any(|position| position.wallet == wallet)
    {
        return;
    }
    paper_state.set_cursor(&wallet, cutoff_unix).unwrap();
    paper_state
        .install_anchors(&[pe_paper_state::AnchorInstallRecord {
            wallet,
            balances: Vec::new(),
            activity_cutoff_unix: cutoff_unix,
            anchored_at_unix: cutoff_unix,
            ledger_hash_after: "scenario-empty".to_owned(),
            positions_proof_hash: "scenario-positions".to_owned(),
            activity_bounds_json: "[]".to_owned(),
            source_log_generation: "scenario".to_owned(),
            proof_json: "{}".to_owned(),
            recorded_at_unix: cutoff_unix,
        }])
        .unwrap();
}

fn activity_body(trade: &IncomingTrade) -> Vec<u8> {
    let side = match trade.side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    };
    serde_json::to_vec(&serde_json::json!([{
        "proxyWallet": trade.wallet.to_string(),
        "timestamp": trade.observed_at.unix_timestamp(),
        "conditionId": trade.market_id.to_string(),
        "type": "TRADE",
        "size": trade.contracts.to_decimal().to_string(),
        "usdcSize": trade.contracts.to_decimal().checked_mul(trade.price.0).unwrap().to_string(),
        "transactionHash": trade.transaction_hash.clone().unwrap_or_else(|| trade.source_trade_id.0.clone()),
        "price": trade.price.0.to_string(),
        "asset": format!("{}-{}", trade.market_id, trade.outcome_id.0),
        "side": side,
        "outcomeIndex": trade.outcome_id.0,
        "outcome": if trade.outcome_id.0 == 0 { "Yes" } else { "No" },
        "isCombo": false,
    }]))
    .unwrap()
}

fn activity_context(trade: &IncomingTrade) -> ActivityParseContext {
    ActivityParseContext {
        source_id: SourceId("scenario.activity".to_owned()),
        observed_at: SourceTimestamp(trade.observed_at),
        received_at: ReceivedAt(trade.received_at),
        transport: ActivityTransport::Rest,
    }
}

pub fn bucket_source_trade_id(trade: &IncomingTrade) -> pe_core_types::SourceTradeId {
    parse_activity_response(
        &activity_body(trade),
        trade.wallet,
        &activity_context(trade),
    )
    .unwrap()
    .aggregates()
    .unwrap()[0]
        .group_id
        .key()
        .clone()
}

pub async fn send_trade_bucket(control: &mpsc::Sender<OrchestratorControl>, trade: IncomingTrade) {
    send_trade_bucket_with_config(
        control,
        trade,
        RuntimeConfig::from_service_config(&ServiceConfig::default()),
    )
    .await;
}

pub async fn send_trade_bucket_with_config(
    control: &mpsc::Sender<OrchestratorControl>,
    trade: IncomingTrade,
    applied_configuration: RuntimeConfig,
) {
    let body = activity_body(&trade);
    let context = activity_context(&trade);
    let window = parse_activity_response(&body, trade.wallet, &context).unwrap();
    let aggregates = window.aggregates().unwrap();
    let observation_provenance = aggregates
        .first()
        .map(|aggregate| HashMap::from([(aggregate.group_id.key().clone(), trade.provenance)]))
        .unwrap_or_default();
    let (committed, acknowledged) = oneshot::channel();
    control
        .send(OrchestratorControl::CommitActivityBucket {
            aggregates,
            context: Arc::new(BucketDecisionContext {
                applied_configuration,
                decision_inputs_json: "{}".to_owned(),
                page_occurrences: vec![page_occurrence()],
                observed_source_receipts: HashMap::new(),
                reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
                signal_config: SignalConfig::default(),
                copy_eligible: true,
                bracket_commit: false,
                recorded_at_unix: trade.received_at.unix_timestamp(),
                observation_provenance,
                no_copy_dispositions: HashMap::new(),
                identity_overrides: HashMap::new(),
                identity_unresolved: Default::default(),
                history_status: None,
            }),
            committed,
        })
        .await
        .unwrap();
    acknowledged.await.unwrap().unwrap();
}
