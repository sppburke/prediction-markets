//! Network-free Polygon receipt fixtures for ordinary-live finality (#545).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use pe_core_types::{
    CollateralAmount, OutcomeId, PolymarketConditionId, PolymarketTokenId, Price, ShareAmount,
};
use pe_venue_polymarket::{
    CTF_EXCHANGE_V2, NEG_RISK_CTF_EXCHANGE_V2, PreparedPolymarketBuy, ReceiptError,
    TOPIC_ORDER_FILLED_V2, canonical_block_matches, decode_order_fills, parse_chain_id_response,
    parse_finalized_block_response, parse_receipt_response,
};

const STANDARD: &[u8] = include_bytes!("fixtures/receipts/standard_v2.json");
const NEG_RISK: &[u8] = include_bytes!("fixtures/receipts/neg_risk_v2.json");

/// PASS: the venue-owned topic stays bound to the exact V2 ABI signature.
#[test]
fn order_filled_topic_matches_v2_signature() {
    assert_eq!(
        alloy_primitives::keccak256(
            b"OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)"
        ),
        TOPIC_ORDER_FILLED_V2
    );
}

fn prepared(neg_risk: bool) -> PreparedPolymarketBuy {
    let (token_id, maker, order_hash, exchange) = if neg_risk {
        (
            "456",
            "0x2222222222222222222222222222222222222222",
            "0x5555555555555555555555555555555555555555555555555555555555555555",
            NEG_RISK_CTF_EXCHANGE_V2,
        )
    } else {
        (
            "123",
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222222222222222222222222222",
            CTF_EXCHANGE_V2,
        )
    };
    PreparedPolymarketBuy {
        condition_id: PolymarketConditionId(format!("0x{}", "77".repeat(32))),
        outcome_id: OutcomeId(0),
        token_id: PolymarketTokenId(token_id.to_owned()),
        maker: maker.to_owned(),
        signer: maker.to_owned(),
        funder: maker.to_owned(),
        verifying_contract: exchange.to_string(),
        spender: exchange.to_string(),
        exchange_domain_version: 2,
        neg_risk,
        side: "BUY".to_owned(),
        salt: "1".to_owned(),
        timestamp_ms: 1,
        expiration: "0".to_owned(),
        maker_collateral: CollateralAmount::from_atomic(2_500_000),
        taker_shares: ShareAmount::from_atomic(3_125_000),
        limit_price: Price::new(rust_decimal_macros::dec!(0.8)).unwrap(),
        minimum_tick_size: Price::new(rust_decimal_macros::dec!(0.01)).unwrap(),
        signature_type: 3,
        order_type: "FOK".to_owned(),
        post_only: false,
        defer_exec: false,
        metadata: format!("0x{}", "00".repeat(32)),
        builder: format!("0x{}", "00".repeat(32)),
        order_hash: order_hash.to_owned(),
        post_body_hash: "post".to_owned(),
        sdk_version: "test".to_owned(),
        sdk_archive_sha256: "test".to_owned(),
        metadata_hashes: Vec::new(),
        worst_case_debit: CollateralAmount::from_atomic(2_500_120),
    }
}

/// PASS: both supported V2 exchanges decode exact fractional quantity and event fee.
#[test]
fn standard_and_neg_risk_receipts_decode_exactly() {
    let standard = parse_receipt_response(STANDARD, &format!("0x{}", "11".repeat(32)))
        .unwrap()
        .unwrap();
    let fills = decode_order_fills(&standard, &prepared(false)).unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].principal, CollateralAmount::from_atomic(2_500_000));
    assert_eq!(fills[0].quantity, ShareAmount::from_atomic(3_125_000));
    assert_eq!(fills[0].fee, CollateralAmount::from_atomic(120));

    let receipt = parse_receipt_response(NEG_RISK, &format!("0x{}", "44".repeat(32)))
        .unwrap()
        .unwrap();
    let fills = decode_order_fills(&receipt, &prepared(true)).unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].quantity, ShareAmount::from_atomic(2_000_000));
}

/// PASS: chain, finalized-head, and canonical-block parsers reject disagreement and null tags.
#[test]
fn finality_responses_are_strict() {
    assert_eq!(
        parse_chain_id_response(br#"{"jsonrpc":"2.0","id":1,"result":"0x89"}"#).unwrap(),
        137
    );
    let head = br#"{"jsonrpc":"2.0","id":2,"result":{"number":"0x64","hash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}"#;
    assert_eq!(parse_finalized_block_response(head).unwrap().number, 100);
    assert!(
        canonical_block_matches(
            head,
            100,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        .is_ok()
    );
    assert_eq!(
        parse_finalized_block_response(br#"{"jsonrpc":"2.0","id":2,"result":null}"#),
        Err(ReceiptError::UnsupportedFinalizedTag)
    );
    assert_eq!(
        canonical_block_matches(head, 99, &format!("0x{}", "aa".repeat(32))),
        Err(ReceiptError::BlockIdentityMismatch)
    );
    assert_eq!(
        canonical_block_matches(head, 100, &format!("0x{}", "bb".repeat(32))),
        Err(ReceiptError::BlockIdentityMismatch)
    );
}

/// PASS: pending, reverted, removed, malformed, and prepared-identity disagreement fail closed.
#[test]
fn receipt_failure_classes_are_typed() {
    assert!(
        parse_receipt_response(
            br#"{"jsonrpc":"2.0","id":3,"result":null}"#,
            &format!("0x{}", "11".repeat(32))
        )
        .unwrap()
        .is_none()
    );

    let mut reverted: serde_json::Value = serde_json::from_slice(STANDARD).unwrap();
    reverted["result"]["status"] = serde_json::json!("0x0");
    assert_eq!(
        parse_receipt_response(
            &serde_json::to_vec(&reverted).unwrap(),
            &format!("0x{}", "11".repeat(32))
        ),
        Err(ReceiptError::Reverted)
    );

    let mut removed: serde_json::Value = serde_json::from_slice(STANDARD).unwrap();
    removed["result"]["logs"][0]["removed"] = serde_json::json!(true);
    let receipt = parse_receipt_response(
        &serde_json::to_vec(&removed).unwrap(),
        &format!("0x{}", "11".repeat(32)),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        decode_order_fills(&receipt, &prepared(false)),
        Err(ReceiptError::RemovedLog)
    );

    let mut wrong = prepared(false);
    wrong.token_id = PolymarketTokenId("124".to_owned());
    let receipt = parse_receipt_response(STANDARD, &format!("0x{}", "11".repeat(32)))
        .unwrap()
        .unwrap();
    assert_eq!(
        decode_order_fills(&receipt, &wrong),
        Err(ReceiptError::OrderIdentityMismatch)
    );

    let mut malformed: serde_json::Value = serde_json::from_slice(STANDARD).unwrap();
    malformed["result"]["logs"][0]["data"] = serde_json::json!("0x00");
    let receipt = parse_receipt_response(
        &serde_json::to_vec(&malformed).unwrap(),
        &format!("0x{}", "11".repeat(32)),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        decode_order_fills(&receipt, &prepared(false)),
        Err(ReceiptError::MalformedOrderFilled)
    );

    let mut v1 = prepared(false);
    v1.exchange_domain_version = 1;
    let receipt = parse_receipt_response(STANDARD, &format!("0x{}", "11".repeat(32)))
        .unwrap()
        .unwrap();
    assert_eq!(
        decode_order_fills(&receipt, &v1),
        Err(ReceiptError::OrderIdentityMismatch)
    );

    let mut nonzero_builder = prepared(false);
    nonzero_builder.builder = format!("0x{}", "01".repeat(32));
    assert_eq!(
        decode_order_fills(&receipt, &nonzero_builder),
        Err(ReceiptError::OrderIdentityMismatch)
    );
}

/// PASS: reuse of a raw log identity conflicts before an unrelated order hash can be filtered.
#[test]
fn filtered_unrelated_log_identity_collision_is_rejected() {
    let mut receipt: serde_json::Value = serde_json::from_slice(STANDARD).unwrap();
    let mut conflicting = receipt["result"]["logs"][0].clone();
    conflicting["topics"][1] = serde_json::json!(format!("0x{}", "ff".repeat(32)));
    receipt["result"]["logs"]
        .as_array_mut()
        .unwrap()
        .push(conflicting);

    assert_eq!(
        parse_receipt_response(
            &serde_json::to_vec(&receipt).unwrap(),
            &format!("0x{}", "11".repeat(32)),
        ),
        Err(ReceiptError::LogIdentityConflict)
    );
}

/// PASS: direct requested-transaction and checked-256-bit amount violations are typed.
#[test]
fn requested_transaction_and_high_amount_words_are_rejected() {
    assert_eq!(
        parse_receipt_response(STANDARD, &format!("0x{}", "12".repeat(32))),
        Err(ReceiptError::TransactionMismatch)
    );

    let mut receipt: serde_json::Value = serde_json::from_slice(STANDARD).unwrap();
    let data = receipt["result"]["logs"][0]["data"]
        .as_str()
        .unwrap()
        .strip_prefix("0x")
        .unwrap();
    let mut words = data
        .as_bytes()
        .chunks_exact(64)
        .map(|word| std::str::from_utf8(word).unwrap().to_owned())
        .collect::<Vec<_>>();
    words[2] = format!("{:064x}", alloy_primitives::U256::from(1u64) << 64);
    receipt["result"]["logs"][0]["data"] = serde_json::json!(format!("0x{}", words.concat()));
    let parsed = parse_receipt_response(
        &serde_json::to_vec(&receipt).unwrap(),
        &format!("0x{}", "11".repeat(32)),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        decode_order_fills(&parsed, &prepared(false)),
        Err(ReceiptError::AmountOverflow)
    );
}
