//! Recorded-fixture proof for shared CLOB payout normalization (#544).
//!
//! The two source rows were captured from the live CLOB API on 2026-09-01
//! (fixture manifest SHA-256 values `a291...165f` and `5007...b613`). Fields
//! outside the parser contract were trimmed while retaining valid JSON.

#![allow(clippy::unwrap_used)]

use pe_source_polymarket_public::{
    ClobCoverageManifest, ClobCoveragePage, ClobPayoutResolution, ClobPayoutUnresolvedReason,
    ClobTokenPrice, parse_clob_market, parse_clob_markets_page,
};

const HALF: &[u8] = include_bytes!("fixtures/clob_market_5050.json");
const WINNER: &[u8] = include_bytes!("fixtures/clob_market_winner.json");

#[test]
fn recorded_half_and_winner_rows_produce_exact_vectors() {
    let half = parse_clob_market(HALF).unwrap().resolution_evidence();
    assert_eq!(half.is_50_50_outcome, Some(true));
    assert_eq!(
        half.payout,
        ClobPayoutResolution::Resolved(
            pe_source_polymarket_public::BinaryPayoutVector::fifty_fifty()
        )
    );
    assert_eq!(
        half.payout.payout_vector_json().as_deref(),
        Some("[\"0.5\",\"0.5\"]")
    );
    assert_eq!(half.tokens.len(), 2);
    assert!(half.tokens.iter().all(|token| token.token_id.is_some()));

    let winner = parse_clob_market(WINNER).unwrap().resolution_evidence();
    assert_eq!(winner.is_50_50_outcome, Some(false));
    assert_eq!(
        winner.payout.payout_vector_json().as_deref(),
        Some("[\"0\",\"1\"]")
    );
    assert_eq!(winner.tokens[1].winner, Some(true));
}

#[test]
fn yes_winner_malformed_open_incomplete_and_conflicting_are_total() {
    let parse = |body: &str| {
        parse_clob_market(body.as_bytes())
            .unwrap()
            .resolution_evidence()
    };
    let yes = parse(
        r#"{"condition_id":"yes","closed":true,"is_50_50_outcome":false,
            "tokens":[{"token_id":"1","outcome":"Yes","price":1,"winner":true},
                      {"token_id":"2","outcome":"No","price":0,"winner":false}]}"#,
    );
    assert_eq!(
        yes.payout.payout_vector_json().as_deref(),
        Some("[\"1\",\"0\"]")
    );

    let malformed = parse(
        r#"{"condition_id":"bad","closed":true,"is_50_50_outcome":false,
            "tokens":[{"token_id":"1","outcome":"Yes","price":"not-price","winner":true},
                      {"token_id":"2","outcome":"No","price":0,"winner":false}]}"#,
    );
    assert!(matches!(
        malformed.tokens[0].price,
        ClobTokenPrice::Malformed(_)
    ));
    assert_eq!(
        malformed.payout,
        ClobPayoutResolution::Unresolved(ClobPayoutUnresolvedReason::MalformedTokenPrice)
    );

    let open = parse(
        r#"{"condition_id":"open","closed":false,"is_50_50_outcome":false,
            "tokens":[{"token_id":"1","outcome":"Yes","price":1,"winner":true},
                      {"token_id":"2","outcome":"No","price":0,"winner":false}]}"#,
    );
    assert_eq!(
        open.payout,
        ClobPayoutResolution::Unresolved(ClobPayoutUnresolvedReason::OpenMarket)
    );

    let incomplete = parse(
        r#"{"condition_id":"incomplete","closed":true,"is_50_50_outcome":false,
            "tokens":[{"token_id":"1","outcome":"Yes","price":1,"winner":true},
                      {"outcome":"No","price":0,"winner":false}]}"#,
    );
    assert_eq!(
        incomplete.payout,
        ClobPayoutResolution::Unresolved(ClobPayoutUnresolvedReason::IncompleteEvidence)
    );

    let conflicting = parse(
        r#"{"condition_id":"conflict","closed":true,"is_50_50_outcome":true,
            "tokens":[{"token_id":"1","outcome":"Yes","price":1,"winner":true},
                      {"token_id":"2","outcome":"No","price":0,"winner":false}]}"#,
    );
    assert_eq!(
        conflicting.payout,
        ClobPayoutResolution::Unresolved(ClobPayoutUnresolvedReason::ConflictingEvidence)
    );
}

#[test]
fn parsed_terminal_page_builds_hash_bound_coverage_manifest() {
    let half = std::str::from_utf8(HALF).unwrap();
    let winner = std::str::from_utf8(WINNER).unwrap();
    let body = format!(r#"{{"data":[{half},{winner}],"next_cursor":"LTE="}}"#);
    let page = parse_clob_markets_page(body.as_bytes()).unwrap();
    let coverage = ClobCoveragePage::from_response(0, None, body.as_bytes(), &page).unwrap();
    let manifest = ClobCoverageManifest::complete(1, vec![coverage]).unwrap();
    assert_eq!(manifest.counts.pages, 1);
    assert_eq!(manifest.counts.markets, 2);
    assert_eq!(manifest.counts.resolved_payouts, 2);
    assert_eq!(manifest.counts.explicit_fifty_fifty, 1);
    assert_eq!(manifest.terminal_proof.terminal_page_sha256.len(), 64);
}
