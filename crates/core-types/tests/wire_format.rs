#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_core_types::{
    BasisPoints, ContractQty, EventSeq, KalshiPriceCents, KellyFraction, MarketId, MarketOutcomeId,
    ModelId, ObservedAtBucket, OrderLocalId, OutcomeId, PolymarketPriceDecimal, Price, PriceDelta,
    Probability, ProbabilityPpm, Quantity, ReconstructionQuality, ResolverCardId, RoundingPolicy,
    Side, SourceId, SourceTimestamp, SourceTradeId, StrategyId, TraderId, VenueAccountId, VenueId,
    VenueMarketId, WalletAddress,
};
use rust_decimal::Decimal;
#[allow(unused_imports)]
use rust_decimal_macros::dec;

// ── Snapshot: one populated value per type ───────────────────────────────────
// Decimal-backed types serialize as JSON strings (rust_decimal canonical format).

#[test]
fn snapshot_price() {
    let v = Price::new(dec!(0.75)).unwrap();
    insta::assert_json_snapshot!(v, @r#""0.75""#);
}

#[test]
fn snapshot_probability() {
    let v = Probability::new(dec!(0.3)).unwrap();
    insta::assert_json_snapshot!(v, @r#""0.3""#);
}

#[test]
fn snapshot_price_delta() {
    let v = PriceDelta(dec!(-0.05));
    insta::assert_json_snapshot!(v, @r#""-0.05""#);
}

#[test]
fn snapshot_kalshi_price_cents() {
    let v = KalshiPriceCents::new(42).unwrap();
    insta::assert_json_snapshot!(v, @"42");
}

#[test]
fn snapshot_polymarket_price_decimal() {
    let v = PolymarketPriceDecimal::new(dec!(0.6250)).unwrap();
    insta::assert_json_snapshot!(v, @r#""0.6250""#);
}

#[test]
fn snapshot_probability_ppm() {
    let v = ProbabilityPpm::new(750_000).unwrap();
    insta::assert_json_snapshot!(v, @"750000");
}

#[test]
fn snapshot_basis_points() {
    let v = BasisPoints(25);
    insta::assert_json_snapshot!(v, @"25");
}

#[test]
fn snapshot_kelly_fraction() {
    let v = KellyFraction::new(dec!(0.1)).unwrap();
    insta::assert_json_snapshot!(v, @r#""0.1""#);
}

#[test]
fn snapshot_contract_qty() {
    let v = ContractQty(1_000);
    insta::assert_json_snapshot!(v, @"1000");
}

#[test]
fn snapshot_quantity() {
    let v = Quantity(ContractQty(500));
    insta::assert_json_snapshot!(v, @"500");
}

#[test]
fn snapshot_event_seq() {
    let v = EventSeq(42);
    insta::assert_json_snapshot!(v, @"42");
}

#[test]
fn snapshot_venue_id_polymarket() {
    let v = VenueId::polymarket();
    insta::assert_json_snapshot!(v, @r#""polymarket""#);
}

#[test]
fn snapshot_venue_id_kalshi() {
    let v = VenueId::kalshi();
    insta::assert_json_snapshot!(v, @r#""kalshi""#);
}

#[test]
fn snapshot_venue_market_id() {
    let v = VenueMarketId("0x1234abc".to_string());
    insta::assert_json_snapshot!(v, @r#""0x1234abc""#);
}

#[test]
fn snapshot_source_id() {
    let v = SourceId("polymarket-clob-ws".to_string());
    insta::assert_json_snapshot!(v, @r#""polymarket-clob-ws""#);
}

#[test]
fn snapshot_strategy_id() {
    let v = StrategyId("winner-follow-v1".to_string());
    insta::assert_json_snapshot!(v, @r#""winner-follow-v1""#);
}

#[test]
fn snapshot_model_id() {
    let v = ModelId("calibration-v2".to_string());
    insta::assert_json_snapshot!(v, @r#""calibration-v2""#);
}

#[test]
fn snapshot_source_trade_id() {
    let v = SourceTradeId("trade-abc-123".to_string());
    insta::assert_json_snapshot!(v, @r#""trade-abc-123""#);
}

#[test]
fn snapshot_resolver_card_id() {
    let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let v = ResolverCardId(uuid);
    insta::assert_json_snapshot!(v, @r#""550e8400-e29b-41d4-a716-446655440000""#);
}

#[test]
fn snapshot_order_local_id() {
    let uuid = uuid::Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
    let v = OrderLocalId(uuid);
    insta::assert_json_snapshot!(v, @r#""6ba7b810-9dad-11d1-80b4-00c04fd430c8""#);
}

#[test]
fn snapshot_outcome_id() {
    let v = OutcomeId(0);
    insta::assert_json_snapshot!(v, @"0");
}

#[test]
fn snapshot_wallet_address() {
    let v = WalletAddress::from_hex("0xAbCd1234567890abcdef1234567890ABCDEF1234").unwrap();
    insta::assert_json_snapshot!(v, @r#""0xabcd1234567890abcdef1234567890abcdef1234""#);
}

#[test]
fn snapshot_trader_id() {
    let addr = WalletAddress::from_hex("0x0000000000000000000000000000000000000001").unwrap();
    let v = TraderId(addr);
    insta::assert_json_snapshot!(v, @r#""0x0000000000000000000000000000000000000001""#);
}

#[test]
fn snapshot_venue_account_id() {
    let v = VenueAccountId("kalshi-user-42".to_string());
    insta::assert_json_snapshot!(v, @r#""kalshi-user-42""#);
}

#[test]
fn snapshot_reconstruction_quality() {
    let v = ReconstructionQuality::new(80).unwrap();
    insta::assert_json_snapshot!(v, @"80");
}

#[test]
fn snapshot_side_buy() {
    let v = Side::Buy;
    insta::assert_json_snapshot!(v, @r#""Buy""#);
}

#[test]
fn snapshot_side_sell() {
    let v = Side::Sell;
    insta::assert_json_snapshot!(v, @r#""Sell""#);
}

#[test]
fn snapshot_observed_at_bucket() {
    let v = ObservedAtBucket::from_observed_at_ms(1_700_000_001_500);
    insta::assert_json_snapshot!(v, @"1700000001");
}

#[test]
fn snapshot_source_timestamp() {
    use time::macros::datetime;
    let v = SourceTimestamp(datetime!(2024-01-15 12:00:00 UTC));
    insta::assert_json_snapshot!(v, @r#""2024-01-15T12:00:00Z""#);
}

#[test]
fn snapshot_market_outcome_id() {
    let market = MarketId(VenueMarketId("market-abc".to_string()));
    let outcome = OutcomeId(0);
    let v = MarketOutcomeId::new(market, outcome);
    insta::assert_json_snapshot!(v);
}

// ── Bounded constructors: out-of-range always Err ────────────────────────────

#[test]
fn price_out_of_range() {
    assert!(Price::new(dec!(1.0001)).is_err());
    assert!(Price::new(dec!(-0.0001)).is_err());
}

#[test]
fn price_boundary_ok() {
    assert!(Price::new(Decimal::ZERO).is_ok());
    assert!(Price::new(Decimal::ONE).is_ok());
}

#[test]
fn probability_out_of_range() {
    assert!(Probability::new(dec!(1.001)).is_err());
    assert!(Probability::new(dec!(-0.001)).is_err());
}

#[test]
fn kalshi_price_cents_out_of_range() {
    assert!(KalshiPriceCents::new(101).is_err());
}

#[test]
fn kalshi_price_cents_boundary_ok() {
    assert!(KalshiPriceCents::new(0).is_ok());
    assert!(KalshiPriceCents::new(100).is_ok());
}

#[test]
fn probability_ppm_out_of_range() {
    assert!(ProbabilityPpm::new(1_000_001).is_err());
}

#[test]
fn probability_ppm_boundary_ok() {
    assert!(ProbabilityPpm::new(0).is_ok());
    assert!(ProbabilityPpm::new(1_000_000).is_ok());
}

#[test]
fn kelly_fraction_out_of_range() {
    assert!(KellyFraction::new(dec!(1.0001)).is_err());
    assert!(KellyFraction::new(dec!(-0.0001)).is_err());
}

#[test]
fn polymarket_price_scale_error() {
    // 5 decimal places → ScaleError
    assert!(PolymarketPriceDecimal::new(dec!(0.12345)).is_err());
}

#[test]
fn polymarket_price_range_error() {
    assert!(PolymarketPriceDecimal::new(dec!(1.0001)).is_err());
}

#[test]
fn reconstruction_quality_out_of_range() {
    assert!(ReconstructionQuality::new(101).is_err());
}

// ── Conversions ───────────────────────────────────────────────────────────────

#[test]
fn kalshi_cents_to_probability() {
    let cents = KalshiPriceCents::new(75).unwrap();
    let p = Probability::from(cents);
    assert_eq!(p.0, dec!(0.75));
}

#[test]
fn polymarket_price_to_price() {
    let pp = PolymarketPriceDecimal::new(dec!(0.6250)).unwrap();
    let p = Price::from(pp);
    assert_eq!(p.0, dec!(0.6250));
}

#[test]
fn probability_to_ppm_lossless() {
    let p = Probability::new(dec!(0.75)).unwrap();
    let ppm = p.to_ppm_lossless().unwrap();
    assert_eq!(ppm.0, 750_000);
}

#[test]
fn probability_to_ppm_lossy_err() {
    // 1/3 cannot be exactly represented in ppm
    let p = Probability::new(dec!(0.333333333)).unwrap();
    assert!(p.to_ppm_lossless().is_err());
}

#[test]
fn ppm_to_probability_roundtrip() {
    let ppm = ProbabilityPpm::new(500_000).unwrap();
    let p = Probability::from(ppm);
    assert_eq!(p.0, dec!(0.5));
    let back = p.to_ppm_lossless().unwrap();
    assert_eq!(back.0, 500_000);
}

#[test]
fn basis_points_to_decimal_roundtrip() {
    let bps = BasisPoints(100);
    let d = bps.to_decimal();
    assert_eq!(d, dec!(0.01));
    let back = BasisPoints::from_decimal(d);
    assert_eq!(back.0, 100);
}

// ── Identifier round-trips ────────────────────────────────────────────────────

#[test]
fn venue_market_id_display_fromstr() {
    let id = VenueMarketId("market-xyz".to_string());
    let s = id.to_string();
    let back: VenueMarketId = s.parse().unwrap();
    assert_eq!(id, back);
}

#[test]
fn wallet_address_from_hex_display_roundtrip() {
    let hex = "0xabcdef1234567890abcdef1234567890abcdef12";
    let addr = WalletAddress::from_hex(hex).unwrap();
    assert_eq!(addr.to_string(), hex);
}

#[test]
fn wallet_address_rejects_no_prefix() {
    assert!(WalletAddress::from_hex("abcdef1234567890abcdef1234567890abcdef12").is_err());
}

#[test]
fn wallet_address_rejects_wrong_length() {
    assert!(WalletAddress::from_hex("0xabc").is_err());
}

#[test]
fn venue_id_display() {
    assert_eq!(VenueId::polymarket().to_string(), "polymarket");
    assert_eq!(VenueId::kalshi().to_string(), "kalshi");
}

#[test]
fn venue_id_serde_roundtrip() {
    let v = VenueId::polymarket();
    let json = serde_json::to_string(&v).unwrap();
    let back: VenueId = serde_json::from_str(&json).unwrap();
    assert_eq!(v, back);
}

#[test]
fn venue_id_rejects_unknown() {
    let result: Result<VenueId, _> = serde_json::from_str(r#""binance""#);
    assert!(result.is_err());
}

#[test]
fn observed_at_bucket_floor_division() {
    // positive: 1500ms → bucket 1
    assert_eq!(ObservedAtBucket::from_observed_at_ms(1_500).0, 1);
    // boundary: 1000ms → bucket 1
    assert_eq!(ObservedAtBucket::from_observed_at_ms(1_000).0, 1);
    // 999ms → bucket 0
    assert_eq!(ObservedAtBucket::from_observed_at_ms(999).0, 0);
    // negative: -1ms → bucket -1 (floor, not truncation)
    assert_eq!(ObservedAtBucket::from_observed_at_ms(-1).0, -1);
    // negative: -1500ms → bucket -2
    assert_eq!(ObservedAtBucket::from_observed_at_ms(-1_500).0, -2);
}

// ── from_f64_rounding: no From<f64> accepted, explicit conversion works ───────

#[test]
fn price_from_f64_rounding_ok() {
    let p = Price::from_f64_rounding(0.5, RoundingPolicy::HalfEven).unwrap();
    assert!(p.0 > Decimal::ZERO && p.0 < Decimal::ONE);
}

#[test]
fn price_from_f64_rounding_out_of_range() {
    assert!(Price::from_f64_rounding(1.5, RoundingPolicy::HalfEven).is_err());
}

#[test]
fn price_from_f64_rounding_nan() {
    assert!(Price::from_f64_rounding(f64::NAN, RoundingPolicy::HalfEven).is_err());
}

// ── proptest: serde round-trips ───────────────────────────────────────────────

use proptest::prelude::*;

proptest! {
    #[test]
    fn proptest_price_serde(n in 0i64..=100i64) {
        let d = Decimal::new(n, 2);
        let price = Price::new(d).unwrap();
        let json = serde_json::to_string(&price).unwrap();
        let back: Price = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(price, back);
    }

    #[test]
    fn proptest_probability_serde(n in 0u32..=1_000_000u32) {
        let ppm = ProbabilityPpm::new(n).unwrap();
        let prob = Probability::from(ppm);
        let json = serde_json::to_string(&prob).unwrap();
        let back: Probability = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(prob, back);
    }

    #[test]
    fn proptest_kalshi_serde(n in 0u8..=100u8) {
        let v = KalshiPriceCents::new(n).unwrap();
        let json = serde_json::to_string(&v).unwrap();
        let back: KalshiPriceCents = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(v, back);
    }

    #[test]
    fn proptest_kelly_serde(n in 0i64..=100i64) {
        let d = Decimal::new(n, 2);
        let v = KellyFraction::new(d).unwrap();
        let json = serde_json::to_string(&v).unwrap();
        let back: KellyFraction = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(v, back);
    }

    #[test]
    fn proptest_price_out_of_range(n in 101i64..=200i64) {
        let d = Decimal::new(n, 2);
        prop_assert!(Price::new(d).is_err());
    }

    #[test]
    fn proptest_probability_ppm_out_of_range(n in 1_000_001u32..=2_000_000u32) {
        prop_assert!(ProbabilityPpm::new(n).is_err());
    }

    #[test]
    fn proptest_wallet_address_roundtrip(bytes in proptest::array::uniform20(0u8..)) {
        let addr = WalletAddress(bytes);
        let hex = addr.to_string();
        let back = WalletAddress::from_hex(&hex).unwrap();
        prop_assert_eq!(addr, back);
    }

    #[test]
    fn proptest_source_id_serde(s in "[a-z][a-z0-9-]{0,30}") {
        let v = SourceId(s.clone());
        let json = serde_json::to_string(&v).unwrap();
        let back: SourceId = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(v, back);
    }

    #[test]
    fn proptest_observed_at_bucket_floor(ms in -1_000_000_000_000_000i64..=1_000_000_000_000_000i64) {
        let bucket = ObservedAtBucket::from_observed_at_ms(ms);
        // bucket * 1000 <= ms < (bucket + 1) * 1000
        prop_assert!(bucket.0 * 1_000 <= ms);
        prop_assert!(ms < (bucket.0 + 1) * 1_000);
    }
}
