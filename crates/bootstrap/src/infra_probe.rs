//! Infra-wallet pre-filter probe (issue #197).
//!
//! Heavy market-maker / treasury / arbitrage wallets have 100k–1M+ trades on
//! Polymarket. Their pagination is slow but actively making progress, so the
//! per-wallet timeout never fires. They pin a concurrency slot for hours and
//! block the broader bootstrap.
//!
//! The probe runs on the FIRST cold-start page of an unfetched wallet. If
//! 500 trades cover less than `threshold_secs` seconds, the wallet is
//! classified as infra and flagged via [`crate::cache::WalletCache::mark_infra`].
//! The probe HTTP call is the same first page the normal fetch would make,
//! so the only marginal cost on a normal wallet is the classify pass.
//!
//! Canonical default: `infra_probe_span_secs = 3600` in `docs/_GLOSSARY.md`
//! "Bootstrap defaults". Env override: `PE_BOOTSTRAP_INFRA_SPAN_SECS`.

use pe_trader_index::snapshot::RawTrade;

/// Default span threshold (seconds). Canonical home in `docs/_GLOSSARY.md`.
pub const DEFAULT_INFRA_SPAN_SECS: i64 = 3600;

/// First-page size required for the probe to be conclusive. Matches the
/// Polymarket `limit=500` page size in `PolymarketEndpoint::UserTradeActivity`.
pub const PROBE_PAGE_SIZE: usize = 500;

/// Stateless classifier. Holds only the configured threshold.
///
/// Construct via [`InfraProbe::default`] to honour the
/// `PE_BOOTSTRAP_INFRA_SPAN_SECS` env override; the env var is read once at
/// construction so the threshold stays constant across an entire run.
#[derive(Debug, Clone, Copy)]
pub struct InfraProbe {
    pub threshold_secs: i64,
}

/// Classification outcome.
///
/// `Infra` triggers `mark_infra` + discard. `NotInfra` and `Inconclusive`
/// both let the wallet proceed with normal fetch. The `reason` on
/// `Inconclusive` is purely for observability (logged at the call site).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeClassification {
    Infra { span_secs: i64 },
    NotInfra { span_secs: i64 },
    Inconclusive { reason: &'static str },
}

impl Default for InfraProbe {
    fn default() -> Self {
        let secs = std::env::var("PE_BOOTSTRAP_INFRA_SPAN_SECS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(DEFAULT_INFRA_SPAN_SECS);
        Self {
            threshold_secs: secs,
        }
    }
}

impl InfraProbe {
    /// Pure classification. No I/O.
    ///
    /// Page-fullness check uses `raw_count` (the raw JSON array length, NOT
    /// `trades.len()`) because `parse_trades_with_count` silently drops
    /// entries that fail `convert_trade` (invalid price/size/side). A full
    /// page with a few conversion failures would otherwise wrongly classify
    /// `Inconclusive`.
    ///
    /// # Precondition
    /// `raw_count` must be the source-of-truth JSON array length from the
    /// Polymarket response, not the post-conversion `trades.len()`.
    pub fn classify(&self, trades: &[RawTrade], raw_count: usize) -> ProbeClassification {
        if raw_count < PROBE_PAGE_SIZE {
            return ProbeClassification::Inconclusive {
                reason: "page_size < 500",
            };
        }
        if trades.is_empty() {
            return ProbeClassification::Inconclusive {
                reason: "all trades unparseable",
            };
        }
        // Both unwraps are safe: `trades` non-empty proven above.
        let oldest = trades
            .iter()
            .map(|t| t.timestamp.0.unix_timestamp())
            .min()
            .unwrap_or(0);
        let newest = trades
            .iter()
            .map(|t| t.timestamp.0.unix_timestamp())
            .max()
            .unwrap_or(0);
        let span = newest - oldest;
        if span < self.threshold_secs {
            ProbeClassification::Infra { span_secs: span }
        } else {
            ProbeClassification::NotInfra { span_secs: span }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_core_types::{
        ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId,
        VenueMarketId, WalletAddress,
    };
    use rust_decimal_macros::dec;
    use time::OffsetDateTime;

    fn trade_at(ts: i64) -> RawTrade {
        RawTrade {
            wallet: WalletAddress::from_hex("0x0000000000000000000000000000000000000001").unwrap(),
            market_id: MarketId(VenueMarketId("0xabc".to_string())),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            price: Price::new(dec!(0.5)).unwrap(),
            contracts: ContractQty(1),
            timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(ts).unwrap()),
            source_trade_id: SourceTradeId(format!("0x{ts:064x}")),
        }
    }

    fn dense_page(n: usize, base_ts: i64) -> Vec<RawTrade> {
        (0..n).map(|i| trade_at(base_ts + i as i64)).collect()
    }

    #[test]
    fn classify_infra_below_threshold() {
        let probe = InfraProbe {
            threshold_secs: 3600,
        };
        // 500 trades over 60 seconds → infra.
        let trades = dense_page(500, 1_700_000_000);
        let trades_truncated: Vec<RawTrade> = trades
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, mut t)| {
                t.timestamp = SourceTimestamp(
                    OffsetDateTime::from_unix_timestamp(1_700_000_000 + (i as i64 % 60)).unwrap(),
                );
                t
            })
            .collect();
        let result = probe.classify(&trades_truncated, 500);
        assert!(matches!(result, ProbeClassification::Infra { .. }));
        if let ProbeClassification::Infra { span_secs } = result {
            assert!(span_secs < 3600);
        }
    }

    #[test]
    fn classify_not_infra_above_threshold() {
        let probe = InfraProbe {
            threshold_secs: 3600,
        };
        // 500 trades over 48 hours → not infra.
        let trades: Vec<RawTrade> = (0..500)
            .map(|i| trade_at(1_700_000_000 + i * 346)) // ~96 sec apart = ~48h span
            .collect();
        let result = probe.classify(&trades, 500);
        assert!(matches!(result, ProbeClassification::NotInfra { .. }));
    }

    #[test]
    fn classify_boundary_at_threshold_minus_one() {
        // span = 3599 < threshold=3600 → Infra.
        let probe = InfraProbe {
            threshold_secs: 3600,
        };
        let mut trades = dense_page(500, 1_700_000_000);
        trades[499].timestamp =
            SourceTimestamp(OffsetDateTime::from_unix_timestamp(1_700_000_000 + 3599).unwrap());
        let result = probe.classify(&trades, 500);
        assert!(matches!(
            result,
            ProbeClassification::Infra { span_secs: 3599 }
        ));
    }

    #[test]
    fn classify_boundary_at_threshold_exact_not_infra() {
        // span == 3600 == threshold → NotInfra (strict < comparison).
        let probe = InfraProbe {
            threshold_secs: 3600,
        };
        let mut trades = dense_page(500, 1_700_000_000);
        trades[499].timestamp =
            SourceTimestamp(OffsetDateTime::from_unix_timestamp(1_700_000_000 + 3600).unwrap());
        let result = probe.classify(&trades, 500);
        assert!(matches!(
            result,
            ProbeClassification::NotInfra { span_secs: 3600 }
        ));
    }

    #[test]
    fn classify_inconclusive_when_raw_count_below_500() {
        let probe = InfraProbe {
            threshold_secs: 3600,
        };
        let trades = dense_page(300, 1_700_000_000);
        let result = probe.classify(&trades, 300);
        assert!(matches!(
            result,
            ProbeClassification::Inconclusive {
                reason: "page_size < 500"
            }
        ));
    }

    #[test]
    fn classify_uses_raw_count_not_trades_len() {
        // raw_count=500 with 3 conversion failures (trades.len()=497) MUST still
        // classify — does NOT short-circuit as Inconclusive on len < 500.
        let probe = InfraProbe {
            threshold_secs: 3600,
        };
        let mut trades = dense_page(497, 1_700_000_000);
        // Make the timestamps dense to trigger Infra.
        for (i, t) in trades.iter_mut().enumerate() {
            t.timestamp = SourceTimestamp(
                OffsetDateTime::from_unix_timestamp(1_700_000_000 + (i as i64 % 60)).unwrap(),
            );
        }
        let result = probe.classify(&trades, 500);
        assert!(matches!(result, ProbeClassification::Infra { .. }));
    }

    #[test]
    fn classify_inconclusive_when_all_unparseable() {
        let probe = InfraProbe {
            threshold_secs: 3600,
        };
        let trades: Vec<RawTrade> = vec![];
        let result = probe.classify(&trades, 500);
        assert!(matches!(
            result,
            ProbeClassification::Inconclusive {
                reason: "all trades unparseable"
            }
        ));
    }

    // Env-var fallback for `InfraProbe::default` is intentionally not unit-
    // tested here: `std::env::remove_var` requires `unsafe` (process-global
    // state) which is denied workspace-wide. The fallback is exercised
    // implicitly by every scenario test in `tests/scenario_infra_probe.rs`
    // that calls `PolymarketBulkFetcher::new()` without setting the env var.
}
