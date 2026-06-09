//! Gamma market-resolution fetch for **realized-edge** scoring — the key-free
//! ground truth that closes the BTC latency-arb thesis without the sponsored
//! Chainlink settlement key (issue #300 / #297).
//!
//! The 5m/15m markets settle on Chainlink BTC/USD, but Gamma records the
//! resolved `outcomePrices` once a market closes, so the winning side is
//! readable for free: `GET /markets?condition_ids={ID}&closed=true` →
//! `outcomePrices[0]` is the Up/YES token's settled value (`"1"` = Up won, `"0"`
//! = Down won), and `clobTokenIds[0]` is exactly the harness's `yes_token_id`.
//! Verified live 2026-06-09 against a resolved `btc-updown-5m` market. Mirrors
//! `pe-paper-pnl::GammaResolutionFetcher` (`closed=true` is required — the plain
//! endpoint returns an empty list for resolved markets).

use std::str::FromStr as _;

use pe_source_polymarket_public::PageFetcher;
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::warn;

/// Error fetching/parsing resolutions.
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("fetch: {0}")]
    Fetch(String),
}

/// Resolved outcome for one market: did the YES (Up) token win?
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketResolution {
    pub condition_id: String,
    pub yes_won: bool,
}

#[derive(Debug, Deserialize)]
struct GammaMarket {
    #[serde(rename = "conditionId")]
    condition_id: String,
    closed: bool,
    /// JSON-encoded decimal-string array, e.g. `"[\"1\",\"0\"]"` (resolved).
    #[serde(rename = "outcomePrices")]
    outcome_prices: Option<String>,
}

/// Parse Gamma's `outcomePrices` field — a JSON-encoded decimal-string array
/// such as `"[\"1\",\"0\"]"` — into `Vec<Decimal>`. `None` on malformed JSON or
/// a non-decimal entry.
pub fn parse_outcome_prices(s: &str) -> Option<Vec<Decimal>> {
    let raw: Vec<String> = serde_json::from_str(s).ok()?;
    raw.iter().map(|x| Decimal::from_str(x).ok()).collect()
}

/// Parse a `/markets?condition_ids=…&closed=true` body for `condition_id`.
/// Returns the resolution iff a row matches that exact `condition_id`, is
/// `closed`, and has a parseable `outcomePrices`. The YES/Up side wins when the
/// first settled price is the decisive `1` (resolved markets settle to exactly
/// `1`/`0`; the `> 0.5` test is robust to either encoding). Pure.
///
/// Limitation: a closed market that settled non-decisively (e.g. a 50/50 void,
/// `["0.5","0.5"]`) would be classified as a YES loss rather than excluded. No
/// such settlement has been observed for these binary BTC up/down markets
/// (live-verified shape is exactly `1`/`0`); since this is a shadow-only
/// measurement harness that places no orders, the YES-side classification is an
/// accepted, documented limitation, not a trading risk.
pub fn parse_resolution(bytes: &[u8], condition_id: &str) -> Option<MarketResolution> {
    let markets: Vec<GammaMarket> = serde_json::from_slice(bytes).ok()?;
    let m = markets.iter().find(|m| m.condition_id == condition_id)?;
    if !m.closed {
        return None;
    }
    let prices = parse_outcome_prices(m.outcome_prices.as_deref()?)?;
    let yes = prices.first().copied()?;
    Some(MarketResolution {
        condition_id: condition_id.to_string(),
        yes_won: yes > Decimal::new(5, 1), // > 0.5
    })
}

/// Fetches closed-market resolutions for the harness's observed markets.
pub struct BtcResolutionFetcher<F: PageFetcher> {
    base_url: String,
    fetcher: F,
}

impl<F: PageFetcher + Send + Sync> BtcResolutionFetcher<F> {
    /// `base_url` is the Gamma API root, e.g. `https://gamma-api.polymarket.com`.
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self { base_url, fetcher }
    }

    fn url(&self, condition_id: &str) -> String {
        format!(
            "{}/markets?condition_ids={}&closed=true",
            self.base_url, condition_id
        )
    }

    /// Fetch resolutions for `condition_ids` (sequential — the per-run market
    /// count is small). Markets that are not yet closed (or unparseable) are
    /// silently skipped, so an in-progress 5m market simply yields no row yet.
    ///
    /// Per-market resilience (mirrors `pe-paper-pnl::GammaResolutionFetcher`): a
    /// transient fetch failure on one market is logged and skipped, never
    /// discarding the resolutions already gathered this run. `resolve` is
    /// re-runnable (idempotent upsert), so the failed market is retried next
    /// time. The `Result` is retained for the public error surface.
    pub async fn fetch_resolutions(
        &self,
        condition_ids: &[String],
    ) -> Result<Vec<MarketResolution>, ResolveError> {
        let mut out = Vec::new();
        for cid in condition_ids {
            match self.fetcher.fetch_page(&self.url(cid)).await {
                Ok(bytes) => {
                    if let Some(r) = parse_resolution(&bytes, cid) {
                        out.push(r);
                    }
                }
                Err(e) => {
                    warn!(condition_id = %cid, error = %e, "shadow: resolution fetch error, skipping");
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // Captured shape (verified live 2026-06-09): Up=0,Down=1 ⇒ Down won.
    const DOWN_WON: &str = r#"[{"conditionId":"0xc","closed":true,
        "outcomePrices":"[\"0\", \"1\"]","outcomes":"[\"Up\", \"Down\"]"}]"#;
    const UP_WON: &str = r#"[{"conditionId":"0xc","closed":true,
        "outcomePrices":"[\"1\", \"0\"]","outcomes":"[\"Up\", \"Down\"]"}]"#;

    #[test]
    fn parses_outcome_prices_array() {
        assert_eq!(
            parse_outcome_prices(r#"["1","0"]"#),
            Some(vec![dec!(1), dec!(0)])
        );
        assert_eq!(parse_outcome_prices("not json"), None);
    }

    #[test]
    fn up_won_is_yes_won() {
        let r = parse_resolution(UP_WON.as_bytes(), "0xc").unwrap();
        assert_eq!(r.condition_id, "0xc");
        assert!(r.yes_won);
    }

    #[test]
    fn down_won_is_not_yes_won() {
        let r = parse_resolution(DOWN_WON.as_bytes(), "0xc").unwrap();
        assert!(!r.yes_won);
    }

    #[test]
    fn unmatched_condition_id_is_none() {
        assert!(parse_resolution(UP_WON.as_bytes(), "0xother").is_none());
    }

    #[test]
    fn open_market_is_not_resolved() {
        let open =
            r#"[{"conditionId":"0xc","closed":false,"outcomePrices":"[\"0.55\",\"0.45\"]"}]"#;
        assert!(parse_resolution(open.as_bytes(), "0xc").is_none());
    }

    #[test]
    fn empty_list_is_none() {
        assert!(parse_resolution(b"[]", "0xc").is_none());
    }

    #[tokio::test]
    async fn fetcher_hits_closed_url_and_parses() {
        use pe_source_polymarket_public::FixtureFetcher;
        use std::collections::HashMap;
        let mut fx = HashMap::new();
        fx.insert(
            "https://g.test/markets?condition_ids=0xc&closed=true".to_string(),
            UP_WON.as_bytes().to_vec(),
        );
        let f = BtcResolutionFetcher::new("https://g.test".to_string(), FixtureFetcher::new(fx));
        let res = f.fetch_resolutions(&["0xc".to_string()]).await.unwrap();
        assert_eq!(res.len(), 1);
        assert!(res[0].yes_won);
    }

    // A transient fetch failure on one market (no fixture → `fetch_page` errors)
    // is skipped, not fatal: the other market still resolves in the same run.
    #[tokio::test]
    async fn one_fetch_error_does_not_discard_the_batch() {
        use pe_source_polymarket_public::FixtureFetcher;
        use std::collections::HashMap;
        let good = r#"[{"conditionId":"0xgood","closed":true,"outcomePrices":"[\"1\",\"0\"]"}]"#;
        let mut fx = HashMap::new();
        fx.insert(
            "https://g.test/markets?condition_ids=0xgood&closed=true".to_string(),
            good.as_bytes().to_vec(),
        );
        // `0xbad` has no fixture → `FixtureFetcher` returns a fatal error.
        let f = BtcResolutionFetcher::new("https://g.test".to_string(), FixtureFetcher::new(fx));
        let res = f
            .fetch_resolutions(&["0xbad".to_string(), "0xgood".to_string()])
            .await
            .unwrap();
        assert_eq!(res.len(), 1, "the failed market is skipped, not fatal");
        assert_eq!(res[0].condition_id, "0xgood");
        assert!(res[0].yes_won);
    }
}
