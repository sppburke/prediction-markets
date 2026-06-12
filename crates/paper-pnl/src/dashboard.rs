//! Portfolio snapshot type and embedded HTML dashboard renderer.

use rust_decimal::Decimal;
use serde::Serialize;

use crate::valuation::ValuationOutput;

/// Point-in-time portfolio summary.
///
/// `total_pnl` keeps its original meaning (`current_bankroll − initial_bankroll`,
/// the realized bankroll delta) for `/paper/pnl` JSON backward-compatibility; the
/// `realized_pnl`/`unrealized_pnl`/`open_market_value` fields are additive and carry
/// the mark-to-market decomposition. The displayed headline is
/// [`displayed_total`](Self::displayed_total) = `realized_pnl + unrealized_pnl`.
#[derive(Debug, Clone, Serialize)]
pub struct PortfolioSnapshot {
    pub current_bankroll: Decimal,
    pub initial_bankroll: Decimal,
    pub total_pnl: Decimal,
    /// P&L from settled markets (`T + open_cost`).
    pub realized_pnl: Decimal,
    /// Mark-to-market of open positions (`open_market_value − open_cost`).
    pub unrealized_pnl: Decimal,
    /// Current marked value of open positions (`Σ (long − short) × mid`).
    pub open_market_value: Decimal,
    pub resolution_credits: Decimal,
    pub settled_markets: usize,
    pub open_position_count: usize,
    pub fills_count: usize,
}

impl PortfolioSnapshot {
    /// Project a [`ValuationOutput`] (+ store-derived counts) into a snapshot. The
    /// single projection used by both the pure ledger and the service helper, so
    /// the summary card cannot drift from the per-trade table.
    pub fn from_valuation(
        valuation: &ValuationOutput,
        current_bankroll: Decimal,
        initial_bankroll: Decimal,
        resolution_credits: Decimal,
        settled_markets: usize,
        fills_count: usize,
    ) -> Self {
        Self {
            current_bankroll,
            initial_bankroll,
            total_pnl: current_bankroll - initial_bankroll,
            realized_pnl: valuation.realized_pnl,
            unrealized_pnl: valuation.unrealized_pnl,
            open_market_value: valuation.open_market_value,
            resolution_credits,
            settled_markets,
            open_position_count: valuation.open_position_count,
            fills_count,
        }
    }

    /// Headline P&L: realized + unrealized (mark-to-market).
    pub fn displayed_total(&self) -> Decimal {
        self.realized_pnl + self.unrealized_pnl
    }

    /// [`displayed_total`](Self::displayed_total) as a percentage of initial bankroll.
    pub fn displayed_total_pct(&self) -> Decimal {
        pct_of(self.displayed_total(), self.initial_bankroll)
    }

    /// Realized bankroll delta (`total_pnl`) as a percentage of initial bankroll.
    /// Retained for `/paper/pnl` backward-compatibility; the dashboard headline
    /// uses [`displayed_total_pct`](Self::displayed_total_pct).
    pub fn pnl_pct(&self) -> Decimal {
        pct_of(self.total_pnl, self.initial_bankroll)
    }
}

/// `value / basis × 100`, rounded to 2 dp; zero when `basis` is zero.
fn pct_of(value: Decimal, basis: Decimal) -> Decimal {
    if basis.is_zero() {
        return Decimal::ZERO;
    }
    (value / basis * Decimal::ONE_HUNDRED).round_dp(2)
}

/// One entered paper trade (a recorded fill), enriched with the leader, entry
/// time and market expiration parsed/resolved by the service tier. Raw Unix
/// timestamps are carried here; the renderer derives the UTC/Central-time strings.
#[derive(Debug, Clone)]
pub struct TradeView {
    pub market_id: String,
    pub outcome_id: u16,
    pub side: String,
    pub contracts: u64,
    pub fill_price: Decimal,
    /// Copied leader wallet (parsed from the idempotency key).
    pub leader: String,
    /// Leader's source trade id (parsed from the idempotency key).
    pub source_trade_id: String,
    /// Event-log sequence number — the chronological order anchor.
    pub event_seq: i64,
    /// Leader trade observed-at (entry signal time), Unix seconds. From the
    /// idempotency-key bucket; `None` if the key could not be parsed.
    pub entry_unix: Option<i64>,
    /// When the contract resolves/expires (Unix seconds): for settled markets the
    /// authoritative `settled_at_unix`; for open markets `umaEndDate`/`endDate`.
    /// `None` if it could not be determined.
    pub resolution_unix: Option<i64>,
    /// Resolution status: `resolved` for settled markets (from the resolution
    /// store), else the open-market UMA status, if known.
    pub resolution_status: Option<String>,
    /// Settled outcome for display: `won`/`lost`; `None` while the market is open.
    pub outcome: Option<String>,
    /// Realized P&L for a settled fill; `None` while the market is open.
    pub realized_pnl: Option<Decimal>,
    /// Current mark: resolved price (settled) or live mid (open); `None` if an open
    /// market has no mid yet.
    pub current_mid: Option<Decimal>,
}

impl TradeView {
    fn notional(&self) -> Decimal {
        self.fill_price * Decimal::from(self.contracts)
    }

    /// Full per-trade detail as a JSON object, including derived notional and
    /// UTC + US-Central timestamp strings. Shared by the dashboard raw-JSON blob
    /// and the `/paper/fills` endpoint so time formatting lives in one place.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "market_id": self.market_id,
            "outcome_id": self.outcome_id,
            "side": self.side,
            "contracts": self.contracts,
            "fill_price": self.fill_price,
            "notional": self.notional(),
            "leader": self.leader,
            "source_trade_id": self.source_trade_id,
            "event_seq": self.event_seq,
            "entry_unix": self.entry_unix,
            "entry_utc": self.entry_unix.and_then(tz::fmt_utc),
            "entry_ct": self.entry_unix.and_then(tz::fmt_ct),
            "resolution_unix": self.resolution_unix,
            "resolution_utc": self.resolution_unix.and_then(tz::fmt_utc),
            "resolution_ct": self.resolution_unix.and_then(tz::fmt_ct),
            "resolution_status": self.resolution_status,
            "outcome": self.outcome,
            "realized_pnl": self.realized_pnl,
            "current_mid": self.current_mid,
        })
    }
}

/// Render the HTML dashboard: summary cards, a per-trade table, and a raw JSON
/// blob carrying the full `{summary, trades}` payload.
pub fn render_dashboard_html(snapshot: &PortfolioSnapshot, trades: &[TradeView]) -> String {
    let raw = serde_json::json!({
        "summary": snapshot,
        "trades": trades.iter().map(TradeView::to_json).collect::<Vec<_>>(),
    });
    let json = serde_json::to_string_pretty(&raw).unwrap_or_else(|_| "{}".to_string());

    // Headline P&L is the mark-to-market total (realized + unrealized); colour and
    // sign follow it, not the open-at-$0 bankroll delta.
    let total = snapshot.displayed_total();
    let pnl_color = if total >= Decimal::ZERO {
        "#2ecc71"
    } else {
        "#e74c3c"
    };
    let total_sign = sign_of(total);
    let realized_sign = sign_of(snapshot.realized_pnl);
    let unrealized_sign = sign_of(snapshot.unrealized_pnl);
    let total_pct = snapshot.displayed_total_pct();
    let trade_rows = render_trade_rows(trades);

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Paper Trader Dashboard</title>
<style>
body{{font-family:monospace;background:#1a1a2e;color:#eee;padding:2rem;}}
h1,h2{{color:#a29bfe;}}
.card{{background:#16213e;border-radius:8px;padding:1.5rem;margin:1rem 0;}}
.stat{{display:inline-block;margin:0.5rem 1rem;}}
.label{{color:#74b9ff;font-size:.85rem;}}
.value{{font-size:1.4rem;font-weight:bold;}}
.pnl{{color:{pnl_color};}}
table{{width:100%;border-collapse:collapse;font-size:.8rem;}}
th,td{{text-align:left;padding:.4rem .6rem;border-bottom:1px solid #0f3460;white-space:nowrap;}}
th{{color:#74b9ff;}}
td.num{{text-align:right;}}
pre{{background:#0f3460;padding:1rem;border-radius:6px;overflow:auto;font-size:.8rem;}}
</style>
</head>
<body>
<h1>Paper Trader Dashboard</h1>
<div class="card">
  <div class="stat"><div class="label">Bankroll</div><div class="value">${current_bankroll}</div></div>
  <div class="stat"><div class="label">Realized</div><div class="value">{realized_sign}{realized_pnl}</div></div>
  <div class="stat"><div class="label">Unrealized</div><div class="value">{unrealized_sign}{unrealized_pnl}</div></div>
  <div class="stat"><div class="label">Total P&amp;L</div><div class="value pnl">{total_sign}{total} ({total_pct}%)</div></div>
  <div class="stat"><div class="label">Resolution Credits</div><div class="value">${resolution_credits}</div></div>
  <div class="stat"><div class="label">Settled Markets</div><div class="value">{settled_markets}</div></div>
  <div class="stat"><div class="label">Open Positions</div><div class="value">{open_position_count}</div></div>
  <div class="stat"><div class="label">Total Fills</div><div class="value">{fills_count}</div></div>
</div>
<div class="card">
  <h2>Entered Trades ({fills_count})</h2>
  <table>
    <thead><tr>
      <th>Entry (CT)</th><th>Market</th><th>Out</th><th>Side</th>
      <th class="num">Contracts</th><th class="num">Fill</th><th class="num">Notional</th>
      <th>Status</th><th>Outcome</th><th class="num">Realized</th>
      <th>Leader</th><th>Resolves (CT)</th>
    </tr></thead>
    <tbody>{trade_rows}</tbody>
  </table>
</div>
<div class="card"><details><summary>Raw JSON</summary><pre>{json}</pre></details></div>
</body></html>"#,
        current_bankroll = snapshot.current_bankroll,
        realized_pnl = snapshot.realized_pnl,
        unrealized_pnl = snapshot.unrealized_pnl,
        resolution_credits = snapshot.resolution_credits,
        settled_markets = snapshot.settled_markets,
        open_position_count = snapshot.open_position_count,
        fills_count = snapshot.fills_count,
    )
}

/// `"+"` for non-negative, `""` otherwise (negatives carry their own `-`).
fn sign_of(value: Decimal) -> &'static str {
    if value >= Decimal::ZERO { "+" } else { "" }
}

fn render_trade_rows(trades: &[TradeView]) -> String {
    if trades.is_empty() {
        return "<tr><td colspan=\"12\">No trades yet.</td></tr>".to_string();
    }
    trades
        .iter()
        .map(|t| {
            let entry = t
                .entry_unix
                .and_then(tz::fmt_ct)
                .unwrap_or_else(|| "—".into());
            let resolves = t
                .resolution_unix
                .and_then(tz::fmt_ct)
                .unwrap_or_else(|| "—".into());
            let status = t.resolution_status.as_deref().unwrap_or("open");
            let outcome = t.outcome.as_deref().unwrap_or("—");
            let realized = match t.realized_pnl {
                Some(p) => format!("{}{p}", sign_of(p)),
                None => "—".to_string(),
            };
            format!(
                "<tr><td>{entry}</td><td title=\"{market_full}\">{market}</td><td>{outcome_id}</td>\
                 <td>{side}</td><td class=\"num\">{contracts}</td><td class=\"num\">{price}</td>\
                 <td class=\"num\">{notional}</td><td>{status}</td><td>{outcome}</td>\
                 <td class=\"num\">{realized}</td><td title=\"{leader_full}\">{leader}</td>\
                 <td>{resolves}</td></tr>",
                market_full = t.market_id,
                market = short_id(&t.market_id),
                outcome_id = t.outcome_id,
                side = t.side,
                contracts = t.contracts,
                price = t.fill_price,
                notional = t.notional(),
                leader_full = t.leader,
                leader = short_id(&t.leader),
            )
        })
        .collect()
}

/// Abbreviate a long hex id (`0xabcd…1234`) for table display. ASCII-safe.
fn short_id(s: &str) -> String {
    if s.len() > 14 {
        format!("{}…{}", &s[..6], &s[s.len() - 4..])
    } else {
        s.to_string()
    }
}

/// US Central-time formatting, DST-aware (America/Chicago) without a tz crate.
mod tz {
    use time::{Date, Month, OffsetDateTime, UtcOffset};

    /// Format a Unix timestamp as `YYYY-MM-DD HH:MM:SS UTC`.
    pub fn fmt_utc(unix: i64) -> Option<String> {
        let dt = OffsetDateTime::from_unix_timestamp(unix).ok()?;
        Some(format!("{} UTC", ymdhms(dt)?))
    }

    /// Format a Unix timestamp in US Central time (`… CST`/`… CDT`), DST-aware.
    pub fn fmt_ct(unix: i64) -> Option<String> {
        let utc = OffsetDateTime::from_unix_timestamp(unix).ok()?;
        let (hours, abbr) = if is_chicago_dst(utc) {
            (-5, "CDT")
        } else {
            (-6, "CST")
        };
        let local = utc.to_offset(UtcOffset::from_hms(hours, 0, 0).ok()?);
        Some(format!("{} {abbr}", ymdhms(local)?))
    }

    fn ymdhms(dt: OffsetDateTime) -> Option<String> {
        let fmt =
            time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
        dt.format(&fmt).ok()
    }

    /// US DST (post-2007): begins 2nd Sunday of March 02:00 CST (08:00 UTC),
    /// ends 1st Sunday of November 02:00 CDT (07:00 UTC).
    fn is_chicago_dst(utc: OffsetDateTime) -> bool {
        let year = utc.year();
        let (Some(start), Some(end)) = (
            nth_sunday_utc(year, Month::March, 2, 8),
            nth_sunday_utc(year, Month::November, 1, 7),
        ) else {
            return false;
        };
        let t = utc.unix_timestamp();
        t >= start && t < end
    }

    /// Unix timestamp of the `n`-th Sunday of `month`/`year` at `hour`:00 UTC.
    fn nth_sunday_utc(year: i32, month: Month, n: u8, hour: u8) -> Option<i64> {
        let mut found = 0u8;
        for day in 1u8..=31 {
            let Ok(date) = Date::from_calendar_date(year, month, day) else {
                break;
            };
            if date.weekday() == time::Weekday::Sunday {
                found += 1;
                if found == n {
                    return Some(
                        date.with_hms(hour, 0, 0)
                            .ok()?
                            .assume_utc()
                            .unix_timestamp(),
                    );
                }
            }
        }
        None
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn trade(entry: Option<i64>, resolves: Option<i64>) -> TradeView {
        TradeView {
            market_id: "0xabcdef0123456789".to_string(),
            outcome_id: 0,
            side: "buy".to_string(),
            contracts: 50,
            fill_price: Decimal::new(40, 2), // 0.40
            leader: "0x1111222233334444555566667777888899990000".to_string(),
            source_trade_id: "0xdeadbeef".to_string(),
            event_seq: 1,
            entry_unix: entry,
            resolution_unix: resolves,
            resolution_status: Some("resolved".to_string()),
            outcome: Some("won".to_string()),
            realized_pnl: Some(Decimal::new(30, 0)),
            current_mid: Some(Decimal::ONE),
        }
    }

    fn snapshot(realized: Decimal, unrealized: Decimal) -> PortfolioSnapshot {
        PortfolioSnapshot {
            current_bankroll: Decimal::new(9743, 0),
            initial_bankroll: Decimal::new(10000, 0),
            total_pnl: Decimal::new(-257, 0),
            realized_pnl: realized,
            unrealized_pnl: unrealized,
            open_market_value: Decimal::new(241, 0),
            resolution_credits: Decimal::ZERO,
            settled_markets: 0,
            open_position_count: 1,
            fills_count: 1,
        }
    }

    #[test]
    fn ct_uses_cst_in_winter_and_cdt_in_summer() {
        // 2024-01-15 12:00:00 UTC → 06:00 CST (UTC-6).
        let cst = super::tz::fmt_ct(1_705_320_000).unwrap();
        assert_eq!(cst, "2024-01-15 06:00:00 CST");
        // 2024-07-15 12:00:00 UTC → 07:00 CDT (UTC-5).
        let cdt = super::tz::fmt_ct(1_721_044_800).unwrap();
        assert_eq!(cdt, "2024-07-15 07:00:00 CDT");
    }

    #[test]
    fn raw_json_includes_trades_with_expiration_and_notional() {
        let snap = snapshot(Decimal::new(8152, 2), Decimal::new(-1646, 2));
        let html = render_dashboard_html(&snap, &[trade(Some(1_705_320_000), Some(1_730_700_000))]);
        // Trade detail and derived fields are present in the page.
        assert!(html.contains("\"notional\""));
        assert!(html.contains("\"resolution_ct\""));
        assert!(html.contains("\"entry_ct\": \"2024-01-15 06:00:00 CST\""));
        assert!(html.contains("Entered Trades (1)"));
        // New mark-to-market fields surface in the per-trade JSON.
        assert!(html.contains("\"outcome\""));
        assert!(html.contains("\"realized_pnl\""));
        assert!(html.contains("\"current_mid\""));
    }

    #[test]
    fn summary_shows_realized_unrealized_and_total() {
        // realized +81.52, unrealized −16.46 → total +65.06.
        let snap = snapshot(Decimal::new(8152, 2), Decimal::new(-1646, 2));
        let html = render_dashboard_html(&snap, &[]);
        assert!(html.contains(">Realized<"));
        assert!(html.contains(">Unrealized<"));
        assert!(html.contains(">Total P&amp;L<"));
        assert!(html.contains("+65.06"));
        assert_eq!(snap.displayed_total(), Decimal::new(6506, 2));
    }

    #[test]
    fn empty_trades_render_placeholder_row() {
        let snap = snapshot(Decimal::ZERO, Decimal::ZERO);
        let html = render_dashboard_html(&snap, &[]);
        assert!(html.contains("No trades yet."));
        // Placeholder colspan matches the 12-column header.
        assert!(html.contains("colspan=\"12\""));
    }
}
