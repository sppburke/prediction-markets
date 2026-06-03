//! Portfolio snapshot type and embedded HTML dashboard renderer.

use rust_decimal::Decimal;
use serde::Serialize;

/// Point-in-time portfolio summary.
#[derive(Debug, Clone, Serialize)]
pub struct PortfolioSnapshot {
    pub current_bankroll: Decimal,
    pub initial_bankroll: Decimal,
    pub total_pnl: Decimal,
    pub resolution_credits: Decimal,
    pub settled_markets: usize,
    pub open_position_count: usize,
    pub fills_count: usize,
}

impl PortfolioSnapshot {
    /// Return value as a percentage of initial bankroll, or zero if initial is zero.
    pub fn pnl_pct(&self) -> Decimal {
        if self.initial_bankroll.is_zero() {
            return Decimal::ZERO;
        }
        (self.total_pnl / self.initial_bankroll * Decimal::ONE_HUNDRED).round_dp(2)
    }
}

/// Render a simple HTML dashboard embedding the snapshot as a JSON blob.
pub fn render_dashboard_html(snapshot: &PortfolioSnapshot) -> String {
    let json = serde_json::to_string_pretty(snapshot).unwrap_or_else(|_| "{}".to_string());
    let pnl_color = if snapshot.total_pnl >= Decimal::ZERO {
        "#2ecc71"
    } else {
        "#e74c3c"
    };
    let pnl_sign = if snapshot.total_pnl >= Decimal::ZERO {
        "+"
    } else {
        ""
    };
    let pnl_pct = snapshot.pnl_pct();
    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Paper Trader Dashboard</title>
<style>
body{{font-family:monospace;background:#1a1a2e;color:#eee;padding:2rem;}}
h1{{color:#a29bfe;}}
.card{{background:#16213e;border-radius:8px;padding:1.5rem;margin:1rem 0;}}
.stat{{display:inline-block;margin:0.5rem 1rem;}}
.label{{color:#74b9ff;font-size:.85rem;}}
.value{{font-size:1.4rem;font-weight:bold;}}
.pnl{{color:{pnl_color};}}
pre{{background:#0f3460;padding:1rem;border-radius:6px;overflow:auto;font-size:.8rem;}}
</style>
</head>
<body>
<h1>Paper Trader Dashboard</h1>
<div class="card">
  <div class="stat"><div class="label">Bankroll</div><div class="value">${current_bankroll}</div></div>
  <div class="stat"><div class="label">P&amp;L</div><div class="value pnl">{pnl_sign}{total_pnl} ({pnl_pct}%)</div></div>
  <div class="stat"><div class="label">Resolution Credits</div><div class="value">${resolution_credits}</div></div>
  <div class="stat"><div class="label">Settled Markets</div><div class="value">{settled_markets}</div></div>
  <div class="stat"><div class="label">Open Positions</div><div class="value">{open_position_count}</div></div>
  <div class="stat"><div class="label">Total Fills</div><div class="value">{fills_count}</div></div>
</div>
<div class="card"><details><summary>Raw JSON</summary><pre>{json}</pre></details></div>
</body></html>"#,
        current_bankroll = snapshot.current_bankroll,
        total_pnl = snapshot.total_pnl,
        resolution_credits = snapshot.resolution_credits,
        settled_markets = snapshot.settled_markets,
        open_position_count = snapshot.open_position_count,
        fills_count = snapshot.fills_count,
    )
}
