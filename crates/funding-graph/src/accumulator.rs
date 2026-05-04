//! Pure accumulator: `PolygonEvent`s → `FundingSnapshot`.

use std::collections::{HashMap, HashSet};

use pe_core_types::{SourceTimestamp, WalletAddress};
use pe_operator_graph::{AddressCategory, FundingEdge, FundingSnapshot};
use pe_source_onchain_polygon::{ExternalAddressKind, PolygonEvent, TxHash};
use time::macros::datetime;

/// Accumulates decoded [`PolygonEvent`]s and produces [`FundingSnapshot`]s for
/// the operator-graph clustering engine.
///
/// Pure and deterministic: same sequence of events always yields the same
/// snapshot.
pub struct FundingGraphAccumulator {
    edges: Vec<FundingEdge>,
    /// First deployment timestamp per proxy wallet.
    wallet_first_seen: HashMap<WalletAddress, SourceTimestamp>,
    /// Categorised external addresses (bridges, CEX deposits, on-ramps).
    known_external: HashMap<WalletAddress, AddressCategory>,
    /// Timestamp of the most-recent ingested event.
    last_timestamp: SourceTimestamp,
    /// Highest block number seen across all ingested events.
    highest_block: u64,
    /// `(tx_hash, from, to)` keys of edges already ingested. Multi-pass
    /// backfill can return the same physical log when wallets appear in more
    /// than one pass's `topic[2]` filter; without this guard we'd push
    /// duplicate `FundingEdge`s and double-weight the funding graph.
    seen_edge_keys: HashSet<(TxHash, WalletAddress, WalletAddress)>,
}

impl FundingGraphAccumulator {
    /// Create an empty accumulator.
    pub fn new() -> Self {
        Self {
            edges: Vec::new(),
            wallet_first_seen: HashMap::new(),
            known_external: HashMap::new(),
            // Sentinel: no events ingested yet.
            last_timestamp: SourceTimestamp(datetime!(1970-01-01 0:00 UTC)),
            highest_block: 0,
            seen_edge_keys: HashSet::new(),
        }
    }

    /// Apply a single [`PolygonEvent`] to the accumulator's state.
    pub fn ingest(&mut self, event: PolygonEvent) {
        match event {
            PolygonEvent::UsdcTransfer {
                from,
                to,
                amount_usd,
                timestamp,
                block_number,
                tx_hash,
                ..
            } => {
                self.update_watermark(&timestamp, block_number);
                if self.seen_edge_keys.insert((tx_hash, from, to)) {
                    self.edges.push(FundingEdge {
                        funder: from,
                        funded: to,
                        amount_usd,
                        timestamp,
                    });
                }
            }

            PolygonEvent::ProxyWalletDeployed {
                proxy,
                timestamp,
                block_number,
                ..
            } => {
                self.update_watermark(&timestamp, block_number);
                // Record only the first-seen timestamp; subsequent deployments ignored.
                self.wallet_first_seen.entry(proxy).or_insert(timestamp);
            }

            PolygonEvent::BridgeOnrampReceipt {
                bridge,
                source_kind,
                timestamp,
                block_number,
                ..
            } => {
                self.update_watermark(&timestamp, block_number);
                self.known_external
                    .insert(bridge, map_external_kind(source_kind));
            }

            PolygonEvent::DepositAddressFunding {
                from,
                deposit_address,
                amount_usd,
                timestamp,
                block_number,
                tx_hash,
                ..
            } => {
                self.update_watermark(&timestamp, block_number);
                if self.seen_edge_keys.insert((tx_hash, from, deposit_address)) {
                    self.edges.push(FundingEdge {
                        funder: from,
                        funded: deposit_address,
                        amount_usd,
                        timestamp: timestamp.clone(),
                    });
                }
                // The deposit address is a CEX intermediary, not a Polymarket proxy.
                self.known_external
                    .insert(deposit_address, AddressCategory::CexDeposit);
            }

            // pUSD mint/burn: update watermark only; no FundingEdge in Phase 0B.
            PolygonEvent::PUsdMint {
                timestamp,
                block_number,
                ..
            }
            | PolygonEvent::PUsdBurn {
                timestamp,
                block_number,
                ..
            } => {
                self.update_watermark(&timestamp, block_number);
            }
        }
    }

    /// Emit a [`FundingSnapshot`] representing current accumulated state.
    ///
    /// `closed_trade_counts` and `realized_pnl_usd` are empty in Phase 0B;
    /// they require Polymarket trade data fed from a separate source.
    ///
    /// **Precondition**: at least one event must have been ingested before calling
    /// this method. If called on an empty accumulator, `snapshot_at` will be the
    /// Unix epoch sentinel (`1970-01-01T00:00:00Z`), causing wallet ages to be
    /// computed as epoch-relative (very large values will silently overflow `u32`
    /// and be dropped from `wallet_ages`).
    pub fn snapshot(&self) -> FundingSnapshot {
        let snapshot_at = self.last_timestamp.clone();
        let snapshot_unix = snapshot_at.0.unix_timestamp();

        let wallet_ages: HashMap<WalletAddress, u32> = self
            .wallet_first_seen
            .iter()
            .filter_map(|(wallet, first_seen)| {
                let age_secs = snapshot_unix.saturating_sub(first_seen.0.unix_timestamp());
                let age_u32 = u32::try_from(age_secs).ok()?;
                Some((*wallet, age_u32))
            })
            .collect();

        FundingSnapshot {
            edges: self.edges.clone(),
            wallet_ages,
            known_external: self.known_external.clone(),
            closed_trade_counts: HashMap::new(),
            realized_pnl_usd: HashMap::new(),
            snapshot_at,
        }
    }

    /// Phase 0B: no-op. Replay reconciliation (rewinding state to a specific
    /// block) is deferred to Phase 1.
    pub fn rewind_to(&mut self, _block: u64) {}

    /// Highest block number seen across all ingested events.
    pub fn highest_block(&self) -> u64 {
        self.highest_block
    }

    fn update_watermark(&mut self, ts: &SourceTimestamp, block: u64) {
        if ts.0 > self.last_timestamp.0 {
            self.last_timestamp = ts.clone();
        }
        if block > self.highest_block {
            self.highest_block = block;
        }
    }
}

impl Default for FundingGraphAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

fn map_external_kind(kind: ExternalAddressKind) -> AddressCategory {
    match kind {
        ExternalAddressKind::CexDeposit => AddressCategory::CexDeposit,
        ExternalAddressKind::Bridge => AddressCategory::Bridge,
        ExternalAddressKind::Onramp => AddressCategory::Onramp,
        ExternalAddressKind::Unknown => AddressCategory::Unknown,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use pe_core_types::WalletAddress;

    use super::*;

    fn addr(hex: &str) -> WalletAddress {
        WalletAddress::from_hex(hex).expect("test address")
    }

    fn ts(s: &str) -> SourceTimestamp {
        use time::format_description::well_known::Rfc3339;
        let odt = time::OffsetDateTime::parse(s, &Rfc3339).expect("test timestamp");
        SourceTimestamp(odt)
    }

    fn tx() -> pe_source_onchain_polygon::event::TxHash {
        pe_source_onchain_polygon::event::TxHash([0u8; 32])
    }

    #[test]
    fn usdc_transfer_creates_edge() {
        let mut acc = FundingGraphAccumulator::new();
        acc.ingest(PolygonEvent::UsdcTransfer {
            from: addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            to: addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            to_collateral_contract: false,
            from_collateral_contract: false,
            amount_usd: rust_decimal::Decimal::ONE,
            block_number: 100,
            tx_hash: tx(),
            timestamp: ts("2024-01-01T00:00:00Z"),
        });
        let snap = acc.snapshot();
        assert_eq!(snap.edges.len(), 1);
        assert_eq!(
            snap.edges[0].funder,
            addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
    }

    #[test]
    fn proxy_deployed_records_wallet_age() {
        let mut acc = FundingGraphAccumulator::new();
        acc.ingest(PolygonEvent::ProxyWalletDeployed {
            proxy: addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            singleton: addr("0x0000000000000000000000000000000000000000"),
            block_number: 50,
            tx_hash: tx(),
            timestamp: ts("2024-01-01T00:00:00Z"),
        });
        // Ingest a later event to advance last_timestamp.
        acc.ingest(PolygonEvent::PUsdMint {
            to: addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            amount_usd: rust_decimal::Decimal::ONE,
            block_number: 100,
            tx_hash: tx(),
            timestamp: ts("2024-01-02T00:00:00Z"),
        });
        let snap = acc.snapshot();
        assert!(
            snap.wallet_ages
                .contains_key(&addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
        );
        // Age should be ~86400 s (1 day).
        let age = snap.wallet_ages[&addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")];
        assert_eq!(age, 86_400);
    }

    #[test]
    fn bridge_receipt_marks_known_external() {
        let mut acc = FundingGraphAccumulator::new();
        acc.ingest(PolygonEvent::BridgeOnrampReceipt {
            to: addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            bridge: addr("0xcccccccccccccccccccccccccccccccccccccccc"),
            amount_usd: rust_decimal::Decimal::ONE,
            source_kind: ExternalAddressKind::Bridge,
            block_number: 200,
            tx_hash: tx(),
            timestamp: ts("2024-01-01T00:00:00Z"),
        });
        let snap = acc.snapshot();
        assert_eq!(
            snap.known_external
                .get(&addr("0xcccccccccccccccccccccccccccccccccccccccc")),
            Some(&AddressCategory::Bridge)
        );
    }

    #[test]
    fn deposit_address_funding_creates_edge_and_external() {
        let mut acc = FundingGraphAccumulator::new();
        acc.ingest(PolygonEvent::DepositAddressFunding {
            from: addr("0xdddddddddddddddddddddddddddddddddddddddd"),
            deposit_address: addr("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
            amount_usd: rust_decimal::Decimal::ONE,
            block_number: 300,
            tx_hash: tx(),
            timestamp: ts("2024-01-01T00:00:00Z"),
        });
        let snap = acc.snapshot();
        assert_eq!(snap.edges.len(), 1);
        assert_eq!(
            snap.known_external
                .get(&addr("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")),
            Some(&AddressCategory::CexDeposit)
        );
    }

    #[test]
    fn duplicate_usdc_transfer_produces_one_edge() {
        let mut acc = FundingGraphAccumulator::new();
        let from = addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let to = addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let same_tx = pe_source_onchain_polygon::event::TxHash([7u8; 32]);
        let event = || PolygonEvent::UsdcTransfer {
            from,
            to,
            to_collateral_contract: false,
            from_collateral_contract: false,
            amount_usd: rust_decimal::Decimal::ONE,
            block_number: 100,
            tx_hash: same_tx,
            timestamp: ts("2024-01-01T00:00:00Z"),
        };
        // Same physical log ingested twice (multi-pass backfill scenario).
        acc.ingest(event());
        acc.ingest(event());
        let snap = acc.snapshot();
        assert_eq!(snap.edges.len(), 1);
    }

    #[test]
    fn distinct_transfers_in_same_tx_are_kept() {
        // A single tx may emit multiple Transfer events (e.g. swap + fee).
        // Dedup key is (tx_hash, from, to), so distinct edge endpoints are kept.
        let mut acc = FundingGraphAccumulator::new();
        let same_tx = pe_source_onchain_polygon::event::TxHash([9u8; 32]);
        acc.ingest(PolygonEvent::UsdcTransfer {
            from: addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            to: addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            to_collateral_contract: false,
            from_collateral_contract: false,
            amount_usd: rust_decimal::Decimal::ONE,
            block_number: 100,
            tx_hash: same_tx,
            timestamp: ts("2024-01-01T00:00:00Z"),
        });
        acc.ingest(PolygonEvent::UsdcTransfer {
            from: addr("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            to: addr("0xcccccccccccccccccccccccccccccccccccccccc"),
            to_collateral_contract: false,
            from_collateral_contract: false,
            amount_usd: rust_decimal::Decimal::ONE,
            block_number: 100,
            tx_hash: same_tx,
            timestamp: ts("2024-01-01T00:00:00Z"),
        });
        let snap = acc.snapshot();
        assert_eq!(snap.edges.len(), 2);
    }

    #[test]
    fn pnl_and_trade_counts_empty_in_phase_0b() {
        let mut acc = FundingGraphAccumulator::new();
        acc.ingest(PolygonEvent::PUsdMint {
            to: addr("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            amount_usd: rust_decimal::Decimal::ONE,
            block_number: 1,
            tx_hash: tx(),
            timestamp: ts("2024-01-01T00:00:00Z"),
        });
        let snap = acc.snapshot();
        assert!(snap.closed_trade_counts.is_empty());
        assert!(snap.realized_pnl_usd.is_empty());
    }
}
