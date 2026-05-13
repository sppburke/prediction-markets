//! Polygon RPC scan for CTF `ConditionResolution` events.
//!
//! Issue #149: this is the primary historical resolution source for the
//! multi-source pipeline. Unlike Gamma (purges resolved markets) or CLOB
//! (approximate `end_date_iso` timestamp), `eth_getLogs` against the CTF
//! contract returns every settled binary market with the authoritative
//! block-timestamp resolution time.
//!
//! The scan resumes from `source_cursor.polygon_ctf_last_block` so daily
//! re-runs only walk new blocks instead of re-scanning the entire chain.
//! When the cursor is absent (first run), `from_block` falls back to
//! [`CTF_DEPLOY_BLOCK`].
//!
//! [`CTF_DEPLOY_BLOCK`]: pe_source_onchain_polygon::contracts::CTF_DEPLOY_BLOCK

use alloy::primitives::{B256, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::{BlockNumberOrTag, Filter, Log};
use alloy::sol_types::SolValue;
use pe_source_onchain_polygon::contracts::{CTF, TOPIC_CONDITION_RESOLUTION};
use time::OffsetDateTime;
use tracing::{info, warn};

use crate::cache::WalletCache;
use crate::error::BootstrapError;

/// `source_cursor` key for the Polygon CTF scan checkpoint. Value is the
/// last successfully-scanned block as a decimal string.
pub const POLYGON_CTF_CURSOR_KEY: &str = "polygon_ctf_last_block";

/// Minimum chunk size used when bisecting on a "response too large" error.
/// One block is the strict floor — if a single block exceeds the cap the
/// caller should switch to a paid RPC tier rather than thrash further.
const MIN_CHUNK_BLOCKS: u64 = 1;

/// Scan the CTF contract for `ConditionResolution` events between
/// `[from_block, to_block_or_head]` and insert each newly-seen resolution
/// into `cache` with `source='polygon'`.
///
/// `to_block = None` means "current chain head" — fetched once at entry via
/// `eth_blockNumber` so a long backfill does not drift forward across many
/// blocks while scanning.
///
/// `chunk_blocks` controls the per-request block range; the bisect-on-cap
/// fallback halves the range and retries on response-too-large errors.
///
/// Skip-set: every market already present in `market_resolutions` (any
/// source) is silently filtered out, so the function is idempotent across
/// repeated invocations and earlier sources (Polygon ran first → CLOB and
/// Dune see no work to do).
///
/// After every successful chunk, the `polygon_ctf_last_block` cursor
/// advances so daily re-runs resume from the new floor. A crash mid-chunk
/// re-processes that chunk on restart; `INSERT OR IGNORE` keeps the result
/// idempotent.
///
/// Returns the count of newly-inserted resolutions.
pub async fn scan_resolutions(
    rpc_url: &str,
    from_block: u64,
    to_block: Option<u64>,
    chunk_blocks: u64,
    cache: &mut WalletCache,
) -> Result<usize, BootstrapError> {
    if chunk_blocks == 0 {
        return Err(BootstrapError::PolygonCtf {
            message: "chunk_blocks must be > 0".to_owned(),
        });
    }
    let http_url: reqwest::Url =
        rpc_url
            .parse()
            .map_err(
                |e: <reqwest::Url as std::str::FromStr>::Err| BootstrapError::PolygonCtf {
                    message: format!("invalid rpc_url: {e}"),
                },
            )?;
    let provider = ProviderBuilder::new().connect_http(http_url);

    let to = match to_block {
        Some(b) => b,
        None => provider
            .get_block_number()
            .await
            .map_err(|e| BootstrapError::PolygonCtf {
                message: format!("get_block_number: {e}"),
            })?,
    };
    if from_block > to {
        info!(from_block, to_block = to, "polygon_ctf: nothing to scan");
        return Ok(0);
    }

    let already_resolved = cache.resolved_market_ids();
    info!(
        from_block,
        to_block = to,
        chunk_blocks,
        already_cached = already_resolved.len(),
        "polygon_ctf: starting scan"
    );

    let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
    let mut inserted = 0usize;
    let mut block = from_block;
    while block <= to {
        // Inclusive upper bound for this chunk.
        let chunk_to = block.saturating_add(chunk_blocks - 1).min(to);
        let logs = get_logs_with_bisect(&provider, block, chunk_to, chunk_blocks).await?;

        for log in &logs {
            let Some((condition_id_hex, winner)) = decode_resolution_log(log) else {
                continue; // non-binary / malformed
            };
            if already_resolved.contains(&condition_id_hex) {
                continue;
            }
            let resolved_at = log
                .block_timestamp
                .map_or(fetched_at, |t| i64::try_from(t).unwrap_or(fetched_at));
            cache.insert_resolution_with_source(
                &condition_id_hex,
                winner,
                resolved_at,
                fetched_at,
                "polygon",
            )?;
            inserted += 1;
        }

        cache.set_source_cursor(POLYGON_CTF_CURSOR_KEY, &chunk_to.to_string())?;
        block = chunk_to.saturating_add(1);
    }

    info!(inserted, "polygon_ctf: scan complete");
    Ok(inserted)
}

/// Issue a single `eth_getLogs` request for `[from, to]`, bisecting on
/// "response too large" errors. The cap-error detection is heuristic
/// (substring match) because providers return non-standard JSON-RPC
/// error codes; the fallback halves the range until one of:
///   - the request succeeds,
///   - the range reaches [`MIN_CHUNK_BLOCKS`] (1 block) and still fails →
///     propagate the error so the caller can lower `chunk_blocks` or
///     switch RPC tiers.
async fn get_logs_with_bisect<P: Provider>(
    provider: &P,
    from: u64,
    to: u64,
    initial_chunk: u64,
) -> Result<Vec<Log>, BootstrapError> {
    let filter = Filter::new()
        .address(CTF)
        .event_signature(TOPIC_CONDITION_RESOLUTION)
        .from_block(BlockNumberOrTag::Number(from))
        .to_block(BlockNumberOrTag::Number(to));

    match provider.get_logs(&filter).await {
        Ok(logs) => Ok(logs),
        Err(e) => {
            let span = to.saturating_sub(from).saturating_add(1);
            if span <= MIN_CHUNK_BLOCKS {
                return Err(BootstrapError::PolygonCtf {
                    message: format!("eth_getLogs failed at minimum range [{from}, {to}]: {e}"),
                });
            }
            let msg = e.to_string().to_lowercase();
            // Heuristic: response-too-large / range-too-wide / log-limit-exceeded.
            let is_cap = msg.contains("too large")
                || msg.contains("too many")
                || msg.contains("response size")
                || msg.contains("limit exceeded")
                || msg.contains("range");
            if !is_cap {
                return Err(BootstrapError::PolygonCtf {
                    message: format!("eth_getLogs [{from}, {to}]: {e}"),
                });
            }
            warn!(
                from,
                to,
                error = %e,
                "polygon_ctf: response cap hit, bisecting"
            );
            let mid = from.saturating_add(span / 2);
            let mut left =
                Box::pin(get_logs_with_bisect(provider, from, mid - 1, initial_chunk)).await?;
            let right = Box::pin(get_logs_with_bisect(provider, mid, to, initial_chunk)).await?;
            left.extend(right);
            Ok(left)
        }
    }
}

/// Decode a `ConditionResolution` log into `(condition_id_hex, winner)`.
///
/// Returns `None` when the log is malformed OR the market is non-binary
/// (`outcomeSlotCount != 2`) OR `payoutNumerators` length disagrees with
/// `outcomeSlotCount`. The caller silently skips these — Dune covers
/// multi-outcome markets as a fallback. Returning `None` (rather than
/// `Some((id, None))`) is load-bearing: a row with `winner=None` would
/// still occupy the `market_resolutions` table and short-circuit the
/// `unresolved_market_ids` filter at the Dune gate, leaving multi-outcome
/// markets permanently unresolved (issue #149 PR #151 code-review fix).
///
/// **Winner rule (issue #149 design) for binary markets:** count the
/// non-zero entries in `payoutNumerators`. Exactly one non-zero →
/// `Some(index)` (YES at 0, NO at 1). Zero or two non-zero →
/// `Some((id, None))` (voided or tied 50/50). Mirrors `gamma.rs`'s
/// "price > 0.5" tie-handling so a `[1, 1]` payout is treated as
/// ambiguous rather than silently labelled YES.
pub fn decode_resolution_log(log: &Log) -> Option<(String, Option<u16>)> {
    let topics = log.topics();
    // topic[0] = event signature, topic[1] = conditionId (the only one we read).
    let condition_id: &B256 = topics.get(1)?;
    let condition_id_hex = format!("0x{condition_id:x}");

    let (slot_count, numerators) = decode_payout_numerators(&log.data().data)?;
    if slot_count != U256::from(2u64) {
        // Non-binary: skip entirely so Dune can resolve it later.
        return None;
    }
    if numerators.len() != 2 {
        // Malformed binary log (slot count and array length disagree); skip.
        return None;
    }
    let nonzero: Vec<usize> = numerators
        .iter()
        .enumerate()
        .filter(|(_, n)| !n.is_zero())
        .map(|(i, _)| i)
        .collect();
    let winner = if nonzero.len() == 1 {
        u16::try_from(nonzero[0]).ok()
    } else {
        None
    };
    Some((condition_id_hex, winner))
}

/// ABI-decode the non-indexed event data
/// `(uint256 outcomeSlotCount, uint256[] payoutNumerators)`.
///
/// Returns `None` on malformed input. The `sol-types` workspace feature
/// brings in the [`SolValue`] codec.
fn decode_payout_numerators(data: &[u8]) -> Option<(U256, Vec<U256>)> {
    <(U256, Vec<U256>)>::abi_decode_params(data).ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use alloy::primitives::{Bytes, LogData};
    use alloy::rpc::types::Log;
    use alloy::sol_types::SolValue;

    fn encode_payout(slot_count: u64, numerators: &[u64]) -> Vec<u8> {
        let nums: Vec<U256> = numerators.iter().copied().map(U256::from).collect();
        (U256::from(slot_count), nums).abi_encode_params()
    }

    fn make_log(condition_id: B256, data: Vec<u8>) -> Log {
        let inner = alloy::primitives::Log {
            address: CTF,
            data: LogData::new_unchecked(
                vec![TOPIC_CONDITION_RESOLUTION, condition_id],
                Bytes::from(data),
            ),
        };
        Log {
            inner,
            ..Default::default()
        }
    }

    #[test]
    fn decode_yes_wins() {
        let cond = B256::repeat_byte(0xaa);
        let data = encode_payout(2, &[1, 0]);
        let (hex, winner) = decode_resolution_log(&make_log(cond, data)).unwrap();
        assert_eq!(hex, format!("0x{cond:x}"));
        assert_eq!(winner, Some(0), "[1, 0] must resolve YES (index 0)");
    }

    #[test]
    fn decode_no_wins() {
        let cond = B256::repeat_byte(0xbb);
        let data = encode_payout(2, &[0, 1]);
        let (_, winner) = decode_resolution_log(&make_log(cond, data)).unwrap();
        assert_eq!(winner, Some(1), "[0, 1] must resolve NO (index 1)");
    }

    #[test]
    fn decode_voided() {
        let cond = B256::repeat_byte(0xcc);
        let data = encode_payout(2, &[0, 0]);
        let (_, winner) = decode_resolution_log(&make_log(cond, data)).unwrap();
        assert!(winner.is_none(), "[0, 0] must be voided (None)");
    }

    #[test]
    fn decode_tied_payout_is_none() {
        // Critical: the "first non-zero" rule would wrongly pick YES here.
        // Issue #149 winner rule requires unique non-zero ⇒ ambiguous → None.
        let cond = B256::repeat_byte(0xdd);
        let data = encode_payout(2, &[1, 1]);
        let (_, winner) = decode_resolution_log(&make_log(cond, data)).unwrap();
        assert!(
            winner.is_none(),
            "[1, 1] (tied 50/50) must NOT be silently labelled YES"
        );
    }

    #[test]
    fn decode_multi_outcome_returns_none_to_skip_entirely() {
        // Multi-outcome (slot_count != 2) must return `None` (not
        // `Some((id, None))`) so the row never enters `market_resolutions`
        // and Dune's `unresolved_market_ids` filter still picks it up.
        // Issue #149 PR #151 code-review fix.
        let cond = B256::repeat_byte(0xee);
        let data = encode_payout(3, &[0, 1, 0]);
        assert!(
            decode_resolution_log(&make_log(cond, data)).is_none(),
            "non-binary markets must be skipped entirely; Dune covers them as fallback"
        );
    }

    #[test]
    fn decode_malformed_data_returns_none() {
        let cond = B256::repeat_byte(0xff);
        let log = make_log(cond, vec![0x00, 0x01, 0x02]);
        assert!(decode_resolution_log(&log).is_none());
    }

    #[test]
    fn decode_missing_condition_topic_returns_none() {
        let data = encode_payout(2, &[1, 0]);
        let inner = alloy::primitives::Log {
            address: CTF,
            data: LogData::new_unchecked(
                vec![TOPIC_CONDITION_RESOLUTION], // only topic0, no conditionId
                Bytes::from(data),
            ),
        };
        let log = Log {
            inner,
            ..Default::default()
        };
        assert!(decode_resolution_log(&log).is_none());
    }
}
