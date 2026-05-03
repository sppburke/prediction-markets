//! [`PolygonReplayConnector`]: replay pre-loaded [`PolygonEvent`]s through the
//! [`SourceConnector`] interface.

use std::collections::VecDeque;

use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp};
use pe_source_core::{SourceConnector, SourceError, SourceEvent, SourceHealth, SourceStatus};

use crate::event::PolygonEvent;

/// A deterministic, replay-only connector that drains a pre-loaded queue of
/// [`PolygonEvent`]s.
///
/// Intended for testing and simulation. When the queue is exhausted,
/// [`SourceConnector::next_event`] returns [`SourceError::Fatal`].
pub struct PolygonReplayConnector {
    source_id: SourceId,
    events: VecDeque<PolygonEvent>,
    last_event_at: Option<SourceTimestamp>,
}

impl PolygonReplayConnector {
    /// Create a new connector that will emit `events` in order.
    pub fn new(source_id: SourceId, events: Vec<PolygonEvent>) -> Self {
        Self {
            source_id,
            events: VecDeque::from(events),
            last_event_at: None,
        }
    }
}

impl SourceConnector for PolygonReplayConnector {
    fn source_id(&self) -> SourceId {
        self.source_id.clone()
    }

    fn health(&self) -> SourceHealth {
        let status = if self.events.is_empty() {
            SourceStatus::Dead
        } else {
            SourceStatus::Healthy
        };
        SourceHealth {
            source_id: self.source_id.clone(),
            last_event_at: self.last_event_at.clone(),
            status,
        }
    }

    async fn next_event(&mut self) -> Result<SourceEvent, SourceError> {
        let event = self.events.pop_front().ok_or_else(|| SourceError::Fatal {
            message: "replay exhausted".into(),
        })?;
        let ts = event.timestamp().clone();
        let payload = serde_json::to_vec(&event).map_err(|e| SourceError::Fatal {
            message: e.to_string(),
        })?;
        self.last_event_at = Some(ts.clone());
        Ok(SourceEvent {
            source_id: self.source_id.clone(),
            schema_version: 1,
            parser_version: 1,
            observed_at: ts,
            received_at: ReceivedAt::now_utc(),
            payload,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::event::{PolygonEvent, TxHash};
    use pe_core_types::{SourceTimestamp, WalletAddress};
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use time::OffsetDateTime;

    fn make_event() -> PolygonEvent {
        let ts = SourceTimestamp(OffsetDateTime::UNIX_EPOCH);
        let tx = TxHash([0u8; 32]);
        let addr: WalletAddress =
            serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap();
        PolygonEvent::PUsdMint {
            to: addr,
            amount_usd: Decimal::from_str("100.00").unwrap(),
            block_number: 1,
            tx_hash: tx,
            timestamp: ts,
        }
    }

    fn sid() -> SourceId {
        SourceId("test".into())
    }

    #[test]
    fn health_healthy_when_events_present() {
        let conn = PolygonReplayConnector::new(sid(), vec![make_event()]);
        assert_eq!(conn.health().status, SourceStatus::Healthy);
    }

    #[test]
    fn health_dead_when_empty() {
        let conn = PolygonReplayConnector::new(sid(), vec![]);
        assert_eq!(conn.health().status, SourceStatus::Dead);
    }

    #[tokio::test]
    async fn next_event_on_empty_returns_fatal() {
        let mut conn = PolygonReplayConnector::new(sid(), vec![]);
        let err = conn.next_event().await.unwrap_err();
        assert!(matches!(err, SourceError::Fatal { .. }));
    }

    #[tokio::test]
    async fn next_event_transitions_health() {
        let mut conn = PolygonReplayConnector::new(sid(), vec![make_event()]);
        assert_eq!(conn.health().status, SourceStatus::Healthy);
        let _ = conn.next_event().await.unwrap();
        assert_eq!(conn.health().status, SourceStatus::Dead);
        assert!(conn.health().last_event_at.is_some());
    }

    #[tokio::test]
    async fn next_event_payload_nonempty() {
        let mut conn = PolygonReplayConnector::new(sid(), vec![make_event()]);
        let ev = conn.next_event().await.unwrap();
        assert!(!ev.payload.is_empty());
    }
}
