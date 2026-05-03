//! [`PolymarketPublicConnector`] — round-robin polling connector.

use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp};
use pe_source_core::{SourceConnector, SourceError, SourceEvent, SourceHealth, SourceStatus};

use crate::{config::PollingConfig, endpoint::PolymarketEndpoint, fetcher::PageFetcher};

/// A [`SourceConnector`] that round-robins over a list of Polymarket public
/// REST endpoints, emitting one [`SourceEvent`] per call to [`next_event`].
///
/// [`next_event`]: PolymarketPublicConnector::next_event
pub struct PolymarketPublicConnector<F: PageFetcher> {
    source_id: SourceId,
    config: PollingConfig,
    endpoints: Vec<PolymarketEndpoint>,
    current_index: usize,
    fetcher: F,
    last_event_at: Option<SourceTimestamp>,
    consecutive_errors: u32,
}

impl<F: PageFetcher> PolymarketPublicConnector<F> {
    /// Create a new connector.
    ///
    /// `endpoints` defines the polling round-robin order. Calling
    /// [`next_event`] with an empty list returns [`SourceError::Fatal`].
    ///
    /// [`next_event`]: PolymarketPublicConnector::next_event
    pub fn new(
        source_id: SourceId,
        config: PollingConfig,
        endpoints: Vec<PolymarketEndpoint>,
        fetcher: F,
    ) -> Self {
        Self {
            source_id,
            config,
            endpoints,
            current_index: 0,
            fetcher,
            last_event_at: None,
            consecutive_errors: 0,
        }
    }
}

impl<F: PageFetcher> SourceConnector for PolymarketPublicConnector<F> {
    fn source_id(&self) -> SourceId {
        self.source_id.clone()
    }

    fn health(&self) -> SourceHealth {
        let status = if self.consecutive_errors >= 5 {
            SourceStatus::Dead
        } else if self.consecutive_errors >= 2 {
            SourceStatus::Degraded
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
        if self.endpoints.is_empty() {
            return Err(SourceError::Fatal {
                message: "no endpoints configured".into(),
            });
        }

        let endpoint = &self.endpoints[self.current_index];
        let url = endpoint.url(&self.config.base_url);

        // Advance round-robin BEFORE fetch so callers see a different endpoint on retry.
        self.current_index = (self.current_index + 1) % self.endpoints.len();

        let result = self.fetcher.fetch_page(&url).await;
        let payload = match result {
            Err(e) => {
                self.consecutive_errors += 1;
                return Err(e);
            }
            Ok(bytes) => bytes,
        };

        self.consecutive_errors = 0;

        let now = time::OffsetDateTime::now_utc();
        let ts = SourceTimestamp(now);
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
