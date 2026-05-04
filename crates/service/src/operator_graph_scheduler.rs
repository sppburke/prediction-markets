//! Periodic operator-graph rebuild scheduler.
//!
//! Runs on a configurable cadence, calls `FundingGraphAccumulator::snapshot()`,
//! feeds the snapshot to `build_operator_identities`, and publishes the result
//! via a `tokio::sync::watch` channel so downstream readers always see the
//! latest cluster list without blocking.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use pe_funding_graph::FundingGraphAccumulator;
use pe_operator_graph::{ClusteringConfig, OperatorIdentity, build_operator_identities};
use tokio::sync::watch;
use tracing::{error, info};

/// Periodically rebuilds operator identities from accumulated funding events and
/// publishes them to a [`watch`] channel.
pub struct OperatorGraphScheduler {
    accumulator: Arc<Mutex<FundingGraphAccumulator>>,
    clustering_config: ClusteringConfig,
    cadence: Duration,
    tx: watch::Sender<Vec<OperatorIdentity>>,
}

impl OperatorGraphScheduler {
    /// Create a new scheduler and return both the scheduler and the watch receiver.
    ///
    /// The receiver's initial value is an empty `Vec` (no operators yet known).
    /// Call [`run`] to start the rebuild loop.
    ///
    /// Pass `ClusteringConfig::default()` for standard production behaviour.
    /// Callers that need non-default thresholds (e.g. integration tests or
    /// operator-specific tuning) can supply a custom config without recompiling.
    pub fn new(
        accumulator: Arc<Mutex<FundingGraphAccumulator>>,
        clustering_config: ClusteringConfig,
        cadence_secs: u64,
    ) -> (Self, watch::Receiver<Vec<OperatorIdentity>>) {
        let (tx, rx) = watch::channel(Vec::new());
        let scheduler = Self {
            accumulator,
            clustering_config,
            cadence: Duration::from_secs(cadence_secs),
            tx,
        };
        (scheduler, rx)
    }

    /// Run the rebuild loop until the watch receiver side is dropped.
    ///
    /// On each tick: snapshot the accumulator, rebuild operator identities, and
    /// send the updated list to all receivers. Errors from clustering are logged
    /// and skipped — the previous snapshot remains visible to readers.
    pub async fn run(self) {
        let mut interval = tokio::time::interval(self.cadence);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            if self.tx.is_closed() {
                break;
            }

            let snapshot = {
                let acc = self
                    .accumulator
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                acc.snapshot()
            };

            match build_operator_identities(&snapshot, &self.clustering_config) {
                Err(e) => error!(error = %e, "operator-graph rebuild failed"),
                Ok(identities) => {
                    let count = identities.len();
                    if self.tx.send(identities).is_err() {
                        break; // all receivers dropped
                    }
                    info!(kind = "operator_graph_rebuilt", operator_count = count);
                }
            }
        }
    }
}
