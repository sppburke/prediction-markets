//! Named production-task supervision and coordinated shutdown (#544).
//!
//! The supervisor classifies *owner exit*, not recoverable per-cycle errors. Every Tokio task
//! spawned by the ordinary `pe-service` binary is registered here before it starts. A critical
//! owner that exits while its shutdown phase is still [`ShutdownPhase::Running`] records one
//! sticky typed failure; best-effort owners record degradation without failing readiness.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use tokio::sync::watch;
use tokio::task::{AbortHandle, Id, JoinSet};

/// Application deadline shared with the existing service shutdown contract.
///
/// The isolated canary already uses a 45-second application deadline; the ordinary service now
/// uses the same bound so its joins finish before an outer service-manager kill backstop.
pub const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(45);

/// Hard bound on post-abort joins and the final join sweep. Aborted tasks
/// normally finish in milliseconds; a task that cannot be joined within this
/// bound is pinned in synchronous work and the process exits instead (#544).
pub const POST_ABORT_JOIN_BOUND: std::time::Duration = std::time::Duration::from_secs(10);

/// Production owner name. The spelling is the stable `status.json` contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskName {
    ActivityIngest,
    PublicActivityPoll,
    Orchestrator,
    ResolutionPoller,
    LiveAccountsPoller,
    LiveFanout,
    WatchlistRefresh,
    WatchlistMaintenance,
    CapacityWorker,
    RuntimeConfigPoller,
    StatusWriter,
    HttpServer,
    SupabaseAnalyticsSink,
    LiquiditySnapshotWorker,
    JsonTracingFullAppender,
    JsonTracingErrorAppender,
}

impl TaskName {
    /// Complete ordinary-service owner inventory used by scenario coverage.
    pub const ALL: [Self; 16] = [
        Self::ActivityIngest,
        Self::PublicActivityPoll,
        Self::Orchestrator,
        Self::ResolutionPoller,
        Self::LiveAccountsPoller,
        Self::LiveFanout,
        Self::WatchlistRefresh,
        Self::WatchlistMaintenance,
        Self::CapacityWorker,
        Self::RuntimeConfigPoller,
        Self::StatusWriter,
        Self::HttpServer,
        Self::SupabaseAnalyticsSink,
        Self::LiquiditySnapshotWorker,
        Self::JsonTracingFullAppender,
        Self::JsonTracingErrorAppender,
    ];

    #[must_use]
    pub const fn class(self) -> TaskClass {
        match self {
            Self::SupabaseAnalyticsSink | Self::LiquiditySnapshotWorker => {
                TaskClass::BestEffortAnalytics
            }
            Self::JsonTracingFullAppender | Self::JsonTracingErrorAppender => {
                TaskClass::BestEffortObservability
            }
            _ => TaskClass::Critical,
        }
    }

    #[must_use]
    pub const fn stop_phase(self) -> ShutdownPhase {
        match self {
            Self::PublicActivityPoll
            | Self::ResolutionPoller
            | Self::LiveAccountsPoller
            | Self::WatchlistRefresh
            | Self::WatchlistMaintenance
            | Self::CapacityWorker
            | Self::RuntimeConfigPoller => ShutdownPhase::StopProducers,
            Self::Orchestrator => ShutdownPhase::DrainOrchestrator,
            Self::ActivityIngest
            | Self::LiveFanout
            | Self::SupabaseAnalyticsSink
            | Self::LiquiditySnapshotWorker => ShutdownPhase::StopSinks,
            Self::HttpServer => ShutdownPhase::StopHttp,
            Self::StatusWriter => ShutdownPhase::FinalStatus,
            Self::JsonTracingFullAppender | Self::JsonTracingErrorAppender => {
                ShutdownPhase::Complete
            }
        }
    }
}

impl fmt::Display for TaskName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ActivityIngest => "activity_ingest",
            Self::PublicActivityPoll => "public_activity_poll",
            Self::Orchestrator => "orchestrator",
            Self::ResolutionPoller => "resolution_poller",
            Self::LiveAccountsPoller => "live_accounts_poller",
            Self::LiveFanout => "live_fanout",
            Self::WatchlistRefresh => "watchlist_refresh",
            Self::WatchlistMaintenance => "watchlist_maintenance",
            Self::CapacityWorker => "capacity_worker",
            Self::RuntimeConfigPoller => "runtime_config_poller",
            Self::StatusWriter => "status_writer",
            Self::HttpServer => "http_server",
            Self::SupabaseAnalyticsSink => "supabase_analytics_sink",
            Self::LiquiditySnapshotWorker => "liquidity_snapshot_worker",
            Self::JsonTracingFullAppender => "json_tracing_full_appender",
            Self::JsonTracingErrorAppender => "json_tracing_error_appender",
        })
    }
}

/// Readiness consequence of an owner exit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskClass {
    Critical,
    BestEffortAnalytics,
    BestEffortObservability,
}

/// Ordered shutdown stages. Owners stop only at their declared stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownPhase {
    Running,
    StopProducers,
    DrainOrchestrator,
    StopSinks,
    StopHttp,
    FinalStatus,
    Complete,
}

/// Why an owner failed or degraded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskFailureKind {
    TypedError,
    EarlyReturn,
    ChannelClosed,
    JoinFailed,
    ShutdownTimeout,
}

/// Sticky typed owner failure surfaced in readiness and `status.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskFailure {
    pub kind: TaskFailureKind,
    pub message: String,
}

impl TaskFailure {
    #[must_use]
    pub fn typed(error: impl fmt::Display) -> Self {
        Self {
            kind: TaskFailureKind::TypedError,
            message: error.to_string(),
        }
    }

    #[must_use]
    pub fn shutdown_timeout() -> Self {
        Self {
            kind: TaskFailureKind::ShutdownTimeout,
            message: "owner exceeded the coordinated shutdown deadline".to_owned(),
        }
    }
}

/// A task future's non-error exit. The supervisor decides whether it was expected from phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskExit {
    /// The owner future returned normally.
    Completed,
    /// The owner observed a named dependency channel close.
    ChannelClosed(&'static str),
    /// The owner reached its explicit coordinated-shutdown branch.
    CleanShutdown,
}

pub type TaskResult = Result<TaskExit, TaskFailure>;

/// Serialized lifecycle state for one registered owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskRunState {
    Running,
    Stopping,
    Stopped,
    Failed,
    Degraded,
}

/// One stable task row in `status.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskStateSnapshot {
    pub name: TaskName,
    pub class: TaskClass,
    pub state: TaskRunState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<TaskFailure>,
}

#[derive(Debug)]
struct TaskStatusInner {
    phase: ShutdownPhase,
    entries: BTreeMap<TaskName, TaskStateSnapshot>,
}

/// Shared task state consumed by readiness and the status writer.
#[derive(Debug, Clone)]
pub struct TaskStatus {
    inner: Arc<Mutex<TaskStatusInner>>,
}

impl Default for TaskStatus {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskStatus {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(TaskStatusInner {
                phase: ShutdownPhase::Running,
                entries: BTreeMap::new(),
            })),
        }
    }

    pub fn register(&self, name: TaskName) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.entries.entry(name).or_insert(TaskStateSnapshot {
            name,
            class: name.class(),
            state: TaskRunState::Running,
            failure: None,
        });
    }

    pub fn advance_phase(&self, phase: ShutdownPhase) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if phase <= inner.phase {
            return;
        }
        inner.phase = phase;
        for entry in inner.entries.values_mut() {
            if entry.state == TaskRunState::Running && entry.name.stop_phase() <= phase {
                entry.state = TaskRunState::Stopping;
            }
        }
    }

    #[must_use]
    pub fn phase(&self) -> ShutdownPhase {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .phase
    }

    #[must_use]
    pub fn snapshot(&self) -> Vec<TaskStateSnapshot> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .values()
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn critical_failed(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .values()
            .any(|entry| entry.class == TaskClass::Critical && entry.state == TaskRunState::Failed)
    }

    fn record_exit(&self, name: TaskName, result: TaskResult) -> Option<TaskFailure> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let phase = inner.phase;
        let entry = inner.entries.entry(name).or_insert(TaskStateSnapshot {
            name,
            class: name.class(),
            state: TaskRunState::Running,
            failure: None,
        });
        let expected = phase >= name.stop_phase();
        let failure = match result {
            Ok(TaskExit::CleanShutdown | TaskExit::Completed) if expected => None,
            Ok(TaskExit::ChannelClosed(_)) if expected => None,
            Ok(TaskExit::Completed | TaskExit::CleanShutdown) => Some(TaskFailure {
                kind: TaskFailureKind::EarlyReturn,
                message: "owner returned before coordinated shutdown".to_owned(),
            }),
            Ok(TaskExit::ChannelClosed(channel)) => Some(TaskFailure {
                kind: TaskFailureKind::ChannelClosed,
                message: format!("dependency channel '{channel}' closed"),
            }),
            Err(failure) => Some(failure),
        };
        if let Some(failure) = failure {
            if entry.failure.is_none() {
                entry.failure = Some(failure.clone());
            }
            entry.state = match entry.class {
                TaskClass::Critical => TaskRunState::Failed,
                TaskClass::BestEffortAnalytics | TaskClass::BestEffortObservability => {
                    TaskRunState::Degraded
                }
            };
            entry.failure.clone()
        } else {
            entry.state = TaskRunState::Stopped;
            None
        }
    }

    fn record_join_failure(&self, name: TaskName, message: String) -> TaskFailure {
        let failure = TaskFailure {
            kind: TaskFailureKind::JoinFailed,
            message,
        };
        self.record_failure(name, failure.clone());
        failure
    }

    pub fn record_failure(&self, name: TaskName, failure: TaskFailure) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = inner.entries.entry(name).or_insert(TaskStateSnapshot {
            name,
            class: name.class(),
            state: TaskRunState::Running,
            failure: None,
        });
        if entry.failure.is_none() {
            entry.failure = Some(failure);
        }
        entry.state = match entry.class {
            TaskClass::Critical => TaskRunState::Failed,
            TaskClass::BestEffortAnalytics | TaskClass::BestEffortObservability => {
                TaskRunState::Degraded
            }
        };
    }

    pub fn mark_stopped(&self, name: TaskName) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = inner.entries.get_mut(&name)
            && entry.failure.is_none()
        {
            entry.state = TaskRunState::Stopped;
        }
    }
}

/// Broadcast side of the ordered shutdown phase.
#[derive(Clone)]
pub struct ShutdownController {
    tx: watch::Sender<ShutdownPhase>,
}

/// Per-owner receiver for the ordered shutdown phase.
pub struct ShutdownReceiver {
    rx: watch::Receiver<ShutdownPhase>,
}

impl ShutdownController {
    #[must_use]
    pub fn new() -> (Self, ShutdownReceiver) {
        let (tx, rx) = watch::channel(ShutdownPhase::Running);
        (Self { tx }, ShutdownReceiver { rx })
    }

    #[must_use]
    pub fn subscribe(&self) -> ShutdownReceiver {
        ShutdownReceiver {
            rx: self.tx.subscribe(),
        }
    }

    pub fn advance(&self, phase: ShutdownPhase) {
        self.tx.send_if_modified(|current| {
            if *current < phase {
                *current = phase;
                true
            } else {
                false
            }
        });
    }

    #[must_use]
    pub fn phase(&self) -> ShutdownPhase {
        *self.tx.borrow()
    }
}

impl ShutdownReceiver {
    pub async fn wait_for(mut self, phase: ShutdownPhase) {
        loop {
            if *self.rx.borrow_and_update() >= phase {
                return;
            }
            if self.rx.changed().await.is_err() {
                return;
            }
        }
    }
}

/// One observed owner completion.
#[derive(Debug, Clone)]
pub struct TaskEvent {
    pub name: TaskName,
    pub class: TaskClass,
    pub failure: Option<TaskFailure>,
}

impl TaskEvent {
    #[must_use]
    pub fn initiates_shutdown(&self) -> bool {
        self.class == TaskClass::Critical && self.failure.is_some()
    }
}

/// The one Tokio task registry owned by ordinary `pe-service`.
pub struct TaskSupervisor {
    tasks: JoinSet<(TaskName, TaskResult)>,
    names_by_id: HashMap<Id, TaskName>,
    aborts: BTreeMap<TaskName, AbortHandle>,
    status: TaskStatus,
}

impl TaskSupervisor {
    #[must_use]
    pub fn new(status: TaskStatus) -> Self {
        Self {
            tasks: JoinSet::new(),
            names_by_id: HashMap::new(),
            aborts: BTreeMap::new(),
            status,
        }
    }

    pub fn register_external(&self, name: TaskName) {
        self.status.register(name);
    }

    pub fn spawn<F>(&mut self, name: TaskName, future: F)
    where
        F: Future<Output = TaskResult> + Send + 'static,
    {
        self.status.register(name);
        let abort = self.tasks.spawn(async move { (name, future.await) });
        self.names_by_id.insert(abort.id(), name);
        self.aborts.insert(name, abort);
    }

    #[must_use]
    pub fn status(&self) -> &TaskStatus {
        &self.status
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.aborts.is_empty()
    }

    #[must_use]
    pub fn is_running(&self, name: TaskName) -> bool {
        self.aborts.contains_key(&name)
    }

    pub async fn observe_next(&mut self) -> Option<TaskEvent> {
        let joined = self.tasks.join_next_with_id().await?;
        Some(match joined {
            Ok((id, (name, result))) => {
                self.names_by_id.remove(&id);
                self.aborts.remove(&name);
                let failure = self.status.record_exit(name, result);
                TaskEvent {
                    name,
                    class: name.class(),
                    failure,
                }
            }
            Err(error) => {
                let id = error.id();
                let name = self
                    .names_by_id
                    .remove(&id)
                    .unwrap_or(TaskName::Orchestrator);
                self.aborts.remove(&name);
                let failure = self.status.record_join_failure(name, error.to_string());
                TaskEvent {
                    name,
                    class: name.class(),
                    failure: Some(failure),
                }
            }
        })
    }

    /// Abort named owners at the application deadline, then observe their join completions.
    /// Every abort is followed by a join; no owner is left detached.
    pub async fn abort_and_join(&mut self, names: &[TaskName]) {
        for name in names {
            if let Some(abort) = self.aborts.get(name) {
                self.status
                    .record_failure(*name, TaskFailure::shutdown_timeout());
                abort.abort();
            }
        }
        // Tokio abort is observed only at a yield point: a task pinned in
        // synchronous work never completes, so the post-abort join must carry
        // its own hard bound or shutdown is unbounded (#544 review).
        let bound = tokio::time::Instant::now() + POST_ABORT_JOIN_BOUND;
        while names.iter().any(|name| self.is_running(*name)) {
            match tokio::time::timeout_at(bound, self.observe_next()).await {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => {
                    for name in names {
                        if self.is_running(*name) {
                            tracing::error!(task = %name, "post-abort join bound expired; task is pinned in non-yielding work");
                        }
                    }
                    break;
                }
            }
        }
    }

    /// Join every remaining owner, hard-bounded: a non-yielding task cannot
    /// hold the process open past the deadline (#544 review). Returns `false`
    /// when the bound expired with owners still unjoined — the caller exits the
    /// process rather than waiting forever (durable state recovers on restart).
    pub async fn join_all_bounded(&mut self) -> bool {
        let bound = tokio::time::Instant::now() + POST_ABORT_JOIN_BOUND;
        loop {
            match tokio::time::timeout_at(bound, self.observe_next()).await {
                Ok(Some(_)) => {}
                Ok(None) => return true,
                Err(_) => return false,
            }
        }
    }
}

/// Wrap a loop that has no native shutdown branch. Dropping it at its declared phase is safe only
/// for owners whose durable publication is atomic and whose unpublished in-progress work may be
/// retried from the last-good snapshot.
pub async fn cancel_at<F>(future: F, shutdown: ShutdownReceiver, phase: ShutdownPhase) -> TaskResult
where
    F: Future<Output = ()>,
{
    tokio::pin!(future);
    tokio::select! {
        biased;
        () = shutdown.wait_for(phase) => Ok(TaskExit::CleanShutdown),
        () = &mut future => Ok(TaskExit::Completed),
    }
}

/// Cancellation wrapper for an owner that already returns a typed error.
pub async fn cancel_result_at<F, E>(
    future: F,
    shutdown: ShutdownReceiver,
    phase: ShutdownPhase,
) -> TaskResult
where
    F: Future<Output = Result<(), E>>,
    E: fmt::Display,
{
    tokio::pin!(future);
    tokio::select! {
        biased;
        () = shutdown.wait_for(phase) => Ok(TaskExit::CleanShutdown),
        result = &mut future => result
            .map(|()| TaskExit::Completed)
            .map_err(TaskFailure::typed),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn inventory_has_one_class_and_stop_phase_per_name() {
        let unique = TaskName::ALL
            .into_iter()
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(unique.len(), TaskName::ALL.len());
        for name in TaskName::ALL {
            assert!(name.stop_phase() > ShutdownPhase::Running);
        }
    }

    #[tokio::test]
    async fn critical_failure_is_sticky_and_best_effort_only_degrades() {
        let status = TaskStatus::new();
        let mut supervisor = TaskSupervisor::new(status.clone());
        supervisor.spawn(TaskName::Orchestrator, async {
            Err(TaskFailure::typed("typed orchestrator failure"))
        });
        let event = supervisor.observe_next().await.unwrap();
        assert!(event.initiates_shutdown());
        assert!(status.critical_failed());
        status.record_failure(
            TaskName::Orchestrator,
            TaskFailure::typed("later failure must not replace first"),
        );
        assert_eq!(
            status.snapshot()[0].failure.as_ref().unwrap().message,
            "typed orchestrator failure"
        );

        let mut supervisor = TaskSupervisor::new(TaskStatus::new());
        supervisor.spawn(TaskName::SupabaseAnalyticsSink, async {
            Ok(TaskExit::Completed)
        });
        let event = supervisor.observe_next().await.unwrap();
        assert!(!event.initiates_shutdown());
        assert_eq!(event.failure.unwrap().kind, TaskFailureKind::EarlyReturn);
        assert!(!supervisor.status().critical_failed());
    }

    #[tokio::test]
    async fn expected_shutdown_channel_closure_is_clean() {
        let status = TaskStatus::new();
        status.advance_phase(ShutdownPhase::StopSinks);
        let mut supervisor = TaskSupervisor::new(status.clone());
        supervisor.spawn(TaskName::LiquiditySnapshotWorker, async {
            Ok(TaskExit::ChannelClosed("snapshot_requests"))
        });
        let event = supervisor.observe_next().await.unwrap();
        assert!(event.failure.is_none());
        assert_eq!(status.snapshot()[0].state, TaskRunState::Stopped);
    }
}
