//! Named-owner failure and bounded-shutdown scenarios (#544).

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used)]

use pe_service::health::{new_shared_health, readiness_issues};
use pe_service::supervisor::{
    ShutdownPhase, TaskClass, TaskExit, TaskFailure, TaskFailureKind, TaskName, TaskSupervisor,
};
use time::OffsetDateTime;
use tokio::time::Instant;

async fn observe(
    name: TaskName,
    result: pe_service::supervisor::TaskResult,
) -> (TaskClass, TaskFailureKind, bool) {
    let health = new_shared_health(false);
    let status = {
        health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .task_status
            .clone()
    };
    let mut supervisor = TaskSupervisor::new(status);
    supervisor.spawn(name, async move { result });
    let event = supervisor.observe_next().await.unwrap();
    let issues = {
        let mut current = health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        current.polymarket_last_event_at = Some(OffsetDateTime::now_utc());
        readiness_issues(&current, OffsetDateTime::now_utc(), Instant::now())
    };
    (
        event.class,
        event.failure.unwrap().kind,
        issues.contains(&"critical_task_failed"),
    )
}

#[tokio::test]
async fn every_named_owner_classifies_typed_early_and_channel_exit() {
    for name in TaskName::ALL {
        for (result, expected_kind) in [
            (
                Err(TaskFailure::typed("injected typed owner error")),
                TaskFailureKind::TypedError,
            ),
            (Ok(TaskExit::Completed), TaskFailureKind::EarlyReturn),
            (
                Ok(TaskExit::ChannelClosed("injected_dependency")),
                TaskFailureKind::ChannelClosed,
            ),
        ] {
            let (class, kind, readiness_failed) = observe(name, result).await;
            assert_eq!(kind, expected_kind, "{name}");
            assert_eq!(
                readiness_failed,
                class == TaskClass::Critical,
                "{name} readiness consequence"
            );
        }
    }
}

#[tokio::test]
async fn shutdown_exit_is_clean_and_pending_owner_is_aborted_then_joined() {
    let status = pe_service::supervisor::TaskStatus::new();
    status.advance_phase(ShutdownPhase::StopSinks);
    let mut supervisor = TaskSupervisor::new(status.clone());
    supervisor.spawn(TaskName::SupabaseAnalyticsSink, async {
        Ok(TaskExit::ChannelClosed("events"))
    });
    assert!(supervisor.observe_next().await.unwrap().failure.is_none());

    supervisor.spawn(TaskName::LiquiditySnapshotWorker, async {
        std::future::pending().await
    });
    supervisor
        .abort_and_join(&[TaskName::LiquiditySnapshotWorker])
        .await;
    assert!(!supervisor.is_running(TaskName::LiquiditySnapshotWorker));
    let snapshot = status.snapshot();
    let worker = snapshot
        .iter()
        .find(|row| row.name == TaskName::LiquiditySnapshotWorker)
        .unwrap();
    assert_eq!(
        worker.failure.as_ref().unwrap().kind,
        TaskFailureKind::ShutdownTimeout
    );
}
