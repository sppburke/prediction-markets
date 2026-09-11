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

/// PASS: producer and orchestrator drain acknowledgments through the real source owner, whose
/// status remains Running until StopSinks; every supervised exit is expected at its own phase.
#[tokio::test(start_paused = true)]
async fn source_acknowledgments_survive_supervised_producer_and_orchestrator_drain() {
    use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp};
    use pe_event_log::{ContentType, EnvelopeIn, Reader};
    use pe_service::activity_ingest::{ActivityIngest, SourceLogHandle};
    use pe_service::source_event_sink::SourceEventSink;
    use pe_service::supervisor::{ShutdownController, TaskRunState, TaskStatus};
    use tokio::sync::{mpsc, oneshot};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.log");
    let status = TaskStatus::new();
    let mut supervisor = TaskSupervisor::new(status.clone());
    let (shutdown, _) = ShutdownController::new();
    let (source, source_rx) = SourceLogHandle::channel(1);
    let (trigger_tx, trigger_rx) = mpsc::channel(1);
    let ingest = ActivityIngest::poll_only(
        SourceEventSink::open(&path).unwrap(),
        source_rx,
        trigger_tx,
        new_shared_health(false),
    );
    let stopping = shutdown.subscribe();
    supervisor.spawn(TaskName::ActivityIngest, async move {
        ingest
            .run_until(stopping.wait_for(TaskName::ActivityIngest.stop_phase()))
            .await
            .map(|()| TaskExit::CleanShutdown)
            .map_err(TaskFailure::typed)
    });
    let frame = |payload| EnvelopeIn {
        source_id: SourceId("scenario.supervised-drain".to_owned()),
        schema_version: 1,
        parser_version: 1,
        observed_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
        received_at: ReceivedAt(OffsetDateTime::UNIX_EPOCH),
        content_type: ContentType::Json,
        payload,
    };
    let (command, command_rx) = oneshot::channel::<oneshot::Sender<()>>();
    let producer_source = source.clone();
    let stopping = shutdown.subscribe();
    supervisor.spawn(TaskName::PublicActivityPoll, async move {
        stopping
            .wait_for(TaskName::PublicActivityPoll.stop_phase())
            .await;
        producer_source.append(frame(b"1".to_vec())).await.unwrap();
        let (ack, acknowledged) = oneshot::channel();
        command.send(ack).unwrap();
        acknowledged.await.unwrap();
        drop(trigger_rx);
        Ok(TaskExit::CleanShutdown)
    });
    let (ack_release, released) = oneshot::channel();
    let (drain_release, drained) = oneshot::channel();
    let (progress, mut progressed) = mpsc::channel(2);
    let stopping = shutdown.subscribe();
    let owner_source = source.clone();
    supervisor.spawn(TaskName::Orchestrator, async move {
        let ack = command_rx.await.unwrap();
        owner_source.append(frame(b"2".to_vec())).await.unwrap();
        progress.send(()).await.unwrap();
        released.await.unwrap();
        ack.send(()).unwrap();
        stopping.wait_for(TaskName::Orchestrator.stop_phase()).await;
        owner_source.append(frame(b"3".to_vec())).await.unwrap();
        progress.send(()).await.unwrap();
        drained.await.unwrap();
        Ok(TaskExit::CleanShutdown)
    });
    let state = |name| {
        status
            .snapshot()
            .into_iter()
            .find(|row| row.name == name)
            .unwrap()
            .state
    };
    status.advance_phase(ShutdownPhase::StopProducers);
    shutdown.advance(ShutdownPhase::StopProducers);
    progressed.recv().await.unwrap();
    assert_eq!(state(TaskName::PublicActivityPoll), TaskRunState::Stopping);
    assert_eq!(state(TaskName::Orchestrator), TaskRunState::Running);
    assert_eq!(state(TaskName::ActivityIngest), TaskRunState::Running);
    ack_release.send(()).unwrap();
    let event = supervisor.observe_next().await.unwrap();
    assert_eq!(event.name, TaskName::PublicActivityPoll);
    assert!(event.failure.is_none());

    status.advance_phase(ShutdownPhase::DrainOrchestrator);
    shutdown.advance(ShutdownPhase::DrainOrchestrator);
    progressed.recv().await.unwrap();
    assert_eq!(state(TaskName::Orchestrator), TaskRunState::Stopping);
    assert_eq!(state(TaskName::ActivityIngest), TaskRunState::Running);
    assert_eq!(Reader::replay(&path).unwrap().count(), 3);
    drain_release.send(()).unwrap();
    let event = supervisor.observe_next().await.unwrap();
    assert_eq!(event.name, TaskName::Orchestrator);
    assert!(event.failure.is_none());

    status.advance_phase(ShutdownPhase::StopSinks);
    shutdown.advance(ShutdownPhase::StopSinks);
    let event = supervisor.observe_next().await.unwrap();
    assert_eq!(event.name, TaskName::ActivityIngest);
    assert!(event.failure.is_none());
    assert!(!status.critical_failed());
    assert!(
        status
            .snapshot()
            .iter()
            .all(|row| row.state == TaskRunState::Stopped)
    );
    drop(source);
}

/// PASS: ingest exit while orchestrator is draining is still an early failure.
#[tokio::test]
async fn ingest_exit_before_sink_phase_is_unexpected() {
    let status = pe_service::supervisor::TaskStatus::new();
    let mut supervisor = TaskSupervisor::new(status.clone());
    supervisor.spawn(TaskName::ActivityIngest, async { Ok(TaskExit::Completed) });
    status.advance_phase(ShutdownPhase::DrainOrchestrator);
    assert_eq!(
        supervisor
            .observe_next()
            .await
            .unwrap()
            .failure
            .unwrap()
            .kind,
        TaskFailureKind::EarlyReturn
    );
}
