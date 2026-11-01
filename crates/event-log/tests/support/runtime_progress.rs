#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// Hold an I/O owner externally while four callers contend, then probe independent runtime work.
pub(crate) fn assert_progress(
    hold: impl FnOnce(SyncSender<()>, Receiver<()>) + Send + 'static,
    call: impl Fn() + Send + Sync + 'static,
) {
    let bound = Duration::from_secs(2);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let listener = runtime
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let address = listener.local_addr().unwrap();
    let released = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let (holder_done_tx, holder_done_rx) = mpsc::sync_channel(1);
    let holder = std::thread::spawn(move || {
        hold(entered_tx, release_rx);
        let _ = holder_done_tx.send(());
    });
    let (finish_tx, finish_rx) = mpsc::sync_channel(1);
    let watchdog_released = Arc::clone(&released);
    let watchdog = std::thread::spawn(move || {
        let _ = finish_rx.recv_timeout(Duration::from_secs(6));
        watchdog_released.store(true, Ordering::Release);
        let _ = release_tx.send(());
    });
    let entered = entered_rx.recv_timeout(bound).is_ok();
    let call = Arc::new(call);
    let (started_tx, started_rx) = mpsc::sync_channel(4);
    let callers = (0..4)
        .map(|_| {
            let started_tx = started_tx.clone();
            let call = Arc::clone(&call);
            runtime.spawn(async move {
                let _ = started_tx.send(());
                call();
            })
        })
        .collect::<Vec<_>>();
    drop(started_tx);
    let deadline = Instant::now() + bound;
    let all_started = (0..4).all(|_| {
        started_rx
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .is_ok()
    });

    let echo = runtime.spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut byte = [0];
        stream.read_exact(&mut byte).await.unwrap();
        stream.write_all(&byte).await.unwrap();
    });
    let (probe_tx, probe_rx) = mpsc::sync_channel(2);
    let tcp_tx = probe_tx.clone();
    let tcp_released = Arc::clone(&released);
    let tcp = runtime.spawn(async move {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(&[42]).await.unwrap();
        let mut byte = [0];
        stream.read_exact(&mut byte).await.unwrap();
        let _ = tcp_tx.send(("tcp", byte == [42] && !tcp_released.load(Ordering::Acquire)));
    });
    let timer = runtime.spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = probe_tx.send(("timer", !released.load(Ordering::Acquire)));
    });
    let deadline = Instant::now() + bound;
    let probes = (0..2)
        .map(|_| probe_rx.recv_timeout(deadline.saturating_duration_since(Instant::now())))
        .collect::<Vec<_>>();
    // This signal also releases on probe errors; the external deadline handles stalled callers.
    let _ = finish_tx.send(());
    let holder_done = holder_done_rx.recv_timeout(bound).is_ok();
    let holder_joined = holder_done && holder.join().is_ok();
    let watchdog_joined = watchdog.join().is_ok();
    let joined = runtime.block_on(async {
        tokio::time::timeout(bound, async {
            let mut success = true;
            for task in callers {
                success &= task.await.is_ok();
            }
            success &= echo.await.is_ok();
            success &= tcp.await.is_ok();
            success &= timer.await.is_ok();
            success
        })
        .await
    });
    runtime.shutdown_timeout(bound);

    assert!(entered, "holder did not enter");
    assert!(all_started, "four callers must start before probes");
    assert!(
        holder_joined && watchdog_joined,
        "external threads did not join"
    );
    assert!(
        matches!(joined, Ok(true)),
        "runtime tasks did not join: {joined:?}"
    );
    assert!(
        probes.iter().all(|probe| matches!(probe, Ok((_, true)))),
        "network and timer must finish within 2 s before release: {probes:?}"
    );
}
