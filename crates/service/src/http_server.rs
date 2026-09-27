use std::future::Future;
use std::io;
use std::time::Duration;

use axum::Router;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tracing::{error, trace};

pub async fn serve(
    listener: TcpListener,
    app: Router,
    shutdown: impl Future<Output = ()>,
    #[cfg(test)] mut connection_started: Option<tokio::sync::oneshot::Sender<()>>,
    #[cfg(test)] accept_stopped: Option<tokio::sync::oneshot::Sender<()>>,
) {
    let graceful = GracefulShutdown::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            () = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    // Register before spawning so shutdown sees even a task not yet polled.
                    let watcher = graceful.watcher();
                    let app = app.clone();
                    #[cfg(test)]
                    let started = connection_started.take();
                    tokio::spawn(async move {
                        let connection = http1::Builder::new()
                            .timer(TokioTimer::new())
                            .serve_connection(TokioIo::new(stream), TowerToHyperService::new(app));
                        let watched = watcher.watch(connection);
                        #[cfg(test)]
                        let result = if let Some(started) = started {
                            let mut watched = Box::pin(watched);
                            let first_poll = std::future::poll_fn(|cx| {
                                std::task::Poll::Ready(watched.as_mut().poll(cx))
                            })
                            .await;
                            let _ = started.send(());
                            match first_poll {
                                std::task::Poll::Ready(result) => result,
                                std::task::Poll::Pending => watched.await,
                            }
                        } else {
                            watched.await
                        };
                        #[cfg(not(test))]
                        let result = watched.await;
                        if let Err(error) = result {
                            trace!(%error, "HTTP connection ended with error");
                        }
                    });
                }
                Err(error) if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionRefused
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset
                ) => {}
                Err(error) => {
                    error!(%error, "HTTP accept error");
                    tokio::select! {
                        () = &mut shutdown => break,
                        () = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }
        }
    }
    drop(listener);
    #[cfg(test)]
    if let Some(accept_stopped) = accept_stopped {
        let _ = accept_stopped.send(());
    }
    graceful.shutdown().await;
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::routing::get;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::{Barrier, oneshot};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn silent_peer_closes_after_header_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop_tx, stop_rx) = oneshot::channel();
        let (started_tx, started_rx) = oneshot::channel();
        let server = tokio::spawn(serve(
            listener,
            Router::new().route("/health/live", get(pe_service::health::live)),
            async {
                let _ = stop_rx.await;
            },
            Some(started_tx),
            None,
        ));
        let mut peer = TcpStream::connect(address).await.unwrap();
        started_rx.await.unwrap();
        tokio::time::advance(Duration::from_secs(29)).await;
        let mut byte = [0];
        assert_eq!(
            peer.try_read(&mut byte).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
        let _ = stop_tx.send(());
        server.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn live_endpoint_returns_ok() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop_tx, stop_rx) = oneshot::channel();
        let server = tokio::spawn(serve(
            listener,
            Router::new().route("/health/live", get(pe_service::health::live)),
            async {
                let _ = stop_rx.await;
            },
            None,
            None,
        ));
        let mut peer = TcpStream::connect(address).await.unwrap();
        peer.write_all(
            b"GET /health/live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
        let mut response = Vec::new();
        peer.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
        assert!(response.ends_with("\r\n\r\nok"), "{response}");
        let _ = stop_tx.send(());
        server.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_drains_in_flight_request_and_stops_accepting() {
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let probes = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/held",
                get({
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    move || {
                        let entered = Arc::clone(&entered);
                        let release = Arc::clone(&release);
                        async move {
                            entered.wait().await;
                            release.wait().await;
                            "done"
                        }
                    }
                }),
            )
            .route(
                "/probe",
                get({
                    let probes = Arc::clone(&probes);
                    move || {
                        let probes = Arc::clone(&probes);
                        async move {
                            probes.fetch_add(1, Ordering::SeqCst);
                            "accepted"
                        }
                    }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop_tx, stop_rx) = oneshot::channel();
        let (accept_stopped_tx, accept_stopped_rx) = oneshot::channel();
        let server = tokio::spawn(serve(
            listener,
            app,
            async {
                let _ = stop_rx.await;
            },
            None,
            Some(accept_stopped_tx),
        ));
        let mut peer = TcpStream::connect(address).await.unwrap();
        peer.write_all(b"GET /held HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        entered.wait().await;
        let _ = stop_tx.send(());
        accept_stopped_rx.await.unwrap();
        assert!(!server.is_finished());
        if let Ok(mut later) = TcpStream::connect(address).await {
            let _ = later
                .write_all(b"GET /probe HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await;
            tokio::time::advance(Duration::from_secs(1)).await;
            assert_eq!(probes.load(Ordering::SeqCst), 0);
        }
        release.wait().await;
        let mut response = Vec::new();
        peer.read_to_end(&mut response).await.unwrap();
        assert!(
            String::from_utf8(response)
                .unwrap()
                .ends_with("\r\n\r\ndone")
        );
        server.await.unwrap();
        assert_eq!(probes.load(Ordering::SeqCst), 0);
    }
}
