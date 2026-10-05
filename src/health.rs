//! `/healthz` — same semantics as k8s-sidecar-rs: readiness once every watch
//! stream completed an initial sync; liveness while each stream has had API
//! contact within 2× its heartbeat and no stream task died.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[derive(Default)]
struct Inner {
    contact: HashMap<String, (Instant, Duration)>,
    alive: HashMap<String, bool>,
}

pub struct HealthState {
    ready: AtomicBool,
    inner: Mutex<Inner>,
}

impl HealthState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            ready: AtomicBool::new(false),
            inner: Mutex::new(Inner::default()),
        })
    }

    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::SeqCst);
    }

    pub fn register_stream(&self, stream: &str, heartbeat: Duration) {
        let mut i = self.inner.lock().unwrap();
        i.alive.insert(stream.to_string(), true);
        i.contact
            .insert(stream.to_string(), (Instant::now(), 2 * heartbeat));
    }

    pub fn contact(&self, stream: &str) {
        if let Ok(mut i) = self.inner.lock()
            && let Some(entry) = i.contact.get_mut(stream)
        {
            entry.0 = Instant::now();
        }
    }

    pub fn stream_dead(&self, stream: &str) {
        if let Ok(mut i) = self.inner.lock() {
            i.alive.insert(stream.to_string(), false);
        }
    }

    /// Revive on a real event only — a restart alone must not mark live.
    pub fn stream_alive(&self, stream: &str) {
        if let Ok(mut i) = self.inner.lock() {
            i.alive.insert(stream.to_string(), true);
        }
    }

    pub fn probe(&self) -> (u16, &'static str) {
        if !self.ready.load(Ordering::SeqCst) {
            return (503, "NOT READY");
        }
        let inner = self.inner.lock().unwrap();
        if inner.alive.values().any(|a| !*a) {
            return (503, "NOT LIVE (watcher task died)");
        }
        let now = Instant::now();
        if inner
            .contact
            .values()
            .any(|(t, th)| now.duration_since(*t) > *th)
        {
            return (503, "NOT LIVE (K8s contact lost)");
        }
        (200, "OK")
    }
}

/// Serves `/healthz`. On bind failure the task parks until cancel rather
/// than exiting — main treats a completed worker as fatal, and upstream
/// (daemon health thread) never let a port conflict kill the process.
pub async fn serve(state: Arc<HealthState>, port: u16, cancel: CancellationToken) {
    let listener =
        match TcpListener::bind(("::", port)).or_else(|_| TcpListener::bind(("0.0.0.0", port))) {
            Ok(l) => l,
            Err(_) => {
                error!(port, "health server failed to bind");
                cancel.cancelled().await;
                return;
            }
        };
    info!(port, "health server listening");

    if let Err(e) = listener.set_nonblocking(true) {
        error!(port, error = %e, "health server failed to configure listener");
        cancel.cancelled().await;
        return;
    }
    let (tx, mut rx) = tokio::sync::mpsc::channel::<std::net::TcpStream>(16);
    let accept_cancel = cancel.clone();
    let accept_thread = std::thread::spawn(move || {
        loop {
            match listener.accept() {
                Ok((s, _)) => {
                    let _ = s.set_nonblocking(false);
                    if tx.blocking_send(s).is_err() {
                        return;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if accept_cancel.is_cancelled() {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(_) => return,
            }
        }
    });

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            Some(stream) = rx.recv() => {
                let st = state.clone();
                tokio::spawn(async move {
                    tokio::task::spawn_blocking(move || handle(stream, st)).await.ok();
                });
            }
        }
    }
    let _ = tokio::task::spawn_blocking(move || accept_thread.join()).await;
}

const MAX_REQUEST_BYTES: u64 = 8 * 1024;

fn handle(stream: std::net::TcpStream, state: Arc<HealthState>) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut reader = BufReader::new((&stream).take(MAX_REQUEST_BYTES));
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() || request_line.is_empty() {
        return;
    }
    let mut parts = request_line.split_whitespace();
    let path = if parts.next() == Some("GET") {
        parts.next().unwrap_or("")
    } else {
        ""
    };
    for line in reader.lines() {
        match line {
            Ok(l) if l.is_empty() => break,
            Ok(_) => continue,
            Err(_) => return,
        }
    }

    let (status, body) = if path == "/healthz" {
        state.probe()
    } else {
        (404, "Not Found")
    };
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        _ => "Service Unavailable",
    };
    let _ = write!(
        &mut &stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
}
