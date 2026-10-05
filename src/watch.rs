//! Watch sessions — port of upstream `WatcherBackgroundService`.
//!
//! A session runs until the apiserver closes the stream
//! (timeoutSeconds), the client hits an absolute deadline (timeout +
//! grace), or a read stalls. On session close the mirrors get
//! `watcher_closed` and the loop relists — upstream relies on the relist
//! replay to re-validate all cached state.

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use kube::api::{Api, WatchEvent, WatchParams};
use kube::{Client, Resource};
use regex::Regex;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::health::HealthState;
use crate::selector::is_namespace_excluded;

/// Grace past the server-side timeout before we abandon the session —
/// upstream adds 3s to `Watcher.Timeout`.
const SESSION_GRACE: Duration = Duration::from_secs(3);
/// Client-side bound on a silent read — same dead-stream guard we added to
/// k8s-sidecar-rs: a half-open socket can't stall past this.
const READ_IDLE_LIMIT: Duration = Duration::from_secs(120);
/// Pause between a faulted session and the reconnect attempt.
const FAULT_BACKOFF: Duration = Duration::from_secs(2);

/// A unit of work dispatched to the mirrors.
#[derive(Debug)]
pub enum Dispatch {
    Secret(crate::kobj::SecretObj),
    ConfigMap(crate::kobj::ConfigMapObj),
    SecretDeleted(crate::kobj::SecretObj),
    ConfigMapDeleted(crate::kobj::ConfigMapObj),
    Namespace(crate::mirror::Namespace),
    NamespaceDeleted(String),
    /// Watcher session ended; mirrors drop the caches for that kind.
    Closed(WatcherKind),
}

#[derive(Debug, Clone, Copy)]
pub enum WatcherKind {
    Secret,
    ConfigMap,
    Namespace,
}

pub struct WatchCtx<K, A, D> {
    pub client: Client,
    pub tx: mpsc::Sender<Dispatch>,
    pub health: Arc<HealthState>,
    pub cancel: CancellationToken,
    pub stream_id: &'static str,
    pub kind: WatcherKind,
    pub timeout_secs: u64,
    pub excluded: Vec<Regex>,
    pub map_upsert: A,
    pub map_delete: D,
    pub _phantom: std::marker::PhantomData<K>,
}

/// Watch a resource kind, forwarding events to `tx` until cancelled.
// Generic future — Send-ness is checked at the `JoinSet::spawn` call site.
#[allow(clippy::future_not_send)]
pub async fn run<K, A, D>(ctx: WatchCtx<K, A, D>)
where
    K: Resource + Clone + Debug + DeserializeOwned + Send + 'static,
    K::DynamicType: Default,
    A: Fn(&K) -> Dispatch + Send + Sync,
    D: Fn(&K) -> Dispatch + Send + Sync,
{
    let api: Api<K> = Api::all(ctx.client.clone());
    loop {
        if ctx.cancel.is_cancelled() {
            return;
        }
        match session(&api, &ctx).await {
            Ok(reason) => info!(stream = ctx.stream_id, %reason, "watch session closed"),
            Err(e) => {
                error!(stream = ctx.stream_id, error = %e, "watch session faulted");
                ctx.health.stream_dead(ctx.stream_id);
                // Back off before relist — a persistent API failure must not
                // hot-loop against the apiserver.
                tokio::select! {
                    () = ctx.cancel.cancelled() => return,
                    () = tokio::time::sleep(FAULT_BACKOFF) => {}
                }
            }
        }
        // Session close → mirrors drop cached state; the relist replays
        // upserts and revalidates everything.
        if ctx.tx.send(Dispatch::Closed(ctx.kind)).await.is_err() {
            return;
        }
        ctx.health.contact(ctx.stream_id);
    }
}

// Generic future — Send-ness is checked at the `JoinSet::spawn` call site.
#[allow(clippy::future_not_send)]
async fn session<K, A, D>(api: &Api<K>, ctx: &WatchCtx<K, A, D>) -> Result<&'static str, String>
where
    K: Resource + Clone + Debug + DeserializeOwned + Send,
    K::DynamicType: Default,
    A: Fn(&K) -> Dispatch + Send + Sync,
    D: Fn(&K) -> Dispatch + Send + Sync,
{
    // No timeoutSeconds: kube validates <295s anyway, and upstream rotates
    // sessions via its own watchdog — our `deadline` below does the same.
    // The apiserver then picks ~30-60min (minRequestTimeout window).
    let wp = WatchParams::default();
    let mut events = api
        .watch(&wp, "0")
        .await
        .map_err(|e| format!("watch request failed: {e}"))?
        .boxed();

    // Contact the moment the stream establishes — upstream's "Requesting
    // V1X resources" line.
    ctx.health.contact(ctx.stream_id);
    ctx.health.stream_alive(ctx.stream_id);
    info!(stream = ctx.stream_id, "requesting resources");

    let deadline =
        tokio::time::Instant::now() + Duration::from_secs(ctx.timeout_secs) + SESSION_GRACE;

    loop {
        let next = tokio::select! {
            () = ctx.cancel.cancelled() => return Ok("cancelled"),
            () = tokio::time::sleep_until(deadline) => return Ok("session timeout reached"),
            item = tokio::time::timeout(READ_IDLE_LIMIT, events.next()) => match item {
                Ok(i) => i,
                Err(_) => return Ok("watch read idle; reconnecting"),
            },
        };

        match next {
            Some(Ok(WatchEvent::Added(o) | WatchEvent::Modified(o))) => {
                ctx.health.contact(ctx.stream_id);
                if is_namespace_excluded(o.meta().namespace.as_deref(), &ctx.excluded) {
                    continue;
                }
                if ctx.tx.send((ctx.map_upsert)(&o)).await.is_err() {
                    return Err("dispatch channel closed".into());
                }
            }
            Some(Ok(WatchEvent::Deleted(o))) => {
                ctx.health.contact(ctx.stream_id);
                if is_namespace_excluded(o.meta().namespace.as_deref(), &ctx.excluded) {
                    continue;
                }
                if ctx.tx.send((ctx.map_delete)(&o)).await.is_err() {
                    return Err("dispatch channel closed".into());
                }
            }
            Some(Ok(WatchEvent::Bookmark(_))) => {
                ctx.health.contact(ctx.stream_id);
            }
            Some(Ok(WatchEvent::Error(e))) => {
                return Err(format!("apiserver error event: {}", e.message));
            }
            Some(Err(e)) => return Err(format!("watch stream failed: {e}")),
            None => return Ok("stream ended"),
        }
    }
}
