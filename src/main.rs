use k8s_openapi::api::core::v1::{ConfigMap, Namespace as K8sNamespace, Secret};
use kube::Client;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;

use kubernetes_reflector_rs::config::Config;
use kubernetes_reflector_rs::health::{self, HealthState};
use kubernetes_reflector_rs::kobj::{ConfigMapObj, SecretObj};
use kubernetes_reflector_rs::mirror::{self, Mirror};
use kubernetes_reflector_rs::selector::parse_glob_patterns;
use kubernetes_reflector_rs::store::KubeStore;
use kubernetes_reflector_rs::watch::{self, Dispatch, WatchCtx, WatcherKind};

const EVENT_QUEUE: usize = 1024;
const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

#[tokio::main]
async fn main() {
    let code = run().await;
    if code != 0 {
        std::process::exit(code);
    }
}

async fn run() -> i32 {
    let cfg = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("config error: {e}");
            return 2;
        }
    };
    init_logging(&cfg.log_level);
    info!("kubernetes-reflector-rs starting");

    let client = match Client::try_default().await {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to build kubernetes client");
            return 2;
        }
    };

    let health = HealthState::new();
    let cancel = CancellationToken::new();
    let (tx, mut rx) = mpsc::channel::<Dispatch>(EVENT_QUEUE);
    let excluded = parse_glob_patterns(&cfg.excluded_namespaces);

    let heartbeat = std::time::Duration::from_secs(cfg.watcher_timeout_secs);
    for s in ["secret", "configmap", "namespace"] {
        health.register_stream(s, heartbeat);
    }

    let mut tasks = JoinSet::new();

    // Three watchers, one dispatch channel. Each kind's events map into
    // Dispatch; the consumer feeds both mirrors (each filters by type).
    {
        let (tx, health, cancel, excluded) =
            (tx.clone(), health.clone(), cancel.clone(), excluded.clone());
        tasks.spawn(watch::run::<Secret, _, _>(WatchCtx {
            client: client.clone(),
            tx,
            health,
            cancel,
            stream_id: "secret",
            kind: WatcherKind::Secret,
            timeout_secs: cfg.watcher_timeout_secs,
            excluded,
            map_upsert: |s: &Secret| Dispatch::Secret(SecretObj(s.clone())),
            map_delete: |s: &Secret| Dispatch::SecretDeleted(SecretObj(s.clone())),
            _phantom: std::marker::PhantomData,
        }));
    }
    {
        let (tx, health, cancel, excluded) =
            (tx.clone(), health.clone(), cancel.clone(), excluded.clone());
        tasks.spawn(watch::run::<ConfigMap, _, _>(WatchCtx {
            client: client.clone(),
            tx,
            health,
            cancel,
            stream_id: "configmap",
            kind: WatcherKind::ConfigMap,
            timeout_secs: cfg.watcher_timeout_secs,
            excluded,
            map_upsert: |c: &ConfigMap| Dispatch::ConfigMap(ConfigMapObj(c.clone())),
            map_delete: |c: &ConfigMap| Dispatch::ConfigMapDeleted(ConfigMapObj(c.clone())),
            _phantom: std::marker::PhantomData,
        }));
    }
    {
        let (tx, health, cancel, ns_client) =
            (tx.clone(), health.clone(), cancel.clone(), client.clone());
        tasks.spawn(async move {
            // Namespaces are cluster-scoped — no excluded-namespace filter
            // applies (is_namespace_excluded returns false for None).
            watch::run::<K8sNamespace, _, _>(WatchCtx {
                client: ns_client,
                tx,
                health,
                cancel,
                stream_id: "namespace",
                kind: WatcherKind::Namespace,
                timeout_secs: cfg.watcher_timeout_secs,
                excluded: vec![],
                map_upsert: |n: &K8sNamespace| Dispatch::Namespace(mirror::Namespace::from(n)),
                map_delete: |n: &K8sNamespace| {
                    Dispatch::NamespaceDeleted(n.metadata.name.clone().unwrap_or_default())
                },
                _phantom: std::marker::PhantomData,
            })
            .await
        });
    }
    drop(tx);

    // Health server.
    {
        let (state, port, c) = (health.clone(), cfg.health_port, cancel.clone());
        tasks.spawn(async move { health::serve(state, port, c).await });
    }

    // Dispatch consumer — owns both mirrors. Like upstream's channel-based
    // event pump, this serializes handling so mirror caches stay consistent.
    let dispatch_handle = tokio::spawn({
        let client = client.clone();
        async move {
            let mut secret_mirror =
                Mirror::new(KubeStore::<SecretObj, Secret>::new(client.clone()));
            let mut configmap_mirror =
                Mirror::new(KubeStore::<ConfigMapObj, ConfigMap>::new(client));

            while let Some(d) = rx.recv().await {
                match d {
                    Dispatch::Secret(o) => secret_mirror.handle(mirror::Event::Upsert(o)).await,
                    Dispatch::SecretDeleted(o) => {
                        secret_mirror.handle(mirror::Event::Delete(o)).await
                    }
                    Dispatch::ConfigMap(o) => {
                        configmap_mirror.handle(mirror::Event::Upsert(o)).await
                    }
                    Dispatch::ConfigMapDeleted(o) => {
                        configmap_mirror.handle(mirror::Event::Delete(o)).await
                    }
                    Dispatch::Namespace(ns) => {
                        secret_mirror
                            .handle(mirror::Event::NamespaceUpsert(ns.clone()))
                            .await;
                        configmap_mirror
                            .handle(mirror::Event::NamespaceUpsert(ns))
                            .await;
                    }
                    Dispatch::NamespaceDeleted(name) => {
                        secret_mirror
                            .handle(mirror::Event::NamespaceDelete(name.clone()))
                            .await;
                        configmap_mirror
                            .handle(mirror::Event::NamespaceDelete(name))
                            .await;
                    }
                    Dispatch::Closed(WatcherKind::Secret) => {
                        secret_mirror.watcher_closed(mirror::WatcherKind::Resource)
                    }
                    Dispatch::Closed(WatcherKind::ConfigMap) => {
                        configmap_mirror.watcher_closed(mirror::WatcherKind::Resource)
                    }
                    Dispatch::Closed(WatcherKind::Namespace) => {
                        secret_mirror.watcher_closed(mirror::WatcherKind::Namespace);
                        configmap_mirror.watcher_closed(mirror::WatcherKind::Namespace);
                    }
                }
            }
        }
    });

    // Readiness: initial sync done when the channel first goes quiet isn't
    // reliable — simplest correct signal is "each stream contacted once",
    // which happens on session start.
    {
        let health = health.clone();
        tokio::spawn(async move {
            // Streams register contact on session start; wait a beat then
            // mark ready — mirrors converge on the replay.
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            health.mark_ready();
        });
    }

    tokio::select! {
        _ = wait_for_shutdown() => {
            info!("shutdown signal received, stopping");
        }
        res = tasks.join_next() => {
            error!(result = ?res, "worker task exited unexpectedly; stopping");
        }
        res = dispatch_handle => {
            error!(result = ?res, "dispatch task exited unexpectedly; stopping");
        }
    }
    cancel.cancel();

    let drain = tokio::time::timeout(SHUTDOWN_GRACE, async {
        while tasks.join_next().await.is_some() {}
    });
    if drain.await.is_err() {
        error!("graceful shutdown timed out");
        return 1;
    }
    0
}

async fn wait_for_shutdown() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).ok();
    let mut int = signal(SignalKind::interrupt()).ok();
    tokio::select! {
        _ = async { term.as_mut().unwrap().recv().await }, if term.is_some() => {}
        _ = async { int.as_mut().unwrap().recv().await }, if int.is_some() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

fn init_logging(level: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(format!(
            "kubernetes_reflector_rs={}",
            match level.to_ascii_lowercase().as_str() {
                "verbose" | "trace" => "trace",
                "debug" => "debug",
                "warning" => "warn",
                "error" => "error",
                _ => "info",
            }
        ))
    });
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_span_events(FmtSpan::NONE)
        .init();
}
