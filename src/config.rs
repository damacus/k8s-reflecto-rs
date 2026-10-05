//! Environment config — mirrors upstream's `ES_Reflector__*` env vars.

use std::env;

/// Watch session lifetime — upstream `Watcher.Timeout` default 3600s. The
/// session closes, caches clear, the relist re-validates everything.
pub const DEFAULT_WATCHER_TIMEOUT_SECS: u64 = 3600;

#[derive(Debug)]
pub struct Config {
    /// `ES_Reflector__Watcher__Timeout` — seconds per watch session.
    pub watcher_timeout_secs: u64,
    /// `ES_Reflector__Watcher__ExcludedNamespaces` — comma-separated globs.
    pub excluded_namespaces: String,
    /// `ES_Ignite__KubernetesClient__SkipTlsVerify`.
    pub skip_tls_verify: bool,
    /// `ES_Serilog__MinimumLevel__Default` — log level name.
    pub log_level: String,
    /// Health probe port — `HEALTH_PORT` (upstream has no health server;
    /// this is our addition for parity with k8s-sidecar-rs deploys).
    pub health_port: u16,
}

impl Config {
    // The designated config loader — the one sanctioned `env::var` site.
    #[allow(clippy::disallowed_methods)]
    pub fn from_env() -> Result<Self, String> {
        // The deployed Helm chart may set the var with an empty value —
        // upstream's IOptions binding treats that as the default.
        let timeout_raw = env::var("ES_Reflector__Watcher__Timeout").unwrap_or_default();
        let watcher_timeout_secs: u64 = if timeout_raw.trim().is_empty() {
            DEFAULT_WATCHER_TIMEOUT_SECS
        } else {
            timeout_raw.trim().parse().map_err(|_| {
                format!(
                    "ES_Reflector__Watcher__Timeout must be a positive integer, got '{timeout_raw}'"
                )
            })?
        };
        if watcher_timeout_secs == 0 {
            return Err("ES_Reflector__Watcher__Timeout must be >= 1".into());
        }
        if watcher_timeout_secs > u64::from(u32::MAX) {
            // WatchParams.timeoutSeconds is u32 — reject values that truncate.
            return Err(format!(
                "ES_Reflector__Watcher__Timeout must be <= {}, got {watcher_timeout_secs}",
                u32::MAX
            ));
        }

        let health_raw = env::var("HEALTH_PORT").unwrap_or_else(|_| "8080".into());
        let health_port: u16 = health_raw.trim().parse().map_err(|_| {
            format!("HEALTH_PORT must be an integer in 1..=65535, got '{health_raw}'")
        })?;
        if health_port == 0 {
            return Err(format!(
                "HEALTH_PORT must be in 1..=65535, got {health_port}"
            ));
        }

        Ok(Self {
            watcher_timeout_secs,
            excluded_namespaces: env::var("ES_Reflector__Watcher__ExcludedNamespaces")
                .unwrap_or_default(),
            skip_tls_verify: truthy_env("ES_Ignite__KubernetesClient__SkipTlsVerify"),
            log_level: env::var("ES_Serilog__MinimumLevel__Default")
                .unwrap_or_else(|_| "Information".into()),
            health_port,
        })
    }
}

// Reads one raw env var for the config loader above.
#[allow(clippy::disallowed_methods)]
fn truthy_env(key: &str) -> bool {
    env::var(key).is_ok_and(|v| matches!(v.as_str(), "true" | "True" | "TRUE" | "1"))
}
