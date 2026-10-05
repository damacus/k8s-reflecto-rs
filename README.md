# kubernetes-reflector-rs

A Rust re-implementation of
[EmberStack/kubernetes-reflector](https://github.com/emberstack/kubernetes-reflector):
watches Secrets and ConfigMaps and mirrors them across namespaces based on
annotations — at a fraction of the .NET original's memory footprint
(~285 MiB resident → low single-digit MiB target).

## What it does

- Watches Secrets, ConfigMaps, and Namespaces cluster-wide.
- Honors the full `reflector.v1.k8s.emberstack.com` annotation surface:
  - `reflection-allowed` + `reflection-allowed-namespaces` /
    `-namespaces-selector` — direct reflection permissions
  - `reflection-auto-enabled` + `reflection-auto-namespaces` /
    `-namespaces-selector` — automatic reflection to matching namespaces
  - `reflects`, `auto-reflects`, `reflected-at`, `reflected-version` —
    reflection bookkeeping (same keys and value formats as upstream, so
    existing reflected objects are adopted seamlessly)
- Creates, patches, and deletes reflections as sources, permissions, and
  namespace labels change.
- Rotates watch sessions on the apiserver's timeout and re-validates all
  state on relist, matching upstream's reconciliation model.

## Configuration

| Environment variable | Default | Description |
|---|---|---|
| `ES_Reflector__Watcher__Timeout` | `3600` | Watch session lifetime (seconds) |
| `ES_Reflector__Watcher__ExcludedNamespaces` | _none_ | Comma-separated glob patterns; matching namespaces are skipped |
| `ES_Serilog__MinimumLevel__Default` | `Information` | Log level (`Verbose`/`Debug`/`Information`/`Warning`/`Error`) |
| `HEALTH_PORT` | `8080` | Port for `/healthz` liveness+readiness |

## Required RBAC

```yaml
rules:
  - apiGroups: [""]
    resources: ["secrets", "configmaps"]
    verbs: ["get", "list", "watch", "create", "patch", "delete"]
  - apiGroups: [""]
    resources: ["namespaces"]
    verbs: ["get", "list", "watch"]
```

## Development

```fish
cargo fmt --all
cargo test
cargo clippy --all-targets -- -D warnings
```
