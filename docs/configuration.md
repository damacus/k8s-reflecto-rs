# Configuration

Environment variables keep upstream's `ES_*` naming so existing
deployments drop in unchanged.

| Environment variable | Default | Description |
|---|---|---|
| `ES_Reflector__Watcher__Timeout` | `3600` | Watch session lifetime (seconds) |
| `ES_Reflector__Watcher__ExcludedNamespaces` | _none_ | Comma-separated glob patterns; matching namespaces are skipped |
| `ES_Serilog__MinimumLevel__Default` | `Information` | Log level (`Verbose`/`Debug`/`Information`/`Warning`/`Error`) |
| `HEALTH_PORT` | `8080` | Port for `/healthz` liveness+readiness |
