# Annotations

k8s-reflecto-rs honors the full
`reflector.v1.k8s.emberstack.com` annotation surface, using the same keys
and value formats as upstream — existing reflected objects are adopted
seamlessly.

## Reflection permissions

| Annotation | Effect |
|---|---|
| `reflection-allowed` | Allow this resource to be reflected at all |
| `reflection-allowed-namespaces` | Comma-separated list of permitted destination namespaces |
| `reflection-allowed-namespaces-selector` | Label selector for permitted destination namespaces |

## Automatic reflection

| Annotation | Effect |
|---|---|
| `reflection-auto-enabled` | Automatically reflect into all matching namespaces |
| `reflection-auto-namespaces` | Comma-separated namespace patterns for auto-reflection |
| `reflection-auto-namespaces-selector` | Label selector for auto-reflection targets |

## Bookkeeping

Written by the controller on sources and copies; same keys and value
formats as upstream.

| Annotation | Effect |
|---|---|
| `reflects` | Source `namespace/name` on a reflected copy |
| `auto-reflects` | Auto-reflection bookkeeping on the source |
| `reflected-at` | Timestamp of last reflection |
| `reflected-version` | Source `resourceVersion` at last reflection |
