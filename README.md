# k8s-reflecto-rs

A Rust re-implementation of
[EmberStack/kubernetes-reflector](https://github.com/emberstack/kubernetes-reflector):
watches Secrets and ConfigMaps and mirrors them across namespaces based on
annotations — at a fraction of the .NET original's memory footprint
(~285 MiB resident → low single-digit MiB target).

## Basic usage

Annotate a source Secret or ConfigMap to allow and automate reflection:

```yaml
metadata:
  annotations:
    reflector.v1.k8s.emberstack.com/reflection-allowed: "true"
    reflector.v1.k8s.emberstack.com/reflection-auto-enabled: "true"
    reflector.v1.k8s.emberstack.com/reflection-auto-namespaces: "app-.*"
```

The resource now appears — and stays in sync — in every matching
namespace.

## Documentation

Full docs — annotation reference, configuration, RBAC, and development
notes — live at <https://damacus.github.io/k8s-reflecto-rs/> (source in
`docs/`).
