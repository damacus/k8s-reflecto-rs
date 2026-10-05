# k8s-reflecto-rs

<!-- markdownlint-disable MD046 -->

A Rust re-implementation of
[EmberStack/kubernetes-reflector](https://github.com/emberstack/kubernetes-reflector):
watches Secrets and ConfigMaps and mirrors them across namespaces based on
annotations — at a fraction of the .NET original's memory footprint
(~285 MiB resident → low single-digit MiB target).

## Why

Same job, a fraction of the footprint. If you run reflector for cert or
registry-credential fan-out, this drops in as a smaller, faster replacement
using the same annotation contract — existing reflected objects are adopted
seamlessly.

## What it does

- Watches Secrets, ConfigMaps, and Namespaces cluster-wide.
- Honors the full `reflector.v1.k8s.emberstack.com` annotation surface (see
  [Annotations](annotations.md)).
- Creates, patches, and deletes reflections as sources, permissions, and
  namespace labels change.
- Rotates watch sessions on the apiserver's timeout and re-validates all
  state on relist, matching upstream's reconciliation model.

## Basic usage

Annotate a source Secret or ConfigMap to allow and automate reflection:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: registry-credentials
  namespace: cert-sources
  annotations:
    reflector.v1.k8s.emberstack.com/reflection-allowed: "true"
    reflector.v1.k8s.emberstack.com/reflection-auto-enabled: "true"
    reflector.v1.k8s.emberstack.com/reflection-auto-namespaces: "app-.*"
```

That Secret now appears — and stays in sync — in every namespace matching
`app-.*`. The controller stamps `reflects` / `reflected-at` /
`reflected-version` bookkeeping annotations on both source and copies,
exactly as upstream does.

Deployment needs the [RBAC](rbac.md) rules; runtime tuning lives under
[Configuration](configuration.md).
