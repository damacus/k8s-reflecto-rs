# Required RBAC

The controller needs cluster-wide read on Secrets, ConfigMaps, and
Namespaces, plus write access where reflections are created.

```yaml
rules:
  - apiGroups: [""]
    resources: ["secrets", "configmaps"]
    verbs: ["get", "list", "watch", "create", "patch", "delete"]
  - apiGroups: [""]
    resources: ["namespaces"]
    verbs: ["get", "list", "watch"]
```

Bind these through a `ClusterRole` + `ClusterRoleBinding` — reflection is
inherently cross-namespace.
