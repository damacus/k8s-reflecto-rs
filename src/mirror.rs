//! Mirror engine — port of upstream `ResourceMirror<T>`: the state machine
//! that tracks sources, direct reflections and auto-reflections and keeps
//! the cluster converged on the annotation-declared desired state.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::json;
use tracing::{debug, info, warn};

use crate::props::{MirroringProperties, NsName, annotations, properties_from};

/// A resource the mirror can reflect — port of upstream's `OnResourceX`
/// hooks. `T` is the raw API payload (Secret or ConfigMap-shaped).
pub trait Mirrorable: Clone + Send + Sync + 'static {
    /// Identity: name/namespace/resourceVersion/annotations.
    fn nsname(&self) -> NsName;
    fn resource_version(&self) -> &str;
    fn annotations(&self) -> Option<&BTreeMap<String, String>>;
    fn properties(&self) -> MirroringProperties {
        properties_from(self.annotations(), self.resource_version())
    }
    /// Shallow data-carrying clone for creating a new reflection —
    /// upstream `OnResourceClone` (Secret: type+data; ConfigMap: data+
    /// binaryData).
    fn clone_for_reflection(&self) -> Self;
    /// JSON-patch ops covering the data fields — upstream
    /// `OnResourceConfigurePatch` (Secret replaces `/data`; ConfigMap
    /// replaces `/data` + `/binaryData`). Called on the *source* object.
    fn data_patch_ops(&self) -> Vec<serde_json::Value>;
    /// Set name/namespace/annotations on a freshly cloned reflection.
    fn set_name_ns_annotations(
        &mut self,
        name: &str,
        namespace: &str,
        annotations: BTreeMap<String, String>,
    );
}

/// Error that distinguishes NotFound so `try_get` can cache it.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("not found")]
    NotFound,
    #[error("conflict")]
    Conflict,
    #[error("{0}")]
    Other(String),
}

/// Cluster operations the engine needs — one impl per resource kind, backed
/// by `kube::Api` in production and by a fake store in tests.
#[allow(async_fn_in_trait)]
pub trait ResourceStore<T: Mirrorable> {
    async fn get(&self, id: &NsName) -> Result<T, ApiError>;
    async fn list_by_name(&self, name: &str) -> Result<Vec<T>, ApiError>;
    async fn list_namespaces(&self) -> Result<Vec<Namespace>, ApiError>;
    async fn create(&self, obj: &T, ns: &str) -> Result<T, ApiError>;
    async fn patch(&self, id: &NsName, patch: serde_json::Value) -> Result<(), ApiError>;
    async fn delete(&self, id: &NsName) -> Result<(), ApiError>;
}

/// Minimal namespace view — only labels matter for selector evaluation.
#[derive(Clone, Debug)]
pub struct Namespace {
    pub name: String,
    pub labels: BTreeMap<String, String>,
}

/// Watch event fed to the mirror.
#[derive(Debug)]
pub enum Event<T: Mirrorable> {
    Upsert(T),
    Delete(T),
    NamespaceUpsert(Namespace),
    NamespaceDelete(String),
}

/// Which watcher's caches to clear on session end — mirrors upstream
/// `WatcherClosed` dispatch (a Namespace close clears only the namespace
/// cache; a resource close clears the resource caches but keeps namespaces).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WatcherKind {
    Resource,
    Namespace,
}

pub struct Mirror<S: ResourceStore<T>, T: Mirrorable> {
    store: S,
    /// source -> is auto-source
    auto_sources: HashSet<NsName>,
    /// source -> its auto-reflections
    auto_reflection_cache: HashMap<NsName, HashSet<NsName>>,
    /// source -> its direct reflections
    direct_reflection_cache: HashMap<NsName, HashSet<NsName>>,
    namespace_cache: HashMap<String, Namespace>,
    not_found_cache: HashSet<NsName>,
    properties_cache: HashMap<NsName, MirroringProperties>,
    last_warned_selector_errors: HashMap<NsName, String>,
    _phantom: std::marker::PhantomData<T>,
}

impl<S: ResourceStore<T>, T: Mirrorable> Mirror<S, T> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            auto_sources: HashSet::new(),
            auto_reflection_cache: HashMap::new(),
            direct_reflection_cache: HashMap::new(),
            namespace_cache: HashMap::new(),
            not_found_cache: HashSet::new(),
            properties_cache: HashMap::new(),
            last_warned_selector_errors: HashMap::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// `WatcherClosed` — upstream clears resource caches on a resource watcher
    /// close but preserves the namespace cache (owned by the namespace
    /// watcher, must survive for selector checks during the replay).
    pub fn watcher_closed(&mut self, kind: WatcherKind) {
        match kind {
            WatcherKind::Namespace => {
                debug!("cleared namespace cache");
                self.namespace_cache.clear();
            }
            WatcherKind::Resource => {
                debug!("cleared resource caches");
                self.auto_sources.clear();
                self.not_found_cache.clear();
                self.properties_cache.clear();
                self.auto_reflection_cache.clear();
                self.direct_reflection_cache.clear();
                self.last_warned_selector_errors.clear();
            }
        }
    }

    pub async fn handle(&mut self, event: Event<T>) {
        match event {
            Event::Upsert(obj) => {
                self.not_found_cache.remove(&obj.nsname());
                self.handle_upsert(&obj).await;
            }
            Event::Delete(obj) => {
                self.not_found_cache.remove(&obj.nsname());
                self.handle_delete(&obj).await;
            }
            Event::NamespaceUpsert(ns) => self.handle_namespace_upsert(ns).await,
            Event::NamespaceDelete(name) => self.handle_namespace_delete(&name),
        }
    }

    async fn handle_delete(&mut self, obj: &T) {
        let nn = obj.nsname();
        self.properties_cache.remove(&nn);
        self.last_warned_selector_errors.remove(&nn);
        let props = obj.properties();

        if !props.is_reflection() {
            // Source deleted → delete all its auto-reflections.
            if props.allowed
                && props.auto_enabled
                && let Some(reflections) = self.auto_reflection_cache.get(&nn)
            {
                for r in reflections.clone() {
                    debug!(reflection = %r, source = %nn, "deleting reflection - source deleted");
                    self.store.delete(&r).await.ok();
                }
            }
            self.auto_sources.remove(&nn);
            self.direct_reflection_cache.remove(&nn);
            self.auto_reflection_cache.remove(&nn);
        } else {
            // A reflection vanished — drop it from both caches.
            for set in self.direct_reflection_cache.values_mut() {
                set.remove(&nn);
            }
            for set in self.auto_reflection_cache.values_mut() {
                set.remove(&nn);
            }
        }
    }

    async fn handle_namespace_upsert(&mut self, ns: Namespace) {
        // Skip re-reconciling when only non-label fields changed — reflection
        // eligibility is purely a function of namespace name and labels.
        if let Some(cached) = self.namespace_cache.get(&ns.name)
            && cached.labels == ns.labels
        {
            self.namespace_cache.insert(ns.name.clone(), ns);
            return;
        }
        self.namespace_cache.insert(ns.name.clone(), ns.clone());

        // Re-evaluate every auto-source against this namespace.
        for source in self.auto_sources.clone() {
            let Some(props) = self.properties_cache.get(&source).cloned() else {
                continue;
            };
            let reflection = source.in_namespace(&ns.name);

            if props.can_be_auto_reflected_to(&ns.name, Some(&ns.labels)) {
                // Create or update the auto-reflection in this namespace.
                self.resource_reflect(&source, &reflection, None, None, true)
                    .await;
                self.auto_reflection_cache
                    .entry(source.clone())
                    .or_default()
                    .insert(reflection);
            } else {
                let removed = self
                    .auto_reflection_cache
                    .get_mut(&source)
                    .map(|s| s.remove(&reflection))
                    .unwrap_or(false);
                if removed {
                    debug!(
                        reflection = %reflection,
                        namespace = %ns.name,
                        source = %source,
                        "deleting reflection - namespace no longer matches selector"
                    );
                    self.store.delete(&reflection).await.ok();
                }
            }
        }

        // Rebalance direct reflections targeting this namespace.
        let direct_snapshot: Vec<(NsName, HashSet<NsName>)> = self
            .direct_reflection_cache
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (source, list) in direct_snapshot {
            let Some(props) = self.properties_cache.get(&source) else {
                continue;
            };
            let stale: Vec<NsName> = list
                .iter()
                .filter(|r| r.namespace == ns.name)
                .cloned()
                .collect();
            if stale.is_empty() {
                continue;
            }
            if self.can_be_reflected_cached(props, &ns.name) {
                continue;
            }
            if let Some(set) = self.direct_reflection_cache.get_mut(&source) {
                for r in stale {
                    info!(
                        source = %source,
                        reflection = %r,
                        "source no longer permits the direct reflection"
                    );
                    set.remove(&r);
                }
            }
        }
    }

    fn handle_namespace_delete(&mut self, name: &str) {
        self.namespace_cache.remove(name);
        for source in self.auto_sources.clone() {
            if let Some(set) = self.auto_reflection_cache.get_mut(&source) {
                set.remove(&source.in_namespace(name));
            }
        }
        for set in self.direct_reflection_cache.values_mut() {
            set.retain(|r| r.namespace != name);
        }
    }

    async fn handle_upsert(&mut self, obj: &T) {
        let nn = obj.nsname();
        let props = obj.properties();
        self.properties_cache.insert(nn.clone(), props.clone());
        self.warn_on_invalid_selectors(&nn, &props);

        if !props.is_reflection() {
            self.handle_source_upsert(obj, &nn, &props).await;
        } else if props.is_auto_reflection {
            self.handle_auto_reflection_upsert(&nn, &props).await;
        } else {
            self.handle_direct_reflection_upsert(obj, &nn, &props).await;
        }
    }

    /// Upsert of a non-reflection resource — a potential source.
    async fn handle_source_upsert(&mut self, obj: &T, nn: &NsName, props: &MirroringProperties) {
        // Drop direct reflections the source no longer permits.
        if let Some(list) = self.direct_reflection_cache.get(nn).cloned() {
            let stale: Vec<NsName> = list
                .iter()
                .filter(|r| !self.can_be_reflected_cached(props, &r.namespace))
                .cloned()
                .collect();
            for r in &stale {
                info!(source = %nn, reflection = %r, "source no longer permits the direct reflection");
                self.direct_reflection_cache
                    .get_mut(nn)
                    .map(|s| s.remove(r));
            }
        }

        // Delete auto-reflections the source no longer permits.
        if let Some(list) = self.auto_reflection_cache.get(nn).cloned() {
            let stale: Vec<NsName> = list
                .iter()
                .filter(|r| !self.can_be_reflected_cached(props, &r.namespace))
                .cloned()
                .collect();
            for r in &stale {
                self.auto_reflection_cache.get_mut(nn).map(|s| s.remove(r));
                info!(source = %nn, reflection = %r, "source no longer permits the auto reflection - deleting");
                self.store.delete(r).await.ok();
            }
        }

        let is_auto_source = props.allowed && props.auto_enabled;
        if is_auto_source {
            self.auto_sources.insert(nn.clone());
        } else {
            self.auto_sources.remove(nn);
            self.auto_reflection_cache.remove(nn);
        }

        if !props.allowed {
            self.direct_reflection_cache.remove(nn);
            return;
        }

        // Refresh permitted direct reflections whose stored version lags.
        if let Some(list) = self.direct_reflection_cache.get(nn).cloned() {
            for r in list {
                let stale = match self.properties_cache.get(&r) {
                    Some(rp) => rp.reflected_version != props.resource_version,
                    None => {
                        self.direct_reflection_cache
                            .get_mut(nn)
                            .map(|s| s.remove(&r));
                        continue;
                    }
                };
                if !stale {
                    debug!(reflection = %r, source = %nn, "source matches reflected version");
                    continue;
                }
                self.resource_reflect(nn, &r, Some(obj.clone()), None, false)
                    .await;
            }
        }

        if is_auto_source {
            self.auto_reflection_for_source(nn, Some(obj.clone())).await;
        }
    }

    /// Upsert of a direct reflection — re-sync from its source.
    async fn handle_direct_reflection_upsert(
        &mut self,
        obj: &T,
        nn: &NsName,
        props: &MirroringProperties,
    ) {
        let Some(source) = props.reflects.clone() else {
            return;
        };

        let source_props = match self.properties_cache.get(&source) {
            Some(p) => p.clone(),
            None => match self.try_get(&source).await {
                Some(s) => {
                    let p = s.properties();
                    self.properties_cache.insert(source.clone(), p.clone());
                    p
                }
                None => {
                    warn!(reflection = %nn, source = %source, "could not update - source not found");
                    return;
                }
            },
        };

        self.direct_reflection_cache
            .entry(source.clone())
            .or_default()
            .insert(nn.clone());

        if !self.can_be_reflected_cached(&source_props, &nn.namespace) {
            warn!(reflection = %nn, source = %source, "source does not permit the reflection");
            self.direct_reflection_cache
                .get_mut(&source)
                .map(|s| s.remove(nn));
            return;
        }

        if source_props.resource_version == props.reflected_version {
            debug!(reflection = %nn, source = %source, "source matches reflected version");
            return;
        }

        self.resource_reflect(&source, nn, None, Some(obj.clone()), false)
            .await;
    }

    /// Upsert of an auto-reflection — verify the source still exists and
    /// permits it; the actual sync happens when the source is handled.
    async fn handle_auto_reflection_upsert(&mut self, nn: &NsName, props: &MirroringProperties) {
        let Some(source) = props.reflects.clone() else {
            return;
        };

        if self.not_found_cache.contains(&source) {
            info!(source = %source, reflection = %nn, "source no longer exists - deleting reflection");
            self.store.delete(nn).await.ok();
            return;
        }

        let source_props = match self.properties_cache.get(&source) {
            Some(p) => p.clone(),
            None => match self.try_get(&source).await {
                Some(s) => {
                    let p = s.properties();
                    self.properties_cache.insert(source.clone(), p.clone());
                    p
                }
                None => {
                    info!(source = %source, reflection = %nn, "source no longer exists - deleting reflection");
                    self.store.delete(nn).await.ok();
                    return;
                }
            },
        };

        if !self.can_be_auto_reflected_cached(&source_props, &nn.namespace) {
            info!(source = %source, reflection = %nn, "source no longer permits the auto reflection - deleting");
            self.store.delete(nn).await.ok();
        }
    }

    /// Full auto-reflection reconciliation for one source — upstream's
    /// `AutoReflectionForSource`: diff the existing same-name objects against
    /// the eligible namespaces, then create/update/delete/validate.
    async fn auto_reflection_for_source(&mut self, source: &NsName, source_obj: Option<T>) {
        debug!(source = %source, "processing auto-reflection source");
        let Some(props) = self.properties_cache.get(source).cloned() else {
            return;
        };

        let matches = self
            .store
            .list_by_name(&source.name)
            .await
            .unwrap_or_default();
        let namespaces = self.store.list_namespaces().await.unwrap_or_default();

        for ns in &namespaces {
            self.namespace_cache.insert(ns.name.clone(), ns.clone());
        }
        let ns_lookup: HashMap<&str, &Namespace> =
            namespaces.iter().map(|n| (n.name.as_str(), n)).collect();

        for m in &matches {
            let mp = m.properties();
            self.properties_cache.insert(m.nsname(), mp);
        }

        let mut to_delete: Vec<NsName> = Vec::new();
        let mut to_update: Vec<NsName> = Vec::new();
        let mut to_skip: Vec<NsName> = Vec::new();
        let mut match_objs: HashMap<NsName, &T> = HashMap::new();

        for m in &matches {
            let mnn = m.nsname();
            match_objs.insert(mnn.clone(), m);
            if mnn.namespace == source.namespace {
                continue;
            }
            let mp = m.properties();
            if mp.reflects.as_ref() != Some(source) {
                continue;
            }
            let ns_ok = ns_lookup
                .get(mnn.namespace.as_str())
                .map(|ns| props.can_be_auto_reflected_to(&ns.name, Some(&ns.labels)))
                .unwrap_or(false);
            if !ns_ok {
                to_delete.push(mnn);
            } else if mp.reflected_version != props.resource_version {
                to_update.push(mnn);
            } else {
                to_skip.push(mnn);
            }
        }

        let existing: HashSet<String> = matches.iter().map(|m| m.nsname().namespace).collect();
        let to_create: Vec<NsName> = namespaces
            .iter()
            .filter(|ns| ns.name != source.namespace)
            .filter(|ns| !existing.contains(&ns.name))
            .filter(|ns| props.can_be_auto_reflected_to(&ns.name, Some(&ns.labels)))
            .map(|ns| source.in_namespace(&ns.name))
            .collect();

        for d in &to_delete {
            self.store.delete(d).await.ok();
        }

        let source_obj = match source_obj {
            Some(o) => Some(o),
            None => match self.try_get(source).await {
                Some(o) => Some(o),
                None => {
                    let set = self
                        .auto_reflection_cache
                        .entry(source.clone())
                        .or_default();
                    set.clear();
                    set.extend(
                        to_create
                            .iter()
                            .chain(to_skip.iter())
                            .chain(to_update.iter())
                            .cloned(),
                    );
                    info!(
                        source = %source,
                        created = to_create.len(),
                        updated = to_update.len(),
                        deleted = to_delete.len(),
                        validated = to_skip.len(),
                        "auto-reflected where permitted"
                    );
                    return;
                }
            },
        };

        {
            let set = self
                .auto_reflection_cache
                .entry(source.clone())
                .or_default();
            set.clear();
            set.extend(
                to_create
                    .iter()
                    .chain(to_skip.iter())
                    .chain(to_update.iter())
                    .cloned(),
            );
        }

        for c in &to_create {
            self.resource_reflect(source, c, source_obj.clone(), None, true)
                .await;
        }
        for u in &to_update {
            let robj = match_objs.get(u).map(|t| (*t).clone());
            self.resource_reflect(source, u, source_obj.clone(), robj, true)
                .await;
        }

        info!(
            source = %source,
            created = to_create.len(),
            updated = to_update.len(),
            deleted = to_delete.len(),
            validated = to_skip.len(),
            "auto-reflected where permitted"
        );
    }

    /// Create or patch one reflection — upstream `ResourceReflect`.
    async fn resource_reflect(
        &mut self,
        source: &NsName,
        reflection: &NsName,
        source_obj: Option<T>,
        reflection_obj: Option<T>,
        auto: bool,
    ) {
        if source == reflection {
            return;
        }
        debug!(source = %source, reflection = %reflection, "reflecting");

        let source_obj = match source_obj {
            Some(o) => o,
            None => match self.try_get(source).await {
                Some(o) => o,
                None => {
                    warn!(reflection = %reflection, source = %source, "could not update - source not found");
                    return;
                }
            },
        };

        // Upstream writes .NET bool.ToString() - capitalized True/False.
        let meta = json!({
            annotations::META_AUTO_REFLECTS: if auto { "True" } else { "False" },
            annotations::REFLECTS: source.to_string(),
            annotations::META_REFLECTED_VERSION: source_obj.resource_version(),
            annotations::META_REFLECTED_AT: chrono_free_now(),
        });

        match reflection_obj {
            None => {
                let mut new_obj = source_obj.clone_for_reflection();
                let anns: BTreeMap<String, String> = meta
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
                    .collect();
                new_obj.set_name_ns_annotations(&reflection.name, &reflection.namespace, anns);
                match self.store.create(&new_obj, &reflection.namespace).await {
                    Ok(_) => {
                        info!(reflection = %reflection, source = %source, "created reflection");
                    }
                    Err(ApiError::Conflict) => {
                        // Exists but wasn't in our list — fall through to patch.
                        match self.store.get(reflection).await {
                            Ok(existing) => {
                                self.patch_reflection(&source_obj, &existing, reflection, meta)
                                    .await;
                            }
                            Err(_) => {
                                warn!(reflection = %reflection, "conflict on create but get failed");
                            }
                        }
                    }
                    Err(e) => {
                        warn!(reflection = %reflection, error = %e, "could not create reflection")
                    }
                }
            }
            Some(existing) => {
                if existing.properties().reflected_version == source_obj.resource_version() {
                    debug!(reflection = %reflection, source = %source, "source matches reflected version");
                    return;
                }
                self.patch_reflection(&source_obj, &existing, reflection, meta)
                    .await;
            }
        }
    }

    async fn patch_reflection(
        &mut self,
        source: &T,
        existing: &T,
        reflection: &NsName,
        meta: serde_json::Value,
    ) {
        // Upstream JSON-patch: replace annotations wholesale (reflection's
        // annotations + the four meta keys) and replace the data fields.
        let mut anns = existing.annotations().cloned().unwrap_or_default();
        for (k, v) in meta.as_object().unwrap() {
            anns.insert(k.clone(), v.as_str().unwrap().to_string());
        }
        let mut ops: Vec<serde_json::Value> = vec![json!({
            "op": "add",
            "path": "/metadata/annotations",
            "value": anns,
        })];
        ops.extend(source.data_patch_ops());
        let patch = json!(ops);
        match self.store.patch(reflection, patch).await {
            Ok(()) => {
                info!(reflection = %reflection, source = %source.nsname(), "patched reflection")
            }
            Err(e) => warn!(reflection = %reflection, error = %e, "could not reflect"),
        }
    }

    async fn try_get(&mut self, id: &NsName) -> Option<T> {
        match self.store.get(id).await {
            Ok(o) => {
                self.not_found_cache.remove(id);
                Some(o)
            }
            Err(ApiError::NotFound) => {
                debug!(id = %id, "not found");
                self.not_found_cache.insert(id.clone());
                None
            }
            Err(e) => {
                warn!(id = %id, error = %e, "get failed");
                None
            }
        }
    }

    fn can_be_reflected_cached(&self, props: &MirroringProperties, ns: &str) -> bool {
        match self.namespace_cache.get(ns) {
            Some(n) => props.can_be_reflected_to(ns, Some(&n.labels)),
            // Fail closed: a label selector can't be evaluated without the
            // namespace object — upstream's cached overload.
            None if !props.allowed_namespaces_selector.is_empty() => false,
            None => props.can_be_reflected_to(ns, None),
        }
    }

    fn can_be_auto_reflected_cached(&self, props: &MirroringProperties, ns: &str) -> bool {
        match self.namespace_cache.get(ns) {
            Some(n) => props.can_be_auto_reflected_to(ns, Some(&n.labels)),
            None if !props.allowed_namespaces_selector.is_empty()
                || !props.auto_namespaces_selector.is_empty() =>
            {
                false
            }
            None => props.can_be_auto_reflected_to(ns, None),
        }
    }

    fn warn_on_invalid_selectors(&mut self, nn: &NsName, props: &MirroringProperties) {
        let errors = props.label_selector_errors();
        if errors.is_empty() {
            self.last_warned_selector_errors.remove(nn);
            return;
        }
        let signature = errors.join("|");
        if self.last_warned_selector_errors.get(nn) == Some(&signature) {
            return;
        }
        self.last_warned_selector_errors
            .insert(nn.clone(), signature);
        for e in errors {
            warn!(source = %nn, "{e}");
        }
    }
}

/// Upstream writes `reflected-at` as `DateTimeOffset.UtcNow` "O" format —
/// ISO-8601 UTC. Computed from the epoch without a chrono dep.
fn chrono_free_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0) as i64;
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let (h, m, s) = (rem / 3600, rem % 3600 / 60, rem % 60);
    // Civil-from-days (Howard Hinnant's algorithm).
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}.0000000+00:00")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_is_iso8601_utc() {
        let t = chrono_free_now();
        // "YYYY-MM-DDTHH:MM:SS.0000000+00:00"
        assert_eq!(t.len(), 33);
        assert!(t.ends_with("+00:00"));
        assert_eq!(&t[4..5], "-");
        assert_eq!(&t[10..11], "T");
    }

    #[test]
    fn nsname_roundtrip() {
        assert_eq!(NsName::parse("a/b"), Some(NsName::new("a", "b")));
        assert_eq!(NsName::parse("a/b").unwrap().to_string(), "a/b");
        assert!(NsName::parse("ab").is_none());
        assert!(NsName::parse("/b").is_none());
    }
}
