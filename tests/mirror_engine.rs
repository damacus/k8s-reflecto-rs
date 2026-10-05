//! Engine-level tests: a fake in-memory store drives `Mirror` through the
//! real event flows — source upsert → auto-reflect, source update → patch,
//! source delete → delete reflections, namespace label change → rebalance.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use k8s_reflecto_rs::mirror::{ApiError, Event, Mirror, Mirrorable, Namespace, ResourceStore};
use k8s_reflecto_rs::props::{NsName, annotations, properties_from};
use serde_json::json;

/// A test resource: annotations + data blob + resourceVersion.
#[derive(Clone, Debug, PartialEq)]
struct TestRes {
    ns: String,
    name: String,
    rv: String,
    annotations: BTreeMap<String, String>,
    data: BTreeMap<String, String>,
}

impl TestRes {
    fn new(ns: &str, name: &str) -> Self {
        Self {
            ns: ns.into(),
            name: name.into(),
            rv: "1".into(),
            annotations: BTreeMap::new(),
            data: BTreeMap::new(),
        }
    }
    fn ann(mut self, k: &str, v: &str) -> Self {
        self.annotations.insert(k.into(), v.into());
        self
    }
    fn rv(mut self, v: &str) -> Self {
        self.rv = v.into();
        self
    }
    fn data(mut self, k: &str, v: &str) -> Self {
        self.data.insert(k.into(), v.into());
        self
    }
}

impl Mirrorable for TestRes {
    fn nsname(&self) -> NsName {
        NsName::new(&self.ns, &self.name)
    }
    fn resource_version(&self) -> &str {
        &self.rv
    }
    fn annotations(&self) -> Option<&BTreeMap<String, String>> {
        Some(&self.annotations)
    }
    fn properties(&self) -> k8s_reflecto_rs::props::MirroringProperties {
        properties_from(Some(&self.annotations), &self.rv)
    }
    fn clone_for_reflection(&self) -> Self {
        Self {
            ns: String::new(),
            name: String::new(),
            rv: String::new(),
            annotations: BTreeMap::new(),
            data: self.data.clone(),
        }
    }
    fn data_patch_ops(&self) -> Vec<serde_json::Value> {
        vec![json!({"op": "replace", "path": "/data", "value": self.data})]
    }
    fn set_name_ns_annotations(
        &mut self,
        name: &str,
        namespace: &str,
        anns: BTreeMap<String, String>,
    ) {
        self.name = name.into();
        self.ns = namespace.into();
        self.annotations = anns;
    }
}

#[derive(Default)]
struct FakeStore {
    objects: Mutex<HashMap<NsName, TestRes>>,
    namespaces: Mutex<HashMap<String, Namespace>>,
}

impl FakeStore {
    fn get_obj(&self, id: &NsName) -> Option<TestRes> {
        self.objects.lock().unwrap().get(id).cloned()
    }
    fn insert_ns(&self, name: &str, labels: &[(&str, &str)]) {
        self.namespaces.lock().unwrap().insert(
            name.into(),
            Namespace {
                name: name.into(),
                labels: labels
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            },
        );
    }
    /// Plant an object as cluster state — watch events reflect real objects.
    fn put(&self, obj: TestRes) {
        self.objects.lock().unwrap().insert(obj.nsname(), obj);
    }
}

impl ResourceStore<TestRes> for Arc<FakeStore> {
    async fn get(&self, id: &NsName) -> Result<TestRes, ApiError> {
        self.get_obj(id).ok_or(ApiError::NotFound)
    }
    async fn list_by_name(&self, name: &str) -> Result<Vec<TestRes>, ApiError> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .values()
            .filter(|o| o.name == name)
            .cloned()
            .collect())
    }
    async fn list_namespaces(&self) -> Result<Vec<Namespace>, ApiError> {
        Ok(self.namespaces.lock().unwrap().values().cloned().collect())
    }
    async fn create(&self, obj: &TestRes, ns: &str) -> Result<TestRes, ApiError> {
        let mut o = obj.clone();
        o.ns = ns.into();
        let id = o.nsname();
        let mut objects = self.objects.lock().unwrap();
        if objects.contains_key(&id) {
            return Err(ApiError::Conflict);
        }
        objects.insert(id, o.clone());
        Ok(o)
    }
    async fn patch(&self, id: &NsName, patch: serde_json::Value) -> Result<(), ApiError> {
        let mut objects = self.objects.lock().unwrap();
        let Some(existing) = objects.get_mut(id) else {
            return Err(ApiError::NotFound);
        };
        for op in patch.as_array().unwrap() {
            match (op["op"].as_str(), op["path"].as_str()) {
                (Some("add" | "replace"), Some("/metadata/annotations")) => {
                    existing.annotations = serde_json::from_value(op["value"].clone()).unwrap();
                }
                (Some("add" | "replace"), Some("/data")) => {
                    existing.data = serde_json::from_value(op["value"].clone()).unwrap();
                }
                _ => {}
            }
        }
        Ok(())
    }
    async fn delete(&self, id: &NsName) -> Result<(), ApiError> {
        self.objects.lock().unwrap().remove(id);
        Ok(())
    }
}

fn src_secret(ns: &str, name: &str, rv: &str) -> TestRes {
    TestRes::new(ns, name)
        .rv(rv)
        .ann(annotations::ALLOWED, "true")
        .ann(annotations::AUTO_ENABLED, "true")
        .ann(annotations::AUTO_NAMESPACES, ".*")
}

fn ns(name: &str) -> Namespace {
    Namespace {
        name: name.into(),
        labels: BTreeMap::new(),
    }
}

async fn mirror_with(
    namespaces: Vec<Namespace>,
) -> (Mirror<Arc<FakeStore>, TestRes>, Arc<FakeStore>) {
    let store = Arc::new(FakeStore::default());
    for n in namespaces {
        store.namespaces.lock().unwrap().insert(n.name.clone(), n);
    }
    (Mirror::new(store.clone()), store)
}

#[tokio::test]
async fn source_upsert_auto_reflects_to_all_namespaces() {
    let (mut m, store) = mirror_with(vec![ns("a"), ns("b"), ns("src")]).await;
    let src = src_secret("src", "creds", "10").data("k", "v");
    store.put(src.clone());

    // Namespace events first (upstream caches them via the ns watcher).
    for n in ["a", "b", "src"] {
        m.handle(Event::NamespaceUpsert(ns(n))).await;
    }
    m.handle(Event::Upsert(src)).await;

    let a = NsName::new("a", "creds");
    let b = NsName::new("b", "creds");
    let ra = store.get_obj(&a).expect("reflection in a");
    let rb = store.get_obj(&b).expect("reflection in b");

    // Reflection carries the meta annotations + copied data.
    assert_eq!(ra.annotations[annotations::REFLECTS], "src/creds");
    assert_eq!(ra.annotations[annotations::META_REFLECTED_VERSION], "10");
    assert_eq!(ra.annotations[annotations::META_AUTO_REFLECTS], "True");
    assert_eq!(ra.data["k"], "v");
    assert_eq!(rb.data["k"], "v");
    // Source itself never gets a reflection.
    let s = store.get_obj(&NsName::new("src", "creds")).unwrap();
    assert!(!s.annotations.contains_key(annotations::REFLECTS));
}

#[tokio::test]
async fn source_update_patches_stale_reflections_only() {
    let (mut m, store) = mirror_with(vec![ns("a"), ns("src")]).await;
    m.handle(Event::NamespaceUpsert(ns("a"))).await;
    m.handle(Event::NamespaceUpsert(ns("src"))).await;

    m.handle(Event::Upsert(
        src_secret("src", "creds", "10").data("k", "v1"),
    ))
    .await;
    // Bump source to rv=20 with new data → reflection must be patched.
    m.handle(Event::Upsert(
        src_secret("src", "creds", "20").data("k", "v2"),
    ))
    .await;

    let r = store.get_obj(&NsName::new("a", "creds")).unwrap();
    assert_eq!(r.data["k"], "v2");
    assert_eq!(r.annotations[annotations::META_REFLECTED_VERSION], "20");
}

#[tokio::test]
async fn source_delete_removes_auto_reflections() {
    let (mut m, store) = mirror_with(vec![ns("a"), ns("b"), ns("src")]).await;
    for n in ["a", "b", "src"] {
        m.handle(Event::NamespaceUpsert(ns(n))).await;
    }
    let src = src_secret("src", "creds", "10");
    m.handle(Event::Upsert(src.clone())).await;
    m.handle(Event::Delete(src)).await;

    assert!(store.get_obj(&NsName::new("a", "creds")).is_none());
    assert!(store.get_obj(&NsName::new("b", "creds")).is_none());
}

#[tokio::test]
async fn disallowed_source_never_reflects() {
    let (mut m, store) = mirror_with(vec![ns("a")]).await;
    m.handle(Event::NamespaceUpsert(ns("a"))).await;
    m.handle(Event::Upsert(TestRes::new("src", "creds").rv("1")))
        .await;
    assert!(store.get_obj(&NsName::new("a", "creds")).is_none());
}

#[tokio::test]
async fn auto_namespaces_pattern_restricts_targets() {
    let (mut m, store) = mirror_with(vec![ns("prod-a"), ns("dev-a"), ns("src")]).await;
    for n in ["prod-a", "dev-a", "src"] {
        m.handle(Event::NamespaceUpsert(ns(n))).await;
    }
    let src = TestRes::new("src", "creds")
        .rv("1")
        .ann(annotations::ALLOWED, "true")
        .ann(annotations::AUTO_ENABLED, "true")
        .ann(annotations::AUTO_NAMESPACES, "prod-.*");
    m.handle(Event::Upsert(src)).await;

    assert!(store.get_obj(&NsName::new("prod-a", "creds")).is_some());
    assert!(store.get_obj(&NsName::new("dev-a", "creds")).is_none());
}

#[tokio::test]
async fn namespace_label_change_rebalances_auto_reflections() {
    let (mut m, store) = mirror_with(vec![]).await;
    let mut ns_a = ns("a");
    ns_a.labels.insert("team".into(), "platform".into());
    let ns_b = ns("b");

    // Namespaces must exist in the store too — auto-reflection lists them.
    store.insert_ns("a", &[("team", "platform")]);
    store.insert_ns("b", &[]);
    store.insert_ns("src", &[]);
    m.handle(Event::NamespaceUpsert(ns_a.clone())).await;
    m.handle(Event::NamespaceUpsert(ns_b)).await;
    m.handle(Event::NamespaceUpsert(ns("src"))).await;

    let src = TestRes::new("src", "creds")
        .rv("1")
        .ann(annotations::ALLOWED, "true")
        .ann(annotations::AUTO_ENABLED, "true")
        .ann(annotations::AUTO_NAMESPACES_SELECTOR, "team=platform");
    store.put(src.clone());
    m.handle(Event::Upsert(src)).await;

    // Only team=platform namespace got a reflection.
    assert!(store.get_obj(&NsName::new("a", "creds")).is_some());
    assert!(store.get_obj(&NsName::new("b", "creds")).is_none());

    // Flip a's label → reflection must be deleted.
    let mut ns_a2 = ns("a");
    ns_a2.labels.insert("team".into(), "ops".into());
    store.insert_ns("a", &[("team", "ops")]);
    m.handle(Event::NamespaceUpsert(ns_a2)).await;
    assert!(store.get_obj(&NsName::new("a", "creds")).is_none());
}

#[tokio::test]
async fn new_namespace_gets_existing_auto_sources() {
    let (mut m, store) = mirror_with(vec![]).await;
    store.insert_ns("src", &[]);
    m.handle(Event::NamespaceUpsert(ns("src"))).await;
    let src = src_secret("src", "creds", "10").data("k", "v");
    store.put(src.clone());
    m.handle(Event::Upsert(src)).await;
    // A namespace created later picks up the reflection.
    store.insert_ns("late", &[]);
    m.handle(Event::NamespaceUpsert(ns("late"))).await;
    let r = store.get_obj(&NsName::new("late", "creds")).unwrap();
    assert_eq!(r.annotations[annotations::META_REFLECTED_VERSION], "10");
}

#[tokio::test]
async fn direct_reflection_syncs_when_source_changes() {
    let (mut m, store) = mirror_with(vec![ns("src"), ns("tgt")]).await;
    m.handle(Event::NamespaceUpsert(ns("src"))).await;
    m.handle(Event::NamespaceUpsert(ns("tgt"))).await;

    // A pre-existing direct reflection object (as if hand-created).
    let refl = TestRes::new("tgt", "creds")
        .rv("5")
        .ann(annotations::REFLECTS, "src/creds")
        .ann(annotations::META_REFLECTED_VERSION, "0");
    store.create(&refl, "tgt").await.unwrap();

    let src = TestRes::new("src", "creds")
        .rv("9")
        .ann(annotations::ALLOWED, "true")
        .data("k", "v");
    // The source must exist in the store — a direct reflection's upsert
    // triggers a source lookup.
    store.put(src.clone());
    m.handle(Event::Upsert(src)).await;
    m.handle(Event::Upsert(refl)).await;

    let r = store.get_obj(&NsName::new("tgt", "creds")).unwrap();
    assert_eq!(r.annotations[annotations::META_REFLECTED_VERSION], "9");
    assert_eq!(r.data["k"], "v");
}

#[tokio::test]
async fn source_disallowing_namespace_deletes_auto_reflection() {
    let (mut m, store) = mirror_with(vec![ns("a"), ns("src")]).await;
    m.handle(Event::NamespaceUpsert(ns("a"))).await;
    m.handle(Event::NamespaceUpsert(ns("src"))).await;

    m.handle(Event::Upsert(src_secret("src", "creds", "1")))
        .await;
    assert!(store.get_obj(&NsName::new("a", "creds")).is_some());

    // Now the source narrows its auto_namespaces to exclude 'a'.
    let narrowed = TestRes::new("src", "creds")
        .rv("2")
        .ann(annotations::ALLOWED, "true")
        .ann(annotations::AUTO_ENABLED, "true")
        .ann(annotations::AUTO_NAMESPACES, "other-.*");
    m.handle(Event::Upsert(narrowed)).await;

    assert!(store.get_obj(&NsName::new("a", "creds")).is_none());
}
