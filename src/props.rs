//! Mirroring properties extracted from object annotations — a direct port of
//! upstream `MirroringPropertiesExtensions` (emberstack/kubernetes-reflector).
//!
//! Annotations (prefix `reflector.v1.k8s.emberstack.com`):
//! - `reflection-allowed` — source permits reflection
//! - `reflection-allowed-namespaces` — comma-separated regex list (full match)
//! - `reflection-allowed-namespaces-selector` — k8s label selector
//! - `reflection-auto-enabled` — source auto-reflects to matching namespaces
//! - `reflection-auto-namespaces` / `-selector` — auto-reflection scoping
//! - `reflects` — marks this object as a reflection of `ns/name`
//! - `auto-reflects`, `reflected-version`, `reflected-at` — reflection metadata

use std::collections::BTreeMap;
use std::fmt;

use crate::selector::{LabelSelector, MatchNamespace};

pub const PREFIX: &str = "reflector.v1.k8s.emberstack.com";

pub mod annotations {
    pub const ALLOWED: &str = "reflector.v1.k8s.emberstack.com/reflection-allowed";
    pub const ALLOWED_NAMESPACES: &str =
        "reflector.v1.k8s.emberstack.com/reflection-allowed-namespaces";
    pub const ALLOWED_NAMESPACES_SELECTOR: &str =
        "reflector.v1.k8s.emberstack.com/reflection-allowed-namespaces-selector";
    pub const AUTO_ENABLED: &str = "reflector.v1.k8s.emberstack.com/reflection-auto-enabled";
    pub const AUTO_NAMESPACES: &str = "reflector.v1.k8s.emberstack.com/reflection-auto-namespaces";
    pub const AUTO_NAMESPACES_SELECTOR: &str =
        "reflector.v1.k8s.emberstack.com/reflection-auto-namespaces-selector";
    pub const REFLECTS: &str = "reflector.v1.k8s.emberstack.com/reflects";
    pub const META_AUTO_REFLECTS: &str = "reflector.v1.k8s.emberstack.com/auto-reflects";
    pub const META_REFLECTED_VERSION: &str = "reflector.v1.k8s.emberstack.com/reflected-version";
    pub const META_REFLECTED_AT: &str = "reflector.v1.k8s.emberstack.com/reflected-at";
}

/// `ns/name` identifier for a namespaced resource.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NsName {
    pub namespace: String,
    pub name: String,
}

impl NsName {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        let (ns, name) = s.split_once('/')?;
        if ns.is_empty() || name.is_empty() || name.contains('/') {
            return None;
        }
        Some(Self::new(ns, name))
    }

    /// Same name, different namespace — used when projecting a source into a
    /// target namespace.
    pub fn in_namespace(&self, namespace: &str) -> Self {
        Self::new(namespace, &self.name)
    }
}

impl fmt::Display for NsName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

#[derive(Clone, Debug, Default)]
pub struct MirroringProperties {
    pub allowed: bool,
    pub allowed_namespaces: String,
    pub allowed_namespaces_selector: String,
    pub auto_enabled: bool,
    pub auto_namespaces: String,
    pub auto_namespaces_selector: String,
    /// `Some(ns/name)` when this object is a reflection.
    pub reflects: Option<NsName>,
    pub resource_version: String,
    pub is_auto_reflection: bool,
    /// `reflected-version` on a reflection — the source resourceVersion it
    /// was last synced from. The change-detection key.
    pub reflected_version: String,
}

impl MirroringProperties {
    pub fn is_reflection(&self) -> bool {
        self.reflects.is_some()
    }

    /// Source permits direct reflection to `ns` — OR between the name-pattern
    /// list and the label selector. `ns_labels: None` means the namespace
    /// object isn't cached, which fails closed when a selector is configured.
    pub fn can_be_reflected_to(
        &self,
        ns: &str,
        ns_labels: Option<&BTreeMap<String, String>>,
    ) -> bool {
        self.allowed
            && MatchNamespace::new(&self.allowed_namespaces, &self.allowed_namespaces_selector)
                .matches(ns, ns_labels)
    }

    /// Source permits auto-reflection to `ns`.
    pub fn can_be_auto_reflected_to(
        &self,
        ns: &str,
        ns_labels: Option<&BTreeMap<String, String>>,
    ) -> bool {
        self.can_be_reflected_to(ns, ns_labels)
            && self.auto_enabled
            && MatchNamespace::new(&self.auto_namespaces, &self.auto_namespaces_selector)
                .matches(ns, ns_labels)
    }

    /// Parse errors for the two selector annotations, one message per
    /// malformed selector — surfaced as warnings so operators get feedback.
    pub fn label_selector_errors(&self) -> Vec<String> {
        let mut errors = Vec::new();
        for (annotation, value) in [
            (
                annotations::ALLOWED_NAMESPACES_SELECTOR,
                &self.allowed_namespaces_selector,
            ),
            (
                annotations::AUTO_NAMESPACES_SELECTOR,
                &self.auto_namespaces_selector,
            ),
        ] {
            if value.trim().is_empty() {
                continue;
            }
            if let Err(parse_errors) = LabelSelector::parse(value) {
                for e in parse_errors {
                    errors.push(format!("{annotation} '{value}': {e}"));
                }
            }
        }
        errors
    }
}

fn truthy(v: Option<&String>) -> bool {
    matches!(
        v.map(String::as_str),
        Some("true") | Some("True") | Some("TRUE")
    )
}

/// Extract mirroring properties from an object's annotations map.
pub fn properties_from(
    annotations: Option<&BTreeMap<String, String>>,
    resource_version: &str,
) -> MirroringProperties {
    let get = |key: &str| annotations.and_then(|a| a.get(key));

    let reflects = get(annotations::REFLECTS)
        .filter(|s| !s.is_empty())
        .and_then(|s| NsName::parse(s));

    MirroringProperties {
        allowed: truthy(get(annotations::ALLOWED)),
        allowed_namespaces: get(annotations::ALLOWED_NAMESPACES)
            .cloned()
            .unwrap_or_default(),
        allowed_namespaces_selector: get(annotations::ALLOWED_NAMESPACES_SELECTOR)
            .cloned()
            .unwrap_or_default(),
        auto_enabled: truthy(get(annotations::AUTO_ENABLED)),
        auto_namespaces: get(annotations::AUTO_NAMESPACES)
            .cloned()
            .unwrap_or_default(),
        auto_namespaces_selector: get(annotations::AUTO_NAMESPACES_SELECTOR)
            .cloned()
            .unwrap_or_default(),
        reflects,
        resource_version: resource_version.to_string(),
        is_auto_reflection: truthy(get(annotations::META_AUTO_REFLECTS)),
        reflected_version: get(annotations::META_REFLECTED_VERSION)
            .filter(|s| !s.trim().is_empty())
            .cloned()
            .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn ann(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn parses_full_source_annotations() {
        let a = ann(&[
            (annotations::ALLOWED, "true"),
            (annotations::ALLOWED_NAMESPACES, "app-.*,kube-.*"),
            (annotations::AUTO_ENABLED, "true"),
            (annotations::AUTO_NAMESPACES, ".*"),
        ]);
        let p = properties_from(Some(&a), "123");
        assert!(p.allowed);
        assert_eq!(p.allowed_namespaces, "app-.*,kube-.*");
        assert!(p.auto_enabled);
        assert_eq!(p.auto_namespaces, ".*");
        assert!(!p.is_reflection());
        assert_eq!(p.resource_version, "123");
    }

    #[test]
    fn parses_reflection_annotations() {
        let a = ann(&[
            (annotations::REFLECTS, "external-secrets/ghcr-credentials"),
            (annotations::META_AUTO_REFLECTS, "True"),
            (annotations::META_REFLECTED_VERSION, "999"),
        ]);
        let p = properties_from(Some(&a), "55");
        assert!(p.is_reflection());
        assert_eq!(
            p.reflects.unwrap().to_string(),
            "external-secrets/ghcr-credentials"
        );
        assert!(p.is_auto_reflection);
        assert_eq!(p.reflected_version, "999");
    }

    #[test]
    fn bool_annotations_are_case_insensitive_like_upstream() {
        // Upstream reads .NET bools — True/true/TRUE all count.
        for v in ["true", "True", "TRUE"] {
            let a = ann(&[(annotations::ALLOWED, v)]);
            assert!(properties_from(Some(&a), "1").allowed, "value={v}");
        }
        for v in ["false", "1", "yes", ""] {
            let a = ann(&[(annotations::ALLOWED, v)]);
            assert!(!properties_from(Some(&a), "1").allowed, "value={v}");
        }
    }

    #[test]
    fn malformed_reflects_is_not_a_reflection() {
        for bad in ["no-slash", "/name", "ns/", "ns/a/b"] {
            let a = ann(&[(annotations::REFLECTS, bad)]);
            assert!(
                !properties_from(Some(&a), "1").is_reflection(),
                "value={bad}"
            );
        }
    }

    #[test]
    fn reflection_requires_allowed() {
        let p = properties_from(None, "1");
        assert!(!p.can_be_reflected_to("any", None));
    }

    #[test]
    fn empty_patterns_allow_all_namespaces() {
        let a = ann(&[(annotations::ALLOWED, "true")]);
        let p = properties_from(Some(&a), "1");
        assert!(p.can_be_reflected_to("anything", None));
        assert!(!p.can_be_auto_reflected_to("anything", None)); // auto not enabled
    }

    #[test]
    fn pattern_list_requires_full_match() {
        let a = ann(&[
            (annotations::ALLOWED, "true"),
            (annotations::ALLOWED_NAMESPACES, "app-.*"),
        ]);
        let p = properties_from(Some(&a), "1");
        assert!(p.can_be_reflected_to("app-foo", None));
        // "xapp-foo" contains a matching substring only if not anchored —
        // upstream requires the match to cover the whole string.
        assert!(!p.can_be_reflected_to("xapp-foo", None));
        assert!(!p.can_be_reflected_to("other", None));
    }

    #[test]
    fn selector_or_patterns_match() {
        let a = ann(&[
            (annotations::ALLOWED, "true"),
            (annotations::ALLOWED_NAMESPACES_SELECTOR, "team=platform"),
        ]);
        let p = properties_from(Some(&a), "1");
        let labels: BTreeMap<String, String> =
            [("team".into(), "platform".into())].into_iter().collect();
        assert!(p.can_be_reflected_to("any-name", Some(&labels)));
        assert!(!p.can_be_reflected_to("any-name", Some(&BTreeMap::new())));
        // Namespace not cached + selector configured → fail closed.
        assert!(!p.can_be_reflected_to("any-name", None));
    }

    #[test]
    fn auto_requires_allowed_and_auto_enabled() {
        let a = ann(&[
            (annotations::ALLOWED, "true"),
            (annotations::AUTO_ENABLED, "true"),
            (annotations::AUTO_NAMESPACES, "prod-.*"),
        ]);
        let p = properties_from(Some(&a), "1");
        assert!(p.can_be_auto_reflected_to("prod-a", None));
        assert!(!p.can_be_auto_reflected_to("dev-a", None));
    }

    #[test]
    fn selector_errors_reported_per_annotation() {
        let a = ann(&[
            (annotations::ALLOWED_NAMESPACES_SELECTOR, "%%%garbage%%%"),
            (annotations::AUTO_NAMESPACES_SELECTOR, "team=ok"),
        ]);
        let p = properties_from(Some(&a), "1");
        let errors = p.label_selector_errors();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("allowed-namespaces-selector"));
    }
}
