//! `Mirrorable` impls for the two resource kinds — wrappers around the
//! k8s-openapi types carrying only what the mirror engine needs.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{ConfigMap, Namespace as K8sNamespace, Secret};
use kube::api::ObjectMeta;
use serde_json::json;

use crate::mirror::{Mirrorable, Namespace};
use crate::props::NsName;

macro_rules! impl_mirrorable {
    ($wrapper:ident, $inner:ty, $clone:expr, $ops:expr) => {
        #[derive(Clone, Debug)]
        pub struct $wrapper(pub $inner);

        impl Mirrorable for $wrapper {
            fn nsname(&self) -> NsName {
                let m = &self.0.metadata;
                NsName::new(
                    m.namespace.clone().unwrap_or_default(),
                    m.name.clone().unwrap_or_default(),
                )
            }

            fn resource_version(&self) -> &str {
                self.0.metadata.resource_version.as_deref().unwrap_or("")
            }

            fn annotations(&self) -> Option<&BTreeMap<String, String>> {
                self.0.metadata.annotations.as_ref()
            }

            fn clone_for_reflection(&self) -> Self {
                #[allow(clippy::redundant_closure_call)]
                Self(($clone)(&self.0))
            }

            fn data_patch_ops(&self) -> Vec<serde_json::Value> {
                #[allow(clippy::redundant_closure_call)]
                ($ops)(&self.0)
            }

            fn set_name_ns_annotations(
                &mut self,
                name: &str,
                namespace: &str,
                annotations: BTreeMap<String, String>,
            ) {
                self.0.metadata = ObjectMeta {
                    name: Some(name.to_string()),
                    namespace: Some(namespace.to_string()),
                    annotations: Some(annotations),
                    ..Default::default()
                };
            }
        }
    };
}

// k8s-openapi 0.28 types carry no api_version/kind fields (implied by type).
// Upstream clone copies type+data for secrets, data+binaryData for configmaps.
impl_mirrorable!(
    SecretObj,
    Secret,
    |s: &Secret| Secret {
        type_: s.type_.clone(),
        data: s.data.clone(),
        immutable: s.immutable,
        ..Default::default()
    },
    |s: &Secret| { vec![json!({"op": "add", "path": "/data", "value": s.data})] }
);

impl_mirrorable!(
    ConfigMapObj,
    ConfigMap,
    |c: &ConfigMap| ConfigMap {
        data: c.data.clone(),
        binary_data: c.binary_data.clone(),
        immutable: c.immutable,
        ..Default::default()
    },
    |c: &ConfigMap| {
        vec![
            json!({"op": "add", "path": "/data", "value": c.data}),
            json!({"op": "add", "path": "/binaryData", "value": c.binary_data}),
        ]
    }
);

impl From<&K8sNamespace> for Namespace {
    fn from(n: &K8sNamespace) -> Self {
        Self {
            name: n.metadata.name.clone().unwrap_or_default(),
            labels: n.metadata.labels.clone().unwrap_or_default(),
        }
    }
}
