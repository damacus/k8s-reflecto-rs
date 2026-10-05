//! `ResourceStore` backed by the real Kubernetes API via `kube`.

use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams, PostParams};
use kube::{Client, Resource};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt::Debug;

use crate::kobj::{ConfigMapObj, SecretObj};
use crate::mirror::{ApiError, Mirrorable, Namespace, ResourceStore};
use crate::props::NsName;

/// Adapts `kube::Api<K>` to the engine's store trait.
///
/// `W` is the `Mirrorable` wrapper (SecretObj/ConfigMapObj), `K` the
/// k8s-openapi type. Holds the client rather than a bound `Api` so
/// namespaced calls can build the right `Api::namespaced` per call (a
/// `Api::all` can't `get` namespaced).
pub struct KubeStore<W, K> {
    client: Client,
    _phantom: std::marker::PhantomData<(W, K)>,
}

impl<W, K> KubeStore<W, K>
where
    K: Resource<Scope = k8s_openapi::NamespaceResourceScope>,
    K::DynamicType: Default,
{
    #[must_use]
    pub const fn new(client: Client) -> Self {
        Self {
            client,
            _phantom: std::marker::PhantomData,
        }
    }

    fn ns_api(&self, ns: &str) -> Api<K> {
        Api::namespaced(self.client.clone(), ns)
    }
}

fn to_api_error(e: &kube::Error) -> ApiError {
    match &e {
        kube::Error::Api(ae) if ae.code == 404 => ApiError::NotFound,
        kube::Error::Api(ae) if ae.code == 409 => ApiError::Conflict,
        _ => ApiError::Other(e.to_string()),
    }
}

/// Trait bridging the wrapper type to its inner k8s type for the store.
pub trait Inner: Sized {
    type Obj: Resource<Scope = k8s_openapi::NamespaceResourceScope>
        + Clone
        + Debug
        + Serialize
        + DeserializeOwned;
    fn into_inner(self) -> Self::Obj;
    fn from_inner(o: Self::Obj) -> Self;
}

impl Inner for SecretObj {
    type Obj = k8s_openapi::api::core::v1::Secret;
    fn into_inner(self) -> Self::Obj {
        self.0
    }
    fn from_inner(o: Self::Obj) -> Self {
        Self(o)
    }
}

impl Inner for ConfigMapObj {
    type Obj = k8s_openapi::api::core::v1::ConfigMap;
    fn into_inner(self) -> Self::Obj {
        self.0
    }
    fn from_inner(o: Self::Obj) -> Self {
        Self(o)
    }
}

impl<W, K> ResourceStore<W> for KubeStore<W, K>
where
    W: Mirrorable + Inner<Obj = K>,
    K: Resource<Scope = k8s_openapi::NamespaceResourceScope>
        + Clone
        + Debug
        + Serialize
        + DeserializeOwned
        + Send
        + Sync,
    K::DynamicType: Default,
{
    async fn get(&self, id: &NsName) -> Result<W, ApiError> {
        self.ns_api(&id.namespace)
            .get(&id.name)
            .await
            .map(W::from_inner)
            .map_err(|e| to_api_error(&e))
    }

    async fn list_by_name(&self, name: &str) -> Result<Vec<W>, ApiError> {
        let lp = ListParams::default().fields(&format!("metadata.name={name}"));
        Api::<K>::all(self.client.clone())
            .list(&lp)
            .await
            .map(|l| l.items.into_iter().map(W::from_inner).collect())
            .map_err(|e| to_api_error(&e))
    }

    async fn list_namespaces(&self) -> Result<Vec<Namespace>, ApiError> {
        Api::<k8s_openapi::api::core::v1::Namespace>::all(self.client.clone())
            .list(&ListParams::default())
            .await
            .map(|l| l.items.iter().map(Namespace::from).collect())
            .map_err(|e| to_api_error(&e))
    }

    async fn create(&self, obj: &W, ns: &str) -> Result<W, ApiError> {
        self.ns_api(ns)
            .create(&PostParams::default(), &obj.clone().into_inner())
            .await
            .map(W::from_inner)
            .map_err(|e| to_api_error(&e))
    }

    async fn patch(&self, id: &NsName, patch: serde_json::Value) -> Result<(), ApiError> {
        // JSON Patch (RFC 6902) — same verb as upstream's JsonPatchDocument.
        let patch = serde_json::from_value::<json_patch::Patch>(patch)
            .map_err(|e| ApiError::Other(format!("invalid patch document: {e}")))?;
        self.ns_api(&id.namespace)
            .patch(&id.name, &PatchParams::default(), &Patch::Json::<K>(patch))
            .await
            .map(|_| ())
            .map_err(|e| to_api_error(&e))
    }

    async fn delete(&self, id: &NsName) -> Result<(), ApiError> {
        self.ns_api(&id.namespace)
            .delete(&id.name, &DeleteParams::default())
            .await
            .map(|_| ())
            .map_err(|e| to_api_error(&e))
    }
}
