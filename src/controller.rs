use std::collections::BTreeMap;
use std::sync::Arc;
use std::vec;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use k8s_openapi::api::batch::v1::{Job, JobSpec};
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, EmptyDirVolumeSource, HostPathVolumeSource,
    PersistentVolume, PersistentVolumeSpec, PodSpec, PodTemplateSpec, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::PostParams;
use kube::{
    api::{Api, ListParams, Patch, PatchParams, ResourceExt},
    client::Client,
    runtime::{
        controller::{Action, Controller},
        events::{Event, EventType, Recorder, Reporter},
        finalizer::{finalizer, Event as Finalizer},
        watcher::Config,
    },
    Resource,
};
use prometheus::core::Collector;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use tokio::{sync::RwLock, time::Duration};
use tracing::*;

use crate::datavolumes::*;
use crate::query_submission::*;
use crate::{
    controller, query_submission, telemetry, Error, Metrics, NeededChanges, QuerySubmissionFailureReason,
    Result,
};

pub static QUERY_FINALIZER: &str = "query.nes.rs";

// Context for our reconciler
#[derive(Clone)]
pub struct Context {
    /// Kubernetes client
    pub client: Client,
    /// Diagnostics read by the web server
    pub diagnostics: Arc<RwLock<Diagnostics>>,
    /// Prometheus metrics
    pub metrics: Metrics,
}

#[instrument(skip(ctx, doc), fields(trace_id))]
async fn reconcile(doc: Arc<QuerySubmission>, ctx: Arc<Context>) -> Result<Action> {
    let trace_id = telemetry::get_trace_id();
    Span::current().record("trace_id", &field::display(&trace_id));
    let _timer = ctx.metrics.count_and_measure();
    ctx.diagnostics.write().await.last_event = Utc::now();
    let ns = doc.namespace().unwrap(); // doc is namespace scoped
    let docs: Api<QuerySubmission> = Api::namespaced(ctx.client.clone(), &ns);

    info!("Reconciling QuerySubmission \"{}\" in {}", doc.name_any(), ns);
    finalizer(&docs, QUERY_FINALIZER, doc, |event| async {
        match event {
            Finalizer::Apply(doc) => doc.reconcile(ctx.clone()).await,
            Finalizer::Cleanup(doc) => doc.cleanup(ctx.clone()).await,
        }
    })
    .await
    .map_err(|e| Error::FinalizerError(Box::new(e)))
}

fn error_policy(doc: Arc<QuerySubmission>, error: &Error, ctx: Arc<Context>) -> Action {
    warn!("reconcile failed: {:?}", error);
    ctx.metrics.reconcile_failure(&doc, error);
    Action::requeue(Duration::from_secs(5 * 60))
}

fn merge_yaml(a: &mut serde_yaml::Value, b: serde_yaml::Value) {
    match (a, b) {
        (a @ &mut serde_yaml::Value::Mapping(_), serde_yaml::Value::Mapping(b)) => {
            let a = a.as_mapping_mut().unwrap();
            for (k, v) in b {
                if v.is_sequence() && a.contains_key(&k) && a[&k].is_sequence() {
                    let _v = v.as_sequence().unwrap();
                    let mut _a = a.get(&k).unwrap().as_sequence().unwrap();
                    if _a.len() == _v.len() {
                        let vec_of_values: Vec<serde_yaml::Value> = _a
                            .iter()
                            .zip(_v.into_iter())
                            .map(|(a, v)| {
                                let mut a = a.to_owned();
                                merge_yaml(&mut a, v.clone());
                                a
                            })
                            .collect::<Vec<_>>();
                        a[&k] = serde_yaml::Value::from(vec_of_values);
                    } else {
                        let mut _b = a.get(&k).unwrap().as_sequence().unwrap().to_owned();
                        _b.append(&mut v.as_sequence().unwrap().to_owned());
                        _b.dedup();
                        a[&k] = serde_yaml::Value::from(_b);
                    }

                    continue;
                }
                if !a.contains_key(&k) {
                    a.insert(k.to_owned(), v.to_owned());
                } else {
                    merge_yaml(&mut a[&k], v);
                }
            }
        }
        (a, b) => *a = b,
    }
}

fn detect_spec_change<T: std::fmt::Debug + serde::Serialize + DeserializeOwned + std::cmp::PartialEq>(
    observed: &T,
    desired: &T,
) -> Result<()> {
    let mut observed_value = serde_yaml::to_value(observed).unwrap();
    let desired_value = serde_yaml::to_value(desired).unwrap();

    merge_yaml(&mut observed_value, desired_value);
    let merged = serde_yaml::from_value::<T>(observed_value).unwrap();
    assert_eq!(observed, &merged);
    if observed != &merged {
        return Err(Error::NeedsChange(NeededChanges::ConfigMaps));
    }

    Ok(())
}

struct UnikernelBuilder {
    name: String,
    owner_reference: OwnerReference,
    spec: QuerySubmissionSpec,
}

const LABEL_PREFIX: &str = "dima.tu.berlin";
const OWNED_BY: &str = "owned-by";

impl UnikernelBuilder {
    fn create_build_container(&self) -> Container {
        let mut container = Container::default();

        container.volume_mounts.replace(vec![
            VolumeMount {
                mount_path: "/input".to_string(),
                name: "export-to-build".to_string(),
                read_only: Some(true),
                ..VolumeMount::default()
            },
            VolumeMount {
                mount_path: "/output".to_string(),
                name: "build-to-upload".to_string(),
                ..VolumeMount::default()
            },
        ]);

        container.image.replace(self.spec.build_image.clone());
        container.name = "build".to_string();
        container
    }
    fn create_upload_container(&self) -> Container {
        let mut container = Container::default();
        container.volume_mounts.replace(vec![VolumeMount {
            mount_path: "/input".to_string(),
            name: "build-to-upload".to_string(),
            read_only: Some(true),
            ..VolumeMount::default()
        }]);
        container.image.replace((&self.spec.upload_image).clone());
        container.name = "upload".to_string();
        container
    }
    fn create_export_container(&self) -> Container {
        let mut container = Container::default();
        container.volume_mounts.replace(vec![
            VolumeMount {
                mount_path: "/input".to_string(),
                name: "config-volume".to_string(),
                read_only: Some(true),
                ..VolumeMount::default()
            },
            VolumeMount {
                mount_path: "/output".to_string(),
                name: "export-to-build".to_string(),
                ..VolumeMount::default()
            },
        ]);
        container.image.replace(self.spec.export_image.clone());
        container.name = "export".to_string();
        container
    }

    fn create_k8s_config_map_manifest(&self) -> ConfigMap {
        let mut config_map = ConfigMap::default();
        config_map.data.replace(BTreeMap::from([(
            "data.yaml".to_string(),
            serde_yaml::to_string(&self.spec).unwrap(),
        )]));
        config_map.immutable.replace(true);

        config_map
            .metadata
            .owner_references
            .replace(vec![self.owner_reference.clone()]);
        config_map.metadata.name.replace(self.get_build_config_map_name());
        config_map.metadata.labels.replace(BTreeMap::from([(
            format!("{LABEL_PREFIX}/{OWNED_BY}"),
            self.owner_reference.name.clone(),
        )]));

        config_map
    }

    fn get_build_config_map_name(&self) -> String {
        format!("{}-build", self.name)
    }

    fn create_persistent_volume(&self, owner_reference: OwnerReference) -> PersistentVolume {
        let mut pv = PersistentVolume::default();
        let mut pv_spec = PersistentVolumeSpec::default();
        pv_spec.access_modes.replace(vec!["ReadWriteOnce".to_string()]);
        pv_spec.capacity.replace(BTreeMap::from([(
            "storage".to_string(),
            Quantity("5Gi".to_string()),
        )]));
        pv_spec.host_path.replace(HostPathVolumeSource {
            path: format!("/data/pv{}", self.name),
            type_: None,
        });
        pv.spec.replace(pv_spec);
        pv.metadata.name.replace(self.name.clone());
        pv.metadata
            .owner_references
            .replace(vec![owner_reference.clone()]);
        pv.metadata.labels.replace(BTreeMap::from([(
            format!("{LABEL_PREFIX}/{OWNED_BY}"),
            owner_reference.name,
        )]));
        return pv;
    }

    fn create_data_volume(&self, idx: usize) -> DataVolume {
        DataVolume {
            metadata: kube::core::ObjectMeta {
                name: Some(self.get_data_volume_name(idx)),
                owner_references: Some(vec![self.owner_reference.clone()]),
                ..Default::default()
            },
            spec: DataVolumeSpec {
                pvc: Some(DataVolumePvc {
                    access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                    resources: Some(DataVolumePvcResources {
                        requests: Some(BTreeMap::from([("storage".to_string(), "500Mi".to_string())])),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            status: None,
        }
    }

    fn create_k8s_job_manifest(&self) -> Job {
        let mut job = Job::default();
        let mut job_spec = JobSpec::default();
        let mut job_pod_template = PodTemplateSpec::default();
        let mut pod_spec = PodSpec::default();
        pod_spec.containers.push(self.create_export_container());
        pod_spec.containers.push(self.create_build_container());
        pod_spec.containers.push(self.create_upload_container());

        let mut config_map_volume = k8s_openapi::api::core::v1::Volume::default();
        config_map_volume.name = "config-volume".to_string();
        config_map_volume.config_map.replace(ConfigMapVolumeSource {
            default_mode: None,
            items: None,
            name: Some(self.get_build_config_map_name()),
            optional: None,
        });

        let mut export_to_build_volume = k8s_openapi::api::core::v1::Volume::default();
        export_to_build_volume.empty_dir.replace(EmptyDirVolumeSource {
            medium: None,
            size_limit: None,
        });
        export_to_build_volume.name = "export-to-build".to_string();

        let mut build_to_upload_volume = export_to_build_volume.clone();
        build_to_upload_volume.name = "build-to-upload".to_string();

        pod_spec.volumes.replace(vec![
            config_map_volume,
            export_to_build_volume,
            build_to_upload_volume,
        ]);

        pod_spec.restart_policy.replace("Never".to_string());
        job_pod_template.spec.replace(pod_spec);
        job_spec.template = job_pod_template;
        job.spec.replace(job_spec);
        job.metadata
            .owner_references
            .replace(vec![self.owner_reference.clone()]);
        job.metadata.name = Some(self.name.clone());
        job.metadata.labels.replace(BTreeMap::from([(
            format!("{LABEL_PREFIX}/{OWNED_BY}"),
            self.name.clone(),
        )]));
        job
    }

    fn get_data_volume_name(&self, idx: usize) -> String {
        format!("{}-{}", self.name, idx)
    }
}

impl QuerySubmission {
    // Reconcile (for non-finalizer related changes)

    fn get_builder(&self, name: String, owner_reference: OwnerReference) -> UnikernelBuilder {
        UnikernelBuilder {
            name,
            owner_reference,
            spec: self.spec.clone(),
        }
    }
    async fn building(&self, ctx: Arc<Context>) -> Result<QuerySubmissionStatus> {
        let mut status = self.status.as_ref().unwrap().clone();
        status.state = QueryState::Building;
        Ok(status)
    }

    async fn deploying(&self, ctx: Arc<Context>) -> Result<QuerySubmissionStatus> {
        let mut status = self.status.as_ref().unwrap().clone();
        status.state = QueryState::Deploying;
        Ok(status)
    }
    async fn pending(&self, ctx: Arc<Context>) -> Result<QuerySubmissionStatus> {
        let client = ctx.client.clone();
        let recorder = ctx.diagnostics.read().await.recorder(client.clone(), self);
        let name = self.name_any();
        let ns = self.namespace().unwrap();

        recorder
            .publish(Event {
                type_: EventType::Normal,
                reason: "Build Job Starting".into(),
                note: Some(format!("Build Started `{name}`")),
                action: "Building".into(),
                secondary: None,
            })
            .await
            .map_err(|e| Error::KubeError(e, "Creating Event"))?;

        let owner_reference = self
            .controller_owner_ref(&())
            .ok_or(Error::OwnerReferenceCreation(self.clone()))?;
        let build = self.get_builder(name, owner_reference);

        let jobs: Api<Job> = Api::namespaced(client.clone(), &ns);
        jobs.create(&PostParams::default(), &build.create_k8s_job_manifest())
            .await
            .map_err(Error::BuilderPodCreation)?;

        let mut status = self.status.as_ref().unwrap().clone();
        status.state = QueryState::Pending;
        Ok(status)
    }

    async fn init(&self, ctx: Arc<Context>) -> Result<QuerySubmissionStatus> {
        let client = ctx.client.clone();
        let recorder = ctx.diagnostics.read().await.recorder(client.clone(), self);
        let name = self.name_any();
        let ns = self.namespace().unwrap();

        if self.spec.topology.workers.is_empty() {
            return Err(Error::IllegalQuerySubmission);
        }

        recorder
            .publish(Event {
                type_: EventType::Normal,
                reason: "Query Submitted".into(),
                note: Some(format!("Submitted `{name}`")),
                action: "Submission".into(),
                secondary: None,
            })
            .await
            .map_err(|e| Error::KubeError(e, "Creating Event"))?;

        let owner_reference = self
            .controller_owner_ref(&())
            .ok_or(Error::OwnerReferenceCreation(self.clone()))?;

        let config_maps: Api<ConfigMap> = Api::namespaced(client, &ns);

        let build = self.get_builder(name, owner_reference);

        config_maps
            .create(&PostParams::default(), &build.create_k8s_config_map_manifest())
            .await
            .map_err(|e| Error::KubeError(e, "Creating Config Map"))?;

        let mut status = self.status.clone().unwrap_or(QuerySubmissionStatus::default());
        status.state = QueryState::Submitted;
        Ok(status)
    }

    async fn compute_next_action(&self, ctx: Arc<Context>) -> Result<QueryState> {
        let client = ctx.client.clone();
        let ns = self.namespace().unwrap();
        let name = self.name_any();
        let config_maps: Api<ConfigMap> = Api::namespaced(client.clone(), &ns);
        let jobs: Api<Job> = Api::namespaced(client.clone(), &ns);

        if self.spec.topology.workers.is_empty() {
            return Err(Error::IllegalQuerySubmission);
        }
        let owner_reference = self.controller_owner_ref(&()).unwrap();
        let build = self.get_builder(name.clone(), owner_reference);

        let mut find_owned_resources_options = ListParams::default();
        find_owned_resources_options
            .label_selector
            .replace(format!("{LABEL_PREFIX}/{OWNED_BY}={name}"));
        let config_maps = config_maps.list(&find_owned_resources_options);
        let jobs = jobs.list(&find_owned_resources_options);
        let (config_maps, jobs) = tokio::join!(config_maps, jobs);
        let config_maps = config_maps.map_err(|e| Error::KubeError(e, "Owned Config Maps"))?;
        let jobs = jobs.map_err(|e| Error::KubeError(e, "Owned Jobs"))?;

        if config_maps.items.is_empty() {
            return Ok(QueryState::Submitted);
        }

        if config_maps.items.len() > 2 {
            return Err(Error::NeedsChange(NeededChanges::ConfigMaps));
        }

        let config_map = &config_maps.items[0];
        detect_spec_change(config_map, &build.create_k8s_config_map_manifest())?;

        if jobs.items.is_empty() {
            return Ok(QueryState::Pending);
        }

        if jobs.items.len() > 2 {
            return Err(Error::NeedsChange(NeededChanges::Jobs));
        }

        let job = &jobs.items[0];
        detect_spec_change(
            job.spec.as_ref().unwrap(),
            build.create_k8s_job_manifest().spec.as_ref().unwrap(),
        )?;

        if let Some(ref status) = job.status {
            let mut completed = false;
            let mut failed = false;
            for condition in status.conditions.iter().flat_map(|v| v.iter()) {
                completed = completed || condition.reason.as_ref().unwrap() == "Completed";
                failed = failed || condition.reason.as_ref().unwrap() == "Failure";
            }

            if failed {
                return Err(Error::QuerySubmissionFailed(QuerySubmissionFailureReason::Job(
                    "Don't know, sorry".to_string(),
                )));
            }
            if !completed {
                return Ok(QueryState::Building);
            }
        } else {
            return Ok(QueryState::Pending);
        }

        return Ok(QueryState::Deploying);
    }
    async fn change_config_map(&self) -> Result<QuerySubmissionStatus> {
        todo!()
    }

    async fn change_job(&self) -> Result<QuerySubmissionStatus> {
        todo!()
    }

    async fn reconcile(&self, ctx: Arc<Context>) -> Result<Action> {
        use controller::Error::*;

        let client = ctx.client.clone();
        let ns = self.namespace().unwrap();
        let name = self.name_any();
        let queries: Api<QuerySubmission> = Api::namespaced(client, &ns);

        let new_status = match self.compute_next_action(ctx.clone()).await {
            Err(e) => match e {
                NeedsChange(r) => match r {
                    NeededChanges::ConfigMaps => self.change_config_map().await,
                    NeededChanges::Jobs => self.change_job().await,
                },
                e => Err(e),
            },
            Ok(s) => match s {
                QueryState::Submitted => self.init(ctx.clone()).await,
                QueryState::Pending => self.pending(ctx.clone()).await,
                QueryState::Building => self.building(ctx.clone()).await,
                QueryState::Deploying => self.deploying(ctx.clone()).await,
                QueryState::Running => {
                    todo!()
                }
                QueryState::Stopped => {
                    todo!()
                }
            },
        }?;

        let new_status = Patch::Apply(json!({
            "apiVersion": "kube.rs/v1",
            "kind": "QuerySubmission",
            "status": new_status
        }));

        let ps = PatchParams::apply("cntrlr").force();
        let _o = queries
            .patch_status(&name, &ps, &new_status)
            .await
            .map_err(|e| Error::KubeError(e, "Patching CRD Status"))?;

        // If no events were received, check back every 5 minutes
        Ok(Action::requeue(Duration::from_secs(5 * 60)))
    }

    // Finalizer cleanup (the object was deleted, ensure nothing is orphaned)
    async fn cleanup(&self, ctx: Arc<Context>) -> Result<Action> {
        let recorder = ctx.diagnostics.read().await.recorder(ctx.client.clone(), self);
        // QuerySubmission doesn't have any real cleanup, so we just publish an event
        recorder
            .publish(Event {
                type_: EventType::Normal,
                reason: "DeleteRequested".into(),
                note: Some(format!("Delete `{}`", self.name_any())),
                action: "Deleting".into(),
                secondary: None,
            })
            .await
            .map_err(|e| Error::KubeError(e, "Deleting CRD"))?;
        Ok(Action::await_change())
    }
}

/// Diagnostics to be exposed by the web server
#[derive(Clone, Serialize)]
pub struct Diagnostics {
    #[serde(deserialize_with = "from_ts")]
    pub last_event: DateTime<Utc>,
    #[serde(skip)]
    pub reporter: Reporter,
}

impl Default for Diagnostics {
    fn default() -> Self {
        Self {
            last_event: Utc::now(),
            reporter: "doc-controller".into(),
        }
    }
}

impl Diagnostics {
    fn recorder(&self, client: Client, doc: &QuerySubmission) -> Recorder {
        Recorder::new(client, self.reporter.clone(), doc.object_ref(&()))
    }
}

/// State shared between the controller and the web server
#[derive(Clone, Default)]
pub struct State {
    /// Diagnostics populated by the reconciler
    diagnostics: Arc<RwLock<Diagnostics>>,
    /// Metrics registry
    registry: prometheus::Registry,
}

/// State wrapper around the controller outputs for the web server
impl State {
    /// Metrics getter
    pub fn metrics(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.registry.gather()
    }

    /// State getter
    pub async fn diagnostics(&self) -> Diagnostics {
        self.diagnostics.read().await.clone()
    }

    // Create a Controller Context that can update State
    pub fn to_context(&self, client: Client) -> Arc<Context> {
        Arc::new(Context {
            client,
            metrics: Metrics::default().register(&self.registry).unwrap(),
            diagnostics: self.diagnostics.clone(),
        })
    }
}

/// Initialize the controller and shared state (given the crd is installed)
pub async fn run(state: State) {
    let client = Client::try_default().await.expect("failed to create kube Client");
    let docs = Api::<QuerySubmission>::all(client.clone());
    if let Err(e) = docs.list(&ListParams::default().limit(1)).await {
        error!("CRD is not queryable; {e:?}. Is the CRD installed?");
        info!("Installation: cargo run --bin crdgen | kubectl apply -f -");
        std::process::exit(1);
    }
    Controller::new(docs, Config::default().any_semantic())
        .owns(
            Api::<Job>::namespaced(client.clone(), &"default"),
            Config::default(),
        )
        .owns(
            Api::<ConfigMap>::namespaced(client.clone(), &"default"),
            Config::default(),
        )
        .shutdown_on_signal()
        .run(reconcile, error_policy, state.to_context(client))
        .filter_map(|x| async move { std::result::Result::ok(x) })
        .for_each(|_| futures::future::ready(()))
        .await;
}

// Mock tests relying on fixtures.rs and its primitive apiserver mocks
#[cfg(test)]
mod test {
    use std::sync::Arc;

    // Integration test without mocks
    use kube::api::{Api, ListParams, Patch, PatchParams};

    use crate::fixtures::{timeout_after_1s, Scenario};
    use crate::query_submission::QuerySubmissionStatus;

    use super::{error_policy, reconcile, Context, QuerySubmission};

    #[tokio::test]
    async fn query_submission_creates_containers_and_config_maps() {
        let (testctx, fakeserver, _) = Context::test();
        let doc = QuerySubmission::test()
            .with_status(QuerySubmissionStatus::default())
            .finalized();
        let mocksrv = fakeserver.run(Scenario::QuerySubmission {
            doc: doc.clone(),
            config_map_name: "test_config_map".to_string(),
            pod_name: "test".to_string(),
            event_reason: "Query Submitted".to_string(),
        });
        reconcile(Arc::new(doc), testctx).await.expect("reconciler");
        timeout_after_1s(mocksrv).await;
    }

    #[tokio::test]
    async fn documents_without_finalizer_gets_a_finalizer() {
        let (testctx, fakeserver, _) = Context::test();
        let doc = QuerySubmission::test();
        let mocksrv = fakeserver.run(Scenario::FinalizerCreation(doc.clone()));
        reconcile(Arc::new(doc), testctx).await.expect("reconciler");
        timeout_after_1s(mocksrv).await;
    }

    #[tokio::test]
    async fn finalized_doc_causes_status_patch() {
        let (testctx, fakeserver, _) = Context::test();
        let doc = QuerySubmission::test().finalized();
        let mocksrv = fakeserver.run(Scenario::EventPublishThenStatusPatch(
            "Query Submitted".into(),
            doc.clone(),
        ));
        reconcile(Arc::new(doc), testctx).await.expect("reconciler");
        timeout_after_1s(mocksrv).await;
    }

    #[tokio::test]
    async fn finalized_doc_with_hide_causes_event_and_hide_patch() {
        let (testctx, fakeserver, _) = Context::test();
        let doc = QuerySubmission::test().finalized();
        let scenario = Scenario::EventPublishThenStatusPatch("Query Submitted".into(), doc.clone());
        let mocksrv = fakeserver.run(scenario);
        reconcile(Arc::new(doc), testctx).await.expect("reconciler");
        timeout_after_1s(mocksrv).await;
    }

    #[tokio::test]
    async fn finalized_doc_with_delete_timestamp_causes_delete() {
        let (testctx, fakeserver, _) = Context::test();
        let doc = QuerySubmission::test().finalized().needs_delete();
        let mocksrv = fakeserver.run(Scenario::Cleanup("DeleteRequested".into(), doc.clone()));
        reconcile(Arc::new(doc), testctx).await.expect("reconciler");
        timeout_after_1s(mocksrv).await;
    }

    #[tokio::test]
    async fn illegal_doc_reconcile_errors_which_bumps_failure_metric() {
        let (testctx, fakeserver, _registry) = Context::test();
        let doc = Arc::new(QuerySubmission::illegal().finalized());
        let mocksrv = fakeserver.run(Scenario::RadioSilence);
        let res = reconcile(doc.clone(), testctx.clone()).await;
        timeout_after_1s(mocksrv).await;
        assert!(res.is_err(), "apply reconciler fails on illegal doc");
        let err = res.unwrap_err();
        assert!(err.to_string().contains("IllegalQuerySubmission"));
        // calling error policy with the reconciler error should cause the correct metric to be set
        error_policy(doc.clone(), &err, testctx.clone());
        //dbg!("actual metrics: {}", registry.gather());
        let failures = testctx
            .metrics
            .failures
            .with_label_values(&["illegal", "finalizererror(applyfailed(illegaldocument))"])
            .get();
        assert_eq!(failures, 1);
    }

    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn integration_reconcile_should_set_status_and_send_event() {
        let client = kube::Client::try_default().await.unwrap();
        let ctx = super::State::default().to_context(client.clone());

        // create a test doc
        let doc = QuerySubmission::test().finalized();
        let docs: Api<QuerySubmission> = Api::namespaced(client.clone(), "default");
        let ssapply = PatchParams::apply("ctrltest");
        let patch = Patch::Apply(doc.clone());
        docs.patch("test", &ssapply, &patch).await.unwrap();

        // reconcile it (as if it was just applied to the cluster like this)
        reconcile(Arc::new(doc), ctx).await.unwrap();

        // verify side-effects happened
        let output = docs.get_status("test").await.unwrap();
        assert!(output.status.is_some());
        // verify hide event was found
        let events: Api<k8s_openapi::api::core::v1::Event> = Api::all(client.clone());
        let opts =
            ListParams::default().fields("involvedObject.kind=QuerySubmission,involvedObject.name=test");
        let event = events
            .list(&opts)
            .await
            .unwrap()
            .into_iter()
            .filter(|e| e.reason.as_deref() == Some("HideRequested"))
            .last()
            .unwrap();
        dbg!("got ev: {:?}", &event);
        assert_eq!(event.action.as_deref(), Some("Hiding"));
    }
}
