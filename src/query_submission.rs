use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(kind = "QuerySubmission", group = "kube.rs", version = "v1", namespaced)]
#[kube(status = "QuerySubmissionStatus", shortname = "query", plural = "queries")]
#[cfg_attr(test, derive(Default))]
pub struct QuerySubmissionSpec {
    pub query: String,
    pub topology: Topology,
    #[serde(default = "default_export_image")]
    pub export_image: String,
    #[serde(default = "default_upload_image")]
    pub upload_image: String,
    #[serde(default = "default_build_image")]
    pub build_image: String,
}

fn default_export_image() -> String {
    "localhost:5000/unikernel-export213:latest".to_string()
}

fn default_upload_image() -> String {
    "localhost:5000/unikernel-upload:latest".to_string()
}

fn default_build_image() -> String {
    "localhost:5000/unikernel-build:latest".to_string()
}

#[derive(Deserialize, Serialize, PartialEq, Eq, Clone, Copy, Default, Debug, JsonSchema)]
pub enum QueryState {
    #[default]
    Submitted,
    Pending,
    Building,
    Deploying,
    Running,
    Stopped,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct QuerySubmissionStatus {
    pub state: QueryState,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
pub struct Topology {
    pub sink: Node,
    pub workers: Vec<WorkerNode>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
pub struct Node {
    #[serde(rename="ip")]
    host: String,
    port: usize,
    resources: usize,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
pub struct WorkerNode {
    #[serde(flatten)]
    node: Node,
    links: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sources: Option<Vec<Sources>>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
pub struct Sources {
    name: String,
    schema: Vec<SchemaField>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub enum SchemaType {
    UINT64,
    INT64,
    FLOAT32,
    FLOAT64,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct SchemaField {
    #[serde(rename="type")]
    pub field_type: SchemaType,
    #[serde(rename="name")]
    pub field_name: String,
}
