use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::datavolumes::DataVolume;

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
    "localhost:5000/unikernel-export:latest".to_string()
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
    Starting,
    Running,
    Stopped,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
pub struct KafkaSinkConfiguration {
    broker: String,
    topic: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct ExportTopologySinkConfiguration {
    #[serde(skip_serializing_if = "Option::is_none")]
    node: Option<Node>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kafka: Option<KafkaSinkConfiguration>,
}

#[cfg(test)]
impl Default for ExportTopologySinkConfiguration {
    fn default() -> Self {
        ExportTopologySinkConfiguration {
            node: Some(Default::default()),
            kafka: None,
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct QuerySubmissionStatus {
    pub state: QueryState,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
pub struct Topology {
    pub sink: ExportTopologySinkConfiguration,
    pub workers: Vec<WorkerNode>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
pub struct Node {
    #[serde(rename = "ip")]
    host: String,
    port: usize,
    resources: usize,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
pub struct WorkerNode {
    node: Node,
    links: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sources: Option<Vec<Sources>>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
pub struct Sources {
    name: String,
    schema: Schema,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub enum SchemaType {
    UINT64,
    INT64,
    FLOAT32,
    FLOAT64,
}

#[test]
fn test_deserialization_of_sink_node() {
    let mut topology = Topology::default();
    topology.workers = vec![];
    assert_eq!(
        serde_yaml::to_string(&topology).unwrap(),
        "sink:\n  node:\n    ip: ''\n    port: 0\n    resources: 0\nworkers: []\n"
    );

    let mut topology = Topology::default();
    topology.sink.node = None;
    topology.sink.kafka = Some(KafkaSinkConfiguration {
        broker: "broker".to_string(),
        topic: "topic".to_string(),
    });
    topology.workers = vec![];
    assert_eq!(
        serde_yaml::to_string(&topology).unwrap(),
        "sink:\n  kafka:\n    broker: broker\n    topic: topic\nworkers: []\n"
    );
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
pub struct Schema {
    fields: Vec<SchemaField>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct SchemaField {
    #[serde(rename = "type")]
    pub field_type: SchemaType,
    #[serde(rename = "name")]
    pub field_name: String,
}
