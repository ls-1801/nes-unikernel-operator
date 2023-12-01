use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("SerializationError: {0}")]
    SerializationError(#[source] serde_json::Error),

    #[error("Kube Error: {0}")]
    KubeError(#[source] kube::Error, &'static str),

    #[error("Could not create BuilderPod: {0}")]
    BuilderPodCreation(#[source] kube::Error),
    #[error("Could not create OwnerReference to {0:?}")]
    OwnerReferenceCreation(QuerySubmission),

    #[error("Finalizer Error: {0}")]
    // NB: awkward type because finalizer::Error embeds the reconciler error (which is this)
    // so boxing this error to break cycles
    FinalizerError(#[source] Box<kube::runtime::finalizer::Error<Error>>),
    #[error("QuerySubmissionFailed Reason: {0}")]
    QuerySubmissionFailed(#[source] QuerySubmissionFailureReason),
    #[error("IllegalDocument")]
    IllegalQuerySubmission,
    #[error("NeedsChange")]
    NeedsChange(NeededChanges),
}

#[derive(Error, Debug)]
pub enum QuerySubmissionFailureReason {
    #[error("Job Failed Reason: {0}")]
    Job(String),
}

#[derive(Debug)]
pub enum NeededChanges {
    ConfigMaps,
    Jobs,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn metric_label(&self) -> String {
        format!("{self:?}").to_lowercase()
    }
}

/// Expose all controller components used by main
pub mod controller;
mod query_submission;

pub use crate::controller::*;

/// Log and trace integrations
pub mod telemetry;

/// Metrics
mod metrics;

use crate::query_submission::{QuerySubmission, QuerySubmissionStatus};
pub use metrics::Metrics;

#[cfg(test)]
pub mod fixtures;
