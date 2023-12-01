mod query_submission;

use kube::CustomResourceExt;
use query_submission::*;

fn main() {
    print!(
        "{}",
        serde_yaml::to_string(&QuerySubmission::crd()).unwrap()
    )
}
