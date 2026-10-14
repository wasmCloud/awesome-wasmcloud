//! Moving values between Temporal's proto types and the WIT types workloads
//! use.
//!
//! The plugin both imports and exports `wasmcloud:temporal/types`, and
//! wit-bindgen generates a separate Rust copy for each. Payloads arrive in
//! both: activity results through the imported `activity`, and heartbeat
//! details through the exported `context`.

use temporalio_protos::temporal::api::common::v1 as common;
use temporalio_protos::temporal::api::failure::v1::{
    ApplicationFailureInfo, Failure as ProtoFailure, failure::FailureInfo,
};
use temporalio_protos::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse;

use crate::bindings::exports::wasmcloud::temporal::types as exported;
use crate::bindings::wasmcloud::temporal::types::{ActivityInfo, Failure, Payload};
use crate::temporal::Task;

/// The payloads a workload returned, for Temporal.
pub fn payloads(payloads: Vec<Payload>) -> common::Payloads {
    common::Payloads {
        payloads: payloads
            .into_iter()
            .map(|p| payload(p.metadata, p.data))
            .collect(),
    }
}

/// Heartbeat details a workload sent, for Temporal.
pub fn exported_payloads(payloads: Vec<exported::Payload>) -> common::Payloads {
    common::Payloads {
        payloads: payloads
            .into_iter()
            .map(|p| payload(p.metadata, p.data))
            .collect(),
    }
}

fn payload(metadata: Vec<(String, Vec<u8>)>, data: Vec<u8>) -> common::Payload {
    common::Payload {
        metadata: metadata.into_iter().collect(),
        data,
        ..Default::default()
    }
}

/// Temporal's payloads, for a workload. Metadata is sorted by key, since the
/// proto keeps it in a map.
fn from_payloads(payloads: Option<common::Payloads>) -> Vec<Payload> {
    payloads
        .map(|p| p.payloads)
        .unwrap_or_default()
        .into_iter()
        .map(|p| {
            let mut metadata: Vec<_> = p.metadata.into_iter().collect();
            metadata.sort();
            Payload {
                metadata,
                data: p.data,
            }
        })
        .collect()
}

/// The task in a non-empty poll response. Temporal does not repeat the task
/// queue or the activity's namespace there, so the caller passes them.
pub fn task(response: PollActivityTaskQueueResponse, namespace: &str, task_queue: &str) -> Task {
    let execution = response.workflow_execution.unwrap_or_default();
    let info = ActivityInfo {
        activity_type: response.activity_type.unwrap_or_default().name,
        activity_id: response.activity_id,
        workflow_id: execution.workflow_id,
        workflow_run_id: execution.run_id,
        workflow_type: response.workflow_type.unwrap_or_default().name,
        namespace: namespace.to_string(),
        task_queue: task_queue.to_string(),
        attempt: u32::try_from(response.attempt).unwrap_or_default(),
        task_token: response.task_token,
        heartbeat_details: from_payloads(response.heartbeat_details),
    };
    Task {
        info,
        input: from_payloads(response.input),
    }
}

/// A workload's failure, as the application failure Temporal reports to the
/// workflow.
pub fn failure(failure: Failure) -> ProtoFailure {
    let details = (!failure.details.is_empty()).then(|| payloads(failure.details));
    ProtoFailure {
        message: failure.message,
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: failure.error_type,
                non_retryable: failure.non_retryable,
                details,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(data: &str) -> (Vec<(String, Vec<u8>)>, Vec<u8>) {
        (
            vec![("encoding".to_string(), b"json/plain".to_vec())],
            data.as_bytes().to_vec(),
        )
    }

    #[test]
    fn payloads_keep_metadata_and_data() {
        let (metadata, data) = json("\"hi\"");
        let proto = payloads(vec![Payload { metadata, data }]);
        assert_eq!(proto.payloads.len(), 1);
        assert_eq!(proto.payloads[0].metadata["encoding"], b"json/plain");
        assert_eq!(proto.payloads[0].data, b"\"hi\"");

        let (metadata, data) = json("1");
        let proto = exported_payloads(vec![exported::Payload { metadata, data }]);
        assert_eq!(proto.payloads[0].metadata["encoding"], b"json/plain");
        assert_eq!(proto.payloads[0].data, b"1");
    }

    #[test]
    fn task_carries_the_activity() {
        let (metadata, data) = json("\"world\"");
        let mut input = payload(metadata, data);
        input.metadata.insert("a".to_string(), b"b".to_vec());
        let response = PollActivityTaskQueueResponse {
            task_token: vec![1, 2, 3],
            workflow_type: Some(common::WorkflowType {
                name: "Greeting".to_string(),
            }),
            workflow_execution: Some(common::WorkflowExecution {
                workflow_id: "wf".to_string(),
                run_id: "run".to_string(),
            }),
            activity_type: Some(common::ActivityType {
                name: "greet".to_string(),
            }),
            activity_id: "1".to_string(),
            input: Some(common::Payloads {
                payloads: vec![input],
            }),
            attempt: 2,
            ..Default::default()
        };

        let Task { info, input } = task(response, "default", "queue");
        assert_eq!(info.activity_type, "greet");
        assert_eq!(info.activity_id, "1");
        assert_eq!(info.workflow_id, "wf");
        assert_eq!(info.workflow_run_id, "run");
        assert_eq!(info.workflow_type, "Greeting");
        assert_eq!(info.namespace, "default");
        assert_eq!(info.task_queue, "queue");
        assert_eq!(info.attempt, 2);
        assert_eq!(info.task_token, [1, 2, 3]);
        assert!(info.heartbeat_details.is_empty());
        assert_eq!(input.len(), 1);
        assert_eq!(
            input[0].metadata,
            [
                ("a".to_string(), b"b".to_vec()),
                ("encoding".to_string(), b"json/plain".to_vec()),
            ]
        );
        assert_eq!(input[0].data, b"\"world\"");
    }

    #[test]
    fn failure_is_an_application_failure() {
        let (metadata, data) = json("{}");
        let proto = failure(Failure {
            message: "no such activity".to_string(),
            error_type: "NotFound".to_string(),
            non_retryable: true,
            details: vec![Payload { metadata, data }],
        });
        assert_eq!(proto.message, "no such activity");
        let Some(FailureInfo::ApplicationFailureInfo(info)) = proto.failure_info else {
            panic!("not an application failure: {:?}", proto.failure_info);
        };
        assert_eq!(info.r#type, "NotFound");
        assert!(info.non_retryable);
        assert_eq!(info.details.unwrap().payloads[0].data, b"{}");
    }

    #[test]
    fn failure_without_details_has_none() {
        let proto = failure(Failure {
            message: "retry me".to_string(),
            error_type: String::new(),
            non_retryable: false,
            details: Vec::new(),
        });
        let Some(FailureInfo::ApplicationFailureInfo(info)) = proto.failure_info else {
            panic!("not an application failure");
        };
        assert!(!info.non_retryable);
        assert!(info.details.is_none());
    }
}
