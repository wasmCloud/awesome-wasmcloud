//! The calls the plugin makes to Temporal's gRPC API, through the client
//! build.rs generates and the `wasi:http` [`Transport`].
//!
//! Generated proto types stay inside this module and `convert`.

use std::time::Duration;

use temporalio_protos::temporal::api::enums::v1::TaskQueueKind;
use temporalio_protos::temporal::api::taskqueue::v1::TaskQueue;
use temporalio_protos::temporal::api::workflowservice::v1::{
    PollActivityTaskQueueRequest, RecordActivityTaskHeartbeatRequest,
    RespondActivityTaskCanceledRequest, RespondActivityTaskCompletedRequest,
    RespondActivityTaskFailedRequest,
};
use tonic::metadata::MetadataValue;
use tonic::{Code, Request, Status};

use crate::bindings::exports::wasmcloud::temporal::types as exported;
use crate::bindings::wasmcloud::temporal::types::{ActivityInfo, Failure, Payload};
use crate::config::Settings;
use crate::convert;
use crate::transport::{Deadline, Transport};

mod proto {
    include!(concat!(
        env!("OUT_DIR"),
        "/temporal.api.workflowservice.v1.rs"
    ));
}

use proto::workflow_service_client::WorkflowServiceClient;

/// How long a poll may wait. Temporal ends an empty poll after about a minute,
/// and rejects one that has no deadline.
const POLL_TIMEOUT: Duration = Duration::from_secs(70);

/// How long any other call may take.
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// The largest response the client accepts, as in Temporal's Go SDK. tonic's
/// own default, 4 MiB, is smaller than the payloads Temporal allows.
const MAX_MESSAGE_SIZE: usize = 128 * 1024 * 1024;

/// One activity task taken from a task queue.
pub struct Task {
    pub info: ActivityInfo,
    pub input: Vec<Payload>,
}

/// Why a call to Temporal failed.
#[derive(Debug)]
pub enum Error {
    /// Temporal does not know the task token (gRPC `NOT_FOUND`).
    NotFound,
    /// Temporal could not be reached, or answered `UNAVAILABLE`.
    Unavailable(String),
    Other(String),
}

impl From<Status> for Error {
    fn from(status: Status) -> Self {
        match status.code() {
            Code::NotFound => Error::NotFound,
            Code::Unavailable => Error::Unavailable(status.message().to_string()),
            code => Error::Other(format!("{code:?}: {}", status.message())),
        }
    }
}

/// What the plugin needs to call Temporal's frontend. Cheap to clone. The gRPC
/// client is built for each call, since it costs nothing to make.
#[derive(Clone)]
pub struct Client {
    transport: Transport,
    namespace: String,
    identity: String,
    api_key: Option<String>,
}

impl Client {
    pub fn new(settings: &Settings) -> Self {
        Self {
            transport: Transport::new(&settings.address),
            namespace: settings.namespace.clone(),
            identity: settings.identity.clone(),
            api_key: settings.api_key.clone(),
        }
    }

    /// Long-polls `task_queue`. `None` when Temporal ended the poll empty.
    pub async fn poll(&self, task_queue: &str) -> Result<Option<Task>, Error> {
        let request = self.request(
            POLL_TIMEOUT,
            PollActivityTaskQueueRequest {
                namespace: self.namespace.clone(),
                task_queue: Some(TaskQueue {
                    name: task_queue.to_string(),
                    kind: TaskQueueKind::Normal.into(),
                    ..Default::default()
                }),
                identity: self.identity.clone(),
                ..Default::default()
            },
        )?;
        let response = self.grpc().poll_activity_task_queue(request).await?;
        let response = response.into_inner();
        if response.task_token.is_empty() {
            return Ok(None);
        }
        Ok(Some(convert::task(response, &self.namespace, task_queue)))
    }

    pub async fn complete(&self, task_token: Vec<u8>, result: Vec<Payload>) -> Result<(), Error> {
        let request = self.request(
            CALL_TIMEOUT,
            RespondActivityTaskCompletedRequest {
                task_token,
                result: Some(convert::payloads(result)),
                identity: self.identity.clone(),
                namespace: self.namespace.clone(),
                ..Default::default()
            },
        )?;
        self.grpc().respond_activity_task_completed(request).await?;
        Ok(())
    }

    pub async fn fail(&self, task_token: Vec<u8>, failure: Failure) -> Result<(), Error> {
        let request = self.request(
            CALL_TIMEOUT,
            RespondActivityTaskFailedRequest {
                task_token,
                failure: Some(convert::failure(failure)),
                identity: self.identity.clone(),
                namespace: self.namespace.clone(),
                ..Default::default()
            },
        )?;
        self.grpc().respond_activity_task_failed(request).await?;
        Ok(())
    }

    /// Returns false when Temporal refused because it had not asked to cancel
    /// the activity. Server 1.32 answers that with `INVALID_ARGUMENT`.
    pub async fn cancelled(&self, task_token: Vec<u8>) -> Result<bool, Error> {
        let request = self.request(
            CALL_TIMEOUT,
            RespondActivityTaskCanceledRequest {
                task_token,
                identity: self.identity.clone(),
                namespace: self.namespace.clone(),
                ..Default::default()
            },
        )?;
        match self.grpc().respond_activity_task_canceled(request).await {
            Ok(_) => Ok(true),
            Err(status)
                if matches!(
                    status.code(),
                    Code::InvalidArgument | Code::FailedPrecondition
                ) =>
            {
                Ok(false)
            }
            Err(status) => Err(status.into()),
        }
    }

    /// Returns true when Temporal has asked to cancel the activity.
    pub async fn heartbeat(
        &self,
        task_token: Vec<u8>,
        details: Vec<exported::Payload>,
    ) -> Result<bool, Error> {
        let request = self.request(
            CALL_TIMEOUT,
            RecordActivityTaskHeartbeatRequest {
                task_token,
                details: Some(convert::exported_payloads(details)),
                identity: self.identity.clone(),
                namespace: self.namespace.clone(),
                ..Default::default()
            },
        )?;
        let response = self.grpc().record_activity_task_heartbeat(request).await?;
        Ok(response.into_inner().cancel_requested)
    }

    fn grpc(&self) -> WorkflowServiceClient<Transport> {
        WorkflowServiceClient::new(self.transport.clone())
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
    }

    /// Wraps `message`, with the API key when there is one. The key is marked
    /// sensitive, and no error names it. `timeout` goes to Temporal as the
    /// call's deadline, and to the transport, which stops waiting a little
    /// after it, should the connection go quiet.
    fn request<T>(&self, timeout: Duration, message: T) -> Result<Request<T>, Error> {
        let mut request = Request::new(message);
        request.set_timeout(timeout);
        request.extensions_mut().insert(Deadline(timeout));
        if let Some(key) = &self.api_key {
            let mut value = MetadataValue::try_from(format!("Bearer {key}"))
                .map_err(|_| Error::Other("the API key is not a valid header value".to_string()))?;
            value.set_sensitive(true);
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }
}
