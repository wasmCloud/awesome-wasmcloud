//! An example workload for the wasmCloud Temporal plugin.
//!
//! The plugin calls `execute` for each activity task it takes from Temporal,
//! and the workload picks what to run by `info.activity_type`, the name the
//! workflow called. Payloads are JSON (`json/plain`), the encoding Temporal's
//! SDKs use by default.
//!
//! - `greet(name)` returns `Hello, <name>!`.
//! - `fail` always fails, and tells Temporal not to retry.
//! - `flaky` fails with a retryable failure on attempts 1 and 2, so Temporal
//!   retries it, and succeeds on attempt 3.
//! - `slow(seconds)` works for `seconds`, heartbeating about once a second,
//!   and stops when Temporal asks to cancel it.

mod bindings {
    wit_bindgen::generate!({ world: "example", generate_all });
}

use bindings::exports::wasmcloud::temporal::activity::Guest;
use bindings::wasi::clocks::monotonic_clock;
use bindings::wasmcloud::temporal::context;
use bindings::wasmcloud::temporal::types::{
    ActivityInfo, ExecuteError, Failure, HeartbeatError, Payload,
};
use serde_json::Value;

struct Component;

impl Guest for Component {
    async fn execute(
        info: ActivityInfo,
        input: Vec<Payload>,
    ) -> Result<Vec<Payload>, ExecuteError> {
        println!(
            "running `{}` (attempt {}) for workflow `{}`",
            info.activity_type, info.attempt, info.workflow_id
        );
        let result = match info.activity_type.as_str() {
            "greet" => {
                let name: String = decode(input.first())?;
                format!("Hello, {name}!")
            }
            "fail" => {
                return Err(failure("this activity always fails", "ExampleError", true));
            }
            // Temporal counts attempts from 1, and raises `attempt` by one on
            // each retry, so a retry reaches the workload as a new call.
            "flaky" if info.attempt < 3 => {
                return Err(failure(
                    &format!("attempt {} failed, try again", info.attempt),
                    "FlakyError",
                    false,
                ));
            }
            "flaky" => format!("succeeded on attempt {}", info.attempt),
            "slow" => slow(&info, decode(input.first())?).await?,
            other => {
                return Err(failure(
                    &format!("unknown activity type `{other}`"),
                    "UnknownActivity",
                    true,
                ));
            }
        };
        Ok(vec![encode(&Value::String(result))])
    }
}

/// Works for `seconds`, one second at a time, and heartbeats after each one.
async fn slow(info: &ActivityInfo, seconds: u32) -> Result<String, ExecuteError> {
    // A retried attempt starts where the last heartbeat of an earlier one left
    // off: Temporal hands back that heartbeat's details.
    let mut done: u32 = match info.heartbeat_details.first() {
        Some(details) => decode(Some(details))?,
        None => 0,
    };
    while done < seconds {
        // `wait-for` is async, so waiting suspends this call without blocking
        // the host, or the plugin's other calls.
        monotonic_clock::wait_for(1_000_000_000).await;
        done += 1;

        // A heartbeat is also how the workload learns of a cancellation:
        // Temporal tells no one until the next heartbeat.
        let details = vec![encode(&Value::from(done))];
        match context::heartbeat(info.task_token.clone(), details).await {
            Ok(false) => {}
            // Cancelled, or Temporal no longer knows this attempt (it timed out
            // or finished). Either way, stop.
            Ok(true) | Err(HeartbeatError::NotFound) => {
                println!("`slow` stopped after {done} of {seconds} seconds");
                return Err(ExecuteError::Cancelled);
            }
            // Heartbeats are best effort: carry on and try again next second.
            Err(err) => println!("heartbeat failed, carrying on: {err:?}"),
        }
    }
    Ok(format!("worked for {seconds} seconds"))
}

/// A JSON payload, marked `json/plain` so Temporal's SDKs can decode it.
fn encode(value: &Value) -> Payload {
    Payload {
        metadata: vec![("encoding".to_string(), b"json/plain".to_vec())],
        data: value.to_string().into_bytes(),
    }
}

/// Decodes a JSON payload. Bad input fails the activity for good, since a
/// retry would get the same input.
fn decode<T: serde::de::DeserializeOwned>(payload: Option<&Payload>) -> Result<T, ExecuteError> {
    let payload = payload.ok_or_else(|| failure("missing argument", "BadInput", true))?;
    serde_json::from_slice(&payload.data)
        .map_err(|err| failure(&format!("bad argument: {err}"), "BadInput", true))
}

fn failure(message: &str, error_type: &str, non_retryable: bool) -> ExecuteError {
    ExecuteError::Failed(Failure {
        message: message.to_string(),
        error_type: error_type.to_string(),
        non_retryable,
        details: vec![],
    })
}

bindings::export!(Component with_types_in bindings);
