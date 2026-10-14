//! A wasmCloud host component plugin that turns workloads into Temporal
//! activity workers.
//!
//! The plugin polls Temporal's task queues and hands each activity task to a
//! workload that exports `wasmcloud:temporal/activity`. A workload in the
//! middle of an activity imports `wasmcloud:temporal/context` to send
//! heartbeats and learn whether the activity was cancelled. See the README for
//! how to deploy it.
//!
//! # How it runs
//!
//! The plugin is one long-lived instance, and a WASI component has no threads,
//! so everything shares one thread on the component-model executor. No tokio
//! is needed: every wait, whether a long poll, a call into a workload or a
//! backoff, is a component-model wait, so while one is pending the plugin
//! serves other calls.
//!
//! - The `wasi:cli/run` task below reads the configuration, connects to
//!   Temporal, and runs the [`worker`]: one poll loop per task queue, and one
//!   task per activity being run.
//! - `context.heartbeat` runs as its own task for each call from a workload,
//!   and shares the run task's client.
//!
//! # Stopping
//!
//! The host gives the run task no signal when it stops. It stops serving the
//! plugin and drops the instance, so the run task, its open polls and any
//! activity still running are dropped where they wait, without running further
//! and without a panic. An activity dropped that way is never reported, and
//! Temporal retries it on another worker once the attempt times out (its
//! start-to-close or heartbeat timeout), exactly as when a worker process
//! dies. An activity that has reported is finished, so the one cost of a stop
//! is that retry.

mod bindings {
    wit_bindgen::generate!({
        world: "plugin",
        generate_all,
    });
}

mod config;
mod convert;
mod log;
mod temporal;
mod transport;
mod worker;

use std::sync::OnceLock;

use bindings::exports::wasi::cli::run::Guest as RunGuest;
use bindings::exports::wasmcloud::temporal::context::Guest as ContextGuest;
use bindings::exports::wasmcloud::temporal::types::{HeartbeatError, Payload};
use config::Settings;

/// The connection to Temporal, set once the run task has started.
static CLIENT: OnceLock<temporal::Client> = OnceLock::new();

struct Component;

impl ContextGuest for Component {
    async fn heartbeat(task_token: Vec<u8>, details: Vec<Payload>) -> Result<bool, HeartbeatError> {
        let client = CLIENT.get().ok_or_else(|| {
            HeartbeatError::Unavailable("the plugin is still starting".to_string())
        })?;
        client
            .heartbeat(task_token, details)
            .await
            .map_err(|err| match err {
                temporal::Error::NotFound => HeartbeatError::NotFound,
                temporal::Error::Unavailable(reason) => HeartbeatError::Unavailable(reason),
                temporal::Error::Other(reason) => HeartbeatError::Other(reason),
            })
    }
}

impl RunGuest for Component {
    async fn run() -> Result<(), ()> {
        let settings = Settings::load()
            .map_err(|reason| log::error(&format!("the plugin is not configured: {reason}")))?;
        log::info(&format!(
            "polling task queues {} in namespace {} at {}",
            settings.task_queues.join(", "),
            settings.namespace,
            settings.address
        ));
        let client = CLIENT
            .get_or_init(|| temporal::Client::new(&settings))
            .clone();
        worker::run(client, &settings).await;
        Ok(())
    }
}

bindings::export!(Component with_types_in bindings);
