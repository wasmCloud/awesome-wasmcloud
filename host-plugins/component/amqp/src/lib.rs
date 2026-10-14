//! A wasmCloud host component plugin that gives workloads an AMQP 0-9-1
//! client, for RabbitMQ and other brokers that speak the protocol.
//!
//! Workloads import `wasmcloud:amqp/publisher` to publish. A workload that
//! exports `wasmcloud:amqp/handler` and names queues in its interface config
//! has each message from them delivered to the handler, and its return value
//! settles the message. See the README for how to deploy it.
//!
//! # How it runs
//!
//! The plugin is one long-lived instance, and a WASI component has no threads,
//! so everything shares one thread, in two halves:
//!
//! - **tokio** runs the client ([`client`]), which owns the connection to the
//!   broker. amqprs is built on tokio, and tokio's WASI networking runs on a
//!   current-thread runtime.
//! - The **component-model executor** runs everything that talks to the host
//!   ([`dispatch`]) and the exported functions that workloads call.
//!
//! tokio cannot simply be left running: `block_on` would hold the thread
//! for ever, and no call from a workload would be served. So the `wasi:cli/run`
//! task below runs tokio in short slices, and between slices yields to the
//! executor, which is when calls from workloads run. The two halves talk
//! through channels, which both sides can use without a runtime.

mod bindings {
    wit_bindgen::generate!({
        world: "plugin",
        generate_all,
        // The tests compare the plugin's own types.
        additional_derives: [PartialEq],
    });
}

mod client;
mod config;
mod convert;
mod dispatch;
mod log;

use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use bindings::exports::wasi::cli::run::Guest as RunGuest;
use bindings::exports::wasmcloud::amqp::publisher::Guest as PublisherGuest;
use bindings::exports::wasmcloud::amqp::types::{Error, Message};
use bindings::exports::wasmcloud::host::workload_lifecycle::{
    Guest as LifecycleGuest, WorkloadInfo,
};
use bindings::wasi::clocks::monotonic_clock;
use bindings::wasmcloud::host::identity;
use client::Command;
use config::Subscription;

/// The longest tokio runs before the executor gets a turn, so it bounds how
/// long a call from a workload can wait to start. It ends sooner when the
/// client has something to hand over.
const SLICE: Duration = Duration::from_millis(20);
/// How long the executor has, between slices, to run calls from workloads.
const YIELD_NS: u64 = 1_000_000;
/// How long to sleep when there is nothing to run.
const IDLE_NS: u64 = 3_600_000_000_000;

const AMQP_NAMESPACE: &str = "wasmcloud";
const AMQP_PACKAGE: &str = "amqp";

struct Component;

impl PublisherGuest for Component {
    async fn publish(exchange: String, routing_key: String, message: Message) -> Result<(), Error> {
        config::broker()
            .map_err(|reason| Error::Other(format!("the plugin is not configured: {reason}")))?;
        let outgoing = convert::outgoing(exchange, routing_key, message)?;

        let (reply, response) = oneshot::channel();
        dispatch::send(Command::Publish {
            workload: identity::get_workload_id(),
            outgoing: Box::new(outgoing),
            reply,
        })?;
        response.await.unwrap_or_else(|_| {
            Err(Error::Unavailable(
                "the plugin stopped before the broker answered".to_string(),
            ))
        })
    }
}

impl LifecycleGuest for Component {
    async fn on_workload_bind(workload: WorkloadInfo) -> Result<(), String> {
        config::broker()
            .map_err(|reason| format!("the wasmcloud:amqp plugin is not configured: {reason}"))?;

        // A workload can spread its `wasmcloud:amqp` config over several
        // entries, such as one per interface. They are one workload's config.
        let ours = workload.interfaces.iter().filter(|binding| {
            binding.namespace == AMQP_NAMESPACE && binding.package == AMQP_PACKAGE
        });
        let handles = ours
            .clone()
            .any(|binding| binding.interfaces.iter().any(|name| name == "handler"));
        let entries: Vec<(String, String)> = ours
            .flat_map(|binding| binding.config.iter().cloned())
            .collect();

        let subscription = Subscription::from_entries(&entries)
            .map_err(|reason| format!("invalid wasmcloud:amqp config: {reason}"))?;
        if subscription.is_some() && !handles {
            return Err(
                "the workload's wasmcloud:amqp config sets `queues`, but its entry does not \
                 list the `handler` interface, so nothing could receive the messages"
                    .to_string(),
            );
        }
        dispatch::bind(workload.id, subscription);
        Ok(())
    }

    async fn on_workload_unbind(id: String) {
        dispatch::unbind(&id);
    }
}

impl RunGuest for Component {
    async fn run() -> Result<(), ()> {
        let broker = match config::broker() {
            Ok(broker) => broker.clone(),
            Err(reason) => {
                log::error(&format!("the plugin is not configured: {reason}"));
                // Stay up. A plugin that exits is restarted, only to fail the
                // same way, and every workload is refused at bind time with
                // this same reason anyway.
                loop {
                    monotonic_clock::wait_for(IDLE_NS).await;
                }
            }
        };
        let Some(commands) = dispatch::take_commands() else {
            log::error("the plugin was started twice");
            return Err(());
        };

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| log::error(&format!("could not start tokio: {err}")))?;
        let (deliveries_tx, deliveries_rx) = mpsc::unbounded_channel();
        runtime.spawn(client::run(broker, commands, deliveries_tx));
        wit_bindgen::spawn_local(dispatch::run(deliveries_rx));

        loop {
            runtime.block_on(async {
                tokio::select! {
                    () = tokio::time::sleep(SLICE) => {}
                    () = client::WAKE.notified() => {}
                }
            });
            monotonic_clock::wait_for(YIELD_NS).await;
        }
    }
}

bindings::export!(Component with_types_in bindings);
