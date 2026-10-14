//! The guest half of the plugin: everything that talks to the host and to
//! workloads, as opposed to the broker.
//!
//! It keeps which workloads are bound and what queues each wants, works out
//! which of them can be called right now, tells the client to consume exactly
//! those queues, and routes each delivery the client sends back to the handler
//! of the right workload.
//!
//! This code runs outside the tokio runtime, on the component-model executor
//! of the `wasi:cli/run` task. That is where a plugin may call into a
//! workload, and where a call is routed by the `target` handle held while it
//! starts.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

use tokio::sync::mpsc;

use crate::bindings::exports::wasmcloud::amqp::types::Error;
use crate::bindings::wasi::clocks::monotonic_clock;
use crate::bindings::wasmcloud::amqp::handler;
use crate::bindings::wasmcloud::amqp::types::Outcome;
use crate::bindings::wasmcloud::host::types::CallError;
use crate::bindings::wasmcloud::host::workload_call::{self, Target};
use crate::client::{Command, Delivery, Subscriptions};
use crate::config::Subscription;
use crate::log;

/// The `handler` interface as `workload-call` names it: the plugin's own import,
/// with its version.
pub const HANDLER: &str = "wasmcloud:amqp/handler@0.1.0";

/// How often to check which bound workloads can be called. A workload that
/// starts or stops is noticed within this long.
const RECONCILE_INTERVAL_NS: u64 = 200_000_000;

struct Channels {
    commands: mpsc::UnboundedSender<Command>,
    receiver: Mutex<Option<mpsc::UnboundedReceiver<Command>>>,
}

/// The channel the plugin's own calls and hooks use to reach the client. It
/// exists from the first call, even before `run` starts the client, so nothing
/// that arrives early is lost.
fn channels() -> &'static Channels {
    static CHANNELS: OnceLock<Channels> = OnceLock::new();
    CHANNELS.get_or_init(|| {
        let (commands, receiver) = mpsc::unbounded_channel();
        Channels {
            commands,
            receiver: Mutex::new(Some(receiver)),
        }
    })
}

/// Sends a command to the client.
pub fn send(command: Command) -> Result<(), Error> {
    channels()
        .commands
        .send(command)
        .map_err(|_| Error::Unavailable("the plugin is shutting down".to_string()))
}

/// The receiving end of the commands, for the client. There is only one.
pub fn take_commands() -> Option<mpsc::UnboundedReceiver<Command>> {
    lock(&channels().receiver).take()
}

/// The workloads that are bound to the plugin and want to consume queues.
static BOUND: Mutex<BTreeMap<String, Subscription>> = Mutex::new(BTreeMap::new());

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Records that a workload wants these queues, or none.
pub fn bind(workload: String, subscription: Option<Subscription>) {
    let mut bound = lock(&BOUND);
    match subscription {
        Some(subscription) => {
            bound.insert(workload, subscription);
        }
        None => {
            bound.remove(&workload);
        }
    }
}

/// Forgets a workload, and tells the client to release what it held.
pub fn unbind(workload: &str) {
    lock(&BOUND).remove(workload);
    // The client may already be gone; then there is nothing to release.
    let _ = send(Command::Forget(workload.to_string()));
}

/// Runs until the plugin stops: feeds the client the set of queues to consume,
/// and delivers what the client receives.
pub async fn run(mut deliveries: mpsc::UnboundedReceiver<Delivery>) {
    wit_bindgen::spawn_local(reconcile());
    while let Some(delivery) = deliveries.recv().await {
        wit_bindgen::spawn_local(deliver(delivery));
    }
}

/// Keeps the client consuming the queues of the workloads that can be called,
/// and only those.
///
/// A workload is bound before it is running, and a delivery for one that is
/// not running cannot be handled. So a bound workload's queues are consumed
/// from the moment it can be called until it cannot, and a message is never
/// taken from the broker while there is no one to hand it to.
async fn reconcile() {
    let mut sent: Option<Subscriptions> = None;
    loop {
        let wanted = callable_subscriptions();
        if sent.as_ref() != Some(&wanted) {
            if send(Command::Subscriptions(wanted.clone())).is_err() {
                return;
            }
            sent = Some(wanted);
        }
        monotonic_clock::wait_for(RECONCILE_INTERVAL_NS).await;
    }
}

/// The subscriptions of the bound workloads that are running and export the
/// handler.
fn callable_subscriptions() -> Subscriptions {
    let callable = workload_call::callable();
    lock(&BOUND)
        .iter()
        .filter(|(workload, _)| {
            callable.iter().any(|(id, interfaces)| {
                id == *workload && interfaces.iter().any(|interface| interface == HANDLER)
            })
        })
        .map(|(workload, subscription)| (workload.clone(), subscription.clone()))
        .collect()
}

/// Hands one delivery to its workload and passes the answer to the client.
async fn deliver(delivery: Delivery) {
    let Delivery {
        workload,
        consumer,
        tag,
        message,
    } = delivery;

    // `open` routes the calls this task makes on the handler to `workload`
    // for as long as the handle lives, and answers `none` when the workload is
    // not running or does not serve the handler. The call is started right
    // after it, with nothing in between that could yield: deliveries run side
    // by side on one task, and each must start its call while its own handle is
    // the one in effect.
    let outcome = match Target::open(&workload, HANDLER) {
        None => Outcome::Nack(true),
        Some(_target) => match handler::handle_delivery(message).await {
            Ok(outcome) => outcome,
            Err(error) => unhandled(&workload, error),
        },
    };

    // The client may have stopped consuming meanwhile, and then the broker has
    // already put the message back, so there is nothing to settle.
    let _ = send(Command::Settle {
        consumer,
        tag,
        outcome,
    });
}

/// What to do with a delivery whose handler call did not complete.
///
/// If the workload could not be reached, the message was not looked at, so it
/// goes back on the queue. If it was reached and the call failed, the message
/// may be what broke it, so it is rejected without requeueing: a message that
/// kept failing would otherwise come back for ever. A queue with a dead-letter
/// exchange keeps it.
fn unhandled(workload: &str, error: CallError) -> Outcome {
    match error {
        CallError::NoTarget(_) | CallError::NotRunning(_) | CallError::NotExported(_) => {
            Outcome::Nack(true)
        }
        CallError::Failed(detail) | CallError::Other(detail) => {
            log::warn(&format!(
                "the handler of workload {workload} failed on a delivery, which is rejected: {}",
                detail.as_deref().unwrap_or("no detail")
            ));
            Outcome::Reject(false)
        }
    }
}
