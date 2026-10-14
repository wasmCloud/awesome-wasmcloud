//! The worker: polls Temporal's task queues and hands each activity task to a
//! workload.
//!
//! Everything here runs on the component-model executor of the `wasi:cli/run`
//! task, one poll loop per task queue, side by side on one thread. A loop that
//! takes a task spawns its execution and goes straight back to polling, so
//! activities from all the queues run concurrently, up to `max-concurrent` at
//! once. A loop takes a slot before it polls, so a task is never taken from
//! Temporal while there is no room to run it. Nor does it poll while no
//! workload exports `activity`: the host starts the plugin before it deploys
//! any workload, and a task taken then would fail, using up a retry.
//!
//! A task goes to one of the running workloads that export
//! `wasmcloud:temporal/activity`, in turn. The call is routed by the
//! `workload-call` target handle held while it starts.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Poll, Waker};

use crate::bindings::wasi::clocks::monotonic_clock;
use crate::bindings::wasmcloud::host::workload_call::{self, Target};
use crate::bindings::wasmcloud::temporal::activity;
use crate::bindings::wasmcloud::temporal::types::{ExecuteError, Failure, Payload};
use crate::config::Settings;
use crate::log;
use crate::temporal::{Client, Task};

/// The `activity` interface as `workload-call` names it: the plugin's own
/// import, with its version.
pub const ACTIVITY: &str = "wasmcloud:temporal/activity@0.1.0";

/// How long to wait after the first failed poll. Each failure in a row doubles
/// it, up to [`BACKOFF_MAX_NS`].
const BACKOFF_START_NS: u64 = 1_000_000_000;
const BACKOFF_MAX_NS: u64 = 30_000_000_000;

/// How often to look again for a workload that exports `activity`.
const WORKLOAD_WAIT_NS: u64 = 1_000_000_000;

/// What the poll loops and the activities they start share.
struct Worker {
    client: Client,
    /// How many more activities may start.
    free: Cell<usize>,
    /// The poll loops waiting for a slot, oldest first.
    waiting: RefCell<VecDeque<Rc<Waiter>>>,
    /// Which callable workload the next task goes to.
    next: Cell<usize>,
}

/// A poll loop waiting for a slot.
#[derive(Default)]
struct Waiter {
    /// Set when a freed slot was handed to this waiter.
    granted: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

/// One slot of `max-concurrent`, given back when dropped.
struct Slot(Rc<Worker>);

impl Drop for Slot {
    /// Hands the slot straight to the oldest waiter, so a loop that frees a
    /// slot and asks again at once, after an empty poll, queues behind the
    /// loops already waiting rather than taking it back.
    fn drop(&mut self) {
        let oldest = self.0.waiting.borrow_mut().pop_front();
        match oldest {
            Some(waiter) => {
                waiter.granted.set(true);
                if let Some(waker) = waiter.waker.take() {
                    waker.wake();
                }
            }
            None => self.0.free.set(self.0.free.get() + 1),
        }
    }
}

impl Worker {
    /// Waits for a free slot and takes it. A slot is free only while no loop
    /// waits, since a freed slot goes to a waiter first.
    async fn slot(self: &Rc<Self>) -> Slot {
        let free = self.free.get();
        if free > 0 {
            self.free.set(free - 1);
        } else {
            let waiter = Rc::new(Waiter::default());
            self.waiting.borrow_mut().push_back(Rc::clone(&waiter));
            poll_fn(|cx| {
                if waiter.granted.get() {
                    Poll::Ready(())
                } else {
                    *waiter.waker.borrow_mut() = Some(cx.waker().clone());
                    Poll::Pending
                }
            })
            .await;
        }
        Slot(Rc::clone(self))
    }
}

/// Polls every task queue until the plugin stops. Never returns.
pub async fn run(client: Client, settings: &Settings) {
    let worker = Rc::new(Worker {
        client,
        free: Cell::new(settings.max_concurrent),
        waiting: RefCell::new(VecDeque::new()),
        next: Cell::new(0),
    });
    for queue in &settings.task_queues {
        wit_bindgen::spawn_local(poll(Rc::clone(&worker), queue.clone()));
    }
    // The poll loops run on this task, and it ends when the host drops it.
    std::future::pending::<()>().await;
}

/// Takes tasks from one queue, for ever.
async fn poll(worker: Rc<Worker>, queue: String) {
    let mut backoff = BACKOFF_START_NS;
    loop {
        wait_for_workload(&queue).await;
        let slot = worker.slot().await;
        match worker.client.poll(&queue).await {
            Ok(task) => {
                backoff = BACKOFF_START_NS;
                if let Some(task) = task {
                    wit_bindgen::spawn_local(execute(Rc::clone(&worker), task, slot));
                }
            }
            Err(err) => {
                drop(slot);
                log::warn(&format!(
                    "polling task queue {queue} failed, retrying in {}s: {err:?}",
                    backoff / 1_000_000_000
                ));
                monotonic_clock::wait_for(backoff).await;
                backoff = (backoff * 2).min(BACKOFF_MAX_NS);
            }
        }
    }
}

/// Waits until some running workload exports `activity`.
async fn wait_for_workload(queue: &str) {
    if !workloads().is_empty() {
        return;
    }
    log::info(&format!(
        "no workload exports {ACTIVITY} yet, so task queue {queue} waits for one"
    ));
    while workloads().is_empty() {
        monotonic_clock::wait_for(WORKLOAD_WAIT_NS).await;
    }
}

/// The running workloads that export `activity`.
fn workloads() -> Vec<String> {
    workload_call::callable()
        .into_iter()
        .filter(|(_, interfaces)| interfaces.iter().any(|interface| interface == ACTIVITY))
        .map(|(id, _)| id)
        .collect()
}

/// Runs one task in a workload and reports the outcome to Temporal. The slot
/// is given back once the report is done.
async fn execute(worker: Rc<Worker>, task: Task, _slot: Slot) {
    let token = task.info.task_token.clone();
    let name = format!(
        "activity {} ({}) of workflow {}",
        task.info.activity_type, task.info.activity_id, task.info.workflow_id
    );

    let client = &worker.client;
    let reported = match call(&worker, task).await {
        Ok(output) => client.complete(token, output).await,
        Err(ExecuteError::Failed(failure)) => client.fail(token, failure).await,
        Err(ExecuteError::Cancelled) => match client.cancelled(token.clone()).await {
            // Temporal records a cancellation only once it has asked for one.
            // Otherwise the workload stopped for its own reasons, which is a
            // failure Temporal may retry.
            Ok(false) => {
                let message = "the workload stopped as cancelled, \
                               but Temporal had not asked to cancel the activity";
                client.fail(token, retryable(message.to_string())).await
            }
            other => other.map(|_| ()),
        },
        Err(ExecuteError::Host(detail)) => {
            let detail = detail.as_deref().unwrap_or("no detail");
            let message = format!("the wasmCloud host could not call the workload: {detail}");
            client.fail(token, retryable(message)).await
        }
    };
    // Temporal times the attempt out and retries it, as if this worker had
    // crashed.
    if let Err(err) = reported {
        log::warn(&format!("could not report {name} to Temporal: {err:?}"));
    }
}

/// Calls `activity.execute` on the next callable workload in turn.
async fn call(worker: &Worker, task: Task) -> Result<Vec<Payload>, ExecuteError> {
    let workloads = workloads();
    let start = worker.next.get();
    worker.next.set(start.wrapping_add(1));
    for offset in 0..workloads.len() {
        let id = &workloads[start.wrapping_add(offset) % workloads.len()];
        // `open` routes the calls this task makes on `activity` to `id` while
        // the handle lives, and answers `none` when the workload stopped since
        // `callable`. The call starts right after it, with nothing in between
        // that could yield: activities run side by side on one task, and each
        // must start its call while its own handle is the one in effect.
        if let Some(_target) = Target::open(id, ACTIVITY) {
            return activity::execute(task.info, task.input).await;
        }
    }
    Err(ExecuteError::Failed(retryable(
        "no workload exports wasmcloud:temporal/activity".to_string(),
    )))
}

/// A failure Temporal retries, by the activity's retry policy.
fn retryable(message: String) -> Failure {
    Failure {
        message,
        error_type: String::new(),
        non_retryable: false,
        details: Vec::new(),
    }
}
