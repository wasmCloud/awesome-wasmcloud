//! The tokio half of the plugin: everything that talks to the broker.
//!
//! One task, [`run`], owns the connection and every channel on it. The rest of
//! the plugin reaches it with [`Command`]s and hears back through the replies
//! inside them and through [`Delivery`]s. No amqprs value ever leaves this
//! task. That matters: amqprs closes a channel or connection it drops by
//! spawning a task, which panics outside a tokio runtime, and the guest side of
//! the plugin runs outside one.
//!
//! The task keeps a [`Session`] for each live connection and replaces it when
//! the connection is lost. Channels are never shared between workloads, so one
//! workload's mistake, such as publishing to an exchange that does not exist
//! (which makes the broker close the channel), cannot disturb another's:
//!
//! - each workload that publishes gets its own channel, in confirm mode;
//! - each queue a workload consumes gets its own channel and consumer.

use std::collections::BTreeMap;
use std::net::{IpAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use amqprs::callbacks::{ChannelCallback, ConnectionCallback};
use amqprs::channel::{
    BasicAckArguments, BasicConsumeArguments, BasicNackArguments, BasicPublishArguments,
    BasicQosArguments, BasicRejectArguments, Channel, ConfirmSelectArguments,
    QueueDeclareArguments,
};
use amqprs::connection::{Connection, OpenConnectionArguments};
use amqprs::consumer::AsyncConsumer;
use amqprs::{Ack, BasicProperties, Cancel, Close, CloseChannel, Deliver, Nack, Return};
use async_trait::async_trait;
use tokio::sync::{Notify, mpsc, oneshot};

use crate::bindings::exports::wasmcloud::amqp::types::Error;
use crate::bindings::wasmcloud::amqp::types::{Delivery as WitDelivery, Outcome};
use crate::config::{Broker, Subscription};
use crate::convert::{self, Outgoing};
use crate::log;

/// How long to wait for the broker to accept a connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long one exchange with the broker, such as opening a channel or starting
/// a consumer, may take. amqprs has no timeouts of its own, and the client task
/// serves every workload, so a broker that stops answering must not be able to
/// hold it.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(15);
/// How long closing a channel may take. Closing is housekeeping, so it waits
/// less than the operations above.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
/// How often the session checks its channels and retries consumers.
const HEAL_INTERVAL: Duration = Duration::from_millis(500);
/// How long a publish waits for the broker to confirm it.
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a connection must stay up before the next failure restarts the
/// reconnect delay from its minimum.
const STABLE_AFTER: Duration = Duration::from_secs(30);

/// Rung when the client has something for the guest side to pick up, so the
/// slice in `lib.rs` can end early instead of running out its clock.
pub static WAKE: Notify = Notify::const_new();

/// The queues each workload consumes, by workload id.
pub type Subscriptions = BTreeMap<String, Subscription>;

/// The answer to a publish, which the client sends when the broker confirms it.
pub type Reply = oneshot::Sender<Result<(), Error>>;

/// What the rest of the plugin asks of the client.
pub enum Command {
    /// Publish on behalf of a workload.
    Publish {
        workload: String,
        /// Boxed because a message with all its properties is far larger than
        /// the other commands.
        outgoing: Box<Outgoing>,
        reply: Reply,
    },
    /// Consume exactly these queues, for these workloads, and no others.
    Subscriptions(Subscriptions),
    /// Tell the broker what became of a delivery.
    Settle {
        consumer: u64,
        tag: u64,
        outcome: Outcome,
    },
    /// A workload is gone: release its channels.
    Forget(String),
}

/// A message the broker delivered, on its way to a workload's handler.
pub struct Delivery {
    pub workload: String,
    /// Which consumer received it, so [`Command::Settle`] finds its channel.
    /// Not the AMQP channel number: the broker reuses those, and a late
    /// settlement must never reach a different consumer's delivery.
    pub consumer: u64,
    /// The broker's tag for the delivery, unique within the consumer's channel.
    pub tag: u64,
    pub message: WitDelivery,
}

/// Why a loop in this module stopped.
#[derive(PartialEq, Eq)]
enum Ended {
    /// The commands channel closed: the plugin is shutting down.
    Shutdown,
    /// The connection is gone.
    Lost,
}

/// Connects to the broker, serves the plugin until the connection is lost, and
/// connects again, for as long as the plugin runs.
pub async fn run(
    broker: Broker,
    mut commands: mpsc::UnboundedReceiver<Command>,
    deliveries: mpsc::UnboundedSender<Delivery>,
) {
    let mut desired = Subscriptions::new();
    let mut backoff = Backoff::new();

    loop {
        match connect(&broker).await {
            Ok(connection) => {
                log::info(&format!(
                    "connected to the broker at {}:{}",
                    broker.host, broker.port
                ));
                let up_since = Instant::now();
                let mut session = Session::new(connection, deliveries.clone());
                let ended = session.serve(&mut commands, &mut desired).await;
                session.fail_pending();
                if ended == Ended::Shutdown {
                    return;
                }
                log::warn("lost the connection to the broker; reconnecting");
                if up_since.elapsed() >= STABLE_AFTER {
                    backoff.reset();
                }
            }
            Err(reason) => log::warn(&format!(
                "could not connect to the broker at {}:{}: {reason}",
                broker.host, broker.port
            )),
        }

        if idle(backoff.next(), &mut commands, &mut desired).await {
            return;
        }
    }
}

/// Opens a connection to the broker.
async fn connect(broker: &Broker) -> Result<Connection, String> {
    // tokio resolves a name on a helper thread, and WASI has none, so a name
    // is resolved here and amqprs is handed an address. The host only answers
    // for the names in the plugin's `allowedHosts` or `allowedIpNameLookups`.
    let addresses: Vec<_> = (broker.host.as_str(), broker.port)
        .to_socket_addrs()
        .map_err(|err| format!("could not resolve `{}`: {err}", broker.host))?
        .collect();

    let mut failure = format!("`{}` has no address", broker.host);
    for address in addresses {
        let host = match address.ip() {
            IpAddr::V4(ip) => ip.to_string(),
            IpAddr::V6(ip) => format!("[{ip}]"),
        };
        let arguments =
            OpenConnectionArguments::new(&host, address.port(), &broker.username, &broker.password)
                .virtual_host(&broker.vhost)
                .connection_name("wasmcloud-amqp-plugin")
                .finish();

        match tokio::time::timeout(CONNECT_TIMEOUT, Connection::open(&arguments)).await {
            Ok(Ok(connection)) => {
                connection
                    .register_callback(ConnectionEvents)
                    .await
                    .map_err(|err| format!("{address}: {err}"))?;
                return Ok(connection);
            }
            Ok(Err(err)) => failure = format!("{address}: {err}"),
            Err(_) => failure = format!("{address}: no answer within {CONNECT_TIMEOUT:?}"),
        }
    }
    Err(failure)
}

/// Waits out the delay before the next attempt to connect, and returns whether
/// the plugin shut down meanwhile. Commands that arrive during the wait are
/// answered as well as they can be without a broker.
async fn idle(
    delay: Duration,
    commands: &mut mpsc::UnboundedReceiver<Command>,
    desired: &mut Subscriptions,
) -> bool {
    let timer = tokio::time::sleep(delay);
    tokio::pin!(timer);
    loop {
        tokio::select! {
            () = &mut timer => return false,
            command = commands.recv() => match command {
                None => return true,
                Some(Command::Publish { reply, .. }) => {
                    let _ = reply.send(Err(Error::Unavailable(
                        "the plugin is not connected to the broker".to_string(),
                    )));
                    WAKE.notify_one();
                }
                Some(Command::Subscriptions(wanted)) => *desired = wanted,
                // Whatever these were about went down with the connection.
                Some(Command::Settle { .. } | Command::Forget(_)) => {}
            },
        }
    }
}

/// Everything tied to one connection. When the connection goes, so does this.
struct Session {
    connection: Connection,
    deliveries: mpsc::UnboundedSender<Delivery>,
    /// One channel per workload that has published.
    publishers: BTreeMap<String, Publisher>,
    consumers: BTreeMap<u64, Consumer>,
    /// Consumers that could not start, and when to try again.
    failures: BTreeMap<(String, String), Failure>,
    next_consumer: u64,
}

struct Publisher {
    channel: Channel,
    state: Arc<ChannelState>,
}

impl Publisher {
    /// Closes the channel, and answers the publishes still waiting on it, which
    /// the broker will now never confirm.
    async fn retire(self) {
        self.state.confirms.fail_all(&Error::Unavailable(
            "the publishing channel was closed".to_string(),
        ));
        close(self.channel).await;
    }
}

struct Consumer {
    workload: String,
    queue: String,
    channel: Channel,
    state: Arc<ChannelState>,
}

struct Failure {
    retry_at: Instant,
    backoff: Backoff,
}

impl Session {
    fn new(connection: Connection, deliveries: mpsc::UnboundedSender<Delivery>) -> Self {
        Self {
            connection,
            deliveries,
            publishers: BTreeMap::new(),
            consumers: BTreeMap::new(),
            failures: BTreeMap::new(),
            next_consumer: 0,
        }
    }

    /// Serves commands until the connection is lost or the plugin shuts down.
    async fn serve(
        &mut self,
        commands: &mut mpsc::UnboundedReceiver<Command>,
        desired: &mut Subscriptions,
    ) -> Ended {
        // Subscribed once, up front: a failure that happened between two
        // subscriptions would be missed.
        let watcher = self.connection.clone();
        let lost = watcher.listen_network_io_failure();
        tokio::pin!(lost);

        let mut tick = tokio::time::interval(HEAL_INTERVAL);
        self.heal(desired).await;
        loop {
            tokio::select! {
                _ = &mut lost => return Ended::Lost,
                command = commands.recv() => match command {
                    None => return Ended::Shutdown,
                    Some(command) => self.handle(command, desired).await,
                },
                _ = tick.tick() => self.heal(desired).await,
            }
        }
    }

    async fn handle(&mut self, command: Command, desired: &mut Subscriptions) {
        match command {
            Command::Publish {
                workload,
                outgoing,
                reply,
            } => self.publish(&workload, *outgoing, reply).await,
            Command::Subscriptions(wanted) => {
                *desired = wanted;
                self.heal(desired).await;
            }
            Command::Settle {
                consumer,
                tag,
                outcome,
            } => self.settle(consumer, tag, outcome).await,
            Command::Forget(workload) => self.forget(&workload).await,
        }
    }

    async fn publish(&mut self, workload: &str, outgoing: Outgoing, reply: Reply) {
        let publisher = match self.publisher(workload).await {
            Ok(publisher) => publisher,
            Err(err) => {
                let _ = reply.send(Err(err));
                WAKE.notify_one();
                return;
            }
        };

        // Expected before it is sent, so a confirm cannot beat the record of it.
        let sequence = publisher.state.confirms.expect(reply);
        let arguments = BasicPublishArguments::new(&outgoing.exchange, &outgoing.routing_key);
        let sent = publisher
            .channel
            .basic_publish(outgoing.properties, outgoing.body, arguments)
            .await;
        if let Err(err) = sent {
            // The broker numbers the publishes it receives, and this one never
            // reached it, so every later confirm on this channel would be
            // matched to the wrong publish. Retire the channel.
            publisher.state.dead.store(true, Ordering::Relaxed);
            if let Some(reply) = publisher.state.confirms.withdraw(sequence) {
                let _ = reply.send(Err(Error::Unavailable(format!(
                    "could not send the message: {err}"
                ))));
                WAKE.notify_one();
            }
        }
    }

    /// The workload's publishing channel, opened on first use and again if the
    /// broker has closed it.
    async fn publisher(&mut self, workload: &str) -> Result<&Publisher, Error> {
        let usable = self
            .publishers
            .get(workload)
            .is_some_and(|publisher| publisher.state.alive(&publisher.channel));
        if !usable {
            if let Some(publisher) = self.publishers.remove(workload) {
                publisher.retire().await;
            }
            let publisher = tokio::time::timeout(OPERATION_TIMEOUT, self.open_publisher(workload))
                .await
                .map_err(|_| {
                    Error::Unavailable("the broker did not open a channel in time".to_string())
                })??;
            self.publishers.insert(workload.to_string(), publisher);
        }
        self.publishers
            .get(workload)
            .ok_or_else(|| Error::Other("the publishing channel vanished".to_string()))
    }

    async fn open_publisher(&self, workload: &str) -> Result<Publisher, Error> {
        let unavailable = |what: &str| {
            let what = what.to_string();
            move |err: amqprs::error::Error| {
                Error::Unavailable(format!("could not {what} for publishing: {err}"))
            }
        };
        let state = Arc::new(ChannelState::new(format!(
            "the publisher for workload {workload}"
        )));
        let channel = self
            .connection
            .open_channel(None)
            .await
            .map_err(unavailable("open a channel"))?;
        channel
            .register_callback(ChannelEvents(Arc::clone(&state)))
            .await
            .map_err(unavailable("watch the channel"))?;
        // From here the broker confirms every publish, which is what lets a
        // publish report that the broker took the message.
        channel
            .confirm_select(ConfirmSelectArguments::new(false))
            .await
            .map_err(unavailable("turn on confirms"))?;
        Ok(Publisher { channel, state })
    }

    async fn settle(&mut self, consumer: u64, tag: u64, outcome: Outcome) {
        // A consumer that is gone had its unsettled messages requeued by the
        // broker, so there is nothing left to settle.
        let Some(consumer) = self.consumers.get(&consumer) else {
            return;
        };
        let channel = &consumer.channel;
        let sent = match outcome {
            Outcome::Ack => channel.basic_ack(BasicAckArguments::new(tag, false)).await,
            Outcome::Nack(requeue) => {
                channel
                    .basic_nack(BasicNackArguments::new(tag, false, requeue))
                    .await
            }
            Outcome::Reject(requeue) => {
                channel
                    .basic_reject(BasicRejectArguments::new(tag, requeue))
                    .await
            }
        };
        if let Err(err) = sent {
            log::warn(&format!(
                "could not settle a delivery from queue `{}`: {err}",
                consumer.queue
            ));
        }
    }

    /// Releases everything a workload held.
    async fn forget(&mut self, workload: &str) {
        if let Some(publisher) = self.publishers.remove(workload) {
            publisher.retire().await;
        }
        let ids: Vec<u64> = self
            .consumers
            .iter()
            .filter(|(_, consumer)| consumer.workload == workload)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            if let Some(consumer) = self.consumers.remove(&id) {
                close(consumer.channel).await;
            }
        }
        self.failures.retain(|(owner, _), _| owner != workload);
    }

    /// Brings the consumers in line with `desired`: stops the ones nobody wants
    /// any more, and starts the ones that are missing, whether because they are
    /// new or because their channel died. Also sweeps up publishes the broker
    /// never confirmed.
    async fn heal(&mut self, desired: &Subscriptions) {
        for publisher in self.publishers.values() {
            publisher.state.confirms.expire(CONFIRM_TIMEOUT);
        }
        let dead: Vec<String> = self
            .publishers
            .iter()
            .filter(|(_, publisher)| !publisher.state.alive(&publisher.channel))
            .map(|(workload, _)| workload.clone())
            .collect();
        for workload in dead {
            if let Some(publisher) = self.publishers.remove(&workload) {
                publisher.retire().await;
            }
        }

        let stale: Vec<u64> = self
            .consumers
            .iter()
            .filter(|(_, consumer)| {
                !wants(desired, &consumer.workload, &consumer.queue)
                    || !consumer.state.alive(&consumer.channel)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in stale {
            let Some(consumer) = self.consumers.remove(&id) else {
                continue;
            };
            if wants(desired, &consumer.workload, &consumer.queue) {
                log::warn(&format!(
                    "the consumer of queue `{}` stopped, and will be restarted",
                    consumer.queue
                ));
            }
            close(consumer.channel).await;
        }

        let now = Instant::now();
        for (workload, subscription) in desired {
            for queue in &subscription.queues {
                let running = self
                    .consumers
                    .values()
                    .any(|consumer| consumer.workload == *workload && consumer.queue == *queue);
                let key = (workload.clone(), queue.clone());
                let waiting = self
                    .failures
                    .get(&key)
                    .is_some_and(|failure| failure.retry_at > now);
                if running || waiting {
                    continue;
                }

                let started = tokio::time::timeout(
                    OPERATION_TIMEOUT,
                    self.start_consumer(workload, queue, subscription),
                )
                .await
                .unwrap_or_else(|_| Err("the broker did not answer in time".to_string()));
                match started {
                    Ok(()) => {
                        self.failures.remove(&key);
                        log::info(&format!("consuming queue `{queue}`"));
                    }
                    Err(reason) => {
                        let failure = self.failures.entry(key).or_insert_with(|| Failure {
                            retry_at: now,
                            backoff: Backoff::new(),
                        });
                        let delay = failure.backoff.next();
                        failure.retry_at = now + delay;
                        log::warn(&format!(
                            "could not consume queue `{queue}`: {reason}. Trying again in {delay:?}"
                        ));
                    }
                }
            }
        }
        self.failures
            .retain(|(workload, queue), _| wants(desired, workload, queue));
    }

    async fn start_consumer(
        &mut self,
        workload: &str,
        queue: &str,
        subscription: &Subscription,
    ) -> Result<(), String> {
        let id = self.next_consumer;
        self.next_consumer += 1;

        let state = Arc::new(ChannelState::new(format!(
            "the consumer of queue `{queue}`"
        )));
        // Returning early below drops the channel, which closes it.
        let channel = self
            .connection
            .open_channel(None)
            .await
            .map_err(|err| format!("could not open a channel: {err}"))?;
        channel
            .register_callback(ChannelEvents(Arc::clone(&state)))
            .await
            .map_err(|err| format!("could not watch the channel: {err}"))?;
        channel
            .basic_qos(BasicQosArguments::new(0, subscription.prefetch, false))
            .await
            .map_err(|err| format!("could not set the prefetch: {err}"))?;
        if subscription.declare {
            channel
                .queue_declare(QueueDeclareArguments::durable_client_named(queue))
                .await
                .map_err(|err| format!("could not declare the queue: {err}"))?;
        }

        let consumer = Forward {
            workload: workload.to_string(),
            queue: queue.to_string(),
            consumer: id,
            deliveries: self.deliveries.clone(),
        };
        let tag = format!("wasmcloud-amqp-{id}");
        channel
            .basic_consume(
                consumer,
                BasicConsumeArguments::new(queue, &tag)
                    .manual_ack(true)
                    .finish(),
            )
            .await
            .map_err(|err| format!("could not start consuming: {err}"))?;

        self.consumers.insert(
            id,
            Consumer {
                workload: workload.to_string(),
                queue: queue.to_string(),
                channel,
                state,
            },
        );
        Ok(())
    }

    /// The connection is gone, and with it every publish still waiting.
    fn fail_pending(&self) {
        for publisher in self.publishers.values() {
            publisher.state.confirms.fail_all(&Error::Unavailable(
                "the connection to the broker was lost".to_string(),
            ));
        }
    }
}

/// Closes a channel, giving up if the broker does not answer. A channel the
/// broker never confirmed closed is left for it to reclaim when the connection
/// ends.
async fn close(channel: Channel) {
    let _ = tokio::time::timeout(CLOSE_TIMEOUT, channel.close()).await;
}

fn wants(desired: &Subscriptions, workload: &str, queue: &str) -> bool {
    desired
        .get(workload)
        .is_some_and(|subscription| subscription.queues.iter().any(|name| name == queue))
}

/// Hands each delivery to the guest side, which routes it to the workload.
struct Forward {
    workload: String,
    queue: String,
    consumer: u64,
    deliveries: mpsc::UnboundedSender<Delivery>,
}

#[async_trait]
impl AsyncConsumer for Forward {
    async fn consume(
        &mut self,
        _channel: &Channel,
        deliver: Deliver,
        properties: BasicProperties,
        content: Vec<u8>,
    ) {
        let delivery = Delivery {
            workload: self.workload.clone(),
            consumer: self.consumer,
            tag: deliver.delivery_tag(),
            message: convert::incoming(&self.queue, &deliver, &properties, content),
        };
        // Unbounded, but the broker's prefetch limit bounds what is in flight.
        if self.deliveries.send(delivery).is_ok() {
            WAKE.notify_one();
        }
    }
}

/// What a channel's callbacks tell the client task about it.
struct ChannelState {
    /// What the channel is for, for log lines.
    purpose: String,
    /// Set once the channel can no longer be used: the broker closed it, or
    /// cancelled the consumer on it.
    dead: AtomicBool,
    /// The publishes the broker has yet to confirm. Used by publishers only.
    confirms: Confirms,
}

impl ChannelState {
    fn new(purpose: String) -> Self {
        Self {
            purpose,
            dead: AtomicBool::new(false),
            confirms: Confirms::default(),
        }
    }

    fn alive(&self, channel: &Channel) -> bool {
        !self.dead.load(Ordering::Relaxed) && channel.is_open()
    }
}

struct ChannelEvents(Arc<ChannelState>);

#[async_trait]
impl ChannelCallback for ChannelEvents {
    async fn close(
        &mut self,
        _channel: &Channel,
        close: CloseChannel,
    ) -> Result<(), amqprs::error::Error> {
        let reason = format!("{} {}", close.reply_code(), close.reply_text());
        log::warn(&format!(
            "the broker closed the channel of {}: {reason}",
            self.0.purpose
        ));
        self.0.dead.store(true, Ordering::Relaxed);
        self.0.confirms.fail_all(&Error::Rejected(format!(
            "the broker closed the channel: {reason}"
        )));
        Ok(())
    }

    async fn cancel(
        &mut self,
        _channel: &Channel,
        cancel: Cancel,
    ) -> Result<(), amqprs::error::Error> {
        log::warn(&format!(
            "the broker cancelled {} (consumer `{}`)",
            self.0.purpose,
            cancel.consumer_tag()
        ));
        self.0.dead.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn flow(
        &mut self,
        _channel: &Channel,
        _active: bool,
    ) -> Result<bool, amqprs::error::Error> {
        Ok(true)
    }

    async fn publish_ack(&mut self, _channel: &Channel, ack: Ack) {
        self.0
            .confirms
            .settle(ack.delivery_tag(), ack.mutiple(), &Ok(()));
    }

    async fn publish_nack(&mut self, _channel: &Channel, nack: Nack) {
        self.0.confirms.settle(
            nack.delivery_tag(),
            nack.multiple(),
            &Err(Error::Rejected(
                "the broker did not accept the message".to_string(),
            )),
        );
    }

    /// Never called: the plugin does not publish with `mandatory`.
    async fn publish_return(
        &mut self,
        _channel: &Channel,
        _ret: Return,
        _properties: BasicProperties,
        _content: Vec<u8>,
    ) {
    }
}

struct ConnectionEvents;

#[async_trait]
impl ConnectionCallback for ConnectionEvents {
    async fn close(
        &mut self,
        _connection: &Connection,
        close: Close,
    ) -> Result<(), amqprs::error::Error> {
        log::warn(&format!(
            "the broker closed the connection: {} {}",
            close.reply_code(),
            close.reply_text()
        ));
        Ok(())
    }

    async fn blocked(&mut self, _connection: &Connection, reason: String) {
        log::warn(&format!("the broker blocked the connection: {reason}"));
    }

    async fn unblocked(&mut self, _connection: &Connection) {
        log::info("the broker unblocked the connection");
    }

    async fn secret_updated(&mut self, _connection: &Connection) {}
}

/// The publishes on one confirm-mode channel that the broker has not yet
/// confirmed.
///
/// The broker numbers the publishes on a channel 1, 2, 3, and confirms them by
/// number, a confirm sometimes covering every number up to the one it names.
#[derive(Default)]
struct Confirms(Mutex<Pending>);

#[derive(Default)]
struct Pending {
    /// How many publishes have been sent on the channel.
    published: u64,
    waiting: BTreeMap<u64, (Reply, Instant)>,
}

impl Confirms {
    fn lock(&self) -> std::sync::MutexGuard<'_, Pending> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records a publish that is about to be sent, and returns its number.
    fn expect(&self, reply: Reply) -> u64 {
        let mut pending = self.lock();
        pending.published += 1;
        let sequence = pending.published;
        pending.waiting.insert(sequence, (reply, Instant::now()));
        sequence
    }

    /// Takes back a publish that could not be sent after all. The number it
    /// used stays spent, so the caller must stop using the channel.
    fn withdraw(&self, sequence: u64) -> Option<Reply> {
        self.lock()
            .waiting
            .remove(&sequence)
            .map(|(reply, _)| reply)
    }

    /// The broker answered for publish `tag`, or for every publish up to it.
    fn settle(&self, tag: u64, multiple: bool, result: &Result<(), Error>) {
        let replies: Vec<Reply> = {
            let mut pending = self.lock();
            let tags: Vec<u64> = if multiple {
                pending.waiting.range(..=tag).map(|(tag, _)| *tag).collect()
            } else {
                vec![tag]
            };
            tags.into_iter()
                .filter_map(|tag| pending.waiting.remove(&tag))
                .map(|(reply, _)| reply)
                .collect()
        };
        Self::answer(replies, result);
    }

    /// Answers every waiting publish with `error`.
    fn fail_all(&self, error: &Error) {
        let replies: Vec<Reply> = {
            let mut pending = self.lock();
            std::mem::take(&mut pending.waiting)
                .into_values()
                .map(|(reply, _)| reply)
                .collect()
        };
        Self::answer(replies, &Err(error.clone()));
    }

    /// Gives up on publishes that have waited longer than `limit`.
    fn expire(&self, limit: Duration) {
        let replies: Vec<Reply> = {
            let mut pending = self.lock();
            let overdue: Vec<u64> = pending
                .waiting
                .iter()
                .filter(|(_, (_, since))| since.elapsed() > limit)
                .map(|(tag, _)| *tag)
                .collect();
            overdue
                .into_iter()
                .filter_map(|tag| pending.waiting.remove(&tag))
                .map(|(reply, _)| reply)
                .collect()
        };
        Self::answer(
            replies,
            &Err(Error::Unavailable(format!(
                "the broker did not confirm the message within {limit:?}"
            ))),
        );
    }

    fn answer(replies: Vec<Reply>, result: &Result<(), Error>) {
        if replies.is_empty() {
            return;
        }
        for reply in replies {
            // The workload may have stopped waiting; nothing to do about that.
            let _ = reply.send(result.clone());
        }
        WAKE.notify_one();
    }
}

/// A delay that doubles, up to a ceiling, until it is reset.
#[derive(Clone, Copy)]
struct Backoff {
    next: Duration,
}

impl Backoff {
    const START: Duration = Duration::from_millis(500);
    const CEILING: Duration = Duration::from_secs(30);

    fn new() -> Self {
        Self { next: Self::START }
    }

    fn reset(&mut self) {
        self.next = Self::START;
    }

    /// The delay to wait now. The one after it is longer.
    fn next(&mut self) -> Duration {
        let delay = self.next;
        self.next = (delay * 2).min(Self::CEILING);
        delay
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reply() -> (Reply, oneshot::Receiver<Result<(), Error>>) {
        oneshot::channel()
    }

    #[test]
    fn backoff_doubles_up_to_its_ceiling_and_resets() {
        let mut backoff = Backoff::new();
        assert_eq!(backoff.next(), Duration::from_millis(500));
        assert_eq!(backoff.next(), Duration::from_secs(1));
        assert_eq!(backoff.next(), Duration::from_secs(2));
        for _ in 0..10 {
            backoff.next();
        }
        assert_eq!(backoff.next(), Duration::from_secs(30));
        backoff.reset();
        assert_eq!(backoff.next(), Duration::from_millis(500));
    }

    #[test]
    fn a_confirm_answers_only_the_publish_it_names() {
        let confirms = Confirms::default();
        let (first, mut first_rx) = reply();
        let (second, mut second_rx) = reply();
        assert_eq!(confirms.expect(first), 1);
        assert_eq!(confirms.expect(second), 2);

        confirms.settle(2, false, &Ok(()));
        assert!(second_rx.try_recv().unwrap().is_ok());
        assert!(first_rx.try_recv().is_err(), "the first is still waiting");
    }

    #[test]
    fn a_multiple_confirm_answers_everything_up_to_its_number() {
        let confirms = Confirms::default();
        let (one, mut one_rx) = reply();
        let (two, mut two_rx) = reply();
        let (three, mut three_rx) = reply();
        confirms.expect(one);
        confirms.expect(two);
        confirms.expect(three);

        confirms.settle(2, true, &Ok(()));
        assert!(one_rx.try_recv().unwrap().is_ok());
        assert!(two_rx.try_recv().unwrap().is_ok());
        assert!(three_rx.try_recv().is_err());
    }

    #[test]
    fn a_nack_reaches_the_publisher_as_rejected() {
        let confirms = Confirms::default();
        let (one, mut one_rx) = reply();
        confirms.expect(one);
        confirms.settle(1, false, &Err(Error::Rejected("no".into())));
        assert!(matches!(
            one_rx.try_recv().unwrap(),
            Err(Error::Rejected(_))
        ));
    }

    #[test]
    fn closing_the_channel_fails_every_waiting_publish() {
        let confirms = Confirms::default();
        let (one, mut one_rx) = reply();
        let (two, mut two_rx) = reply();
        confirms.expect(one);
        confirms.expect(two);

        confirms.fail_all(&Error::Rejected("closed".into()));
        assert!(matches!(
            one_rx.try_recv().unwrap(),
            Err(Error::Rejected(_))
        ));
        assert!(matches!(
            two_rx.try_recv().unwrap(),
            Err(Error::Rejected(_))
        ));
        // Nothing is left to answer.
        confirms.settle(1, true, &Ok(()));
    }

    #[test]
    fn a_withdrawn_publish_is_not_answered_again() {
        let confirms = Confirms::default();
        let (one, _one_rx) = reply();
        let sequence = confirms.expect(one);
        assert!(confirms.withdraw(sequence).is_some());
        assert!(confirms.withdraw(sequence).is_none());
    }

    #[test]
    fn overdue_publishes_expire_and_recent_ones_do_not() {
        let confirms = Confirms::default();
        let (old, mut old_rx) = reply();
        confirms.expect(old);
        std::thread::sleep(Duration::from_millis(20));
        let (recent, mut recent_rx) = reply();
        confirms.expect(recent);

        confirms.expire(Duration::from_millis(10));
        assert!(matches!(
            old_rx.try_recv().unwrap(),
            Err(Error::Unavailable(_))
        ));
        assert!(recent_rx.try_recv().is_err());
    }

    #[test]
    fn wants_matches_a_workload_and_queue_pair() {
        let mut desired = Subscriptions::new();
        desired.insert(
            "w1".into(),
            Subscription {
                queues: vec!["a".into(), "b".into()],
                prefetch: 1,
                declare: false,
            },
        );
        assert!(wants(&desired, "w1", "a"));
        assert!(wants(&desired, "w1", "b"));
        assert!(!wants(&desired, "w1", "c"));
        assert!(!wants(&desired, "w2", "a"));
    }
}
