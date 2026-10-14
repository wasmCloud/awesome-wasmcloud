//! What the plugin is configured with, and by whom.
//!
//! There are two sources, and they stay apart on purpose:
//!
//! - The **broker** (address and credentials) comes from the plugin's own host
//!   configuration, which only the operator writes. [`Broker::load`] reads it
//!   through `wasi:config`, and nothing a workload sends can reach it.
//! - A workload's **subscription** (which queues it consumes) comes from the
//!   `config` of the workload's own `wasmcloud:amqp` interface entry, which the
//!   host hands the plugin when the workload is bound. See [`Subscription`].

use std::fmt;
use std::sync::OnceLock;

/// The port AMQP 0-9-1 brokers listen on without TLS.
const DEFAULT_PORT: u16 = 5672;
/// The virtual host every RabbitMQ broker starts with.
const DEFAULT_VHOST: &str = "/";
/// How many unsettled messages the broker may have outstanding per queue.
pub const DEFAULT_PREFETCH: u16 = 16;
/// The longest name AMQP can carry, in bytes. Queue, exchange and routing-key
/// names are all AMQP short strings.
pub const MAX_NAME_BYTES: usize = 255;

/// How to reach the broker.
#[derive(Clone, PartialEq, Eq)]
pub struct Broker {
    /// A host name or an IP address. A name has to be one the host lets the
    /// plugin resolve: list it in the plugin's `allowedHosts`.
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub vhost: String,
}

impl Broker {
    /// Reads the plugin's own configuration.
    fn load() -> Result<Self, String> {
        use crate::bindings::wasi::config::store;

        let entries = store::get_all()
            .map_err(|err| format!("could not read the plugin's configuration: {err:?}"))?;
        Self::from_entries(&entries)
    }

    fn from_entries(entries: &[(String, String)]) -> Result<Self, String> {
        let get = |key: &str| value(entries, key);
        let required = |key: &str| {
            get(key)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| format!("the plugin's config needs a `{key}`"))
        };
        let port = match get("port") {
            None => DEFAULT_PORT,
            Some(port) => port.parse().map_err(|_| {
                format!("the plugin's config `port` must be a port number, not `{port}`")
            })?,
        };
        Ok(Self {
            host: required("host")?,
            port,
            username: required("username")?,
            // The password may legitimately be empty, but the key must be
            // there, so a secret that failed to resolve is not mistaken for one.
            password: get("password")
                .map(str::to_string)
                .ok_or_else(|| "the plugin's config needs a `password`".to_string())?,
            vhost: get("vhost").unwrap_or(DEFAULT_VHOST).to_string(),
        })
    }
}

/// Hides the password, so a `{:?}` in a log line cannot leak it.
impl fmt::Debug for Broker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Broker")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("vhost", &self.vhost)
            .finish()
    }
}

/// The plugin's broker settings, read once. An error means the operator has not
/// configured the plugin, or configured it wrongly.
pub fn broker() -> Result<&'static Broker, &'static str> {
    static BROKER: OnceLock<Result<Broker, String>> = OnceLock::new();
    BROKER
        .get_or_init(Broker::load)
        .as_ref()
        .map_err(String::as_str)
}

/// The queues one workload consumes, and how.
///
/// A workload asks for them with the `config` of its `wasmcloud:amqp` entry
/// under `hostInterfaces`:
///
/// | key              | meaning                                                      |
/// |------------------|--------------------------------------------------------------|
/// | `queues`         | comma-separated queue names to consume. Required to consume. |
/// | `prefetch`       | unsettled messages the broker may send at once, per queue.   |
/// | `declare-queues` | `true` declares each queue, durable, before consuming.       |
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subscription {
    pub queues: Vec<String>,
    pub prefetch: u16,
    pub declare: bool,
}

impl Subscription {
    /// Reads a subscription from the `config` of a workload's interface entry.
    /// `None` means the workload consumes nothing.
    pub fn from_entries(entries: &[(String, String)]) -> Result<Option<Self>, String> {
        let Some(raw) = value(entries, "queues") else {
            return Ok(None);
        };

        let mut queues: Vec<String> = Vec::new();
        for name in raw
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            if name.len() > MAX_NAME_BYTES {
                return Err(format!(
                    "a queue name in `queues` is {} bytes, and AMQP allows {MAX_NAME_BYTES}",
                    name.len()
                ));
            }
            if !queues.iter().any(|queue| queue == name) {
                queues.push(name.to_string());
            }
        }
        if queues.is_empty() {
            return Err("`queues` is set but names no queue".to_string());
        }

        let prefetch = match value(entries, "prefetch") {
            None => DEFAULT_PREFETCH,
            Some(prefetch) => prefetch
                .trim()
                .parse::<u16>()
                .ok()
                .filter(|prefetch| *prefetch > 0)
                .ok_or_else(|| {
                    format!("`prefetch` must be a number from 1 to 65535, not `{prefetch}`")
                })?,
        };

        let declare = match value(entries, "declare-queues").map(str::trim) {
            None => false,
            Some(flag) if flag.eq_ignore_ascii_case("true") => true,
            Some(flag) if flag.eq_ignore_ascii_case("false") => false,
            Some(flag) => {
                return Err(format!(
                    "`declare-queues` must be `true` or `false`, not `{flag}`"
                ));
            }
        };

        Ok(Some(Self {
            queues,
            prefetch,
            declare,
        }))
    }
}

/// The value of `key`. When a key repeats, the last entry wins, as when
/// configuration layers merge.
fn value<'a>(entries: &'a [(String, String)], key: &str) -> Option<&'a str> {
    entries
        .iter()
        .rev()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    fn broker_entries() -> Vec<(String, String)> {
        entries(&[
            ("host", "rabbitmq.internal"),
            ("username", "wasmcloud"),
            ("password", "s3cret"),
        ])
    }

    #[test]
    fn broker_uses_amqp_defaults() {
        let broker = Broker::from_entries(&broker_entries()).unwrap();
        assert_eq!(broker.host, "rabbitmq.internal");
        assert_eq!(broker.port, 5672);
        assert_eq!(broker.vhost, "/");
    }

    #[test]
    fn broker_reads_every_key() {
        let mut config = broker_entries();
        config.extend(entries(&[("port", "5673"), ("vhost", "orders")]));
        let broker = Broker::from_entries(&config).unwrap();
        assert_eq!(broker.port, 5673);
        assert_eq!(broker.vhost, "orders");
        assert_eq!(broker.username, "wasmcloud");
        assert_eq!(broker.password, "s3cret");
    }

    #[test]
    fn broker_needs_host_username_and_password() {
        for missing in ["host", "username", "password"] {
            let config: Vec<_> = broker_entries()
                .into_iter()
                .filter(|(key, _)| key != missing)
                .collect();
            let err = Broker::from_entries(&config).unwrap_err();
            assert!(err.contains(missing), "{err}");
        }
    }

    #[test]
    fn broker_accepts_an_empty_password_but_not_an_empty_host() {
        let mut config = broker_entries();
        config.retain(|(key, _)| key != "password");
        config.push(("password".into(), String::new()));
        assert!(Broker::from_entries(&config).is_ok());

        let mut config = broker_entries();
        config.retain(|(key, _)| key != "host");
        config.push(("host".into(), String::new()));
        assert!(Broker::from_entries(&config).is_err());
    }

    #[test]
    fn broker_rejects_a_bad_port() {
        let mut config = broker_entries();
        config.push(("port".into(), "amqp".into()));
        assert!(Broker::from_entries(&config).unwrap_err().contains("port"));
    }

    #[test]
    fn broker_debug_hides_the_password() {
        let broker = Broker::from_entries(&broker_entries()).unwrap();
        let shown = format!("{broker:?}");
        assert!(!shown.contains("s3cret"), "{shown}");
        assert!(shown.contains("rabbitmq.internal"));
    }

    #[test]
    fn no_queues_means_no_subscription() {
        assert_eq!(Subscription::from_entries(&[]).unwrap(), None);
        let other = entries(&[("prefetch", "4")]);
        assert_eq!(Subscription::from_entries(&other).unwrap(), None);
    }

    #[test]
    fn subscription_defaults() {
        let sub = Subscription::from_entries(&entries(&[("queues", "orders")]))
            .unwrap()
            .unwrap();
        assert_eq!(sub.queues, ["orders"]);
        assert_eq!(sub.prefetch, DEFAULT_PREFETCH);
        assert!(!sub.declare);
    }

    #[test]
    fn subscription_splits_trims_and_dedupes_queues() {
        let sub = Subscription::from_entries(&entries(&[("queues", " a, b ,,a,c ")]))
            .unwrap()
            .unwrap();
        assert_eq!(sub.queues, ["a", "b", "c"]);
    }

    #[test]
    fn subscription_reads_prefetch_and_declare() {
        let sub = Subscription::from_entries(&entries(&[
            ("queues", "q"),
            ("prefetch", "1"),
            ("declare-queues", "True"),
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(sub.prefetch, 1);
        assert!(sub.declare);
    }

    #[test]
    fn subscription_rejects_bad_values() {
        let bad = [
            [("queues", " , ")].as_slice(),
            &[("queues", "q"), ("prefetch", "0")],
            &[("queues", "q"), ("prefetch", "many")],
            &[("queues", "q"), ("prefetch", "70000")],
            &[("queues", "q"), ("declare-queues", "yes")],
        ];
        for config in bad {
            assert!(
                Subscription::from_entries(&entries(config)).is_err(),
                "{config:?}"
            );
        }
        let long = "q".repeat(MAX_NAME_BYTES + 1);
        assert!(Subscription::from_entries(&entries(&[("queues", &long)])).is_err());
    }

    #[test]
    fn the_last_entry_for_a_key_wins() {
        let sub = Subscription::from_entries(&entries(&[("queues", "old"), ("queues", "new")]))
            .unwrap()
            .unwrap();
        assert_eq!(sub.queues, ["new"]);
    }
}
