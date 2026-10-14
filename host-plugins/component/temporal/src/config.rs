//! The plugin's configuration, read from its own host configuration through
//! `wasi:config`. Only the operator writes it, and nothing a workload sends can
//! reach it.
//!
//! | key              | meaning                                                        |
//! |------------------|----------------------------------------------------------------|
//! | `address`        | Temporal's frontend, `http://host:port` or `https://host:port`. Required. |
//! | `namespace`      | The Temporal namespace. Defaults to `default`.                 |
//! | `task-queues`    | Comma-separated task queues to poll. Required.                 |
//! | `identity`       | The worker identity Temporal shows. Defaults to `wasmcloud-temporal`. |
//! | `api-key`        | Sent as a bearer token, for Temporal Cloud. Optional.          |
//! | `max-concurrent` | How many activities may run at once. Defaults to 16.           |

use std::fmt;

const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_IDENTITY: &str = "wasmcloud-temporal";
const DEFAULT_MAX_CONCURRENT: usize = 16;

/// How to reach Temporal, and what to poll.
#[derive(Clone, PartialEq, Eq)]
pub struct Settings {
    /// Temporal's frontend, as `http://host:port` or `https://host:port`.
    pub address: String,
    pub namespace: String,
    /// The task queues to poll. At least one.
    pub task_queues: Vec<String>,
    /// The worker identity Temporal shows for this plugin.
    pub identity: String,
    /// Sent as `Authorization: Bearer <key>`, for Temporal Cloud.
    pub api_key: Option<String>,
    /// How many activities may run at once, across all task queues.
    pub max_concurrent: usize,
}

impl Settings {
    /// Reads and checks the plugin's configuration.
    pub fn load() -> Result<Self, String> {
        use crate::bindings::wasi::config::store;

        let entries = store::get_all()
            .map_err(|err| format!("could not read the plugin's configuration: {err:?}"))?;
        Self::from_entries(&entries)
    }

    pub fn from_entries(entries: &[(String, String)]) -> Result<Self, String> {
        let get = |key: &str| value(entries, key).map(str::trim);
        let optional = |key: &str| get(key).filter(|value| !value.is_empty());

        let address = optional("address")
            .ok_or_else(|| "the plugin's config needs an `address`".to_string())?;
        let Some(authority) = address
            .strip_prefix("http://")
            .or_else(|| address.strip_prefix("https://"))
        else {
            return Err(format!(
                "the plugin's config `address` must start with `http://` or `https://`, \
                 not `{address}`"
            ));
        };
        // The gRPC calls go to fixed paths on the frontend, so a path here would
        // be dropped without a word.
        let authority = authority.strip_suffix('/').unwrap_or(authority);
        if authority.is_empty() || authority.contains(['/', '?', '#']) {
            return Err(format!(
                "the plugin's config `address` must be just a scheme, host and port, \
                 like `http://temporal:7233`, not `{address}`"
            ));
        }

        let mut task_queues: Vec<String> = Vec::new();
        for name in get("task-queues")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            if !task_queues.iter().any(|queue| queue == name) {
                task_queues.push(name.to_string());
            }
        }
        if task_queues.is_empty() {
            return Err("the plugin's config needs `task-queues`, naming at least one".to_string());
        }

        let max_concurrent = match get("max-concurrent") {
            None => DEFAULT_MAX_CONCURRENT,
            Some(max) => max
                .parse::<usize>()
                .ok()
                .filter(|max| *max > 0)
                .ok_or_else(|| {
                    format!(
                        "the plugin's config `max-concurrent` must be a number above 0, \
                         not `{max}`"
                    )
                })?,
        };

        let api_key = optional("api-key").map(str::to_string);
        if api_key.is_some() && address.starts_with("http://") {
            return Err(
                "the plugin's config sets an `api-key` with an `http://` address, \
                 which would send the key in cleartext: use `https://`"
                    .to_string(),
            );
        }

        Ok(Self {
            address: address.to_string(),
            namespace: optional("namespace")
                .unwrap_or(DEFAULT_NAMESPACE)
                .to_string(),
            task_queues,
            identity: optional("identity").unwrap_or(DEFAULT_IDENTITY).to_string(),
            api_key,
            max_concurrent,
        })
    }
}

/// Hides the API key, so a `{:?}` in a log line cannot leak it.
impl fmt::Debug for Settings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Settings")
            .field("address", &self.address)
            .field("namespace", &self.namespace)
            .field("task_queues", &self.task_queues)
            .field("identity", &self.identity)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("max_concurrent", &self.max_concurrent)
            .finish()
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

    fn required() -> Vec<(String, String)> {
        entries(&[
            ("address", "http://temporal.internal:7233"),
            ("task-queues", "orders"),
        ])
    }

    #[test]
    fn uses_defaults() {
        let settings = Settings::from_entries(&required()).unwrap();
        assert_eq!(settings.address, "http://temporal.internal:7233");
        assert_eq!(settings.namespace, "default");
        assert_eq!(settings.task_queues, ["orders"]);
        assert_eq!(settings.identity, "wasmcloud-temporal");
        assert_eq!(settings.api_key, None);
        assert_eq!(settings.max_concurrent, 16);
    }

    #[test]
    fn reads_every_key() {
        let mut config = required();
        config.extend(entries(&[
            ("address", "https://ns.tmprl.cloud:7233"),
            ("namespace", "prod"),
            ("identity", "host-1"),
            ("api-key", "s3cret"),
            ("max-concurrent", "4"),
        ]));
        let settings = Settings::from_entries(&config).unwrap();
        assert_eq!(settings.address, "https://ns.tmprl.cloud:7233");
        assert_eq!(settings.namespace, "prod");
        assert_eq!(settings.identity, "host-1");
        assert_eq!(settings.api_key.as_deref(), Some("s3cret"));
        assert_eq!(settings.max_concurrent, 4);
    }

    #[test]
    fn needs_address_and_task_queues() {
        for missing in ["address", "task-queues"] {
            let config: Vec<_> = required()
                .into_iter()
                .filter(|(key, _)| key != missing)
                .collect();
            let err = Settings::from_entries(&config).unwrap_err();
            assert!(err.contains(missing), "{err}");
        }
    }

    #[test]
    fn splits_trims_and_dedupes_task_queues() {
        let mut config = required();
        config.push(("task-queues".into(), " a, b ,,a,c ".into()));
        let settings = Settings::from_entries(&config).unwrap();
        assert_eq!(settings.task_queues, ["a", "b", "c"]);
    }

    #[test]
    fn rejects_bad_values() {
        let bad = [
            ("address", "temporal:7233"),
            ("address", "grpc://temporal:7233"),
            ("address", " "),
            ("address", "http://"),
            ("address", "http://temporal:7233/api"),
            ("address", "http://temporal:7233/?x=1"),
            ("task-queues", " , "),
            ("max-concurrent", "0"),
            ("max-concurrent", "many"),
            ("max-concurrent", "-1"),
        ];
        for (key, value) in bad {
            let mut config = required();
            config.push((key.into(), value.into()));
            let err = Settings::from_entries(&config).unwrap_err();
            assert!(err.contains(key), "{key}={value}: {err}");
        }
    }

    #[test]
    fn allows_one_trailing_slash() {
        let mut config = required();
        config.push(("address".into(), "http://temporal:7233/".into()));
        assert!(Settings::from_entries(&config).is_ok());
    }

    #[test]
    fn rejects_an_api_key_over_http() {
        let mut config = required();
        config.push(("api-key".into(), "s3cret".into()));
        let err = Settings::from_entries(&config).unwrap_err();
        assert!(err.contains("api-key"), "{err}");
        assert!(!err.contains("s3cret"), "{err}");
    }

    #[test]
    fn an_empty_api_key_is_no_api_key() {
        let mut config = required();
        config.push(("api-key".into(), String::new()));
        assert_eq!(Settings::from_entries(&config).unwrap().api_key, None);
    }

    #[test]
    fn debug_hides_the_api_key() {
        let mut config = required();
        config.push(("address".into(), "https://temporal.internal:7233".into()));
        config.push(("api-key".into(), "s3cret".into()));
        let shown = format!("{:?}", Settings::from_entries(&config).unwrap());
        assert!(!shown.contains("s3cret"), "{shown}");
        assert!(shown.contains("<redacted>"), "{shown}");
        assert!(shown.contains("temporal.internal"), "{shown}");
    }
}
