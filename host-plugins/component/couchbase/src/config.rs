//! Per-workload cluster configuration: what a workload's interface config has
//! to say, and what it means once validated.
//!
//! Parsing lives here, apart from the WIT bindings, so it compiles and tests on
//! the host. The plugin calls [`Binding::from_config`] from `on-workload-bind`,
//! which is what makes a missing endpoint or credential a failed *deploy* with a
//! named cause rather than a surprise on the workload's first query.

use base64::Engine as _;

/// Couchbase's own name for the scope and collection every bucket starts with.
const DEFAULT_NAME: &str = "_default";

/// Requests wait this long by default before giving up on the cluster.
const DEFAULT_TIMEOUT_MS: u32 = 30_000;

/// A validated Couchbase binding for one workload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    /// `true` when the endpoint is HTTPS. Capella is always HTTPS; plain HTTP
    /// is allowed so the plugin can be pointed at a local cluster.
    pub secure: bool,
    /// `host[:port]` of the Data API endpoint.
    pub authority: String,
    /// Path the Data API is mounted under, without a trailing slash. Empty for
    /// the usual case where it sits at the root.
    pub base_path: String,
    /// A ready-to-send `Authorization` header value.
    pub authorization: String,
    /// The one bucket this binding may reach.
    pub bucket: String,
    /// Scope used when a call leaves `location.scope` unset.
    pub scope: String,
    /// Collection used when a call leaves `location.collection` unset.
    pub collection: String,
    /// Per-request time limit in milliseconds.
    pub timeout_ms: u32,
}

impl Binding {
    /// A copy of this binding with a per-call timeout applied.
    ///
    /// The interface's option records all carry a `timeout-ns`, and a caller
    /// that sets one means it: silently serving the binding default instead
    /// would leave a call running far longer than asked. `none` and `0` both
    /// mean "unset" and keep the binding's own timeout.
    ///
    /// Sub-millisecond values round up to 1ms rather than to zero, which would
    /// otherwise turn a very short timeout into an immediate failure.
    pub fn with_timeout_ns(&self, timeout_ns: Option<u64>) -> Self {
        let Some(ns) = timeout_ns.filter(|ns| *ns > 0) else {
            return self.clone();
        };
        let ms = ns.div_ceil(1_000_000).max(1).min(u32::MAX.into()) as u32;
        Self {
            timeout_ms: ms,
            ..self.clone()
        }
    }
}

impl Binding {
    /// Validate one workload's interface config into a binding.
    ///
    /// `config` is the manifest's interface-level key/value list, as delivered
    /// on the `interface-binding`. The error string is surfaced verbatim in the
    /// workload's deploy failure, so it names the key at fault.
    pub fn from_config(config: &[(String, String)]) -> Result<Self, String> {
        let get = |key: &str| -> Option<&str> {
            config
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.trim())
                .filter(|v| !v.is_empty())
        };
        let require = |key: &str| -> Result<&str, String> {
            get(key).ok_or_else(|| format!("missing required config key `{key}`"))
        };

        let (secure, authority, base_path) = parse_endpoint(require("endpoint")?)?;
        let username = require("username")?;
        let password = require("password")?;

        let timeout_ms = match get("timeout-ms") {
            Some(raw) => raw
                .parse::<u32>()
                .map_err(|_| format!("config key `timeout-ms` must be a whole number of milliseconds, got `{raw}`"))?,
            None => DEFAULT_TIMEOUT_MS,
        };
        if timeout_ms == 0 {
            return Err("config key `timeout-ms` must be greater than zero".to_string());
        }

        let credentials = base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));

        Ok(Self {
            secure,
            authority,
            base_path,
            authorization: format!("Basic {credentials}"),
            bucket: require("bucket")?.to_string(),
            scope: get("scope").unwrap_or(DEFAULT_NAME).to_string(),
            collection: get("collection").unwrap_or(DEFAULT_NAME).to_string(),
            timeout_ms,
        })
    }

}

/// Split an endpoint into `(secure, authority, base_path)`.
///
/// Accepts `https://host`, `http://host:port/prefix`, or a bare `host:port`.
/// A bare authority is treated as HTTPS, because that is what every Capella
/// endpoint is and defaulting the other way would silently downgrade a
/// misconfigured deployment to plaintext.
fn parse_endpoint(raw: &str) -> Result<(bool, String, String), String> {
    let (secure, rest) = match raw.split_once("://") {
        Some(("https", rest)) => (true, rest),
        Some(("http", rest)) => (false, rest),
        Some((scheme, _)) => {
            return Err(format!(
                "config key `endpoint` must use http or https, got scheme `{scheme}`"
            ))
        }
        None => (true, raw),
    };

    let (authority, path) = match rest.split_once('/') {
        Some((authority, path)) => (authority, path.trim_end_matches('/')),
        None => (rest, ""),
    };

    if authority.is_empty() {
        return Err("config key `endpoint` has no host".to_string());
    }

    let base_path = if path.is_empty() {
        String::new()
    } else {
        format!("/{path}")
    };

    Ok((secure, authority.to_string(), base_path))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn minimal() -> Vec<(String, String)> {
        config(&[
            ("endpoint", "https://abc.data.cloud.couchbase.com"),
            ("bucket", "travel-sample"),
            ("username", "app"),
            ("password", "s3cret"),
        ])
    }

    #[test]
    fn accepts_a_minimal_config_and_defaults_the_rest() {
        let binding = Binding::from_config(&minimal()).unwrap();
        assert!(binding.secure);
        assert_eq!(binding.authority, "abc.data.cloud.couchbase.com");
        assert_eq!(binding.base_path, "");
        assert_eq!(binding.bucket, "travel-sample");
        assert_eq!(binding.scope, "_default");
        assert_eq!(binding.collection, "_default");
        assert_eq!(binding.timeout_ms, DEFAULT_TIMEOUT_MS);
    }

    #[test]
    fn builds_a_basic_authorization_header() {
        let binding = Binding::from_config(&minimal()).unwrap();
        // base64("app:s3cret")
        assert_eq!(binding.authorization, "Basic YXBwOnMzY3JldA==");
    }

    #[test]
    fn names_the_missing_key() {
        for missing in ["endpoint", "bucket", "username", "password"] {
            let pairs: Vec<_> = minimal().into_iter().filter(|(k, _)| k != missing).collect();
            let err = Binding::from_config(&pairs).unwrap_err();
            assert!(
                err.contains(missing),
                "error for a missing `{missing}` should name it, got: {err}"
            );
        }
    }

    #[test]
    fn treats_a_blank_value_as_missing() {
        let mut pairs = minimal();
        pairs.push(("bucket".to_string(), "   ".to_string()));
        let pairs: Vec<_> = pairs.into_iter().filter(|(k, v)| k != "bucket" || v.trim().is_empty()).collect();
        let err = Binding::from_config(&pairs).unwrap_err();
        assert!(err.contains("bucket"), "got: {err}");
    }

    #[test]
    fn parses_endpoint_forms() {
        assert_eq!(
            parse_endpoint("https://host.example:18093").unwrap(),
            (true, "host.example:18093".to_string(), String::new())
        );
        assert_eq!(
            parse_endpoint("http://127.0.0.1:8093").unwrap(),
            (false, "127.0.0.1:8093".to_string(), String::new())
        );
        // A bare authority defaults to HTTPS rather than silently downgrading.
        assert_eq!(
            parse_endpoint("host.example").unwrap(),
            (true, "host.example".to_string(), String::new())
        );
        // A mount prefix is kept, without its trailing slash.
        assert_eq!(
            parse_endpoint("https://host.example/data/").unwrap(),
            (true, "host.example".to_string(), "/data".to_string())
        );
    }

    #[test]
    fn rejects_unusable_endpoints() {
        assert!(parse_endpoint("ftp://host.example").is_err());
        assert!(parse_endpoint("https://").is_err());
    }

    #[test]
    fn rejects_an_unparseable_or_zero_timeout() {
        let mut pairs = minimal();
        pairs.push(("timeout-ms".to_string(), "soon".to_string()));
        assert!(Binding::from_config(&pairs).unwrap_err().contains("timeout-ms"));

        let mut pairs = minimal();
        pairs.push(("timeout-ms".to_string(), "0".to_string()));
        assert!(Binding::from_config(&pairs).unwrap_err().contains("timeout-ms"));
    }

    #[test]
    fn a_per_call_timeout_overrides_the_bindings() {
        let binding = Binding::from_config(&minimal()).unwrap();
        assert_eq!(binding.with_timeout_ns(Some(5_000_000_000)).timeout_ms, 5_000);
        // Unset in either spelling keeps the binding's own timeout.
        assert_eq!(binding.with_timeout_ns(None).timeout_ms, DEFAULT_TIMEOUT_MS);
        assert_eq!(binding.with_timeout_ns(Some(0)).timeout_ms, DEFAULT_TIMEOUT_MS);
        // Sub-millisecond rounds up, so a tiny timeout is not silently zero.
        assert_eq!(binding.with_timeout_ns(Some(1)).timeout_ms, 1);
        assert_eq!(binding.with_timeout_ns(Some(1_500_000)).timeout_ms, 2);
    }

    #[test]
    fn scope_and_collection_come_from_the_binding() {
        // The interface takes only a document id, exactly as 0.1.0-draft did:
        // the keyspace is the binding's, so a workload cannot reach a scope or
        // collection it was not granted.
        let mut pairs = minimal();
        pairs.push(("scope".to_string(), "inventory".to_string()));
        pairs.push(("collection".to_string(), "airline".to_string()));
        let binding = Binding::from_config(&pairs).unwrap();

        assert_eq!(binding.scope, "inventory");
        assert_eq!(binding.collection, "airline");
    }
}
