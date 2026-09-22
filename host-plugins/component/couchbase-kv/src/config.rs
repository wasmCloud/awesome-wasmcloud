//! Per-workload cluster configuration for the KV transport.
//!
//! Bindings-free so it compiles and tests on the host. Validated in
//! `on-workload-bind`, which is what makes a missing connection string a failed
//! deploy with a named cause rather than a surprise on the first call.

/// Couchbase's own name for the scope and collection every bucket starts with.
const DEFAULT_NAME: &str = "_default";

/// How long a SQL++ query may run before the server gives up.
///
/// It does not reach document operations: `couchbase` 1.0.1 takes no timeout on
/// one, so those run under the SDK's own defaults and a per-call `timeout-ns`
/// is refused rather than silently dropped.
const DEFAULT_TIMEOUT_MS: u32 = 30_000;

/// A validated Couchbase binding for one workload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    /// The connection string handed to the SDK, e.g.
    /// `couchbases://cb.abc.cloud.couchbase.com`.
    pub connection_string: String,
    pub username: String,
    pub password: String,
    pub bucket: String,
    pub scope: String,
    pub collection: String,
    pub timeout_ms: u32,
}

impl Binding {
    /// Validate one workload's interface config into a binding.
    ///
    /// The error string is surfaced verbatim in the workload's deploy failure,
    /// so it names the key at fault.
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

        let connection_string = normalize_connection_string(require("endpoint")?)?;

        let timeout_ms = match get("timeout-ms") {
            Some(raw) => raw.parse::<u32>().map_err(|_| {
                format!("config key `timeout-ms` must be a whole number of milliseconds, got `{raw}`")
            })?,
            None => DEFAULT_TIMEOUT_MS,
        };
        if timeout_ms == 0 {
            return Err("config key `timeout-ms` must be greater than zero".to_string());
        }

        Ok(Self {
            connection_string,
            username: require("username")?.to_string(),
            password: require("password")?.to_string(),
            bucket: require("bucket")?.to_string(),
            scope: get("scope").unwrap_or(DEFAULT_NAME).to_string(),
            collection: get("collection").unwrap_or(DEFAULT_NAME).to_string(),
            timeout_ms,
        })
    }

    /// Key for the connection cache: everything that decides which cluster and
    /// identity a connection belongs to. Two workloads sharing all of these
    /// share a connection; differing in any of them do not.
    pub fn connection_key(&self) -> String {
        format!(
            "{}|{}|{}",
            self.connection_string, self.username, self.bucket
        )
    }
}

/// Accept the connection-string forms a Couchbase user would reach for, and
/// reject the ones that belong to the sibling Data API plugin.
///
/// A bare host is promoted to `couchbases://` rather than `couchbase://`: this
/// transport exists mainly for Capella, which is TLS-only, and defaulting the
/// other way would silently downgrade a misconfigured deployment to plaintext.
fn normalize_connection_string(raw: &str) -> Result<String, String> {
    let raw = raw.trim();
    match raw.split_once("://") {
        Some(("couchbases", rest)) | Some(("couchbase", rest)) if rest.is_empty() => {
            Err("config key `endpoint` has no host".to_string())
        }
        Some(("couchbases", _)) | Some(("couchbase", _)) => Ok(raw.to_string()),
        Some(("http", _)) | Some(("https", _)) => Err(
            "config key `endpoint` is an HTTP URL; that is the Data API, served by the `couchbase` plugin. This plugin speaks the KV protocol and wants `couchbases://host`"
                .to_string(),
        ),
        Some((scheme, _)) => Err(format!(
            "config key `endpoint` must use couchbases:// or couchbase://, got scheme `{scheme}`"
        )),
        None if raw.is_empty() => Err("config key `endpoint` has no host".to_string()),
        None => Ok(format!("couchbases://{raw}")),
    }
}

/// Split a connection string into `(scheme, host, port_suffix)`.
///
/// The host is returned bare so it can be resolved; `port_suffix` keeps the
/// `:port` exactly as written, or is empty, so the string can be rebuilt
/// without inventing a default port the SDK would otherwise choose itself.
/// An IPv6 literal in brackets is returned with its brackets, since it needs no
/// resolution and must keep them to be parsed again.
pub fn split_connection_string(raw: &str) -> (&str, &str, &str) {
    let (scheme, rest) = raw.split_once("://").unwrap_or(("couchbases", raw));
    // Only the first host matters here: a multi-node string is left alone by
    // the caller, which is what `needs_resolution` decides.
    if let Some(close) = rest.strip_prefix('[').and_then(|_| rest.find(']')) {
        let (host, port) = rest.split_at(close + 1);
        return (scheme, host, port);
    }
    match rest.split_once(':') {
        Some((host, _)) => (scheme, host, &rest[host.len()..]),
        None => (scheme, rest, ""),
    }
}

/// Whether `host` has to be resolved before the SDK sees it.
///
/// An IP literal, and anything naming more than one node, is left alone: the
/// first is already an address, and the second is the SDK's job to spread
/// across nodes.
pub fn needs_resolution(host: &str) -> bool {
    !host.contains(',')
        && !host.starts_with('[')
        && host.parse::<std::net::IpAddr>().is_err()
        && !host.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_a_connection_string_into_its_parts() {
        assert_eq!(
            split_connection_string("couchbases://cb.example.com"),
            ("couchbases", "cb.example.com", "")
        );
        assert_eq!(
            split_connection_string("couchbase://10.0.0.4:11210"),
            ("couchbase", "10.0.0.4", ":11210")
        );
        // An IPv6 literal keeps its brackets, which it needs to parse again.
        assert_eq!(
            split_connection_string("couchbase://[::1]:11210"),
            ("couchbase", "[::1]", ":11210")
        );
    }

    #[test]
    fn only_hostnames_need_resolving() {
        assert!(needs_resolution("cb.example.com"));
        assert!(!needs_resolution("10.0.0.4"));
        assert!(!needs_resolution("[::1]"));
        // A multi-node string is the SDK's to spread across nodes.
        assert!(!needs_resolution("node1.example.com,node2.example.com"));
    }

    fn config(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn minimal() -> Vec<(String, String)> {
        config(&[
            ("endpoint", "couchbases://cb.abc.cloud.couchbase.com"),
            ("bucket", "travel-sample"),
            ("username", "app"),
            ("password", "s3cret"),
        ])
    }

    #[test]
    fn accepts_a_minimal_config_and_defaults_the_rest() {
        let b = Binding::from_config(&minimal()).unwrap();
        assert_eq!(b.connection_string, "couchbases://cb.abc.cloud.couchbase.com");
        assert_eq!(b.scope, "_default");
        assert_eq!(b.collection, "_default");
        assert_eq!(b.timeout_ms, DEFAULT_TIMEOUT_MS);
    }

    #[test]
    fn a_bare_host_becomes_tls_not_plaintext() {
        assert_eq!(
            normalize_connection_string("cb.abc.cloud.couchbase.com").unwrap(),
            "couchbases://cb.abc.cloud.couchbase.com"
        );
        // An explicit plaintext scheme is still honoured, for a local cluster.
        assert_eq!(
            normalize_connection_string("couchbase://127.0.0.1").unwrap(),
            "couchbase://127.0.0.1"
        );
    }

    #[test]
    fn an_http_endpoint_points_at_the_other_plugin() {
        let err = normalize_connection_string("https://abc.data.cloud.couchbase.com").unwrap_err();
        assert!(err.contains("Data API"), "got: {err}");
        assert!(err.contains("couchbases://"), "got: {err}");
    }

    #[test]
    fn rejects_unusable_endpoints() {
        assert!(normalize_connection_string("ftp://host").is_err());
        assert!(normalize_connection_string("couchbases://").is_err());
        assert!(normalize_connection_string("").is_err());
    }

    #[test]
    fn names_the_missing_key() {
        for missing in ["endpoint", "bucket", "username", "password"] {
            let pairs: Vec<_> = minimal().into_iter().filter(|(k, _)| k != missing).collect();
            let err = Binding::from_config(&pairs).unwrap_err();
            assert!(err.contains(missing), "error should name `{missing}`, got: {err}");
        }
    }

    #[test]
    fn connections_are_keyed_by_cluster_identity_and_bucket() {
        let a = Binding::from_config(&minimal()).unwrap();
        let mut other = minimal();
        other.push(("username".to_string(), "someone-else".to_string()));
        let other: Vec<_> = other.into_iter().filter(|(k, v)| k != "username" || v == "someone-else").collect();
        let b = Binding::from_config(&other).unwrap();
        assert_ne!(
            a.connection_key(),
            b.connection_key(),
            "a different credential must not reuse another's connection"
        );
    }
}
