//! Per-workload cluster configuration.
//!
//! Bindings-free, so it compiles and tests on the host. Validated in
//! `on-workload-bind`, which makes a bad endpoint a failed deploy with a named
//! cause rather than a surprise on the first call.

/// Couchbase's own name for the scope and collection every bucket starts with.
const DEFAULT_NAME: &str = "_default";

/// The KV port a Couchbase node listens on.
const DEFAULT_KV_PORT: u16 = 11210;

/// The query service's HTTP port, for SQL++.
const DEFAULT_QUERY_PORT: u16 = 8093;

/// How long a call may run before it is given up on.
const DEFAULT_TIMEOUT_MS: u32 = 30_000;

/// A validated binding for one workload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    /// Host of the cluster node, without a port.
    pub host: String,
    /// KV port.
    pub port: u16,
    /// Query service port, for the SQL++ passthrough.
    pub query_port: u16,
    pub username: String,
    pub password: String,
    pub bucket: String,
    pub scope: String,
    pub collection: String,
    pub timeout_ms: u32,
}

impl Binding {
    /// Validate one workload's interface config.
    ///
    /// The error string is surfaced verbatim in the deploy failure, so it names
    /// the key at fault.
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

        let (host, port) = parse_endpoint(require("endpoint")?)?;

        let timeout_ms = match get("timeout-ms") {
            Some(raw) => raw.parse::<u32>().map_err(|_| {
                format!("config key `timeout-ms` must be a whole number of milliseconds, got `{raw}`")
            })?,
            None => DEFAULT_TIMEOUT_MS,
        };
        if timeout_ms == 0 {
            return Err("config key `timeout-ms` must be greater than zero".to_string());
        }

        let query_port = match get("query-port") {
            Some(raw) => raw
                .parse::<u16>()
                .map_err(|_| format!("config key `query-port` must be a port number, got `{raw}`"))?,
            None => DEFAULT_QUERY_PORT,
        };

        Ok(Self {
            host,
            port,
            query_port,
            username: require("username")?.to_string(),
            password: require("password")?.to_string(),
            bucket: require("bucket")?.to_string(),
            scope: get("scope").unwrap_or(DEFAULT_NAME).to_string(),
            collection: get("collection").unwrap_or(DEFAULT_NAME).to_string(),
            timeout_ms,
        })
    }

    /// Everything that decides which cluster and identity a connection serves.
    /// Two workloads matching on all of it share a connection.
    pub fn connection_key(&self) -> String {
        format!(
            "{}:{}|{}|{}|{}.{}",
            self.host, self.port, self.username, self.bucket, self.scope, self.collection
        )
    }
}

/// Split `couchbase://host[:port]` into its host and port.
///
/// `couchbases://` is refused rather than silently downgraded: this
/// implementation has no TLS yet, and quietly sending credentials in the clear
/// because the scheme was misread would be the worst possible reading of it.
fn parse_endpoint(raw: &str) -> Result<(String, u16), String> {
    let rest = match raw.split_once("://") {
        Some(("couchbase", rest)) => rest,
        Some(("couchbases", _)) => {
            return Err(
                "config key `endpoint` uses couchbases://, and this plugin has no TLS yet; use the `couchbase` (Data API) plugin for a TLS endpoint, or couchbase:// on a trusted network"
                    .to_string(),
            )
        }
        Some(("http", _)) | Some(("https", _)) => {
            return Err(
                "config key `endpoint` is an HTTP URL; that is the Data API, served by the `couchbase` plugin. This one speaks the KV protocol and wants `couchbase://host`"
                    .to_string(),
            )
        }
        Some((scheme, _)) => {
            return Err(format!(
                "config key `endpoint` must use couchbase://, got scheme `{scheme}`"
            ))
        }
        None => raw,
    };

    if rest.is_empty() {
        return Err("config key `endpoint` has no host".to_string());
    }
    // Only the last colon separates a port, so an IPv6 literal in brackets
    // keeps its own.
    match rest.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() && !host.ends_with(']') => {
            let port = port
                .parse::<u16>()
                .map_err(|_| format!("config key `endpoint` has an invalid port: `{port}`"))?;
            Ok((host.to_string(), port))
        }
        _ => Ok((rest.to_string(), DEFAULT_KV_PORT)),
    }
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
            ("endpoint", "couchbase://10.0.0.4"),
            ("bucket", "travel-sample"),
            ("username", "app"),
            ("password", "s3cret"),
        ])
    }

    #[test]
    fn accepts_a_minimal_config_and_defaults_the_rest() {
        let b = Binding::from_config(&minimal()).unwrap();
        assert_eq!(b.host, "10.0.0.4");
        assert_eq!(b.port, DEFAULT_KV_PORT);
        assert_eq!(b.query_port, DEFAULT_QUERY_PORT);
        assert_eq!(b.scope, "_default");
        assert_eq!(b.collection, "_default");
        assert_eq!(b.timeout_ms, DEFAULT_TIMEOUT_MS);
    }

    #[test]
    fn an_explicit_port_is_kept() {
        assert_eq!(parse_endpoint("couchbase://host:12000").unwrap(), ("host".to_string(), 12000));
        assert_eq!(parse_endpoint("host").unwrap(), ("host".to_string(), DEFAULT_KV_PORT));
    }

    #[test]
    fn tls_is_refused_rather_than_downgraded() {
        let err = parse_endpoint("couchbases://host").unwrap_err();
        assert!(err.contains("no TLS"), "got: {err}");
        // And an HTTP endpoint points at the sibling plugin.
        assert!(parse_endpoint("https://host").unwrap_err().contains("Data API"));
    }

    #[test]
    fn rejects_unusable_endpoints() {
        assert!(parse_endpoint("ftp://host").is_err());
        assert!(parse_endpoint("couchbase://").is_err());
        assert!(parse_endpoint("couchbase://host:not-a-port").is_err());
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
    fn connections_are_keyed_by_identity_and_keyspace() {
        let a = Binding::from_config(&minimal()).unwrap();
        let mut other = minimal();
        other.push(("collection".to_string(), "airline".to_string()));
        let b = Binding::from_config(&other).unwrap();
        assert_ne!(
            a.connection_key(),
            b.connection_key(),
            "a different collection must not reuse another's connection"
        );
    }
}
