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

/// A connection's authentication and keyspace identity, without ambiguous
/// separators between user-controlled strings.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConnectionKey {
    host: String,
    port: u16,
    tls: bool,
    username: String,
    password: String,
    bucket: String,
    scope: String,
    collection: String,
}

/// A validated binding for one workload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    /// Host of the cluster node, without a port.
    pub host: String,
    /// KV port.
    pub port: u16,
    /// Verify TLS using `host` as the certificate identity and SNI name.
    pub tls: bool,
    /// Query service port, for the SQL++ passthrough.
    pub query_port: u16,
    pub username: String,
    pub password: String,
    pub bucket: String,
    pub scope: String,
    pub collection: String,
    /// How long a call may run, in milliseconds.
    pub timeout_ms: u32,
}

impl Binding {
    /// A copy of this binding with a per-call timeout applied.
    ///
    /// `none` and `0` both mean "unset" and keep the binding's own timeout.
    /// Sub-millisecond values round up to 1ms rather than down to zero.
    pub fn with_timeout_ns(&self, timeout_ns: Option<u64>) -> Self {
        let Some(ns) = timeout_ns.filter(|ns| *ns > 0) else {
            return self.clone();
        };
        Self {
            timeout_ms: ns.div_ceil(1_000_000).min(u32::MAX.into()) as u32,
            ..self.clone()
        }
    }

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
        // Credentials are opaque: trimming them changes the identity sent to
        // the server, including passwords supplied by a secret provider.
        let credential = |key: &str| -> Result<&str, String> {
            config
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, value)| value.as_str())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| format!("missing required config key `{key}`"))
        };

        let (host, port, tls) = parse_endpoint(require("endpoint")?)?;

        let timeout_ms = match get("timeout-ms") {
            Some(raw) => raw.parse::<u32>().map_err(|_| {
                "config key `timeout-ms` must be a whole number of milliseconds".to_string()
            })?,
            None => DEFAULT_TIMEOUT_MS,
        };
        if timeout_ms == 0 {
            return Err("config key `timeout-ms` must be greater than zero".to_string());
        }

        let query_port = match get("query-port") {
            Some(raw) => raw
                .parse::<u16>()
                .map_err(|_| "config key `query-port` must be a port number".to_string())?,
            None => {
                if tls {
                    18093
                } else {
                    DEFAULT_QUERY_PORT
                }
            }
        };

        Ok(Self {
            host,
            port,
            tls,
            query_port,
            username: credential("username")?.to_string(),
            password: credential("password")?.to_string(),
            bucket: require("bucket")?.to_string(),
            scope: get("scope").unwrap_or(DEFAULT_NAME).to_string(),
            collection: get("collection").unwrap_or(DEFAULT_NAME).to_string(),
            timeout_ms,
        })
    }

    /// Everything that decides which cluster and identity a connection serves.
    /// Two workloads matching on all of it share a connection.
    ///
    /// The password is part of it: a connection is authenticated once, so
    /// without it a binding holding the wrong password would be handed another's
    /// connection and never be told.
    pub fn connection_key(&self) -> ConnectionKey {
        ConnectionKey {
            host: self.host.clone(),
            port: self.port,
            tls: self.tls,
            username: self.username.clone(),
            password: self.password.clone(),
            bucket: self.bucket.clone(),
            scope: self.scope.clone(),
            collection: self.collection.clone(),
        }
    }

    pub fn query_authority(&self) -> String {
        if self.host.parse::<std::net::Ipv6Addr>().is_ok() {
            format!("[{}]:{}", self.host, self.query_port)
        } else {
            format!("{}:{}", self.host, self.query_port)
        }
    }
}

/// Split a Couchbase endpoint into its host, port and transport mode.
///
/// A TLS endpoint is never downgraded when TLS support was not compiled in.
fn parse_endpoint(raw: &str) -> Result<(String, u16, bool), String> {
    let (rest, tls) = match raw.split_once("://") {
        Some(("couchbase", rest)) => (rest, false),
        Some(("couchbases", rest)) if cfg!(feature = "tls") => (rest, true),
        Some(("couchbases", _)) => return Err(
            "config key `endpoint` uses couchbases://; rebuild this plugin with --features tls to enable verified TLS".to_string(),
        ),
        Some(("http", _)) | Some(("https", _)) => {
            return Err(
                "config key `endpoint` is an HTTP URL; that is the Data API, served by the `couchbase` plugin. This one speaks the KV protocol and wants `couchbase://host`"
                    .to_string(),
            )
        }
        Some((_, _)) => {
            return Err("config key `endpoint` must use couchbase://".to_string())
        }
        None => (raw, false),
    };
    let default_port = if tls { 11207 } else { DEFAULT_KV_PORT };

    if rest.is_empty() {
        return Err("config key `endpoint` has no host".to_string());
    }
    if let Some(bracketed) = rest.strip_prefix('[') {
        let (host, suffix) = bracketed
            .split_once(']')
            .ok_or("config key `endpoint` has an unclosed IPv6 address")?;
        let address = host
            .parse::<std::net::Ipv6Addr>()
            .map_err(|_| "config key `endpoint` has an invalid IPv6 address".to_string())?;
        let port = if suffix.is_empty() {
            default_port
        } else {
            let raw = suffix
                .strip_prefix(':')
                .ok_or("config key `endpoint` has unexpected text after its IPv6 address")?;
            raw.parse::<u16>()
                .map_err(|_| "config key `endpoint` has an invalid port".to_string())?
        };
        return Ok((address.to_string(), port, tls));
    }
    if rest.matches(':').count() > 1 {
        return Err("config key `endpoint` must enclose an IPv6 address in brackets".to_string());
    }
    match rest.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() => {
            let port = port
                .parse::<u16>()
                .map_err(|_| "config key `endpoint` has an invalid port".to_string())?;
            Ok((host.to_string(), port, tls))
        }
        _ => Ok((rest.to_string(), default_port, tls)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_errors_do_not_echo_secret_derived_config_values() {
        for (key, value) in [
            ("timeout-ms", "synthetic-private-value"),
            ("query-port", "synthetic-private-value"),
            ("endpoint", "synthetic-private-value://backend"),
            ("endpoint", "couchbase://[synthetic-private-value]"),
            ("endpoint", "couchbase://backend:synthetic-private-value"),
            ("endpoint", "couchbase://[::1]:synthetic-private-value"),
        ] {
            let mut pairs = minimal();
            pairs.retain(|(k, _)| k != key);
            pairs.push((key.into(), value.into()));
            let error = Binding::from_config(&pairs).unwrap_err();
            assert!(
                error.contains(key),
                "the diagnostic should identify the key"
            );
            assert!(!error.contains("synthetic-private-value"), "{error}");
        }
    }

    #[test]
    fn delimiters_in_credentials_do_not_alias_another_identity() {
        let mut first = Binding::from_config(&minimal()).unwrap();
        first.username = "app|extra".into();
        first.password = "secret".into();
        let second = Binding {
            username: "app".into(),
            password: "extra|secret".into(),
            ..first.clone()
        };
        assert_ne!(first.connection_key(), second.connection_key());
    }

    #[test]
    fn secret_values_retain_every_password_byte() {
        for password in [" secret ", "  a pass phrase  ", "      "] {
            let mut pairs = minimal();
            pairs.retain(|(key, _)| key != "password");
            pairs.push(("password".into(), password.into()));
            assert_eq!(Binding::from_config(&pairs).unwrap().password, password);
        }
    }

    #[test]
    fn ipv6_hosts_and_ports_are_separated_and_http_authorities_are_bracketed() {
        for (endpoint, host, port) in [
            ("couchbase://[::1]", "::1", 11210),
            ("couchbase://[2001:db8::1]:12000", "2001:db8::1", 12000),
        ] {
            assert_eq!(
                parse_endpoint(endpoint).unwrap(),
                (host.into(), port, false)
            );
            let mut binding = Binding::from_config(&minimal()).unwrap();
            binding.host = host.into();
            assert_eq!(binding.query_authority(), format!("[{host}]:8093"));
        }
        for invalid in [
            "couchbase://[::1",
            "couchbase://[bad]:123",
            "couchbase://[::1]extra",
            "couchbase://2001:db8::1",
        ] {
            assert!(parse_endpoint(invalid).is_err(), "{invalid}");
        }
    }

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
        assert_eq!(
            parse_endpoint("couchbase://host:12000").unwrap(),
            ("host".to_string(), 12000, false)
        );
        assert_eq!(
            parse_endpoint("host").unwrap(),
            ("host".to_string(), DEFAULT_KV_PORT, false)
        );
    }

    #[test]
    #[cfg(not(feature = "tls"))]
    fn tls_is_refused_rather_than_downgraded() {
        let err = parse_endpoint("couchbases://host").unwrap_err();
        assert!(err.contains("--features tls"), "got: {err}");
        // And an HTTP endpoint points at the sibling plugin.
        assert!(parse_endpoint("https://host")
            .unwrap_err()
            .contains("Data API"));
    }

    #[test]
    #[cfg(feature = "tls")]
    fn tls_preserves_identity_and_defaults_both_encrypted_ports() {
        for (endpoint, host, port) in [
            ("couchbases://db.example", "db.example", 11207),
            ("couchbases://db.example:12000", "db.example", 12000),
            ("couchbases://[2001:db8::1]", "2001:db8::1", 11207),
        ] {
            let mut pairs = minimal();
            pairs[0].1 = endpoint.into();
            let binding = Binding::from_config(&pairs).unwrap();
            assert_eq!(
                (binding.host.as_str(), binding.port, binding.tls),
                (host, port, true)
            );
            assert_eq!(binding.query_port, 18093);
            let plain = Binding {
                tls: false,
                ..binding.clone()
            };
            assert_ne!(binding.connection_key(), plain.connection_key());
            let other_name = Binding {
                host: "other.example".into(),
                ..binding.clone()
            };
            assert_ne!(binding.connection_key(), other_name.connection_key());
        }
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
            let pairs: Vec<_> = minimal()
                .into_iter()
                .filter(|(k, _)| k != missing)
                .collect();
            let err = Binding::from_config(&pairs).unwrap_err();
            assert!(
                err.contains(missing),
                "error should name `{missing}`, got: {err}"
            );
        }
    }

    #[test]
    fn a_per_call_timeout_overrides_the_bindings() {
        let binding = Binding::from_config(&minimal()).unwrap();
        assert_eq!(
            binding.with_timeout_ns(Some(5_000_000_000)).timeout_ms,
            5_000
        );
        assert_eq!(binding.with_timeout_ns(None).timeout_ms, DEFAULT_TIMEOUT_MS);
        assert_eq!(
            binding.with_timeout_ns(Some(0)).timeout_ms,
            DEFAULT_TIMEOUT_MS
        );
        assert_eq!(binding.with_timeout_ns(Some(1)).timeout_ms, 1);
        assert_eq!(binding.with_timeout_ns(Some(1_500_000)).timeout_ms, 2);
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

        let mut wrong_password = minimal();
        wrong_password.retain(|(k, _)| k != "password");
        wrong_password.push(("password".to_string(), "not-it".to_string()));
        let c = Binding::from_config(&wrong_password).unwrap();
        assert_ne!(
            a.connection_key(),
            c.connection_key(),
            "a wrong password must not reuse another's authenticated connection"
        );
    }
}
