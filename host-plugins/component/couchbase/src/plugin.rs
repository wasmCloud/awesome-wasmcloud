//! The WIT bindings glue: lifecycle hooks, the exported capability, and the
//! `wasi:http/client` request path they share.
//!
//! Compiled only for wasm. Everything that can be decided without the bindings
//! lives in [`crate::api`], [`crate::config`] and [`crate::timefmt`].

mod bindings {
    #![allow(unsafe_code)]
    wit_bindgen::generate!({ world: "couchbase-plugin", generate_all });
}

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

use crate::api::{self, Failure};
use crate::config::Binding;
use crate::timefmt;

use bindings::exports::wasmcloud::couchbase::binary::{
    CounterOptions, CounterResult, Guest as BinaryGuest,
};
use bindings::exports::wasmcloud::couchbase::document::Guest as DocumentGuest;
use bindings::exports::wasmcloud::couchbase::query::{
    Guest as QueryGuest, QueryMetrics, QueryOptions, QueryResult, ScanConsistency,
};
use bindings::exports::wasmcloud::couchbase::types::{
    Document, DurabilityLevel, Error, Location, MutationOptions, MutationResult, ServerError,
};
use bindings::exports::wasmcloud::host::workload_lifecycle::{
    Guest as LifecycleGuest, WorkloadInfo,
};
use bindings::wasi::clocks::system_clock;
use bindings::wasi::http::client;
use bindings::wasi::http::types::{
    ErrorCode, Fields, Method, Request, RequestOptions, Response, Scheme,
};
use bindings::wasmcloud::host::{cancel, identity};

/// Each workload's validated Couchbase binding, keyed by workload id.
///
/// Held only inside synchronous blocks, never across an `.await`, so a plain
/// `Mutex` is enough even though concurrent capability calls interleave
/// cooperatively on this one pinned instance.
///
/// This resets when the plugin's store restarts. That is safe because the host
/// replays `on-workload-bind` for every still-bound workload before serving any
/// queued call — which is also why the bind hook must stay idempotent.
static BINDINGS: Mutex<BTreeMap<String, Binding>> = Mutex::new(BTreeMap::new());

struct Component;

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

impl LifecycleGuest for Component {
    /// Validate and record a workload's cluster configuration.
    ///
    /// Returning an error here fails the *deploy* with this message, which is
    /// why every required key is checked now rather than on the workload's
    /// first call.
    async fn on_workload_bind(workload: WorkloadInfo) -> Result<(), String> {
        let Some(iface) = workload
            .interfaces
            .iter()
            .find(|i| i.namespace == "wasmcloud" && i.package == "couchbase")
        else {
            // The host only binds workloads that matched one of our exports, so
            // this should not happen; tolerate it rather than failing a deploy
            // over a binding we have nothing to configure for.
            return Ok(());
        };

        let binding = Binding::from_config(&iface.config).map_err(|reason| {
            format!(
                "workload `{}` in namespace `{}`: wasmcloud:couchbase {reason}",
                workload.name, workload.namespace
            )
        })?;

        // Idempotent by construction: a replayed bind overwrites the entry with
        // an identical one.
        lock(&BINDINGS).insert(workload.id, binding);
        Ok(())
    }

    /// Best-effort cleanup; tolerates ids never bound or already unbound.
    async fn on_workload_unbind(id: String) {
        lock(&BINDINGS).remove(&id);
    }
}

/// Take a lock, treating poisoning as recoverable.
///
/// A trapped call can poison the mutex while the store itself survives, and the
/// map's invariant does not depend on any single call completing. Panicking
/// here would take down every other tenant over one caller's fault.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The calling workload's binding.
///
/// `get-workload-id` is resolved from the in-flight capability call, so it is
/// exact even while other tenants' calls interleave on this instance.
fn caller_binding() -> Result<Binding, Error> {
    let workload = identity::get_workload_id();
    if workload.is_empty() {
        return Err(Error::NotConfigured(
            "no in-flight caller: wasmcloud:couchbase can only be used from a workload's capability call".to_string(),
        ));
    }
    lock(&BINDINGS).get(&workload).cloned().ok_or_else(|| {
        Error::NotConfigured(format!(
            "workload `{workload}` has no wasmcloud:couchbase binding on this plugin"
        ))
    })
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

/// A completed Data API round-trip.
struct Reply {
    status: u16,
    headers: Vec<(String, Vec<u8>)>,
    body: Vec<u8>,
}

impl Reply {
    fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// First value of a header, compared case-insensitively.
    fn header(&self, name: &str) -> Option<String> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .and_then(|(_, v)| String::from_utf8(v.clone()).ok())
    }

    /// The CAS the cluster reported for this response, or `0` if it sent none.
    fn cas(&self) -> u64 {
        self.header("etag").map(|e| api::parse_cas(&e)).unwrap_or(0)
    }

    /// Turn a non-2xx response into the WIT error it represents.
    fn failure(&self) -> Error {
        to_error(api::classify(self.status, &self.body))
    }
}

fn to_error(failure: Failure) -> Error {
    match failure {
        Failure::NotFound => Error::NotFound,
        Failure::AlreadyExists => Error::AlreadyExists,
        Failure::CasMismatch => Error::CasMismatch,
        Failure::InvalidArgument(m) => Error::InvalidArgument(m),
        Failure::Unauthorized => Error::Unauthorized,
        Failure::Timeout => Error::Timeout,
        Failure::Server {
            status,
            code,
            message,
        } => Error::Server(ServerError {
            status,
            code,
            message,
        }),
    }
}

/// Send one request to the cluster and read the whole response.
///
/// The body is buffered rather than streamed: every Data API response this
/// plugin issues is a single JSON document or a single stored value, and the
/// WIT returns owned values, so there is nothing for a stream to buy here.
async fn send(
    binding: &Binding,
    method: Method,
    path: &str,
    extra_headers: &[(&str, String)],
    body: Option<Vec<u8>>,
) -> Result<Reply, Error> {
    // Cancellation is checked here, before committing to a round-trip. The
    // capability call itself has no host-imposed timeout, so a teardown that
    // arrives while calls are queued returns immediately instead of each one
    // waiting out the cluster. An already-issued request still runs to
    // completion or to its own transport timeout.
    if cancel::is_cancelled() {
        return Err(Error::Other(
            "invocation was cancelled before the request was sent".to_string(),
        ));
    }

    let fields = Fields::new();
    set_header(&fields, "authorization", &binding.authorization)?;
    set_header(&fields, "accept", "application/json")?;
    for (name, value) in extra_headers {
        set_header(&fields, name, value)?;
    }

    let (trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));
    let contents = match body {
        Some(bytes) => {
            let (mut tx, rx) = bindings::wit_stream::new();
            wit_bindgen::spawn_local(async move {
                tx.write_all(bytes).await;
                drop(tx);
                let _ = trailers_tx.write(Ok(None)).await;
            });
            Some(rx)
        }
        None => {
            // Dropping the unwritten writer resolves the future with the
            // constructor's `Ok(None)` default as soon as the host polls it.
            drop(trailers_tx);
            None
        }
    };

    let options = RequestOptions::new();
    let timeout_ns = u64::from(binding.timeout_ms).saturating_mul(1_000_000);
    let _ = options.set_connect_timeout(Some(timeout_ns));
    let _ = options.set_first_byte_timeout(Some(timeout_ns));
    let _ = options.set_between_bytes_timeout(Some(timeout_ns));

    let (request, _sent) = Request::new(fields, contents, trailers_rx, Some(options));
    let scheme = if binding.secure {
        Scheme::Https
    } else {
        Scheme::Http
    };
    request
        .set_method(&method)
        .map_err(|()| Error::Other("wasi:http rejected the request method".to_string()))?;
    request
        .set_scheme(Some(&scheme))
        .map_err(|()| Error::Other("wasi:http rejected the request scheme".to_string()))?;
    request
        .set_authority(Some(&binding.authority))
        .map_err(|()| {
            Error::InvalidArgument(format!(
                "`{}` is not a valid host for the configured endpoint",
                binding.authority
            ))
        })?;
    request
        .set_path_with_query(Some(path))
        .map_err(|()| Error::InvalidArgument(format!("`{path}` is not a valid request path")))?;

    let response = client::send(request).await.map_err(transport_error)?;

    let status = response.get_status_code();
    // Headers stay valid after `consume_body` moves the response, so read them
    // first and keep the borrow-free copy.
    let headers = response.get_headers().copy_all();

    let (res_tx, res_rx) = bindings::wit_future::new(|| Ok(()));
    let (body_stream, _trailers) = Response::consume_body(response, res_rx);
    let body = body_stream.collect().await;
    drop(res_tx);

    Ok(Reply {
        status,
        headers,
        body,
    })
}

fn set_header(fields: &Fields, name: &str, value: &str) -> Result<(), Error> {
    fields
        .append(name, value.as_bytes())
        .map_err(|e| Error::InvalidArgument(format!("header `{name}` was rejected: {e:?}")))
}

/// Map a `wasi:http` transport failure onto the interface's error model.
fn transport_error(code: ErrorCode) -> Error {
    match code {
        // The host refused the egress: this plugin's own `allowedHosts` does
        // not cover the configured endpoint. That is an operator
        // misconfiguration, not a credential problem, so say so precisely —
        // reporting it as `unauthorized` would send people off to rotate
        // perfectly good Couchbase credentials.
        ErrorCode::HttpRequestDenied => Error::NotConfigured(
            "outbound HTTP was denied by this plugin's allowedHosts policy; add the Couchbase endpoint to the host plugin declaration".to_string(),
        ),
        ErrorCode::DestinationIpProhibited => Error::NotConfigured(
            "the Couchbase endpoint resolved to an address this host forbids".to_string(),
        ),
        ErrorCode::DnsTimeout
        | ErrorCode::ConnectionTimeout
        | ErrorCode::ConnectionReadTimeout
        | ErrorCode::ConnectionWriteTimeout
        | ErrorCode::HttpResponseTimeout => Error::Timeout,
        other => Error::RequestFailed(format!("{other:?}")),
    }
}

// ---------------------------------------------------------------------------
// Request construction
// ---------------------------------------------------------------------------

/// Build the Data API path for one document.
fn document_path(binding: &Binding, loc: &Location) -> String {
    format!(
        "{}/v1/buckets/{}/scopes/{}/collections/{}/documents/{}",
        binding.base_path,
        api::encode_segment(&binding.bucket),
        api::encode_segment(binding.scope_or_default(loc.scope.as_deref())),
        api::encode_segment(binding.collection_or_default(loc.collection.as_deref())),
        api::encode_segment(&loc.key),
    )
}

/// Headers common to the write operations.
///
/// `If-Match` carries the CAS, so a conditional write is rejected by the
/// cluster rather than by anything this plugin does.
fn mutation_headers(options: Option<&MutationOptions>) -> Vec<(&'static str, String)> {
    let mut headers = vec![("content-type", "application/json".to_string())];
    let Some(options) = options else {
        return headers;
    };
    if let Some(cas) = options.cas.filter(|c| *c != 0) {
        headers.push(("if-match", format!("\"{cas}\"")));
    }
    if let Some(expiry) = options.expiry_seconds {
        headers.push(("expires", api::expiry_duration(expiry)));
    }
    if let Some(level) = options.durability {
        headers.push(("x-cb-durabilitylevel", durability(level).to_string()));
    }
    if let Some(flags) = options.flags {
        headers.push(("x-cb-flags", flags.to_string()));
    }
    headers
}

fn durability(level: DurabilityLevel) -> &'static str {
    api::durability_header(match level {
        DurabilityLevel::None => 0,
        DurabilityLevel::Majority => 1,
        DurabilityLevel::MajorityAndPersistToActive => 2,
        DurabilityLevel::PersistToMajority => 3,
    })
}

/// Run a write and report its resulting CAS.
async fn write(
    binding: &Binding,
    method: Method,
    path: String,
    headers: Vec<(&'static str, String)>,
    body: Option<Vec<u8>>,
) -> Result<MutationResult, Error> {
    let reply = send(binding, method, &path, &headers, body).await?;
    if !reply.ok() {
        return Err(reply.failure());
    }
    Ok(MutationResult { cas: reply.cas() })
}

// ---------------------------------------------------------------------------
// document
// ---------------------------------------------------------------------------

impl DocumentGuest for Component {
    async fn get(loc: Location, project: Vec<String>) -> Result<Option<Document>, Error> {
        let binding = caller_binding()?;
        let mut path = document_path(&binding, &loc);
        if !project.is_empty() {
            let query = project
                .iter()
                .map(|field| format!("project={}", api::encode_segment(field)))
                .collect::<Vec<_>>()
                .join("&");
            path.push('?');
            path.push_str(&query);
        }

        let reply = send(&binding, Method::Get, &path, &[], None).await?;
        if reply.status == 404 {
            return Ok(None);
        }
        if !reply.ok() {
            return Err(reply.failure());
        }
        Ok(Some(Document {
            cas: reply.cas(),
            flags: reply
                .header("x-cb-flags")
                .and_then(|f| f.parse::<u32>().ok())
                .unwrap_or(0),
            content: reply.body,
        }))
    }

    /// The Data API exposes no existence check that skips the value, so this is
    /// a read whose body is discarded. Prefer `get` when the value is wanted
    /// anyway, rather than paying for two round-trips.
    async fn exists(loc: Location) -> Result<bool, Error> {
        let binding = caller_binding()?;
        let path = document_path(&binding, &loc);
        let reply = send(&binding, Method::Get, &path, &[], None).await?;
        match reply.status {
            404 => Ok(false),
            _ if reply.ok() => Ok(true),
            _ => Err(reply.failure()),
        }
    }

    async fn insert(
        loc: Location,
        content: Vec<u8>,
        options: Option<MutationOptions>,
    ) -> Result<MutationResult, Error> {
        let binding = caller_binding()?;
        let path = document_path(&binding, &loc);
        write(
            &binding,
            Method::Post,
            path,
            mutation_headers(options.as_ref()),
            Some(content),
        )
        .await
    }

    async fn upsert(
        loc: Location,
        content: Vec<u8>,
        options: Option<MutationOptions>,
    ) -> Result<MutationResult, Error> {
        let binding = caller_binding()?;
        let path = document_path(&binding, &loc);
        write(
            &binding,
            Method::Put,
            path,
            mutation_headers(options.as_ref()),
            Some(content),
        )
        .await
    }

    /// `replace` is `upsert` plus a precondition that the document already
    /// exists: the caller's CAS when they gave one, otherwise the RFC 7232
    /// wildcard, which asks the cluster to reject the write if nothing is
    /// there.
    async fn replace(
        loc: Location,
        content: Vec<u8>,
        options: Option<MutationOptions>,
    ) -> Result<MutationResult, Error> {
        let binding = caller_binding()?;
        let path = document_path(&binding, &loc);
        let mut headers = mutation_headers(options.as_ref());
        if !headers.iter().any(|(name, _)| *name == "if-match") {
            headers.push(("if-match", "*".to_string()));
        }
        write(&binding, Method::Put, path, headers, Some(content)).await
    }

    async fn remove(
        loc: Location,
        options: Option<MutationOptions>,
    ) -> Result<MutationResult, Error> {
        let binding = caller_binding()?;
        let path = document_path(&binding, &loc);
        // A delete carries no body, so the content-type header that
        // `mutation_headers` leads with is dropped.
        let headers: Vec<_> = mutation_headers(options.as_ref())
            .into_iter()
            .filter(|(name, _)| *name != "content-type")
            .collect();
        write(&binding, Method::Delete, path, headers, None).await
    }

    async fn touch(loc: Location, expiry_seconds: u32) -> Result<MutationResult, Error> {
        if expiry_seconds == 0 {
            return Err(Error::InvalidArgument(
                "touch requires a non-zero expiry; clear an expiry with upsert and expiry-seconds 0"
                    .to_string(),
            ));
        }
        let binding = caller_binding()?;

        // The touch endpoint takes an absolute instant, so the relative TTL is
        // added to the wall clock here.
        let now = system_clock::now();
        let expires_at = now.seconds.saturating_add(i64::from(expiry_seconds));
        let body = serde_json::json!({
            "expiry": timefmt::iso8601_utc(expires_at),
            "returnContent": false,
        });

        let path = format!("{}/touch", document_path(&binding, &loc));
        write(
            &binding,
            Method::Post,
            path,
            vec![("content-type", "application/json".to_string())],
            Some(body.to_string().into_bytes()),
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// binary
// ---------------------------------------------------------------------------

/// What the counter endpoints return: the new value, alongside the usual CAS.
#[derive(serde::Deserialize)]
struct CounterBody {
    #[serde(default)]
    value: u64,
}

impl BinaryGuest for Component {
    async fn increment(
        loc: Location,
        options: Option<CounterOptions>,
    ) -> Result<CounterResult, Error> {
        counter(loc, options, "increment").await
    }

    async fn decrement(
        loc: Location,
        options: Option<CounterOptions>,
    ) -> Result<CounterResult, Error> {
        counter(loc, options, "decrement").await
    }

    async fn append(
        loc: Location,
        content: Vec<u8>,
        options: Option<MutationOptions>,
    ) -> Result<MutationResult, Error> {
        concat(loc, content, options, "append").await
    }

    async fn prepend(
        loc: Location,
        content: Vec<u8>,
        options: Option<MutationOptions>,
    ) -> Result<MutationResult, Error> {
        concat(loc, content, options, "prepend").await
    }
}

async fn counter(
    loc: Location,
    options: Option<CounterOptions>,
    op: &str,
) -> Result<CounterResult, Error> {
    let binding = caller_binding()?;
    let path = format!("{}/{op}", document_path(&binding, &loc));

    let mut body = serde_json::Map::new();
    let mut headers = vec![("content-type", "application/json".to_string())];
    if let Some(options) = options.as_ref() {
        if let Some(delta) = options.delta {
            body.insert("delta".to_string(), delta.into());
        }
        if let Some(initial) = options.initial {
            body.insert("initial".to_string(), initial.into());
        }
        if let Some(expiry) = options.expiry_seconds {
            headers.push(("expires", api::expiry_duration(expiry)));
        }
        if let Some(level) = options.durability {
            headers.push(("x-cb-durabilitylevel", durability(level).to_string()));
        }
    }

    let payload = serde_json::Value::Object(body).to_string().into_bytes();
    let reply = send(&binding, Method::Post, &path, &headers, Some(payload)).await?;
    if !reply.ok() {
        return Err(reply.failure());
    }

    let parsed: CounterBody = serde_json::from_slice(&reply.body).map_err(|e| {
        Error::Other(format!(
            "the counter response was not the expected JSON object: {e}"
        ))
    })?;
    Ok(CounterResult {
        value: parsed.value,
        cas: reply.cas(),
    })
}

async fn concat(
    loc: Location,
    content: Vec<u8>,
    options: Option<MutationOptions>,
    op: &str,
) -> Result<MutationResult, Error> {
    let binding = caller_binding()?;
    let path = format!("{}/{op}", document_path(&binding, &loc));
    // Concatenation targets raw stored bytes, so it must not claim JSON.
    let headers: Vec<_> = mutation_headers(options.as_ref())
        .into_iter()
        .map(|(name, value)| {
            if name == "content-type" {
                (name, "application/octet-stream".to_string())
            } else {
                (name, value)
            }
        })
        .collect();
    write(&binding, Method::Post, path, headers, Some(content)).await
}

// ---------------------------------------------------------------------------
// query
// ---------------------------------------------------------------------------

/// The Query Service response envelope.
#[derive(serde::Deserialize)]
struct QueryBody {
    #[serde(default)]
    results: Vec<serde_json::Value>,
    #[serde(default)]
    metrics: Option<QueryMetricsBody>,
    #[serde(default)]
    errors: Vec<serde_json::Value>,
}

#[derive(serde::Deserialize)]
struct QueryMetricsBody {
    #[serde(rename = "elapsedTime", default)]
    elapsed_time: String,
    #[serde(rename = "executionTime", default)]
    execution_time: String,
    #[serde(rename = "resultCount", default)]
    result_count: u64,
    #[serde(rename = "resultSize", default)]
    result_size: u64,
}

impl QueryGuest for Component {
    async fn query(statement: String, options: Option<QueryOptions>) -> Result<QueryResult, Error> {
        let binding = caller_binding()?;

        let mut request = serde_json::Map::new();
        request.insert("statement".to_string(), statement.into());

        // Default the query context to the binding's own bucket and scope so an
        // unqualified `SELECT * FROM orders` resolves inside what the workload
        // was granted rather than against the whole cluster.
        let mut context = format!("default:{}.{}", binding.bucket, binding.scope);
        let mut timeout_ms = binding.timeout_ms;

        if let Some(options) = options.as_ref() {
            if !options.positional_parameters.is_empty() {
                let args = parse_json_values(&options.positional_parameters)?;
                request.insert("args".to_string(), serde_json::Value::Array(args));
            }
            for (name, raw) in &options.named_parameters {
                let value = serde_json::from_str::<serde_json::Value>(raw).map_err(|e| {
                    Error::InvalidArgument(format!("named parameter `{name}` is not valid JSON: {e}"))
                })?;
                request.insert(format!("${name}"), value);
            }
            if let Some(requested) = options.query_context.as_ref().filter(|c| !c.is_empty()) {
                context = requested.clone();
            }
            if let Some(consistency) = options.scan_consistency {
                let value = match consistency {
                    ScanConsistency::NotBounded => "not_bounded",
                    ScanConsistency::RequestPlus => "request_plus",
                };
                request.insert("scan_consistency".to_string(), value.into());
            }
            if let Some(requested) = options.timeout_ms.filter(|t| *t > 0) {
                timeout_ms = requested;
            }
            if let Some(read_only) = options.read_only {
                request.insert("readonly".to_string(), read_only.into());
            }
        }

        request.insert("query_context".to_string(), context.into());
        request.insert("timeout".to_string(), format!("{timeout_ms}ms").into());

        // The Data API fronts the Query Service on this passthrough path, so a
        // query travels the same credentialed endpoint as a KV call.
        let path = format!("{}/_p/query/query/service", binding.base_path);
        let payload = serde_json::Value::Object(request).to_string().into_bytes();

        // A per-query timeout must also bound the transport, or a query that
        // outlives it would still hold the caller open.
        let binding = Binding {
            timeout_ms,
            ..binding
        };
        let reply = send(
            &binding,
            Method::Post,
            &path,
            &[("content-type", "application/json".to_string())],
            Some(payload),
        )
        .await?;
        if !reply.ok() {
            return Err(reply.failure());
        }

        let parsed: QueryBody = serde_json::from_slice(&reply.body).map_err(|e| {
            Error::Other(format!(
                "the Query Service response was not the expected JSON object: {e}"
            ))
        })?;

        // A 200 can still carry statement-level errors in the envelope; surface
        // them rather than returning an empty, apparently-successful result.
        if !parsed.errors.is_empty() {
            return Err(to_error(api::classify(200, &reply.body)));
        }

        Ok(QueryResult {
            rows: parsed.results.iter().map(|row| row.to_string()).collect(),
            metrics: parsed.metrics.map(|m| QueryMetrics {
                elapsed_time: m.elapsed_time,
                execution_time: m.execution_time,
                result_count: m.result_count,
                result_size: m.result_size,
            }),
        })
    }
}

fn parse_json_values(raw: &[String]) -> Result<Vec<serde_json::Value>, Error> {
    raw.iter()
        .enumerate()
        .map(|(index, value)| {
            serde_json::from_str(value).map_err(|e| {
                Error::InvalidArgument(format!(
                    "positional parameter {} is not valid JSON: {e}",
                    index + 1
                ))
            })
        })
        .collect()
}

mod export {
    #![allow(unsafe_code)]
    use super::{bindings, Component};
    bindings::export!(Component with_types_in bindings);
}
