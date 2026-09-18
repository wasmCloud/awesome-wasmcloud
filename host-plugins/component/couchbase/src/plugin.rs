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
use crate::mutation;
use crate::timefmt;

use bindings::exports::wasmcloud::couchbase::document::{
    DocumentGetAndLockOptions, DocumentGetAndTouchOptions, DocumentGetOptions, DocumentGetResult,
    DocumentInsertOptions, DocumentRemoveOptions, DocumentReplaceOptions, DocumentTouchOptions,
    DocumentUnlockOptions, DocumentUpsertOptions, Guest as DocumentGuest,
};
use bindings::exports::wasmcloud::couchbase::sqlpp::{
    Guest as SqlppGuest, SqlppQueryOptions, SqlppValue,
};
use bindings::exports::wasmcloud::couchbase::sqlpp_types::SqlppQueryError;
use bindings::exports::wasmcloud::couchbase::types::{
    Document, DocumentError, DurabilityLevel, MutationMetadata, Time,
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
fn caller_binding() -> Result<Binding, DocumentError> {
    let workload = identity::get_workload_id();
    if workload.is_empty() {
        return Err(DocumentError::NotConfigured(
            "no in-flight caller: wasmcloud:couchbase can only be used from a workload's capability call".to_string(),
        ));
    }
    lock(&BINDINGS).get(&workload).cloned().ok_or_else(|| {
        DocumentError::NotConfigured(format!(
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

    fn header(&self, name: &str) -> Option<String> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .and_then(|(_, v)| String::from_utf8(v.clone()).ok())
    }

    /// The CAS the cluster reported, or `0` if it sent none.
    fn cas(&self) -> u64 {
        self.header("etag").map(|e| api::parse_cas(&e)).unwrap_or(0)
    }

    /// The mutation token the cluster reported, if any.
    fn mutation_token(&self) -> Option<mutation::Token> {
        self.header("x-cb-mutationtoken")
            .as_deref()
            .and_then(mutation::parse)
    }

    /// The common flags the cluster reported, or `0` if it sent none.
    fn flags(&self) -> u32 {
        self.header("x-cb-flags")
            .and_then(|f| f.parse::<u32>().ok())
            .unwrap_or(0)
    }

    fn failure(&self) -> DocumentError {
        to_document_error(api::classify(self.status, &self.body))
    }
}


fn to_document_error(failure: Failure) -> DocumentError {
    match failure {
        Failure::NotFound => DocumentError::NotFound,
        Failure::AlreadyExists => DocumentError::AlreadyExists,
        Failure::CasMismatch => DocumentError::CasMismatch,
        Failure::Locked => DocumentError::Locked,
        Failure::NotLocked => DocumentError::NotLocked,
        Failure::InvalidArgument(m) => DocumentError::InvalidArgument(m),
        Failure::Unauthorized => DocumentError::Unauthorized,
        Failure::Timeout => DocumentError::Timeout,
        Failure::Server {
            status,
            code,
            message,
        } => DocumentError::Other(format!(
            "Couchbase returned HTTP {status}{}: {message}",
            code.map(|c| format!(" ({c})")).unwrap_or_default()
        )),
    }
}

fn to_query_error(failure: Failure) -> SqlppQueryError {
    match failure {
        Failure::InvalidArgument(m) => SqlppQueryError::InvalidArgument(m),
        Failure::Unauthorized => SqlppQueryError::Unauthorized,
        Failure::Timeout => SqlppQueryError::Timeout,
        Failure::NotFound => SqlppQueryError::InvalidArgument(
            "the statement referenced a keyspace that does not exist".to_string(),
        ),
        Failure::AlreadyExists | Failure::CasMismatch | Failure::Locked | Failure::NotLocked => {
            SqlppQueryError::Unexpected("unexpected document conflict from a query".to_string())
        }
        Failure::Server {
            status,
            code,
            message,
        } => SqlppQueryError::Server(format!(
            "HTTP {status}{}: {message}",
            code.map(|c| format!(" ({c})")).unwrap_or_default()
        )),
    }
}

/// A `document-error` restated as a `sqlpp-query-error`, so the query path can
/// reuse the shared binding/transport helpers without losing its meaning.
fn document_error_as_query_error(err: DocumentError) -> SqlppQueryError {
    match err {
        DocumentError::NotConfigured(m) => SqlppQueryError::NotConfigured(m),
        DocumentError::RequestFailed(m) => SqlppQueryError::RequestFailed(m),
        DocumentError::Unauthorized => SqlppQueryError::Unauthorized,
        DocumentError::Timeout => SqlppQueryError::Timeout,
        DocumentError::InvalidArgument(m) => SqlppQueryError::InvalidArgument(m),
        other => SqlppQueryError::Unexpected(format!("{other:?}")),
    }
}

/// Send one request to the cluster and read the whole response.
async fn send(
    binding: &Binding,
    method: Method,
    path: &str,
    extra_headers: &[(&str, String)],
    body: Option<Vec<u8>>,
) -> Result<Reply, DocumentError> {
    // Cancellation is checked before committing to a round-trip. A capability
    // call has no host-imposed timeout, so a teardown arriving while calls are
    // queued returns them immediately rather than waiting out the cluster.
    if cancel::is_cancelled() {
        return Err(DocumentError::Other(
            "invocation was cancelled before the request was sent".to_string(),
        ));
    }

    let fields = Fields::new();
    set_header(&fields, "authorization", &binding.authorization)?;
    set_header(&fields, "accept", "application/json")?;
    for (name, value) in extra_headers {
        set_header(&fields, name, value)?;
    }

    // Declare the body length up front, including the zero-length case. The
    // payload is always a complete `Vec<u8>`, but a `wasi:http` request body is
    // a stream, so without this the transport has no length to advertise and
    // falls back to `Transfer-Encoding: chunked` on every request — even a
    // `DELETE` carrying no body at all. Plenty of API gateways and WAFs in
    // front of a database handle a chunked request body poorly or reject it
    // outright, and the Data API reference documents neither.
    let content_length = body.as_ref().map_or(0, Vec::len);
    set_header(&fields, "content-length", &content_length.to_string())?;

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
        .map_err(|()| DocumentError::Other("wasi:http rejected the request method".to_string()))?;
    request
        .set_scheme(Some(&scheme))
        .map_err(|()| DocumentError::Other("wasi:http rejected the request scheme".to_string()))?;
    request
        .set_authority(Some(&binding.authority))
        .map_err(|()| {
            DocumentError::InvalidArgument(format!(
                "`{}` is not a valid host for the configured endpoint",
                binding.authority
            ))
        })?;
    request.set_path_with_query(Some(path)).map_err(|()| {
        DocumentError::InvalidArgument(format!("`{path}` is not a valid request path"))
    })?;

    let response = client::send(request).await.map_err(transport_error)?;

    let status = response.get_status_code();
    // Headers stay valid after `consume_body` moves the response.
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

fn set_header(fields: &Fields, name: &str, value: &str) -> Result<(), DocumentError> {
    fields.append(name, value.as_bytes()).map_err(|e| {
        DocumentError::InvalidArgument(format!("header `{name}` was rejected: {e:?}"))
    })
}

/// Map a `wasi:http` transport failure onto the interface's error model.
fn transport_error(code: ErrorCode) -> DocumentError {
    match code {
        // The host refused the egress: this plugin's own `allowedHosts` does
        // not cover the configured endpoint. That is an operator
        // misconfiguration, not a credential problem — reporting it as
        // `unauthorized` would send people off to rotate working credentials.
        ErrorCode::HttpRequestDenied => DocumentError::NotConfigured(
            "outbound HTTP was denied by this plugin's allowedHosts policy; add the Couchbase endpoint to the host plugin declaration".to_string(),
        ),
        ErrorCode::DestinationIpProhibited => DocumentError::NotConfigured(
            "the Couchbase endpoint resolved to an address this host forbids".to_string(),
        ),
        ErrorCode::DnsTimeout
        | ErrorCode::ConnectionTimeout
        | ErrorCode::ConnectionReadTimeout
        | ErrorCode::ConnectionWriteTimeout
        | ErrorCode::HttpResponseTimeout => DocumentError::Timeout,
        other => DocumentError::RequestFailed(format!("{other:?}")),
    }
}

// ---------------------------------------------------------------------------
// Request construction
// ---------------------------------------------------------------------------

/// The Data API path for one document.
///
/// Bucket, scope and collection come from the caller's binding rather than the
/// call, matching 0.1.0-draft, where they came from the link configuration.
fn document_path(binding: &Binding, id: &str) -> String {
    format!(
        "{}/v1/buckets/{}/scopes/{}/collections/{}/documents/{}",
        binding.base_path,
        api::encode_segment(&binding.bucket),
        api::encode_segment(&binding.scope),
        api::encode_segment(&binding.collection),
        api::encode_segment(id),
    )
}

/// The `X-CB-DurabilityLevel` value, or `None` to leave the header off.
fn durability(level: DurabilityLevel) -> Option<&'static str> {
    match level {
        // The 0.1.0-draft enum leads with `unknown`, which is how a caller says
        // "not set" in a record whose field is not an `option`.
        DurabilityLevel::Unknown => None,
        DurabilityLevel::None => Some("None"),
        DurabilityLevel::ReplicateMajority => Some("Majority"),
        DurabilityLevel::ReplicateMajorityPersistMaster => Some("MajorityAndPersistOnMaster"),
        DurabilityLevel::PersistMajority => Some("PersistToMajority"),
    }
}

/// Couchbase TTLs have second granularity, so a sub-second `expires-in-ns`
/// would silently become "no expiry"; round it up to one second instead.
fn expiry_header(expires_in_ns: u64) -> Option<(&'static str, String)> {
    if expires_in_ns == 0 {
        return None;
    }
    let seconds = expires_in_ns.div_ceil(1_000_000_000).max(1);
    Some(("expires", api::expiry_duration(seconds.min(u32::MAX.into()) as u32)))
}

/// A write's headers: content type, plus whichever options were set.
fn write_headers(
    cas: u64,
    expires_in_ns: u64,
    level: DurabilityLevel,
    flags: Option<u32>,
) -> Vec<(&'static str, String)> {
    // Content-Type is what the Data API derives flags from by default;
    // `X-CB-Flags` overrides it outright, so an explicit caller value wins.
    let mut headers = vec![("content-type", "application/json".to_string())];
    if let Some(flags) = flags {
        headers.push(("x-cb-flags", flags.to_string()));
    }
    if cas != 0 {
        headers.push(("if-match", api::format_cas(cas)));
    }
    if let Some(expiry) = expiry_header(expires_in_ns) {
        headers.push(expiry);
    }
    if let Some(level) = durability(level) {
        headers.push(("x-cb-durabilitylevel", level.to_string()));
    }
    headers
}


/// Reject option fields whose silent omission would change what gets stored or
/// what guarantee the caller believes they have.
///
/// `wasmcloud:keyvalue@0.2.0` states the rule this follows: a backend that
/// cannot honour an option "MUST raise an error rather than silently ignoring"
/// it. The line drawn here is *silently wrong* versus *safe-fail*:
///
/// - Rejected: `preserve-expiry` (Couchbase clears a TTL on write unless told
///   otherwise, so ignoring it silently drops the document's expiry), and
///   `persist-to`/`replicate-to` (a durability guarantee the caller thinks
///   they have).
/// - Accepted and not implemented: `retry-strategy` (not retrying surfaces more
///   errors, never a wrong value), `parent-span`, and the Query Service tuning
///   knobs, none of which change a result — only performance or telemetry.
fn reject_unhonoured_write_options(
    preserve_expiry: bool,
    persist_to: u64,
    replicate_to: u64,
) -> Result<(), DocumentError> {
    if preserve_expiry {
        return Err(DocumentError::Unsupported(
            "preserve-expiry has no Data API equivalent; Couchbase clears a document's expiry on write, so the TTL would be silently lost. Re-apply it with `expires-in-ns`, or read the current expiry first".to_string(),
        ));
    }
    if persist_to != 0 || replicate_to != 0 {
        return Err(DocumentError::Unsupported(
            "persist-to/replicate-to are the pre-6.0 durability settings and have no Data API equivalent; use `durability-level` instead".to_string(),
        ));
    }
    Ok(())
}

/// A successful write, reported as 0.1.0-draft's `mutation-metadata`.
///
/// The CAS comes from `etag`. The vbucket fields come from the Data API's
/// `X-CB-MutationToken`, when that token arrives in a form
/// [`mutation::parse`] can take apart; otherwise they stay `0` and the token
/// travels verbatim in `raw-token` rather than being guessed at. See
/// [`crate::mutation`] for why guessing is not an option here.
fn mutation(binding: &Binding, reply: &Reply) -> MutationMetadata {
    let token = reply.mutation_token();
    let parts = token.as_ref().and_then(mutation::Token::parts);
    MutationMetadata {
        cas: reply.cas(),
        bucket: binding.bucket.clone(),
        partition_id: parts.map_or(0, |p| p.partition_id),
        partition_uuid: parts.map_or(0, |p| p.partition_uuid),
        seq: parts.map_or(0, |p| p.sequence_number),
        raw_token: token.as_ref().and_then(|t| t.raw()).map(str::to_string),
    }
}

async fn write(
    binding: &Binding,
    method: Method,
    path: String,
    headers: Vec<(&'static str, String)>,
    body: Option<Vec<u8>>,
) -> Result<MutationMetadata, DocumentError> {
    let reply = send(binding, method, &path, &headers, body).await?;
    if !reply.ok() {
        return Err(reply.failure());
    }
    Ok(mutation(binding, &reply))
}

/// A GET whose body becomes a `document-get-result`.
async fn read(binding: &Binding, path: &str) -> Result<DocumentGetResult, DocumentError> {
    let reply = send(binding, Method::Get, path, &[], None).await?;
    if !reply.ok() {
        return Err(reply.failure());
    }
    Ok(DocumentGetResult {
        cas: reply.cas(),
        flags: reply.flags(),
        // Bytes exactly as stored: a Couchbase document need not be JSON, and
        // decoding it here would fail on every binary value.
        // The read reports the absolute expiry in an `Expires` header, present
        // only when the document has a TTL. It is filled whenever it is sent,
        // which `with-expiry` permits: the WIT says only that the field "may
        // not be present" without it. The relative, deprecated field is left
        // absent — deriving it would need a clock read and go stale at once.
        expires_at: reply
            .header("expires")
            .as_deref()
            .and_then(api::parse_expires)
            .map(|e| Time {
                offset: 0,
                year: e.year,
                month: e.month,
                day: e.day,
                hour: e.hour,
                minute: e.minute,
                second: e.second,
                milliseconds: 0,
                nanoseconds: 0,
            }),
        document: reply.body,
        expires_in_ns: None,
    })
}

// ---------------------------------------------------------------------------
// document
// ---------------------------------------------------------------------------

impl DocumentGuest for Component {
    async fn insert(
        id: String,
        doc: Document,
        options: Option<DocumentInsertOptions>,
    ) -> Result<MutationMetadata, DocumentError> {
        let binding = caller_binding()?;
        let (expires, level) = options
            .as_ref()
            .map_or((0, DurabilityLevel::Unknown), |o| {
                (o.expires_in_ns, o.durability_level)
            });
        if let Some(o) = options.as_ref() {
            reject_unhonoured_write_options(false, o.persist_to, o.replicate_to)?;
        }
        let binding = binding.with_timeout_ns(options.as_ref().and_then(|o| o.timeout_ns));
        let path = document_path(&binding, &id);
        write(
            &binding,
            Method::Post,
            path,
            write_headers(0, expires, level, options.as_ref().and_then(|o| o.flags)),
            Some(doc),
        )
        .await
    }

    async fn replace(
        id: String,
        doc: Document,
        options: Option<DocumentReplaceOptions>,
    ) -> Result<MutationMetadata, DocumentError> {
        let binding = caller_binding()?;
        let (cas, expires, level) = options
            .as_ref()
            .map_or((0, 0, DurabilityLevel::Unknown), |o| {
                (o.cas, o.expires_in_ns, o.durability_level)
            });
        if let Some(o) = options.as_ref() {
            reject_unhonoured_write_options(o.preserve_expiry, o.persist_to, o.replicate_to)?;
        }
        let binding = binding.with_timeout_ns(options.as_ref().and_then(|o| o.timeout_ns));
        let mut headers = write_headers(cas, expires, level, options.as_ref().and_then(|o| o.flags));
        // `replace` requires the document to exist. With no caller CAS to
        // condition on, the RFC 7232 wildcard asks the cluster to reject the
        // write when nothing is there, which is what keeps `replace` from
        // behaving like `upsert`.
        if cas == 0 {
            // Verified against Capella: `*` on an absent key answers
            // DocumentNotFound, which is exactly `replace`'s precondition.
            headers.push(("if-match", "*".to_string()));
        }
        let path = document_path(&binding, &id);
        write(
            &binding,
            Method::Put,
            path,
            headers,
            Some(doc),
        )
        .await
    }

    async fn upsert(
        id: String,
        doc: Document,
        options: Option<DocumentUpsertOptions>,
    ) -> Result<MutationMetadata, DocumentError> {
        let binding = caller_binding()?;
        let (expires, level) = options
            .as_ref()
            .map_or((0, DurabilityLevel::Unknown), |o| {
                (o.expires_in_ns, o.durability_level)
            });
        if let Some(o) = options.as_ref() {
            reject_unhonoured_write_options(o.preserve_expiry, o.persist_to, o.replicate_to)?;
        }
        let binding = binding.with_timeout_ns(options.as_ref().and_then(|o| o.timeout_ns));
        let path = document_path(&binding, &id);
        write(
            &binding,
            Method::Put,
            path,
            write_headers(0, expires, level, options.as_ref().and_then(|o| o.flags)),
            Some(doc),
        )
        .await
    }

    async fn get(
        id: String,
        options: Option<DocumentGetOptions>,
    ) -> Result<DocumentGetResult, DocumentError> {
        let binding = caller_binding()?;

        let binding = binding.with_timeout_ns(options.as_ref().and_then(|o| o.timeout_ns));
        let mut path = document_path(&binding, &id);
        if let Some(project) = options.as_ref().and_then(|o| o.project.as_ref()) {
            if !project.is_empty() {
                // One comma-separated `project`, not a repeated parameter: the
                // Data API's OpenAPI spec declares it `style: form,
                // explode: false`. Field names are still escaped, so a name
                // containing a comma cannot forge a separator.
                let fields = project
                    .iter()
                    .map(|field| api::encode_segment(field))
                    .collect::<Vec<_>>()
                    .join(",");
                path.push_str("?project=");
                path.push_str(&fields);
            }
        }
        read(&binding, &path).await
    }





    async fn remove(
        id: String,
        options: Option<DocumentRemoveOptions>,
    ) -> Result<MutationMetadata, DocumentError> {
        let binding = caller_binding()?;
        let (cas, level) = options
            .as_ref()
            .map_or((0, DurabilityLevel::Unknown), |o| (o.cas, o.durability_level));
        if let Some(o) = options.as_ref() {
            reject_unhonoured_write_options(false, o.persist_to, o.replicate_to)?;
        }
        let binding = binding.with_timeout_ns(options.as_ref().and_then(|o| o.timeout_ns));
        // A delete carries no body, so drop the content-type that leads the
        // write headers.
        let headers: Vec<_> = write_headers(cas, 0, level, None)
            .into_iter()
            .filter(|(name, _)| *name != "content-type")
            .collect();
        let path = document_path(&binding, &id);
        write(&binding, Method::Delete, path, headers, None).await
    }



    /// Not servable over the Data API: its OpenAPI specification has no
    /// locking path. A binary-protocol implementation of this interface serves
    /// it.
    async fn get_and_lock(
        _id: String,
        _options: Option<DocumentGetAndLockOptions>,
    ) -> Result<DocumentGetResult, DocumentError> {
        Err(DocumentError::Unsupported(
            "get-and-lock is a KV-protocol operation with no Data API endpoint; use `get` plus a CAS-conditional `replace` for optimistic concurrency, or an implementation that speaks couchbases://".to_string(),
        ))
    }

    /// Not servable over the Data API; see `get-and-lock`.
    async fn unlock(
        _id: String,
        _options: Option<DocumentUnlockOptions>,
    ) -> Result<(), DocumentError> {
        Err(DocumentError::Unsupported(
            "unlock is a KV-protocol operation with no Data API endpoint; nothing can be locked through this implementation".to_string(),
        ))
    }

    async fn touch(
        id: String,
        options: Option<DocumentTouchOptions>,
    ) -> Result<MutationMetadata, DocumentError> {
        let binding = caller_binding()?;
        let expires_in = options.as_ref().map_or(0, |o| o.expires_in);
        let binding = binding.with_timeout_ns(options.as_ref().and_then(|o| o.timeout_ns));
        let body = touch_body(expires_in, false)?;
        let path = format!("{}/touch", document_path(&binding, &id));
        write(
            &binding,
            Method::Post,
            path,
            vec![("content-type", "application/json".to_string())],
            Some(body),
        )
        .await
    }

    async fn get_and_touch(
        id: String,
        options: Option<DocumentGetAndTouchOptions>,
    ) -> Result<DocumentGetResult, DocumentError> {
        let binding = caller_binding()?;
        let expires_in = options.as_ref().map_or(0, |o| o.expires_in);
        let binding = binding.with_timeout_ns(options.as_ref().and_then(|o| o.timeout_ns));
        let body = touch_body(expires_in, true)?;
        let path = format!("{}/touch", document_path(&binding, &id));

        let reply = send(
            &binding,
            Method::Post,
            &path,
            &[("content-type", "application/json".to_string())],
            Some(body),
        )
        .await?;
        if !reply.ok() {
            return Err(reply.failure());
        }

        // `returnContent` asks the touch endpoint for the value back, so this
        // stays a single round-trip rather than a touch followed by a get.
        Ok(DocumentGetResult {
            cas: reply.cas(),
            flags: reply.flags(),
            document: reply.body,
            expires_in_ns: Some(expires_in),
            expires_at: None,
        })
    }
}

/// The touch endpoint's body: an absolute ISO 8601 instant, plus whether to
/// send the value back.
fn touch_body(expires_in_ns: u64, return_content: bool) -> Result<Vec<u8>, DocumentError> {
    if expires_in_ns == 0 {
        return Err(DocumentError::InvalidArgument(
            "touch requires a non-zero expires-in; clear an expiry with `upsert` and expires-in-ns 0"
                .to_string(),
        ));
    }
    let seconds = expires_in_ns.div_ceil(1_000_000_000).max(1);
    // The touch endpoint takes an absolute instant, so a relative TTL has to be
    // added to the wall clock here.
    let now = system_clock::now();
    let expires_at = now.seconds.saturating_add(seconds as i64);
    Ok(serde_json::json!({
        "expiry": timefmt::iso8601_utc(expires_at),
        "returnContent": return_content,
    })
    .to_string()
    .into_bytes())
}

// ---------------------------------------------------------------------------
// sqlpp
// ---------------------------------------------------------------------------

/// The Query Service response envelope.
#[derive(serde::Deserialize)]
struct QueryBody {
    #[serde(default)]
    results: Vec<serde_json::Value>,
    #[serde(default)]
    errors: Vec<serde_json::Value>,
}

impl SqlppGuest for Component {
    async fn query(
        query: String,
        params: Vec<SqlppValue>,
        options: Option<SqlppQueryOptions>,
    ) -> Result<SqlppValue, SqlppQueryError> {
        let binding = caller_binding().map_err(document_error_as_query_error)?;

        let mut request = serde_json::Map::new();
        request.insert("statement".to_string(), query.into());

        if !params.is_empty() {
            let mut args = Vec::with_capacity(params.len());
            for (index, param) in params.iter().enumerate() {
                args.push(match param {
                    SqlppValue::Null => serde_json::Value::Null,
                    SqlppValue::Json(text) => {
                        serde_json::from_str(text).map_err(|e| {
                            SqlppQueryError::InvalidArgument(format!(
                                "positional parameter {} is not valid JSON: {e}",
                                index + 1
                            ))
                        })?
                    }
                });
            }
            request.insert("args".to_string(), serde_json::Value::Array(args));
        }

        // Default the query context to the binding's own bucket and scope so an
        // unqualified `SELECT * FROM orders` resolves inside what the workload
        // was granted rather than against the whole cluster.
        request.insert(
            "query_context".to_string(),
            format!("default:{}.{}", binding.bucket, binding.scope).into(),
        );

        // `consistent-with` is served by turning the caller's tokens into the
        // query service's sparse scan vector. Setting one implies `at_plus`,
        // matching how the Couchbase SDKs treat `consistentWith`, so it
        // overrides whatever `scan-consistency` the caller also set.
        let mut scan_vector = None;
        if let Some(state) = options.as_ref().and_then(|o| o.consistent_with.as_ref()) {
            let tokens: Vec<mutation::Token> = state
                .tokens
                .iter()
                .map(|t| {
                    mutation::Token::Structured(mutation::Parts {
                        bucket: t.bucket_name.clone(),
                        partition_id: t.partition_id,
                        partition_uuid: t.partition_uuid,
                        sequence_number: t.sequence_number,
                    })
                })
                .collect();

            // A scan vector covers one keyspace. A token from another bucket
            // cannot constrain this query, and quietly dropping it would leave
            // the caller believing in a guarantee they do not have.
            if let Some(other) = state
                .tokens
                .iter()
                .find(|t| !t.bucket_name.is_empty() && t.bucket_name != binding.bucket)
            {
                return Err(SqlppQueryError::InvalidArgument(format!(
                    "consistent-with carries a token for bucket `{}`, but this binding is bound to `{}`",
                    other.bucket_name, binding.bucket
                )));
            }

            match mutation::scan_vector(&tokens) {
                Some(vector) => scan_vector = Some(vector),
                None => {
                    return Err(SqlppQueryError::InvalidArgument(
                        "consistent-with needs a mutation token with a non-zero partition-uuid for every entry; a zeroed token means none was available, and a partial scan vector would let the query run without waiting. Use scan-consistency request-plus instead".to_string(),
                    ))
                }
            }
        }

        let mut timeout_ms = binding.timeout_ms;
        if let Some(options) = options.as_ref() {
            let consistency = match options.scan_consistency {
                bindings::exports::wasmcloud::couchbase::types::QueryScanConsistency::NotBounded => {
                    "not_bounded"
                }
                bindings::exports::wasmcloud::couchbase::types::QueryScanConsistency::RequestPlus => {
                    "request_plus"
                }
            };
            request.insert("scan_consistency".to_string(), consistency.into());
            request.insert("readonly".to_string(), options.readonly.into());
            request.insert("metrics".to_string(), options.metrics.into());
            if let Some(id) = options.client_context_id.as_ref() {
                request.insert("client_context_id".to_string(), id.clone().into());
            }
            if options.timeout_ns > 0 {
                timeout_ms = options.timeout_ns.div_ceil(1_000_000).max(1) as u32;
            }
        }
        request.insert("timeout".to_string(), format!("{timeout_ms}ms").into());

        // Applied last: a scan vector implies `at_plus`, which must override the
        // `scan-consistency` set from the options above rather than lose to it.
        if let Some(vector) = scan_vector {
            request.insert("scan_vector".to_string(), vector);
            request.insert("scan_consistency".to_string(), "at_plus".into());
        }

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
        .await
        .map_err(document_error_as_query_error)?;

        if !reply.ok() {
            return Err(to_query_error(api::classify(reply.status, &reply.body)));
        }

        let parsed: QueryBody = serde_json::from_slice(&reply.body).map_err(|e| {
            SqlppQueryError::Unexpected(format!(
                "the Query Service response was not the expected JSON object: {e}"
            ))
        })?;

        // A 200 can still carry statement-level errors in the envelope; surface
        // them rather than returning an empty, apparently-successful result.
        if !parsed.errors.is_empty() {
            return Err(to_query_error(api::classify(200, &reply.body)));
        }

        Ok(SqlppValue::Json(
            serde_json::Value::Array(parsed.results).to_string(),
        ))
    }
}

mod export {
    #![allow(unsafe_code)]
    use super::{bindings, Component};
    bindings::export!(Component with_types_in bindings);
}
