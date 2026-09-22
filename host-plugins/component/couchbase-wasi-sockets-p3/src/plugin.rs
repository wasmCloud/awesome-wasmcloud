//! The WIT bindings glue.
//!
//! Compiled only for wasm. [`crate::proto`] and [`crate::config`] hold
//! everything decidable without the bindings, and test on the host.
//!
//! Unlike the SDK-based sibling, nothing here blocks: every wait is an `await`
//! on `wasi:sockets@0.3.0`, so a call waiting on the cluster yields this
//! plugin's executor to its other callers.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

use crate::bindings;
use crate::conn::Connection;
use crate::config::Binding;
use crate::proto::{self, op, status};

use bindings::exports::wasmcloud::couchbase::document::{
    DocumentGetAllReplicaOptions, DocumentGetAndLockOptions, DocumentGetAndTouchOptions,
    DocumentGetAnyReplicaOptions, DocumentGetOptions, DocumentGetReplicaResult, DocumentGetResult,
    DocumentInsertOptions, DocumentRemoveOptions, DocumentReplaceOptions, DocumentTouchOptions,
    DocumentUnlockOptions, DocumentUpsertOptions, Guest as DocumentGuest,
};
use bindings::exports::wasmcloud::couchbase::sqlpp::{
    Guest as SqlppGuest, SqlppQueryOptions, SqlppValue,
};
use bindings::exports::wasmcloud::couchbase::sqlpp_types::SqlppQueryError;
use bindings::exports::wasmcloud::couchbase::types::{
    DocumentError, MutationMetadata, ReplicaReadLevel, Time,
};
use bindings::exports::wasmcloud::host::workload_lifecycle::{Guest as LifecycleGuest, WorkloadInfo};
use bindings::wasi::sockets::ip_name_lookup;
use bindings::wasmcloud::host::identity;

/// Each workload's validated binding, keyed by workload id.
static BINDINGS: Mutex<BTreeMap<String, Binding>> = Mutex::new(BTreeMap::new());

thread_local! {
    /// Live connections, keyed by [`Binding::connection_key`]. A connection is
    /// taken out while it is in use and put back afterwards, so two concurrent
    /// calls on one binding open two connections rather than interleaving
    /// frames on one -- the protocol would allow pipelining, but only with a
    /// reader that demultiplexes on the opaque.
    static CONNECTIONS: RefCell<BTreeMap<String, Connection>> = RefCell::new(BTreeMap::new());
}

/// What the server uses when a lock request names no duration.
const DEFAULT_LOCK_SECONDS: u32 = 15;

struct Component;

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

impl LifecycleGuest for Component {
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

        // Connect during bind so a bad endpoint or credential fails the deploy
        // with a message, rather than the workload's first call.
        connect(&binding).await.map_err(|e| {
            format!(
                "workload `{}`: could not reach Couchbase at `{}:{}`: {e}",
                workload.name, binding.host, binding.port
            )
        })?;

        lock(&BINDINGS).insert(workload.id, binding);
        Ok(())
    }

    async fn on_workload_unbind(id: String) {
        lock(&BINDINGS).remove(&id);
        // The connection stays cached: another workload may share it, and the
        // handshake is expensive. It goes when the plugin's store restarts.
    }
}

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

/// Resolve the binding's host and open a connection, unless one is cached.
async fn connect(binding: &Binding) -> Result<(), String> {
    let key = binding.connection_key();
    if CONNECTIONS.with(|c| c.borrow().contains_key(&key)) {
        return Ok(());
    }

    let addresses = ip_name_lookup::resolve_addresses(binding.host.clone())
        .await
        .map_err(|e| format!("could not resolve `{}`: {e:?}", binding.host))?;
    let address = addresses
        .into_iter()
        .next()
        .ok_or_else(|| format!("`{}` resolved to no addresses", binding.host))?;

    let conn = Connection::open(
        address,
        binding.port,
        &binding.username,
        &binding.password,
        &binding.bucket,
        &binding.scope,
        &binding.collection,
    )
    .await?;
    CONNECTIONS.with(|c| c.borrow_mut().insert(key, conn));
    Ok(())
}

/// Run one operation on this binding's connection.
///
/// The connection is taken out of the cache for the duration: a `RefCell`
/// borrow cannot be held across an `await`, and two concurrent callers must not
/// interleave frames on one socket.
async fn with_connection<F, T>(binding: &Binding, f: F) -> Result<T, DocumentError>
where
    F: AsyncFnOnce(&mut Connection) -> Result<T, DocumentError>,
{
    connect(binding).await.map_err(DocumentError::RequestFailed)?;
    let key = binding.connection_key();
    let mut conn = CONNECTIONS
        .with(|c| c.borrow_mut().remove(&key))
        .ok_or_else(|| DocumentError::Other("connection vanished from the cache".to_string()))?;

    let result = f(&mut conn).await;

    // A desynchronized connection must not be reused: its next reply would be
    // the previous call's. Anything else is still good.
    let reusable = !matches!(result, Err(DocumentError::Other(_)) | Err(DocumentError::RequestFailed(_)));
    if reusable {
        CONNECTIONS.with(|c| c.borrow_mut().insert(key, conn));
    }
    result
}

/// Map a KV status onto the interface's error model.
///
/// `had_cas` disambiguates `key exists`, which the protocol returns both for an
/// `ADD` over a live key and for any write whose CAS did not match. Without it
/// a stale-CAS `replace` reports as `already-exists`, sending the caller to
/// retry a conflict that is really a lost update.
fn to_document_error(code: u16, had_cas: bool) -> DocumentError {
    match code {
        status::NOT_FOUND => DocumentError::NotFound,
        status::EXISTS if had_cas => DocumentError::CasMismatch,
        status::EXISTS => DocumentError::AlreadyExists,
        status::NOT_STORED => DocumentError::NotFound,
        status::LOCKED => DocumentError::Locked,
        status::NOT_LOCKED => DocumentError::NotLocked,
        status::AUTH_ERROR => DocumentError::Unauthorized,
        status::TOO_LARGE => {
            DocumentError::InvalidArgument("the document is larger than the cluster allows".to_string())
        }
        status::DELTA_BAD_VALUE => {
            DocumentError::InvalidArgument("the document is not a number".to_string())
        }
        status::UNKNOWN_COLLECTION => {
            DocumentError::NotConfigured("the bound scope or collection no longer exists".to_string())
        }
        status::NOT_MY_VBUCKET => DocumentError::Other(
            "the cluster moved this vbucket to another node; this plugin does not follow a rebalance"
                .to_string(),
        ),
        status::UNKNOWN_COMMAND | status::NOT_SUPPORTED => {
            DocumentError::Unsupported("the cluster does not support this operation".to_string())
        }
        other => DocumentError::Other(format!("Couchbase status 0x{other:04x}")),
    }
}

/// Couchbase expiries are whole seconds, so a sub-second value rounds up rather
/// than to zero -- which would mean "no expiry".
fn expiry_seconds(expires_in_ns: u64) -> u32 {
    if expires_in_ns == 0 {
        return 0;
    }
    expires_in_ns.div_ceil(1_000_000_000).clamp(1, u64::from(u32::MAX)) as u32
}

/// Build the mutation metadata a write reports.
fn mutation(binding: &Binding, vbucket: u16, cas: u64, extras: &[u8]) -> MutationMetadata {
    let (uuid, seq) = proto::parse_mutation_token(extras).unwrap_or((0, 0));
    MutationMetadata {
        cas,
        bucket: binding.bucket.clone(),
        partition_id: u64::from(vbucket),
        partition_uuid: uuid,
        seq,
        raw_token: None,
    }
}

/// Refuse the pre-6.0 durability settings, which this implementation does not
/// send. Accepting them would promise a durability the write never had.
fn reject_unhonoured(persist_to: u64, replicate_to: u64) -> Result<(), DocumentError> {
    if persist_to != 0 || replicate_to != 0 {
        return Err(DocumentError::Unsupported(
            "persist-to/replicate-to are the pre-6.0 durability settings; use `durability-level`".to_string(),
        ));
    }
    Ok(())
}

/// Refuse `preserve-expiry`: a plain write clears the document's TTL, and
/// honouring the flag needs the server-side preserve-TTL path this
/// implementation does not use.
fn reject_preserve_expiry(preserve: bool) -> Result<(), DocumentError> {
    if preserve {
        return Err(DocumentError::Unsupported(
            "preserve-expiry is not implemented on this transport; re-apply the TTL with `expires-in-ns`".to_string(),
        ));
    }
    Ok(())
}

fn reject_replica(level: Option<ReplicaReadLevel>) -> Result<(), DocumentError> {
    match level {
        Some(ReplicaReadLevel::On) => Err(DocumentError::Unsupported(
            "this plugin reads only the active copy; it does not track the replica topology".to_string(),
        )),
        _ => Ok(()),
    }
}


// ---------------------------------------------------------------------------
// document
// ---------------------------------------------------------------------------

/// A write carrying flags and an expiry: insert, upsert, replace.
async fn store(
    opcode: u8,
    id: String,
    document: Vec<u8>,
    flags: Option<u32>,
    expires_in_ns: u64,
    cas: u64,
) -> Result<MutationMetadata, DocumentError> {
    let binding = caller_binding()?;
    let extras =
        proto::store_extras(flags.unwrap_or(proto::JSON_FLAGS), expiry_seconds(expires_in_ns));
    let b = binding.clone();
    with_connection(&binding, async move |conn| {
        let vbucket = conn.vbucket_for(&id);
        let key = conn.wire_key(&id);
        let reply = conn
            .call(opcode, vbucket, &key, &extras, &document, cas)
            .await
            .map_err(DocumentError::RequestFailed)?;
        if !reply.ok() {
            return Err(to_document_error(reply.header.status, cas != 0));
        }
        Ok(mutation(&b, vbucket, reply.header.cas, &reply.extras))
    })
    .await
}

/// The flags a read reports, which a GET carries in its extras.
fn flags_of(extras: &[u8]) -> u32 {
    if extras.len() >= 4 {
        u32::from_be_bytes([extras[0], extras[1], extras[2], extras[3]])
    } else {
        0
    }
}

/// Select `paths` out of a JSON document, rebuilding the nesting so the result
/// is a subset of the original shape.
fn project(document: &[u8], paths: &[String]) -> Result<Vec<u8>, DocumentError> {
    let parsed: serde_json::Value = serde_json::from_slice(document).map_err(|_| {
        DocumentError::NotJson
    })?;
    let mut out = serde_json::Map::new();
    for path in paths {
        let mut node = Some(&parsed);
        for segment in path.split('.') {
            node = node.and_then(|n| n.get(segment));
        }
        // An absent path is omitted rather than nulled: absent and
        // present-but-null are different answers.
        let Some(value) = node else { continue };
        let mut cursor = &mut out;
        let mut keys = path.split('.').peekable();
        while let Some(key) = keys.next() {
            if keys.peek().is_none() {
                cursor.insert(key.to_string(), value.clone());
                break;
            }
            let entry = cursor
                .entry(key.to_string())
                .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
            if !entry.is_object() {
                *entry = serde_json::Value::Object(serde_json::Map::new());
            }
            cursor = entry.as_object_mut().expect("just ensured an object");
        }
    }
    Ok(serde_json::Value::Object(out).to_string().into_bytes())
}

/// A plain read, shared by `get`, `get-and-touch` and `get-and-lock`.
async fn read(
    opcode: u8,
    id: String,
    extras: Vec<u8>,
    paths: Vec<String>,
    with_expiry: bool,
) -> Result<DocumentGetResult, DocumentError> {
    let binding = caller_binding()?;
    with_connection(&binding, async move |conn| {
        let vbucket = conn.vbucket_for(&id);
        let key = conn.wire_key(&id);
        let reply = conn
            .call(opcode, vbucket, &key, &extras, &[], 0)
            .await
            .map_err(DocumentError::RequestFailed)?;
        if !reply.ok() {
            return Err(to_document_error(reply.header.status, false));
        }
        // Projection happens here rather than through a subdoc lookup: the
        // answer is the same and it is one round trip either way.
        let document = if paths.is_empty() {
            reply.value
        } else {
            project(&reply.value, &paths)?
        };
        // A GET does not report the expiry, so `with-expiry` costs a second
        // round trip. It is only paid when asked for.
        let expires_at = if with_expiry {
            let meta = conn
                .call(op::GET_META, vbucket, &key, &[], &[], 0)
                .await
                .map_err(DocumentError::RequestFailed)?;
            if meta.ok() {
                proto::parse_meta_expiry(&meta.extras).map(|seconds| {
                    let civil = proto::civil_from_unix(i64::from(seconds));
                    Time {
                        offset: 0,
                        year: civil.year,
                        month: civil.month,
                        day: civil.day,
                        hour: civil.hour,
                        minute: civil.minute,
                        second: civil.second,
                        milliseconds: 0,
                        nanoseconds: 0,
                    }
                })
            } else {
                None
            }
        } else {
            None
        };

        Ok(DocumentGetResult {
            document,
            flags: flags_of(&reply.extras),
            cas: reply.header.cas,
            expires_in_ns: None,
            expires_at,
        })
    })
    .await
}

impl DocumentGuest for Component {
    async fn insert(
        id: String,
        document: Vec<u8>,
        options: Option<DocumentInsertOptions>,
    ) -> Result<MutationMetadata, DocumentError> {
        if let Some(o) = options.as_ref() {
            reject_unhonoured(o.persist_to, o.replicate_to)?;
        }
        let (flags, expires) = options.as_ref().map_or((None, 0), |o| (o.flags, o.expires_in_ns));
        store(op::ADD, id, document, flags, expires, 0).await
    }

    async fn upsert(
        id: String,
        document: Vec<u8>,
        options: Option<DocumentUpsertOptions>,
    ) -> Result<MutationMetadata, DocumentError> {
        if let Some(o) = options.as_ref() {
            reject_unhonoured(o.persist_to, o.replicate_to)?;
            reject_preserve_expiry(o.preserve_expiry)?;
        }
        let (flags, expires) = options.as_ref().map_or((None, 0), |o| (o.flags, o.expires_in_ns));
        store(op::SET, id, document, flags, expires, 0).await
    }

    async fn replace(
        id: String,
        document: Vec<u8>,
        options: Option<DocumentReplaceOptions>,
    ) -> Result<MutationMetadata, DocumentError> {
        if let Some(o) = options.as_ref() {
            reject_unhonoured(o.persist_to, o.replicate_to)?;
            reject_preserve_expiry(o.preserve_expiry)?;
        }
        let (flags, expires, cas) =
            options.as_ref().map_or((None, 0, 0), |o| (o.flags, o.expires_in_ns, o.cas));
        store(op::REPLACE, id, document, flags, expires, cas).await
    }

    async fn get(
        id: String,
        options: Option<DocumentGetOptions>,
    ) -> Result<DocumentGetResult, DocumentError> {
        reject_replica(options.as_ref().and_then(|o| o.use_replica))?;
        let paths: Vec<String> = options
            .as_ref()
            .and_then(|o| o.project.as_ref())
            .map(|p| p.iter().filter(|p| !p.is_empty()).cloned().collect())
            .unwrap_or_default();
        let with_expiry = options.as_ref().is_some_and(|o| o.with_expiry);
        read(op::GET, id, Vec::new(), paths, with_expiry).await
    }

    async fn remove(
        id: String,
        options: Option<DocumentRemoveOptions>,
    ) -> Result<MutationMetadata, DocumentError> {
        let binding = caller_binding()?;
        if let Some(o) = options.as_ref() {
            reject_unhonoured(o.persist_to, o.replicate_to)?;
        }
        let cas = options.as_ref().map_or(0, |o| o.cas);
        let b = binding.clone();
        with_connection(&binding, async move |conn| {
            let vbucket = conn.vbucket_for(&id);
            let key = conn.wire_key(&id);
            let reply = conn
                .call(op::DELETE, vbucket, &key, &[], &[], cas)
                .await
                .map_err(DocumentError::RequestFailed)?;
            if !reply.ok() {
                return Err(to_document_error(reply.header.status, cas != 0));
            }
            Ok(mutation(&b, vbucket, reply.header.cas, &reply.extras))
        })
        .await
    }

    async fn touch(
        id: String,
        options: Option<DocumentTouchOptions>,
    ) -> Result<MutationMetadata, DocumentError> {
        let binding = caller_binding()?;
        let seconds = expiry_seconds(options.as_ref().map_or(0, |o| o.expires_in));
        if seconds == 0 {
            return Err(DocumentError::InvalidArgument(
                "touch requires a non-zero expires-in; clear an expiry with `upsert` and expires-in-ns 0".to_string(),
            ));
        }
        let b = binding.clone();
        with_connection(&binding, async move |conn| {
            let vbucket = conn.vbucket_for(&id);
            let key = conn.wire_key(&id);
            let reply = conn
                .call(op::TOUCH, vbucket, &key, &seconds.to_be_bytes(), &[], 0)
                .await
                .map_err(DocumentError::RequestFailed)?;
            if !reply.ok() {
                return Err(to_document_error(reply.header.status, false));
            }
            Ok(mutation(&b, vbucket, reply.header.cas, &reply.extras))
        })
        .await
    }

    async fn get_and_touch(
        id: String,
        options: Option<DocumentGetAndTouchOptions>,
    ) -> Result<DocumentGetResult, DocumentError> {
        let seconds = expiry_seconds(options.as_ref().map_or(0, |o| o.expires_in));
        if seconds == 0 {
            return Err(DocumentError::InvalidArgument(
                "get-and-touch requires a non-zero expires-in".to_string(),
            ));
        }
        read(op::GET_AND_TOUCH, id, seconds.to_be_bytes().to_vec(), Vec::new(), false).await
    }

    async fn get_and_lock(
        id: String,
        options: Option<DocumentGetAndLockOptions>,
    ) -> Result<DocumentGetResult, DocumentError> {
        // `lock-time` is not an option in the WIT, so `0` means "the default".
        let seconds = match options.as_ref().map_or(0, |o| o.lock_time) {
            0 => DEFAULT_LOCK_SECONDS,
            ns => expiry_seconds(ns),
        };
        read(op::GET_AND_LOCK, id, seconds.to_be_bytes().to_vec(), Vec::new(), false).await
    }

    async fn unlock(
        id: String,
        options: Option<DocumentUnlockOptions>,
    ) -> Result<(), DocumentError> {
        let binding = caller_binding()?;
        let cas = options.as_ref().map_or(0, |o| o.cas);
        if cas == 0 {
            return Err(DocumentError::InvalidArgument(
                "unlock needs the CAS returned by get-and-lock".to_string(),
            ));
        }
        with_connection(&binding, async move |conn| {
            let vbucket = conn.vbucket_for(&id);
            let key = conn.wire_key(&id);
            let reply = conn
                .call(op::UNLOCK, vbucket, &key, &[], &[], cas)
                .await
                .map_err(DocumentError::RequestFailed)?;
            if !reply.ok() {
                return Err(to_document_error(reply.header.status, cas != 0));
            }
            Ok(())
        })
        .await
    }

    /// Reading a replica needs the vbucket-to-node map and a connection per
    /// node, neither of which this implementation keeps.
    async fn get_any_replicas(
        _id: String,
        _options: Option<DocumentGetAnyReplicaOptions>,
    ) -> Result<DocumentGetReplicaResult, DocumentError> {
        Err(DocumentError::Unsupported(
            "this plugin connects to one node and does not track the replica topology".to_string(),
        ))
    }

    /// Not served; see `get-any-replicas`.
    async fn get_all_replicas(
        _id: String,
        _options: Option<DocumentGetAllReplicaOptions>,
    ) -> Result<Vec<DocumentGetReplicaResult>, DocumentError> {
        Err(DocumentError::Unsupported(
            "this plugin connects to one node and does not track the replica topology".to_string(),
        ))
    }
}


// ---------------------------------------------------------------------------
// sqlpp
// ---------------------------------------------------------------------------

/// SQL++ is an HTTP service, not a KV one, so it goes out over `wasi:http`
/// rather than this plugin's socket.
async fn query_service(
    binding: &Binding,
    body: Vec<u8>,
) -> Result<(u16, Vec<u8>), SqlppQueryError> {
    use bindings::wasi::http::client;
    use bindings::wasi::http::types::{Fields, Method, Request, RequestOptions, Response, Scheme};

    let fields = Fields::new();
    let credentials = base64(format!("{}:{}", binding.username, binding.password).as_bytes());
    let set = |name: &str, value: &[u8]| -> Result<(), SqlppQueryError> {
        fields
            .append(name, value)
            .map_err(|e| SqlppQueryError::Unexpected(format!("header `{name}` rejected: {e:?}")))
    };
    set("authorization", format!("Basic {credentials}").as_bytes())?;
    set("content-type", b"application/json")?;
    set("content-length", body.len().to_string().as_bytes())?;

    let (trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));
    let (mut tx, rx) = bindings::wit_stream::new();
    wit_bindgen::spawn_local(async move {
        tx.write_all(body).await;
        drop(tx);
        let _ = trailers_tx.write(Ok(None)).await;
    });

    let options = RequestOptions::new();
    let timeout_ns = u64::from(binding.timeout_ms).saturating_mul(1_000_000);
    let _ = options.set_connect_timeout(Some(timeout_ns));
    let _ = options.set_first_byte_timeout(Some(timeout_ns));
    let _ = options.set_between_bytes_timeout(Some(timeout_ns));

    let (request, _sent) = Request::new(fields, Some(rx), trailers_rx, Some(options));
    let _ = request.set_method(&Method::Post);
    let _ = request.set_scheme(Some(&Scheme::Http));
    let _ = request.set_authority(Some(&format!("{}:{}", binding.host, binding.query_port)));
    let _ = request.set_path_with_query(Some("/query/service"));

    let response = client::send(request)
        .await
        .map_err(|e| SqlppQueryError::RequestFailed(format!("{e:?}")))?;
    let status = response.get_status_code();
    let (res_tx, res_rx) = bindings::wit_future::new(|| Ok(()));
    let (stream, _trailers) = Response::consume_body(response, res_rx);
    let received = stream.collect().await;
    drop(res_tx);
    Ok((status, received))
}

/// Standard base64, for the Basic credential.
fn base64(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { TABLE[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { TABLE[n as usize & 63] as char } else { '=' });
    }
    out
}

impl SqlppGuest for Component {
    async fn query(
        query: String,
        params: Vec<SqlppValue>,
        options: Option<SqlppQueryOptions>,
    ) -> Result<SqlppValue, SqlppQueryError> {
        let binding = caller_binding().map_err(|e| match e {
            DocumentError::NotConfigured(m) => SqlppQueryError::NotConfigured(m),
            other => SqlppQueryError::Unexpected(format!("{other:?}")),
        })?;

        let mut args = Vec::with_capacity(params.len());
        for (index, param) in params.iter().enumerate() {
            args.push(match param {
                SqlppValue::Null => serde_json::Value::Null,
                SqlppValue::Json(text) => serde_json::from_str(text).map_err(|e| {
                    SqlppQueryError::InvalidArgument(format!(
                        "positional parameter {} is not valid JSON: {e}",
                        index + 1
                    ))
                })?,
            });
        }

        let mut request = serde_json::Map::new();
        request.insert("statement".to_string(), query.into());
        // The binding's keyspace is implicit, so an unqualified `FROM` names a
        // collection inside it rather than resolving against the cluster.
        request.insert(
            "query_context".to_string(),
            format!("default:{}.{}", binding.bucket, binding.scope).into(),
        );
        if !args.is_empty() {
            request.insert("args".to_string(), serde_json::Value::Array(args));
        }
        if let Some(options) = options.as_ref() {
            use bindings::exports::wasmcloud::couchbase::types::QueryScanConsistency as Wit;
            request.insert("readonly".to_string(), options.readonly.into());

            // `consistent-with` implies `at_plus`, matching how the SDKs treat
            // it, so it overrides whatever scan-consistency was also set.
            if let Some(state) = options.consistent_with.as_ref() {
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
                // A zeroed partition-uuid is how `mutation-metadata` reports
                // "no token was available". Sent on, it builds a scan vector the
                // query service satisfies immediately.
                if state.tokens.is_empty() || state.tokens.iter().any(|t| t.partition_uuid == 0) {
                    return Err(SqlppQueryError::InvalidArgument(
                        "consistent-with needs a mutation token with a non-zero partition-uuid for every entry; a zeroed token means none was available, and a partial scan vector would let the query run without waiting. Use scan-consistency request-plus instead".to_string(),
                    ));
                }
                let mut vector = serde_json::Map::new();
                for token in &state.tokens {
                    vector.insert(
                        token.partition_id.to_string(),
                        serde_json::Value::Array(vec![
                            token.sequence_number.into(),
                            token.partition_uuid.to_string().into(),
                        ]),
                    );
                }
                let mut buckets = serde_json::Map::new();
                buckets.insert(binding.bucket.clone(), serde_json::Value::Object(vector));
                request.insert("scan_consistency".to_string(), "at_plus".into());
                request.insert("scan_vectors".to_string(), serde_json::Value::Object(buckets));
            } else {
                request.insert(
                    "scan_consistency".to_string(),
                    match options.scan_consistency {
                        Wit::NotBounded => "not_bounded",
                        Wit::RequestPlus => "request_plus",
                    }
                    .into(),
                );
            }
        }
        let body = serde_json::Value::Object(request).to_string().into_bytes();

        let (status, payload) = query_service(&binding, body).await?;
        let parsed: serde_json::Value = serde_json::from_slice(&payload)
            .map_err(|e| SqlppQueryError::Unexpected(format!("the query service did not answer JSON: {e}")))?;

        // The query service reports failures in an `errors` array, under any
        // status; its own code decides over the HTTP status.
        if let Some(error) = parsed.get("errors").and_then(|e| e.as_array()).and_then(|e| e.first()) {
            let message = error
                .get("msg")
                .and_then(|m| m.as_str())
                .unwrap_or("the query failed")
                .to_string();
            return Err(match error.get("code").and_then(|c| c.as_u64()) {
                Some(3000) => SqlppQueryError::InvalidArgument(message),
                Some(12003) | Some(12021) => SqlppQueryError::InvalidArgument(format!(
                    "the statement referenced a keyspace that does not exist: {message}"
                )),
                Some(13014) => SqlppQueryError::Unauthorized,
                _ => SqlppQueryError::Server(format!("HTTP {status}: {message}")),
            });
        }

        let rows = parsed
            .get("results")
            .cloned()
            .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
        Ok(SqlppValue::Json(rows.to_string()))
    }
}

mod export {
    #![allow(unsafe_code)]
    use super::{bindings, Component};
    bindings::export!(Component with_types_in bindings);
}
