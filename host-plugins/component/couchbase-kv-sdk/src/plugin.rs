//! The WIT bindings glue, over the Couchbase SDK's KV transport.
//!
//! Compiled only for wasm. Config parsing lives in [`crate::config`], which
//! compiles and tests on the host.
//!
//! # How the SDK's runtime meets the component executor
//!
//! Every exported capability function is an `async fn` driven by wit-bindgen's
//! component-model executor. The SDK's futures want a Tokio context — for its
//! sockets and timers — and the two executors are unrelated, so SDK work is run
//! on a single Tokio current-thread runtime via `block_on`.
//!
//! The cost is real and worth stating plainly: `block_on` does not yield to the
//! component executor, so while one capability call is waiting on Couchbase,
//! the plugin's other in-flight calls do not progress. A host component plugin
//! is a single pinned instance shared by every workload on the host, so calls
//! through this plugin **serialize**. The sibling Data API plugin does not have
//! this property: it awaits `wasi:http` through the same executor that drives
//! its exports, so its calls interleave.
//!
//! # Two things that are easy to get wrong here
//!
//! **A runtime context is needed for more than `await`.** Several SDK methods
//! that look like plain accessors — `Cluster::bucket` above all — spawn
//! background work, so they panic with Tokio's `CONTEXT_MISSING_ERROR` unless a
//! runtime context is current. `with_collection` and `with_scope` enter one for
//! exactly those calls, and drop it again before `block_on`, which panics in
//! turn if called from *inside* a runtime context.
//!
//! **The SDK must never be handed a hostname.** Tokio resolves names on its
//! blocking pool, and wasm has no threads, so it aborts the instance with `Not
//! supported (os error 58)` — the failure is the thread spawn, before any
//! lookup happens, so it is not something a DNS grant can fix. `std`'s resolver
//! has no such problem: it calls `wasi:sockets/ip-name-lookup` directly.
//! [`resolve_endpoint`] therefore resolves the name here and passes the SDK an
//! address literal, which is what lets `endpoint` name a host at all.

mod bindings {
    #![allow(unsafe_code)]
    wit_bindgen::generate!({ world: "couchbase-kv-sdk-plugin", generate_all });
}

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::Duration;

use crate::config::Binding;

use couchbase::authenticator::{Authenticator, PasswordAuthenticator};
use couchbase::cluster::Cluster;
use couchbase::collection::Collection;
use couchbase::error::Error as CbError;
use couchbase::options::cluster_options::ClusterOptions;

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
    DocumentError, DurabilityLevel, MutationMetadata, ReplicaReadLevel,
};
use bindings::exports::wasmcloud::host::workload_lifecycle::{
    Guest as LifecycleGuest, WorkloadInfo,
};
use bindings::wasmcloud::host::identity;

/// Each workload's validated binding, keyed by workload id.
static BINDINGS: Mutex<BTreeMap<String, Binding>> = Mutex::new(BTreeMap::new());

/// The Tokio runtime every SDK call runs on. Built once; wasm has no threads,
/// so a current-thread runtime is the only shape available anyway.
static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

thread_local! {
    /// Connected clusters, keyed by [`Binding::connection_key`]. Connecting is
    /// expensive — it bootstraps the cluster map and authenticates — so it
    /// happens once per distinct cluster/credential/bucket rather than per call.
    static CLUSTERS: RefCell<BTreeMap<String, Cluster>> = RefCell::new(BTreeMap::new());
}

struct Component;

/// What the server uses when a lock request names no duration.
const DEFAULT_LOCK_SECONDS: u64 = 15;


/// Refuse a replica read this transport cannot serve.
///
/// Returning the active copy instead would answer a question the caller did
/// not ask: a replica read trades freshness for availability, and silently
/// serving the active node hides that the trade did not happen.
fn reject_replica_read(level: Option<ReplicaReadLevel>) -> Result<(), DocumentError> {
    match level {
        Some(ReplicaReadLevel::On) => Err(DocumentError::Unsupported(
            "the Couchbase Rust SDK exposes no replica read".to_string(),
        )),
        _ => Ok(()),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread Tokio runtime is always constructible")
    })
}

/// Run one SDK future to completion.
///
/// See the module docs: this blocks the plugin's whole store for the duration.
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    runtime().block_on(fut)
}

/// Why an operation failed before it produced an SDK result.
enum OpError {
    Sdk(CbError),
    /// The deadline passed. The call is abandoned, not cancelled: the server
    /// may still apply a write whose reply never arrived, which is what
    /// `document-error.timeout` means everywhere else too.
    Timeout,
}

impl From<CbError> for OpError {
    fn from(e: CbError) -> Self {
        Self::Sdk(e)
    }
}

fn op_error_to_document_error(err: OpError) -> DocumentError {
    match err {
        OpError::Sdk(e) => to_document_error(e),
        OpError::Timeout => DocumentError::Timeout,
    }
}

/// Run one SDK future under a deadline.
///
/// `couchbase` 1.0.1 takes no timeout on a document operation, so the bound is
/// applied here instead. That matters more for this transport than for the
/// sibling: these calls hold the plugin's only store, so one call waiting
/// forever stops every workload on the host.
fn block_on_within<T>(
    timeout_ms: u32,
    fut: impl std::future::Future<Output = Result<T, CbError>>,
) -> Result<T, OpError> {
    let deadline = Duration::from_millis(u64::from(timeout_ms));
    // Built inside `block_on`, not outside: a `Sleep` registers with the timer
    // driver when it is constructed, so constructing one without a runtime
    // context panics with `CONTEXT_MISSING_ERROR` before anything is awaited.
    match block_on(async move { tokio::time::timeout(deadline, fut).await }) {
        Ok(result) => result.map_err(OpError::Sdk),
        Err(_elapsed) => Err(OpError::Timeout),
    }
}

/// The deadline for one call: the caller's, or the binding's when unset.
fn effective_timeout_ms(binding: &Binding, timeout_ns: Option<u64>) -> u32 {
    match timeout_ns.unwrap_or(0) {
        0 => binding.timeout_ms,
        ns => ns.div_ceil(1_000_000).clamp(1, u64::from(u32::MAX)) as u32,
    }
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

        // Connect during bind rather than lazily on the first call. Given the
        // executor problem described in the module docs, this is also what
        // turns a hang into a bounded, legible failure: the lifecycle hook has
        // a 30s budget, so a deploy fails with a message instead of a
        // capability call blocking forever.
        connect(&binding).map_err(|e| {
            format!(
                "workload `{}`: could not reach Couchbase at `{}`: {e}",
                workload.name, binding.connection_string
            )
        })?;

        lock(&BINDINGS).insert(workload.id, binding);
        Ok(())
    }

    async fn on_workload_unbind(id: String) {
        lock(&BINDINGS).remove(&id);
        // The connection is deliberately left cached: another workload may
        // share it, and reconnecting is expensive. It is released when the
        // plugin's store restarts.
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

/// Resolve a hostname in the connection string to an address before the SDK
/// sees it.
///
/// Tokio resolves names on its blocking pool, and wasm has no threads, so its
/// resolver aborts the instance with `Not supported (os error 58)` — the
/// failure is the thread spawn, before any lookup happens. `std`'s resolver has
/// no such problem: it goes straight to `wasi:sockets/ip-name-lookup`, which
/// the host grants through `allowedIpNameLookups`. Resolving here means the SDK
/// only ever receives an address literal and never reaches its own resolver.
///
/// Nothing is resolved for an address literal or a multi-node string; see
/// [`config::needs_resolution`](crate::config::needs_resolution).
fn resolve_endpoint(connection_string: &str) -> Result<String, ConnectError> {
    use std::net::ToSocketAddrs as _;

    let (scheme, host, port) = crate::config::split_connection_string(connection_string);
    if !crate::config::needs_resolution(host) {
        return Ok(connection_string.to_string());
    }

    // The port here only satisfies `to_socket_addrs`; only the address is kept,
    // and `port` (which may be empty) is what gets written back.
    let addr = (host, 0u16)
        .to_socket_addrs()
        .map_err(|e| {
            ConnectError::Resolve(format!(
                "could not resolve `{host}`: {e}. A host component plugin needs `allowedIpNameLookups` to cover it, or `endpoint` can name an address literal instead"
            ))
        })?
        .next()
        .ok_or_else(|| ConnectError::Resolve(format!("`{host}` resolved to no addresses")))?
        .ip();

    Ok(match addr {
        std::net::IpAddr::V4(v4) => format!("{scheme}://{v4}{port}"),
        std::net::IpAddr::V6(v6) => format!("{scheme}://[{v6}]{port}"),
    })
}

/// Why a connection attempt failed.
///
/// Resolution happens before the SDK is involved, so its failure is not an
/// `Error` the SDK could have produced and needs its own variant.
enum ConnectError {
    Sdk(CbError),
    Resolve(String),
}

impl From<CbError> for ConnectError {
    fn from(e: CbError) -> Self {
        Self::Sdk(e)
    }
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sdk(e) => write!(f, "{e}"),
            Self::Resolve(m) => f.write_str(m),
        }
    }
}

fn connect_error_to_document_error(err: ConnectError) -> DocumentError {
    match err {
        ConnectError::Sdk(e) => to_document_error(e),
        // The name is a config value, so a name that does not resolve is a
        // configuration fault rather than a failed request.
        ConnectError::Resolve(m) => DocumentError::NotConfigured(m),
    }
}

/// A connected cluster for this binding, from cache or freshly connected.
fn connect(binding: &Binding) -> Result<(), ConnectError> {
    let key = binding.connection_key();
    let cached = CLUSTERS.with(|c| c.borrow().contains_key(&key));
    if cached {
        return Ok(());
    }
    let opts = ClusterOptions::new(Authenticator::PasswordAuthenticator(
        PasswordAuthenticator::new(binding.username.clone(), binding.password.clone()),
    ));
    let endpoint = resolve_endpoint(&binding.connection_string)?;
    let cluster = block_on(Cluster::connect(&endpoint, opts))?;
    CLUSTERS.with(|c| c.borrow_mut().insert(key, cluster));
    Ok(())
}

/// Run `f` against the collection this binding addresses.
fn with_collection<T>(
    binding: &Binding,
    f: impl FnOnce(Collection) -> Result<T, OpError>,
) -> Result<T, DocumentError> {
    connect(binding).map_err(connect_error_to_document_error)?;
    let key = binding.connection_key();
    CLUSTERS.with(|clusters| {
        let clusters = clusters.borrow();
        let cluster = clusters.get(&key).ok_or_else(|| {
            DocumentError::Other("cluster connection vanished from the cache".to_string())
        })?;
        // `bucket()` looks like a plain accessor but is not: it spawns the
        // background task that resolves the bucket's agent, so it panics with
        // `CONTEXT_MISSING_ERROR` unless a runtime context is current. The
        // guard has to be dropped again before `f`, because `block_on` from
        // *inside* a runtime context panics in turn. The spawned task is not
        // starved by that: `f` drives this same current-thread runtime, which
        // is where the task gets to run.
        let collection = {
            let _guard = runtime().enter();
            cluster
                .bucket(&binding.bucket)
                .scope(&binding.scope)
                .collection(&binding.collection)
        };
        f(collection).map_err(op_error_to_document_error)
    })
}

/// Run `f` against the scope this binding addresses.
///
/// SQL++ goes through the *scope*, not the cluster. A cluster-level query
/// resolves an unqualified keyspace against the cluster, so `FROM _default`
/// fails with `No bucket named _default` — the binding's bucket is never
/// consulted. Running at scope level makes the binding's keyspace implicit,
/// which is both what the statement means and the boundary the workload was
/// granted. See `with_collection` for why `bucket()` needs a runtime context.
fn with_scope<T>(
    binding: &Binding,
    f: impl FnOnce(couchbase::scope::Scope) -> Result<T, SqlppQueryError>,
) -> Result<T, SqlppQueryError> {
    connect(binding).map_err(|e| document_error_as_query_error(connect_error_to_document_error(e)))?;
    let key = binding.connection_key();
    CLUSTERS.with(|clusters| {
        let clusters = clusters.borrow();
        let cluster = clusters.get(&key).ok_or_else(|| {
            SqlppQueryError::Unexpected("cluster connection vanished".to_string())
        })?;
        let scope = {
            let _guard = runtime().enter();
            cluster.bucket(&binding.bucket).scope(&binding.scope)
        };
        f(scope)
    })
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Map an SDK error onto the interface's error model.
///
/// The SDK's error kinds are matched on their rendered form: `couchbase::Error`
/// does not expose a stable public discriminant to match structurally in 1.0.1,
/// and a string match that misses simply falls through to `other` with the full
/// message rather than mislabelling the failure.
fn to_document_error(err: CbError) -> DocumentError {
    let text = err.to_string();
    let lower = text.to_ascii_lowercase();
    if lower.contains("document not found") || lower.contains("documentnotfound") {
        DocumentError::NotFound
    } else if lower.contains("document exists") || lower.contains("documentexists") {
        DocumentError::AlreadyExists
    } else if lower.contains("cas mismatch") || lower.contains("casmismatch") {
        DocumentError::CasMismatch
    } else if lower.contains("document locked") || lower.contains("documentlocked") {
        DocumentError::Locked
    } else if lower.contains("not locked") || lower.contains("documentnotlocked") {
        DocumentError::NotLocked
    } else if lower.contains("authentication") || lower.contains("unauthorized")
        || lower.contains("permission")
    {
        DocumentError::Unauthorized
    } else if lower.contains("timeout") || lower.contains("timed out") {
        DocumentError::Timeout
    } else if lower.contains("invalid argument") || lower.contains("invalidargument") {
        DocumentError::InvalidArgument(text)
    } else {
        DocumentError::Other(text)
    }
}

fn to_query_error(err: CbError) -> SqlppQueryError {
    // The query service classifies its own failures, so read the typed kind
    // before falling back to the KV classifier's text matching. A malformed
    // statement and a missing keyspace are both the caller's mistake, not the
    // server's, and `server` would tell them to go looking in the wrong place.
    use couchbase::error::ErrorKind as K;
    match err.kind() {
        K::ParsingFailure => {
            return SqlppQueryError::InvalidArgument(format!("malformed SQL++ statement: {err}"))
        }
        K::BucketNotFound | K::ScopeNotFound | K::CollectionNotFound | K::IndexNotFound => {
            return SqlppQueryError::InvalidArgument(format!(
                "the statement referenced a keyspace that does not exist: {err}"
            ))
        }
        _ => {}
    }
    match to_document_error(err) {
        DocumentError::Unauthorized => SqlppQueryError::Unauthorized,
        DocumentError::Timeout => SqlppQueryError::Timeout,
        DocumentError::InvalidArgument(m) => SqlppQueryError::InvalidArgument(m),
        DocumentError::NotFound => SqlppQueryError::InvalidArgument(
            "the statement referenced a keyspace that does not exist".to_string(),
        ),
        other => SqlppQueryError::Server(format!("{other:?}")),
    }
}

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

// ---------------------------------------------------------------------------
// Option translation
// ---------------------------------------------------------------------------

fn durability(level: DurabilityLevel) -> Option<couchbase::durability_level::DurabilityLevel> {
    use couchbase::durability_level::DurabilityLevel as D;
    match level {
        // 0.1.0-draft's enum leads with `unknown`, which is how a caller says
        // "not set" in a field that is not an option.
        DurabilityLevel::Unknown => None,
        DurabilityLevel::None => None,
        DurabilityLevel::ReplicateMajority => Some(D::MAJORITY),
        DurabilityLevel::ReplicateMajorityPersistMaster => Some(D::MAJORITY_AND_PERSIST_ACTIVE),
        DurabilityLevel::PersistMajority => Some(D::PERSIST_TO_MAJORITY),
    }
}

/// Couchbase TTLs have second granularity, so a sub-second value would silently
/// become "no expiry"; round up instead.
fn expiry(expires_in_ns: u64) -> Option<Duration> {
    if expires_in_ns == 0 {
        return None;
    }
    Some(Duration::from_secs(expires_in_ns.div_ceil(1_000_000_000).max(1)))
}

/// Reject option fields whose silent omission would change what gets stored.
///
/// Same rule the sibling plugin follows, and the one `wasmcloud:keyvalue@0.2.0`
/// states: a backend that cannot honour an option must raise rather than
/// silently ignore it.
fn reject_unhonoured(persist_to: u64, replicate_to: u64) -> Result<(), DocumentError> {
    if persist_to != 0 || replicate_to != 0 {
        return Err(DocumentError::Unsupported(
            "persist-to/replicate-to are the pre-6.0 durability settings; use `durability-level`"
                .to_string(),
        ));
    }
    Ok(())
}

fn mutation(binding: &Binding, result: &couchbase::results::kv_results::MutationResult) -> MutationMetadata {
    let token = result.mutation_token();
    MutationMetadata {
        cas: result.cas(),
        bucket: binding.bucket.clone(),
        partition_id: token.map_or(0, |t| u64::from(t.partition_id())),
        partition_uuid: token.map_or(0, |t| t.partition_uuid()),
        seq: token.map_or(0, |t| t.sequence_number()),
        // The SDK hands back a structured token, so there is never an
        // unparsed form to carry.
        raw_token: None,
    }
}

fn get_result(res: &couchbase::results::kv_results::GetResult) -> DocumentGetResult {
    let (bytes, flags) = res.content_as_raw();
    DocumentGetResult {
        document: bytes.to_vec(),
        flags,
        cas: res.cas(),
        expires_in_ns: None,
        // Only present when the caller set `with-expiry`; the SDK leaves it
        // unset otherwise rather than reporting "no expiry".
        expires_at: res.expiry_time().map(|t| to_wit_time(t)),
    }
}

/// Convert an SDK expiry timestamp into the interface's `time` record.
///
/// The record is UTC-based — `offset` is hours against GMT — and the SDK hands
/// back a `DateTime<Utc>`, so the offset is always zero here.
fn to_wit_time(t: &chrono::DateTime<chrono::Utc>) -> bindings::exports::wasmcloud::couchbase::types::Time {
    use chrono::{Datelike, Timelike};
    bindings::exports::wasmcloud::couchbase::types::Time {
        offset: 0,
        year: t.year(),
        month: t.month() as u8,
        day: t.day() as u8,
        hour: t.hour() as u8,
        minute: t.minute() as u8,
        second: t.second() as u8,
        // These are separate units, not two views of the same value: a
        // consumer adding both must not double-count the milliseconds.
        milliseconds: t.nanosecond() / 1_000_000,
        nanoseconds: t.nanosecond() % 1_000_000,
    }
}

// ---------------------------------------------------------------------------
// document
// ---------------------------------------------------------------------------

impl DocumentGuest for Component {
    async fn insert(
        id: String,
        doc: Vec<u8>,
        options: Option<DocumentInsertOptions>,
    ) -> Result<MutationMetadata, DocumentError> {
        let binding = caller_binding()?;
        let deadline = effective_timeout_ms(&binding, options.as_ref().and_then(|o| o.timeout_ns));
        if let Some(o) = options.as_ref() {
            reject_unhonoured(o.persist_to, o.replicate_to)?;
        }
        let flags = options.as_ref().and_then(|o| o.flags).unwrap_or(JSON_FLAGS);
        let expiry = options.as_ref().and_then(|o| expiry(o.expires_in_ns));
        let level = options.as_ref().and_then(|o| durability(o.durability_level));
        with_collection(&binding, |c| {
            let mut opts = couchbase::options::kv_options::InsertOptions::default();
            opts.expiry = expiry;
            opts.durability_level = level;
            block_on_within(deadline, c.insert_raw(&id, &doc, flags, opts))
        })
        .map(|r| mutation(&binding, &r))
    }

    async fn upsert(
        id: String,
        doc: Vec<u8>,
        options: Option<DocumentUpsertOptions>,
    ) -> Result<MutationMetadata, DocumentError> {
        let binding = caller_binding()?;
        let deadline = effective_timeout_ms(&binding, options.as_ref().and_then(|o| o.timeout_ns));
        if let Some(o) = options.as_ref() {
            reject_unhonoured(o.persist_to, o.replicate_to)?;
        }
        let flags = options.as_ref().and_then(|o| o.flags).unwrap_or(JSON_FLAGS);
        let expiry = options.as_ref().and_then(|o| expiry(o.expires_in_ns));
        let level = options.as_ref().and_then(|o| durability(o.durability_level));
        let preserve = options.as_ref().is_some_and(|o| o.preserve_expiry);
        with_collection(&binding, |c| {
            let mut opts = couchbase::options::kv_options::UpsertOptions::default();
            opts.expiry = expiry;
            opts.durability_level = level;
            opts.preserve_expiry = if preserve { Some(true) } else { None };
            block_on_within(deadline, c.upsert_raw(&id, &doc, flags, opts))
        })
        .map(|r| mutation(&binding, &r))
    }

    async fn replace(
        id: String,
        doc: Vec<u8>,
        options: Option<DocumentReplaceOptions>,
    ) -> Result<MutationMetadata, DocumentError> {
        let binding = caller_binding()?;
        let deadline = effective_timeout_ms(&binding, options.as_ref().and_then(|o| o.timeout_ns));
        if let Some(o) = options.as_ref() {
            reject_unhonoured(o.persist_to, o.replicate_to)?;
        }
        let flags = options.as_ref().and_then(|o| o.flags).unwrap_or(JSON_FLAGS);
        let expiry = options.as_ref().and_then(|o| expiry(o.expires_in_ns));
        let level = options.as_ref().and_then(|o| durability(o.durability_level));
        let preserve = options.as_ref().is_some_and(|o| o.preserve_expiry);
        let cas = options.as_ref().map_or(0, |o| o.cas);
        with_collection(&binding, |c| {
            let mut opts = couchbase::options::kv_options::ReplaceOptions::default();
            opts.expiry = expiry;
            opts.durability_level = level;
            opts.preserve_expiry = if preserve { Some(true) } else { None };
            opts.cas = if cas == 0 { None } else { Some(cas) };
            block_on_within(deadline, c.replace_raw(&id, &doc, flags, opts))
        })
        .map(|r| mutation(&binding, &r))
    }

    async fn get(
        id: String,
        options: Option<DocumentGetOptions>,
    ) -> Result<DocumentGetResult, DocumentError> {
        let binding = caller_binding()?;
        let deadline = effective_timeout_ms(&binding, options.as_ref().and_then(|o| o.timeout_ns));
        // Projection and expiry are both the SDK's own `GetOptions`: it turns a
        // projection into a subdoc lookup, falls back to a full fetch past the
        // server's 16-path limit, and reassembles the projected document. Doing
        // that here instead would be a second copy of Couchbase's path rules.
        let project: Option<Vec<String>> = options
            .as_ref()
            .and_then(|o| o.project.as_ref())
            .map(|p| p.iter().filter(|p| !p.is_empty()).cloned().collect::<Vec<_>>())
            .filter(|p| !p.is_empty());
        let with_expiry = options.as_ref().is_some_and(|o| o.with_expiry);
        reject_replica_read(options.as_ref().and_then(|o| o.use_replica))?;

        with_collection(&binding, |c| {
            let mut opts = couchbase::options::kv_options::GetOptions::default();
            opts.projections = project;
            opts.expiry = if with_expiry { Some(true) } else { None };
            block_on_within(deadline, c.get(&id, opts))
        })
        .map(|r| get_result(&r))
    }



    async fn remove(
        id: String,
        options: Option<DocumentRemoveOptions>,
    ) -> Result<MutationMetadata, DocumentError> {
        let binding = caller_binding()?;
        let deadline = effective_timeout_ms(&binding, options.as_ref().and_then(|o| o.timeout_ns));
        if let Some(o) = options.as_ref() {
            reject_unhonoured(o.persist_to, o.replicate_to)?;
        }
        let level = options.as_ref().and_then(|o| durability(o.durability_level));
        let cas = options.as_ref().map_or(0, |o| o.cas);
        with_collection(&binding, |c| {
            let mut opts = couchbase::options::kv_options::RemoveOptions::default();
            opts.durability_level = level;
            opts.cas = if cas == 0 { None } else { Some(cas) };
            block_on_within(deadline, c.remove(&id, opts))
        })
        .map(|r| mutation(&binding, &r))
    }

    /// Served here, unlike on the Data API, which has no locking endpoint.
    async fn get_and_lock(
        id: String,
        options: Option<DocumentGetAndLockOptions>,
    ) -> Result<DocumentGetResult, DocumentError> {
        let binding = caller_binding()?;
        let deadline = effective_timeout_ms(&binding, options.as_ref().and_then(|o| o.timeout_ns));
        // `lock-time` is not an option in the WIT, so `0` is how a caller says
        // "use the default". Rounding it up to one second would hand back a
        // lock that lapses almost immediately.
        let lock_time = match options.as_ref().map_or(0, |o| o.lock_time) {
            0 => Duration::from_secs(DEFAULT_LOCK_SECONDS),
            ns => Duration::from_secs(ns.div_ceil(1_000_000_000).max(1)),
        };
        with_collection(&binding, |c| {
            block_on_within(deadline, c.get_and_lock(
                &id,
                lock_time,
                couchbase::options::kv_options::GetAndLockOptions::default(),
            ))
        })
        .map(|r| get_result(&r))
    }

    /// Served here; see `get-and-lock`.
    async fn unlock(
        id: String,
        options: Option<DocumentUnlockOptions>,
    ) -> Result<(), DocumentError> {
        let binding = caller_binding()?;
        let deadline = effective_timeout_ms(&binding, options.as_ref().and_then(|o| o.timeout_ns));
        let cas = options.as_ref().map_or(0, |o| o.cas);
        if cas == 0 {
            return Err(DocumentError::InvalidArgument(
                "unlock needs the CAS returned by get-and-lock".to_string(),
            ));
        }
        with_collection(&binding, |c| {
            block_on_within(deadline, c.unlock(&id, cas, couchbase::options::kv_options::UnlockOptions::default()))
        })
    }

    async fn touch(
        id: String,
        options: Option<DocumentTouchOptions>,
    ) -> Result<MutationMetadata, DocumentError> {
        let binding = caller_binding()?;
        let deadline = effective_timeout_ms(&binding, options.as_ref().and_then(|o| o.timeout_ns));
        let expires_in = options.as_ref().map_or(0, |o| o.expires_in);
        let Some(ttl) = expiry(expires_in) else {
            return Err(DocumentError::InvalidArgument(
                "touch requires a non-zero expires-in; clear an expiry with `upsert` and expires-in-ns 0".to_string(),
            ));
        };
        let cas = with_collection(&binding, |c| {
            block_on_within(deadline, c.touch(&id, ttl, couchbase::options::kv_options::TouchOptions::default()))
                .map(|r| r.cas())
        })?;
        Ok(MutationMetadata {
            cas,
            bucket: binding.bucket.clone(),
            partition_id: 0,
            partition_uuid: 0,
            seq: 0,
            raw_token: None,
        })
    }

    /// The SDK exposes no replica read: `couchbase` 1.0.1 has no
    /// `get_any_replica`/`get_all_replicas`. The protocol supports it, so this
    /// is a client gap, not a cluster one.
    async fn get_any_replicas(
        _id: String,
        _options: Option<DocumentGetAnyReplicaOptions>,
    ) -> Result<DocumentGetReplicaResult, DocumentError> {
        Err(DocumentError::Unsupported(
            "the Couchbase Rust SDK exposes no replica read".to_string(),
        ))
    }

    /// Not exposed by the SDK; see `get-any-replicas`.
    async fn get_all_replicas(
        _id: String,
        _options: Option<DocumentGetAllReplicaOptions>,
    ) -> Result<Vec<DocumentGetReplicaResult>, DocumentError> {
        Err(DocumentError::Unsupported(
            "the Couchbase Rust SDK exposes no replica read".to_string(),
        ))
    }

    async fn get_and_touch(
        id: String,
        options: Option<DocumentGetAndTouchOptions>,
    ) -> Result<DocumentGetResult, DocumentError> {
        let binding = caller_binding()?;
        let deadline = effective_timeout_ms(&binding, options.as_ref().and_then(|o| o.timeout_ns));
        let expires_in = options.as_ref().map_or(0, |o| o.expires_in);
        let Some(ttl) = expiry(expires_in) else {
            return Err(DocumentError::InvalidArgument(
                "get-and-touch requires a non-zero expires-in".to_string(),
            ));
        };
        with_collection(&binding, |c| {
            block_on_within(deadline, c.get_and_touch(
                &id,
                ttl,
                couchbase::options::kv_options::GetAndTouchOptions::default(),
            ))
        })
        .map(|r| get_result(&r))
    }
}

/// The common-flags value every SDK writes for JSON.
const JSON_FLAGS: u32 = 0x0200_0006;



/// Translate the caller's scan-consistency request into the SDK's.
///
/// Setting `consistent-with` implies `at_plus`, matching how the Couchbase SDKs
/// treat `consistentWith`, so it overrides whatever `scan-consistency` the
/// caller also set. The two rejections below mirror the Data API plugin: a
/// scan vector the query service would accept but that cannot mean what the
/// caller thinks is worse than an error, because the query still returns —
/// just without having waited for anything.
fn query_scan_consistency(
    options: Option<&SqlppQueryOptions>,
    binding: &Binding,
) -> Result<Option<couchbase::options::query_options::ScanConsistency>, SqlppQueryError> {
    use bindings::exports::wasmcloud::couchbase::types::QueryScanConsistency as Wit;
    use couchbase::options::query_options::ScanConsistency as Sdk;

    let Some(options) = options else {
        return Ok(None);
    };

    if let Some(state) = options.consistent_with.as_ref() {
        // A scan vector covers one keyspace. A token from another bucket cannot
        // constrain this query, and quietly dropping it would leave the caller
        // believing in a guarantee they do not have.
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
        // A zeroed partition-uuid is how `mutation-metadata` reports "no token
        // was available". Sent on, it produces a scan vector the query service
        // accepts and then satisfies immediately.
        if state.tokens.iter().any(|t| t.partition_uuid == 0) {
            return Err(SqlppQueryError::InvalidArgument(
                "consistent-with needs a mutation token with a non-zero partition-uuid for every entry; a zeroed token means none was available, and a partial scan vector would let the query run without waiting. Use scan-consistency request-plus instead".to_string(),
            ));
        }
        if state.tokens.is_empty() {
            return Err(SqlppQueryError::InvalidArgument(
                "consistent-with was set with no mutation tokens; omit it, or use scan-consistency request-plus".to_string(),
            ));
        }

        let mut mutation_state = couchbase::mutation_state::MutationState::new();
        for t in &state.tokens {
            let partition_id = u16::try_from(t.partition_id).map_err(|_| {
                SqlppQueryError::InvalidArgument(format!(
                    "consistent-with partition-id {} is not a valid vbucket id",
                    t.partition_id
                ))
            })?;
            mutation_state = mutation_state.push_token(
                couchbase::mutation_state::MutationToken::from_parts(
                    partition_id,
                    t.partition_uuid,
                    t.sequence_number,
                    binding.bucket.clone(),
                ),
            );
        }
        return Ok(Some(Sdk::AtPlus(mutation_state)));
    }

    Ok(match options.scan_consistency {
        Wit::NotBounded => Some(Sdk::NotBounded),
        Wit::RequestPlus => Some(Sdk::RequestPlus),
    })
}


// ---------------------------------------------------------------------------
// sqlpp
// ---------------------------------------------------------------------------

impl SqlppGuest for Component {
    async fn query(
        query: String,
        params: Vec<SqlppValue>,
        options: Option<SqlppQueryOptions>,
    ) -> Result<SqlppValue, SqlppQueryError> {
        let binding = caller_binding().map_err(document_error_as_query_error)?;
        let scan_consistency = query_scan_consistency(options.as_ref(), &binding)?;
        // A per-call timeout overrides the binding's; `0` means unset.
        let timeout_ms = match options.as_ref().map_or(0, |o| o.timeout_ns) {
            0 => binding.timeout_ms,
            ns => ns.div_ceil(1_000_000).clamp(1, u64::from(u32::MAX)) as u32,
        };

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

        let rows = with_scope(&binding, |scope| {
            let mut opts = couchbase::options::query_options::QueryOptions::default();
            if !args.is_empty() {
                opts.positional_parameters = Some(args);
            }
            opts.scan_consistency = scan_consistency;
            opts.server_timeout = Some(Duration::from_millis(u64::from(timeout_ms)));
            block_on(async {
                let mut result = scope.query(&query, opts).await.map_err(to_query_error)?;
                // `rows` is a Stream, so it is drained rather than iterated.
                let mut rows = Vec::new();
                {
                    let stream = result.rows::<serde_json::Value>();
                    futures_lite::pin!(stream);
                    while let Some(row) = futures_lite::StreamExt::next(&mut stream).await {
                        rows.push(row.map_err(to_query_error)?);
                    }
                }
                Ok::<_, SqlppQueryError>(rows)
            })
        })?;

        Ok(SqlppValue::Json(
            serde_json::Value::Array(rows).to_string(),
        ))
    }
}

mod export {
    #![allow(unsafe_code)]
    use super::{bindings, Component};
    bindings::export!(Component with_types_in bindings);
}
