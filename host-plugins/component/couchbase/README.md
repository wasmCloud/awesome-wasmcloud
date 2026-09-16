# Couchbase Component Host Plugin

A [component host plugin](https://wasmcloud.com/docs/runtime/creating-component-host-plugins)
that provides `wasmcloud:couchbase` — document CRUD and SQL++ queries — to every
workload on a wasmCloud host that imports it.

It is a WebAssembly component, not a native plugin, so it runs inside the
sandbox and reaches Couchbase over the [Capella Data API](https://docs.couchbase.com/cloud/data-api-guide/data-api-intro.html)'s
HTTPS surface through `wasi:http/client@0.3.0` rather than over the binary KV
protocol.

> **Status: experimental, but exercised end to end.** It has been run in a real
> wasmCloud host against a real Couchbase cluster — 27 scenario steps covering
> every servable operation, including CAS conflicts, TTLs and parameterized
> SQL++. One caveat matters: Couchbase's Data API is a **Capella** service, and
> the cluster available for testing was self-managed, so the KV calls were
> served by a stand-in gateway in front of that real cluster. See
> [What is and is not verified](#what-is-and-is-not-verified).

## Relationship to `wasmcloud:couchbase@0.1.0-draft`

There is already a published `wasmcloud:couchbase` — the WIT in
[`Couchbase-Ecosystem/wasmcloud-provider-couchbase`](https://github.com/Couchbase-Ecosystem/wasmcloud-provider-couchbase),
at `@0.1.0-draft`. **This package deliberately mirrors it**, so that code
written against the provider ports across with as little friction as possible.

It could not simply reuse it. That package is entirely synchronous — 27
functions, none `async` — and a component host plugin serves its capability
across a store boundary where the cross-store shim registers and type-matches
**only asynchronous** imports. A synchronous interface cannot be served by a
plugin at all. So this is the same API at `@0.2.0` with `async func`
throughout, the same move wasmCloud made revising `wasmcloud:secrets` from
`1.0.0` to `2.0.0`.

Porting is mostly adding `.await`. Names, records, option fields and signatures
are otherwise unchanged.

Five intentional differences, each marked with a `DIFFERS FROM 0.1.0-draft`
note at the point it occurs in [`interface/couchbase.wit`](interface/couchbase.wit):

1. **`get-any-repliacs` is spelled `get-any-replicas`.** 0.1.0-draft has the
   letters transposed. This is the only rename in the package, so porting means
   fixing one call site that the compiler points straight at. `0.2.0` is a
   breaking version regardless, and carrying a typo across it would have meant
   carrying it indefinitely.
2. **`document-error` and `sqlpp-query-error` gain cases.** 0.1.0-draft's
   `document-error` can only describe what a *document* did wrong — it has no
   way to say "the credentials were refused", "the cluster never answered", or
   "this workload has no binding". Every original case is unchanged, so existing
   match arms keep working.
3. **`sqlpp-value` gains a `json` case.** 0.1.0-draft declares it with `null` as
   its only case, which cannot carry a parameter into a query or a row out of
   one.
4. **A document's value is `list<u8>` plus `document-flags`,** where
   0.1.0-draft has `variant document { raw(json-string), %resource(document-value) }`.
   See [below](#a-documents-value-bytes-and-flags).
5. **`get-any-repliacs` aside, the operation set is unchanged.** Locking and
   replica reads are kept: they are real KV-protocol operations that an
   implementation speaking `couchbases://` serves, even though the Data API has
   no endpoint for them. See [below](#operations-this-implementation-cannot-serve).

## Requirements

- **Rust** with the `wasm32-wasip2` target (`rustup target add wasm32-wasip2`).
- **`wash`** built from wasmCloud `main` (tested at `b8e535692`).
- **A wasmCloud host built with the `host-component-plugins` Cargo feature.**
  This is opt-in and is *not* in the released `ghcr.io/wasmcloud/wash` images or
  the `:canary` tag. Build one with
  `cargo build --bin wash --features host-component-plugins`. A host without the
  feature rejects a plugin declaration at startup with a clear error rather than
  silently dropping it.
- **A Couchbase cluster with the Data API enabled** (that means Capella; see
  [`verification/`](verification/) for the self-hosted alternative), plus a
  database credential scoped to the bucket you intend to bind.

## Build

```console
wash build
```

That fetches WIT dependencies and produces
`target/wasm32-wasip2/release/couchbase_plugin.wasm`.

The bindings-free logic — config validation, Data API error classification, CAS
and URL handling, and the calendar math the touch endpoint needs — is compiled
for the host too, so it can be tested directly:

```console
cargo test
```

## Deploy

A component host plugin is declared in **host** configuration, not in a
workload's manifest: it serves every workload on the host that imports its
interface, so it is a privileged install the operator controls.

Unlike a workload, a plugin's `allowedHosts` denies **every** outbound host by
default. The Couchbase endpoint has to be listed or every call fails. The plugin
reports that case as `not-configured` naming the policy, rather than as a
credentials problem.

That matters for how you declare it: the `wash host --host-plugin` flag grammar
accepts only `id`, `image`/`file`, `pull`, `max-restarts` and `digest`, so it
**cannot express `allowedHosts`** and a plugin declared that way reaches
nothing. Use the config-file form. In `.wash/config.yaml`:

```yaml
host:
  hostPlugins:
    - id: couchbase
      file: ./host-plugins/component/couchbase/target/wasm32-wasip2/release/couchbase_plugin.wasm
      allowedHosts:
        - "*.data.cloud.couchbase.com"
```

`wash dev` takes the same entries under `dev.host_plugins` — note the snake_case
key, with camelCase fields inside it. For Kubernetes, the Helm chart takes the
same shape per host group under `runtime.hostGroups[].hostPlugins`.

## Configuring a workload

The plugin holds no cluster configuration of its own. Each workload supplies its
own through **interface-level config**, and the plugin captures it in
`on-workload-bind` — so a workload with a missing endpoint or credential **fails
to deploy**, with a message naming the key at fault, instead of failing on its
first query.

Two workloads sharing this plugin never see each other's configuration: every
call resolves its caller through `wasmcloud:host/identity` and looks up only
that workload's binding.

| Key | Required | Default | Meaning |
|---|---|---|---|
| `endpoint` | yes | — | Data API base URL: `https://host[:port]`, optionally with a mount path. A bare `host:port` is treated as HTTPS. |
| `bucket` | yes | — | The one bucket this binding may reach. |
| `username` | yes | — | Database access credential. |
| `password` | yes | — | Database access secret. Source it from `secretFrom`, not a literal. |
| `scope` | no | `_default` | Scope this binding operates in. |
| `collection` | no | `_default` | Collection this binding operates in. |
| `timeout-ms` | no | `30000` | Per-request time limit. Also bounds the transport, so a hung cluster cannot hold a caller open indefinitely. |

Bucket, scope and collection come from configuration rather than from call
parameters — exactly as in 0.1.0-draft, where they came from the link config.
A workload therefore cannot reach a keyspace it was not granted.

Because a capability call into a plugin has **no host-imposed timeout**, this
`timeout-ms` is what actually bounds a call.

## Using it from a workload

Copy [`interface/couchbase.wit`](interface/couchbase.wit) into your component's
`wit/deps/wasmcloud-couchbase/`, then import what you need:

```wit
world my-app {
    import wasmcloud:couchbase/types@0.2.0;
    import wasmcloud:couchbase/document@0.2.0;
    import wasmcloud:couchbase/sqlpp@0.2.0;
    import wasmcloud:couchbase/sqlpp-types@0.2.0;
    export wasi:http/handler@0.3.0;
}
```

Records generated from WIT carry no `Default`, and 0.1.0-draft's option records
are wide, so a helper per operation keeps call sites readable:

```rust
use bindings::wasmcloud::couchbase::document::{self, DocumentInsertOptions, DocumentReplaceOptions};
use bindings::wasmcloud::couchbase::types::{Document, DurabilityLevel};

fn insert_opts(expires_in_ns: u64) -> DocumentInsertOptions {
    DocumentInsertOptions {
        expires_in_ns,
        persist_to: 0,
        replicate_to: 0,
        // `unknown` is how a caller says "not set" in a field that is not an option.
        durability_level: DurabilityLevel::Unknown,
        timeout_ns: None,
        retry_strategy: None,
        parent_span: None,
    }
}

// A document's value is bytes. Expiry is in nanoseconds, as in 0.1.0-draft.
let written = document::insert(
    "user:42".to_string(),
    br#"{"name":"Ada"}"#.to_vec(),
    Some(insert_opts(600 * 1_000_000_000)),
).await?;

// Conditional update: fails with `cas-mismatch` if anything wrote in between.
document::replace(
    "user:42".to_string(),
    br#"{"name":"Ada Lovelace"}"#.to_vec(),
    Some(DocumentReplaceOptions { cas: written.cas, ..replace_defaults() }),
).await?;

// A missing document is `not-found`, matching 0.1.0-draft.
match document::get("user:42".to_string(), None).await {
    Ok(got) => { /* got.document: Vec<u8>, got.flags, got.cas */ }
    Err(DocumentError::NotFound) => { /* absent */ }
    Err(e) => return Err(e),
}

// Anything that is not JSON needs its flags set, or a reader will
// transcode it wrongly.
document::upsert(
    "thumbnail:42".to_string(),
    png_bytes,
    Some(DocumentUpsertOptions { flags: Some(0x0300_0000), ..upsert_defaults() }),
).await?;
```

Bind user input as query parameters rather than interpolating it into the
statement, so the cluster treats it as a value and never as SQL++ syntax:

```rust
use bindings::wasmcloud::couchbase::sqlpp;
use bindings::wasmcloud::couchbase::sqlpp_types::SqlppValue;

let result = sqlpp::query(
    "SELECT RAW d.name FROM _default AS d WHERE d.city = $1 LIMIT 10".to_string(),
    vec![SqlppValue::Json(r#""Berlin""#.to_string())],
    Some(query_opts()),
).await?;

// The result is the service's `results` array as JSON text.
if let SqlppValue::Json(rows) = result { /* decode `rows` */ }
```

An unqualified keyspace resolves against the binding's own bucket and scope,
which the plugin sets as the query context — so `FROM _default` means the
configured collection, not a bucket named `_default`.

A complete, compiling example of every operation is
[`verification/scenario/src/lib.rs`](verification/scenario/src/lib.rs).

## How the interface maps onto the Data API

| WIT | Request |
|---|---|
| `document.get` | `GET …/documents/{id}`, `?project=` per field |
| `document.insert` | `POST …/documents/{id}` |
| `document.upsert` | `PUT …/documents/{id}` |
| `document.replace` | `PUT …/documents/{id}` with `If-Match` — the caller's CAS, or `*` |
| `document.remove` | `DELETE …/documents/{id}` |
| `document.touch` | `POST …/documents/{id}/touch` |
| `document.get-and-touch` | `POST …/documents/{id}/touch` with `returnContent` |
| `sqlpp.query` | `POST /_p/query/query/service` |

The four operations absent from this table — `get-and-lock`, `unlock`, and the
two replica reads — have no Data API endpoint and return `unsupported` here.

Options travel as headers: CAS as `If-Match`, TTL as `Expires` (a Go duration
string, so no calendar arithmetic is involved), durability as
`X-CB-DurabilityLevel`, and common flags as `X-CB-Flags` — which the Data API
documents as overriding the flags it would otherwise derive from `Content-Type`.
Responses report CAS in `etag` and flags in `X-CB-Flags`.

Errors are classified from Couchbase's machine-readable `code` first and HTTP
status only as a fallback, because status alone is ambiguous — a `409` is both
"you inserted over an existing document" and "your CAS was stale", and callers
must handle those differently.

### Operations this implementation cannot serve

`get-and-lock`, `unlock`, `get-any-replicas` and `get-all-replicas` are part of
the interface, and this implementation returns `document-error.unsupported` for
all four, naming what to use instead.

That split is deliberate. The interface describes **Couchbase**, not the Data
API. Pessimistic locking and replica reads are real KV-protocol operations that
an implementation speaking `couchbases://` serves. The Data API simply has no
path for them — its OpenAPI specification's complete path set is
`callerIdentity`, document `GET`/`POST`/`PUT`/`DELETE`, `append`, `prepend`,
`increment`, `decrement`, `touch`, and the query and search passthroughs.

So this is exactly what `unsupported` is for: one interface, implementations
with different reach, and a caller told plainly which it got rather than being
handed a silently degraded result. `document-get-options.use-replica` is
refused here for the same reason.

`mutation-metadata` keeps 0.1.0-draft's shape and is filled from the Data API's
`X-CB-MutationToken`; see [Mutation tokens](#mutation-tokens).

### A document's value: bytes and flags

A Couchbase document value is **bytes plus a `u32` of common flags** saying how
those bytes are encoded — JSON, raw binary, or a UTF-8 string. That pair is what
the cluster stores and what every SDK transcoder converts to and from. So:

```wit
type document-flags = u32;   // JSON is 0x02000006
type document = list<u8>;
```

Writes take `flags: option<document-flags>` (`none` means JSON, the common
case); reads report `flags` alongside the value.

**Why not `json-string`.** Couchbase is not a JSON-only store. Binary documents
are first-class — they are what `append`/`prepend` and the counter operations
act on, and what flags exist to describe. A `string` cannot hold one: any value
that is not valid UTF-8 would have to fail. That was not hypothetical here — the
earlier shape returned `not-json` for such a document, so it could not round-trip
one at all. The [verification scenario](verification/scenario/src/lib.rs) now
stores `[0x00, 0xFF, 0xFE, 0x01, 0x80, 0x7F]` and reads it back byte-for-byte
with its flags intact.

**Why not a resource.** 0.1.0-draft's `document-value` offered an "efficient
implementer-specific" JSON representation. Two problems. It wrapped a string in
practice, so it bought nothing. And a resource the *caller* constructs cannot
cross a component host plugin's store boundary at all:

```
component imports instance `wasmcloud:couchbase/types`, but a matching
implementation was not found in the linker
  instance export `document-value` has the wrong type
  resource implementation is missing
```

The rule, visible in `wasmcloud:keyvalue@0.2.0`: a plugin capability may
**return** resources — keyvalue's `bucket` comes out of `store.open` and is
passed back as `borrow<bucket>` — but it cannot **accept** guest-constructed
ones. `document-value` had a constructor and a static, so its handles flow
inward, which is what fails. (wasi-http's guest-constructible `fields` and
`request` are not a counterexample: wasi-http is host-native, not dispatched
across a plugin store.) Verified both with and without other functions in the
same instance, so it is the resource, not the shape of the instance around it.

`list<u8>` is both the honest model and the portable one.

## Design notes

- **Handle-free.** Every operation takes and returns plain values. Handles
  crossing a store boundary are relocated by the runtime rather than shared, so
  a handle-free interface stays on the fast path.
- **Buffered, not streamed.** The Query Service returns its whole result set as
  one JSON document, so a streaming signature would be streaming a payload that
  had already arrived in full. Bound result sets with `LIMIT`.
- **Cancellation is checked before a request is issued**, via
  `wasmcloud:host/cancel`. A teardown arriving while calls are queued returns
  those calls immediately; a request already in flight still runs to its own
  transport timeout.
- **No `wasi:cli/run`.** There is no background work between calls, and a pure
  capability plugin is allowed to omit it.

## What is and is not verified

### Run live

The plugin was loaded into a wasmCloud host built from `main` with
`host-component-plugins`, bound to a test workload, and driven over HTTP against
Couchbase Server 7.6.4 in Docker. All 27 scenario steps passed:

- **Round trips**: insert → get returns the same bytes and the same CAS;
  remove → get returns `not-found`.
- **CAS**: a mutation changes the CAS; a `replace` carrying the superseded CAS
  is rejected as `cas-mismatch`; one carrying the current CAS succeeds. CAS
  values are ~1.7×10¹⁸, past 2⁵³, which is why they are carried as strings
  rather than JSON numbers.
- **Existence preconditions**: `insert` over a live key is `already-exists`;
  `replace` and `remove` on an absent key are `not-found`.
- **TTL**: `upsert` with a 600-second `expires-in-ns` produced a document whose
  expiration in the cluster was exactly 600 seconds after the write.
- **Binary documents**: `[0x00, 0xFF, 0xFE, 0x01, 0x80, 0x7F]` written with raw
  flags reads back byte-for-byte, flags intact — a value the earlier
  `json-string` shape could not represent.
- **SQL++**: positional and null parameters both bind, `request-plus` scan
  consistency is accepted. A malformed statement surfaces as `invalid-argument`
  carrying the cluster's own message, never as an empty success.
- **Options that cannot be honoured are rejected**, not silently dropped:
  `preserve-expiry`, `persist-to`/`replicate-to`, and `consistent-with`. A
  per-call `timeout-ns` is honoured.
- **Deploy-time config validation**: dropping `bucket` from the workload's
  interface config made the deploy *fail* with
  `wasmcloud:couchbase missing required config key 'bucket'`.
- **Egress policy**: pointing the endpoint outside the plugin's `allowedHosts`
  produced `not-configured` naming the policy — not a misleading credentials
  error.

Live testing changed the code twice, and both were worth it. Every request was
going out as `Transfer-Encoding: chunked`, because a `wasi:http` body is a
stream and the transport had no length to advertise — even a body-less `DELETE`
was chunked; the plugin now sets `Content-Length` explicitly. And the
`document-value` resource turned out not to link at all, which is why it is
absent.

The harness is in [`verification/`](verification/) — `docker compose up -d`
brings up Couchbase plus the Data API server — so this is reproducible rather
than a claim.

### Also verified

- The component's embedded WIT shows every capability function and both
  lifecycle hooks as `async func` — the plugin contract's hard requirement.
- Its imports are `wasi:http/client@0.3.0`, `wasi:clocks/system-clock`,
  `wasmcloud:host/identity` and `wasmcloud:host/cancel`; its exports are the
  four `wasmcloud:couchbase` interfaces plus
  `wasmcloud:host/workload-lifecycle@0.1.1`.
- 24 unit tests cover config validation, endpoint parsing, error classification,
  CAS parsing, URL segment encoding, and the ISO 8601 conversion.
- A clean checkout builds with no warnings.

### Verified against real Capella

Every question that was previously open is now closed, against a live Capella
Data API endpoint. Two were closed by *finding bugs*:

- **The ETag is bare, unquoted, 16-digit hex** (`18c86cb5894f0000`), not
  decimal. It was being parsed as decimal, so every CAS would have come back
  `0`.
- **`If-Match` must not be quoted.** Capella answers a quoted value with
  `InvalidArgument: Invalid etag format '"..."'`. The RFC-style quotes were
  being sent, so *every CAS-conditional write would have failed outright* —
  `replace` with a CAS, `remove` with a CAS.

Neither is visible from the specification; both took a live endpoint. The rest
confirmed the implementation as it stood:

| Question | Answer from Capella |
|---|---|
| Does `PUT` create an absent document? | **Yes** — `upsert` → `PUT` is correct |
| Is `If-Match: *` honoured? | **Yes** — absent key answers `DocumentNotFound`, which is `replace`'s precondition |
| `POST` over an existing key | `DocumentExists` — `insert` is correct |
| `project` syntax | `?project=a,b`; the **repeated form is rejected** (`parameter 'project' is not exploded`) |
| Binary documents | Round-trip byte-for-byte with flags preserved |
| `X-CB-MutationToken` format | `bucket:vbid:vbuuid-hex:seqno`, e.g. `travel-sample:16:794f18f71747:512` |

The mutation token now parses structurally from a real response
(`vbid=34 vbuuid=88826486882458 seq=474`), and feeding it back as
`consistent-with` produces a working `at_plus` query.

This was also the first exercise of **TLS egress** from the plugin; every
earlier run was plaintext HTTP to localhost.

### Still unverified

- **`couchbases://`.** This implementation speaks the Data API over HTTPS. A
  transport speaking the binary KV protocol is a separate piece of work; see
  [Operations this implementation cannot serve](#operations-this-implementation-cannot-serve)
  for what that would unlock.
- **Durability levels beyond the default.** `X-CB-DurabilityLevel` is sent but
  its effect has not been observed on a multi-node cluster; the test cluster is
  single-node, where `majority` is trivially satisfied.
- **`with-expiry`.** Capella returns an `Expires` response header on `GET`, so
  `document-get-options.with-expiry` is satisfiable, but it is not implemented:
  `expires-at` always comes back `none`.

## License

Apache-2.0. See [LICENSE](LICENSE). Vendored WIT and its provenance are
documented in [`wit-deps/README.md`](wit-deps/README.md). The interface mirrors
`wasmcloud:couchbase@0.1.0-draft` from
[`Couchbase-Ecosystem/wasmcloud-provider-couchbase`](https://github.com/Couchbase-Ecosystem/wasmcloud-provider-couchbase),
which is Apache-2.0.
