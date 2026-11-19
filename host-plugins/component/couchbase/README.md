# Couchbase Component Host Plugin

A [component host plugin](https://wasmcloud.com/docs/runtime/creating-component-host-plugins)
that provides `wasmcloud:couchbase` — document CRUD and SQL++ queries — to every
workload on a wasmCloud host that imports it.

It is a WebAssembly component, not a native plugin, so it runs inside the
sandbox and reaches Couchbase over the [Capella Data API](https://docs.couchbase.com/cloud/data-api-guide/data-api-intro.html)'s
HTTPS surface through `wasi:http/client@0.3.0` rather than over the binary KV
protocol.

Document HTTP bodies use incremental reads with a 32 MiB limit, including chunked replies.
SQL++ rows stream through the WIT interface as complete JSON values, with
backpressure and an explicit final-status check.
Queries permit up to 32 MiB per row, 64 KiB of metadata, 256 MiB per response
and one million rows, within one overall deadline.

> **Status: experimental, but exercised end to end.** It has been run in a real
> wasmCloud host against live Capella, and — reproducibly, with no cloud account
> — against a self-hosted cluster fronted by Couchbase's **Cloud Native
> Gateway**, which is what serves the Data API. 40 scenario steps cover every
> servable operation, including CAS conflicts, TTLs, binary documents and
> parameterized SQL++. See [What is and is not verified](#what-is-and-is-not-verified).

## Interfaces and migration

The capability contract is `wasmcloud:couchbase@0.3.0`.
All operations are asynchronous and can be implemented by another component.

- `document-read` grants `get`, `get-any-replicas` and `get-all-replicas`.
  It cannot change documents, expiry or locks.
- `document-write` grants insert, replace, upsert, remove, touch,
  get-and-touch, get-and-lock and unlock.
- `sqlpp` grants arbitrary SQL++ statements and streamed results.
  It may mutate data; a component that should only read documents should
  import `document-read` without `sqlpp` or `document-write`.
  Database RBAC still limits the bound credential.
- Shared document option and result records live in `types`.
  SQL++ parameter values and typed errors live in `sqlpp-types`.

Migrating from this example's `0.2.0` contract requires changing WIT imports
and manifest versions to `0.3.0`, selecting the separate read/write interfaces,
and importing document option/result records from `types`.
Interfaces using the same binding label must share the same effective config.
Use one binding entry listing the required interfaces, or distinct named
imports when reads, writes or queries need different credentials or keyspaces.
Conflicting configs for one label are rejected during binding.
SQL++ returns `query-result`: consume its row stream, then call
`query-completion.finish` to check final service and transport status.
The earlier synchronous `0.1.0-draft` provider interface additionally requires
async calls and adapting document values to owned bytes plus flags.

## Requirements

- **Rust** with the `wasm32-wasip2` target (`rustup target add wasm32-wasip2`).
- **`wash`** 2.12.0 or newer, for the `wasmcloud:host@0.1.6` WIT.
- **A wasmCloud host built with the `host-component-plugins` Cargo feature.**
  Standard images omit this opt-in feature. Use
  `ghcr.io/wasmcloud/wash:2.12.1-all-features`, or build one with
  `cargo build --bin wash --features host-component-plugins`. A host without the
  feature rejects a plugin declaration at startup with a clear error rather than
  silently dropping it.
- **A Couchbase cluster with the Data API enabled** (that means Capella; see
  [`verification/`](verification/) for the self-hosted alternative), plus a
  database credential scoped to the bucket you intend to bind.

## Build

Prebuilt plugins and the WIT package are published to GHCR:

| Artifact | Reference |
|---|---|
| WIT contract | `ghcr.io/wasmcloud/interfaces/wasmcloud/couchbase:0.3.0` |
| Data API plugin | `ghcr.io/wasmcloud/components/couchbase:0.1.0` |
| KV socket plugin | `ghcr.io/wasmcloud/components/couchbase-wasi-sockets-p3:0.1.0` |
| KV socket plugin with TLS | `ghcr.io/wasmcloud/components/couchbase-wasi-sockets-p3-tls:0.1.0` |

The TLS variant requires a host with the upstream
[component plugin TLS fix](https://github.com/wasmCloud/wasmCloud/pull/6014)
and both `host-component-plugins` and `wasi-tls` enabled.
See the [socket plugin's TLS requirements](../couchbase-wasi-sockets-p3/README.md#tls)
before selecting a host release.

To build from source instead:

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
      image: ghcr.io/wasmcloud/components/couchbase:0.1.0
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
call resolves its caller through `wasmcloud:host/identity` and looks up that
workload's binding by its import label. Multiple named bindings can use distinct
clusters, credentials, and keyspaces within one workload.

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

Declare the required imports in your workload's WIT, then run `wash wit fetch`.
Normal wash registry resolution fetches `wasmcloud:couchbase@0.3.0` from GHCR;
no checkout or `wit.sources` override is needed.

```wit
world my-app {
    import wasmcloud:couchbase/types@0.3.0;
    import wasmcloud:couchbase/document-read@0.3.0;
    import wasmcloud:couchbase/document-write@0.3.0;
    import wasmcloud:couchbase/sqlpp@0.3.0;
    import wasmcloud:couchbase/sqlpp-types@0.3.0;
    export wasi:http/handler@0.3.0;
}
```

When developing changes to the contract locally, override the published package
with [`interface/couchbase.wit`](interface/couchbase.wit):

```yaml
wit:
  sources:
    "wasmcloud:couchbase": ../awesome-wasmcloud/host-plugins/component/couchbase/interface
```

Records generated from WIT carry no `Default`, and 0.1.0-draft's option records
are wide, so a helper per operation keeps call sites readable:

```rust
use bindings::wasmcloud::couchbase::{document_read, document_write};
use bindings::wasmcloud::couchbase::types::{DocumentInsertOptions, DocumentReplaceOptions};
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
let written = document_write::insert(
    "user:42".to_string(),
    br#"{"name":"Ada"}"#.to_vec(),
    Some(insert_opts(600 * 1_000_000_000)),
).await?;

// Conditional update: fails with `cas-mismatch` if anything wrote in between.
document_write::replace(
    "user:42".to_string(),
    br#"{"name":"Ada Lovelace"}"#.to_vec(),
    Some(DocumentReplaceOptions { cas: written.cas, ..replace_defaults() }),
).await?;

// A missing document is `not-found`, matching 0.1.0-draft.
match document_read::get("user:42".to_string(), None).await {
    Ok(got) => { /* got.document: Vec<u8>, got.flags, got.cas */ }
    Err(DocumentError::NotFound) => { /* absent */ }
    Err(e) => return Err(e),
}

// Anything that is not JSON needs its flags set, or a reader will
// transcode it wrongly.
document_write::upsert(
    "thumbnail:42".to_string(),
    png_bytes,
    Some(DocumentUpsertOptions { flags: Some(0x0300_0000), ..upsert_defaults() }),
).await?;
```

Bind user input as query parameters rather than interpolating it into the
statement, so the cluster treats it as a value and never as SQL++ syntax:

```rust
use bindings::wasmcloud::couchbase::sqlpp::{self, QueryCompletion};
use bindings::wasmcloud::couchbase::sqlpp_types::SqlppValue;

let result = sqlpp::query(
    "SELECT RAW d.name FROM _default AS d WHERE d.city = $1 LIMIT 10".to_string(),
    vec![SqlppValue::Json(r#""Berlin""#.to_string())],
    None,
).await?;

let mut rows = result.rows;
let mut count = 0;
loop {
    let (_, chunk) = rows.read(Vec::with_capacity(1)).await;
    if chunk.is_empty() { break; }
    for _row in chunk {
        // Process this JSON row without retaining the whole result set.
        count += 1;
    }
}
QueryCompletion::finish(result.completion).await?;
// Only now is the query successful; earlier rows were provisional.
```

An unqualified keyspace resolves against the binding's own bucket and scope,
which the plugin sets as the query context — so `FROM _default` means the
configured collection, not a bucket named `_default`.

A complete, compiling example of every operation is
[`verification/scenario/src/lib.rs`](verification/scenario/src/lib.rs).

## How the interface maps onto the Data API

| WIT | Request |
|---|---|
| `document-read.get` | `GET …/documents/{id}`, `?project=` per field |
| `document-write.insert` | `POST …/documents/{id}` |
| `document-write.upsert` | `PUT …/documents/{id}` |
| `document-write.replace` | `PUT …/documents/{id}` with `If-Match` — the caller's CAS, or `*` |
| `document-write.remove` | `DELETE …/documents/{id}` |
| `document-write.touch` | `POST …/documents/{id}/touch` |
| `document-write.get-and-touch` | `POST …/documents/{id}/touch` with `returnContent` |
| `sqlpp.query` | `POST /_p/query/query/service` |

`get-and-lock` and `unlock` use `POST …/documents/{id}/lock` and `/unlock`
under `/v1.alpha`, which a gateway serves only with `--alpha-endpoints`. The two
replica reads return `unsupported`.

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

`get-any-replicas` and `get-all-replicas` return `document-error.unsupported`.
Reading a replica means addressing one node, and the Data API is a single
endpoint in front of the cluster.

That split is deliberate. The interface describes **Couchbase**, not one
transport. A replica read is a real operation that a transport addressing
individual nodes serves.

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
that is not valid UTF-8 would have to fail. The
[verification scenario](verification/scenario/src/lib.rs) stores
`[0x00, 0xFF, 0xFE, 0x01, 0x80, 0x7F]` and reads it back byte-for-byte with its
flags intact.

Owned document bytes preserve binary values and avoid a document-specific
resource representation.
The query completion handle is created by the plugin and proxied by the host;
it carries no caller-constructed resource into the plugin.

## Streaming query results

The plugin decodes the Query Service envelope incrementally and emits one JSON
row per `stream<string>` item.
It does not buffer the result array.
Only one row and bounded metadata are retained by the decoder;
stream writes supply backpressure through the component boundary.

Drain rows before awaiting `QueryCompletion::finish(result.completion)`.
That consuming call returns a typed error for late query failures, malformed
or oversized results, transport truncation, and deadlines.
EOF alone is insufficient: a server can emit valid rows before reporting an
error at the end of its envelope.
Dropping the completion handle cancels outstanding reads and writes.
The deadline includes the initial request, row delivery and downstream backpressure.

A read-only workload needs only:

```wit
world reader {
    import wasmcloud:couchbase/document-read@0.3.0;
    export wasi:http/handler@0.3.0;
}
```

Use the same restricted interface list in its host binding.
The [read-only verification caller](../../../tests/probe/wit/world.wit)
provides a runnable example.
SQL++ and document-write are separate grants; omitting those imports prevents
these operations from being linked into the caller.

## What is and is not verified

### Run live

The plugin was loaded into a wasmCloud host built from `main` with
`host-component-plugins`, bound to a test workload, and driven against Couchbase
Server 7.6.4 in Docker, through the Cloud Native Gateway's Data API over
verified TLS. All 34 scenario steps pass:

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
  flags reads back byte-for-byte, flags intact.
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

Every request carries an explicit `Content-Length`. A `wasi:http` body is a
stream with no length to advertise, so without it every request goes out
`Transfer-Encoding: chunked`, body-less `DELETE`s included.

The harness is in [`verification/`](verification/): `docker compose up -d`
brings up Couchbase fronted by the Cloud Native Gateway, and `demo.sh` runs the
whole thing. The gateway is the real Data API, not a reimplementation of it.

### `with-expiry`

Relative TTLs are rounded up to whole seconds. Insert, upsert, replace, touch
and get-and-touch reject an expiry beyond Couchbase's unsigned 32-bit Unix
timestamp range (ending in 2106) before sending a mutation. Large TTLs are never
clamped or allowed to wrap into the past.

The Data API gateway normalizes dot-only URL segments, including percent-encoded
forms. Document IDs `.` and `..` therefore return `unsupported` on this transport;
the sockets plugin can address them directly.

A read reports the document's absolute expiry in an `Expires` header, present
only when the document has a TTL. `expires-at` is filled from it.

The gateway writes that header's zone as `UTC` where HTTP-date requires `GMT`, so
a strict HTTP-date parser rejects every value it sends. The plugin accepts both
spellings. The deprecated relative `expires-in-ns` stays empty: deriving it needs
a clock read, and it would be stale on arrival.

### Also verified

- The component's embedded WIT shows every capability function and both
  lifecycle hooks as `async func` — the plugin contract's hard requirement.
- Its imports are `wasi:http/client@0.3.0`, `wasi:clocks/system-clock`,
  `wasmcloud:host/identity` and `wasmcloud:host/cancel`; its exports are the
  five `wasmcloud:couchbase` interfaces plus
  `wasmcloud:host/workload-lifecycle@0.1.6`.
- 40 unit tests cover config validation, endpoint parsing, error classification,
  CAS and mutation-token parsing, URL segment encoding, the ISO 8601 conversion,
  and the `Expires` header.
- `get-and-lock` and `unlock` go to `/v1.alpha`, which a gateway serves only
  with `--alpha-endpoints`. An endpoint without them answers 404, which reads as
  `not-found`.
- A clean checkout builds with no warnings.

### Verified against real Capella

Run against a live Capella Data API endpoint. Two details are not visible from
the specification, and both are load-bearing:

- **The ETag is bare, unquoted, 16-digit hex** (`18c86cb5894f0000`), not
  decimal. Parsed as decimal, every CAS reads `0`.
- **`If-Match` must not be quoted.** Capella answers a quoted value with
  `InvalidArgument: Invalid etag format '"..."'`, which fails every
  CAS-conditional write: `replace` with a CAS, `remove` with a CAS.

Both are reproducible without a Capella account — the Cloud Native Gateway sends
the same ETag and mutation-token formats. The rest:

| Question | Answer from Capella |
|---|---|
| Does `PUT` create an absent document? | **Yes** — `upsert` → `PUT` is correct |
| Is `If-Match: *` honoured? | **Yes** — absent key answers `DocumentNotFound`, which is `replace`'s precondition |
| `POST` over an existing key | `DocumentExists` — `insert` is correct |
| `project` syntax | `?project=a,b`; the **repeated form is rejected** (`parameter 'project' is not exploded`) |
| Binary documents | Round-trip byte-for-byte with flags preserved |
| `X-CB-MutationToken` format | `bucket:vbid:vbuuid-hex:seqno`, e.g. `travel-sample:16:794f18f71747:512` |

A real mutation token parses structurally
(`vbid=34 vbuuid=88826486882458 seq=474`), and feeding it back as
`consistent-with` produces a working `at_plus` query.

### Still unverified

- **`couchbases://` against Capella.** This implementation speaks the Data API
  over HTTPS. The sibling
  [`couchbase-wasi-sockets-p3`](../couchbase-wasi-sockets-p3/) plugin speaks the
  binary KV protocol and serves what this one cannot, but does not support TLS,
  so it has been run only against a self-hosted cluster.
- **Durability levels beyond the default.** `X-CB-DurabilityLevel` is sent but
  its effect has not been observed on a multi-node cluster; the test cluster is
  single-node, where `majority` is trivially satisfied.

## License

Apache-2.0. See [LICENSE](LICENSE).
Host, secrets and WASI WIT dependencies are fetched from registries by wash.
The interface mirrors
`wasmcloud:couchbase@0.1.0-draft` from
[`Couchbase-Ecosystem/wasmcloud-provider-couchbase`](https://github.com/Couchbase-Ecosystem/wasmcloud-provider-couchbase),
which is Apache-2.0.
