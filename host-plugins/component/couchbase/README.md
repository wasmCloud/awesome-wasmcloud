# Couchbase Component Host Plugin

A [component host plugin](https://wasmcloud.com/docs/runtime/creating-component-host-plugins)
that provides `wasmcloud:couchbase` — key-value document access, atomic
counters, byte concatenation, and SQL++ queries — to every workload on a
wasmCloud host that imports it.

It is a WebAssembly component, not a native plugin, so it runs inside the
sandbox and reaches Couchbase over the [Capella Data API](https://docs.couchbase.com/cloud/data-api-guide/data-api-intro.html)'s
HTTPS surface through `wasi:http/client@0.3.0` rather than over the binary KV
protocol. Every operation is an `async func`, which is a requirement rather than
a preference: a component host plugin serves its capability across a store
boundary, and the cross-store shim only type-matches asynchronous interfaces, so
a synchronous one cannot be served by a plugin at all.

> **Status: experimental.** It builds, its WIT and its bindings-free logic are
> tested, and its component surface has been verified against the plugin
> contract. It has **not** been run against a live Couchbase cluster. See
> [What is and is not verified](#what-is-and-is-not-verified) before depending
> on it.

## Requirements

- **Rust** with the `wasm32-wasip2` target (`rustup target add wasm32-wasip2`).
- **`wash`** 2.5.1 or newer. Built against wasmCloud `main` at commit
  `b8e535692`.
- **A wasmCloud host built with the `host-component-plugins` Cargo feature.**
  This is opt-in and is *not* in the released `ghcr.io/wasmcloud/wash` images or
  the `:canary` tag — both ship default features only. Build a host with
  `CARGO_FEATURES=host-component-plugins` (the source Dockerfile takes it as a
  build arg). A host without the feature rejects a plugin declaration at startup
  with a clear error rather than silently dropping it.
- **A Couchbase cluster with the Data API enabled**, plus a database access
  credential scoped to the bucket you intend to bind.

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
default — a plugin is operator-controlled and more privileged than a workload,
so it gets no ergonomic allow-all. The Couchbase endpoint has to be listed or
every call fails. The plugin reports that case as `not-configured` with a
message naming the policy, rather than as a credentials problem.

That matters for how you declare it: the `wash host --host-plugin` flag grammar
accepts only `id`, `image`/`file`, `pull`, `max-restarts` and `digest`, so it
**cannot express `allowedHosts`** and a plugin declared that way reaches nothing.
Use the config-file form instead. In `.wash/config.yaml`:

```yaml
host:
  hostPlugins:
    - id: couchbase
      file: ./host-plugins/component/couchbase/target/wasm32-wasip2/release/couchbase_plugin.wasm
      allowedHosts:
        - "*.data.cloud.couchbase.com"
```

`wash dev` takes the same entries under `dev.host_plugins` — note the snake_case
key, with camelCase fields inside it:

```yaml
dev:
  host_plugins:
    - id: couchbase
      file: ./host-plugins/component/couchbase/target/wasm32-wasip2/release/couchbase_plugin.wasm
      allowedHosts:
        - "*.data.cloud.couchbase.com"
```

For Kubernetes, the Helm chart takes the same shape per host group under
`runtime.hostGroups[].hostPlugins`.

## Configuring a workload

The plugin holds no cluster configuration of its own. Each workload supplies its
own through **interface-level config** on its `wasmcloud:couchbase` import, and
the plugin captures it in `on-workload-bind` — so a workload with a missing
endpoint or credential **fails to deploy**, with a message naming the key at
fault, instead of failing on its first query.

Two workloads sharing this plugin never see each other's configuration: every
call resolves its caller through `wasmcloud:host/identity` and looks up only
that workload's binding.

| Key | Required | Default | Meaning |
|---|---|---|---|
| `endpoint` | yes | — | Data API base URL: `https://host[:port]`, optionally with a mount path. A bare `host:port` is treated as HTTPS. |
| `bucket` | yes | — | The one bucket this binding may reach. Deliberately not addressable per call. |
| `username` | yes | — | Database access credential. |
| `password` | yes | — | Database access secret. Source it from `secretFrom`, not a literal. |
| `scope` | no | `_default` | Scope used when a call leaves `location.scope` unset. |
| `collection` | no | `_default` | Collection used when a call leaves `location.collection` unset. |
| `timeout-ms` | no | `30000` | Per-request time limit. Also bounds the transport, so a hung cluster cannot hold a caller open indefinitely. |

Because a capability call into a plugin has **no host-imposed timeout**, this
`timeout-ms` is what actually bounds a call. Leaving it at the default is fine;
setting it to something enormous is not.

## Using it from a workload

Copy [`interface/couchbase.wit`](interface/couchbase.wit) into your component's
`wit/deps/wasmcloud-couchbase/`, then import what you need:

```wit
world my-app {
    import wasmcloud:couchbase/document@0.1.0;
    import wasmcloud:couchbase/query@0.1.0;
    export wasi:http/handler@0.3.0;
}
```

Records generated from WIT carry no `Default`, so a small helper for "no options
set" keeps call sites readable:

```rust
use bindings::wasmcloud::couchbase::document;
use bindings::wasmcloud::couchbase::types::{Location, MutationOptions};

fn no_options() -> MutationOptions {
    MutationOptions { cas: None, expiry_seconds: None, durability: None, flags: None }
}

fn key(id: &str) -> Location {
    Location { scope: None, collection: None, key: id.to_string() }
}

// Write with a 10-minute TTL.
let written = document::upsert(
    key("user:42"),
    br#"{"name":"Ada"}"#.to_vec(),
    Some(MutationOptions { expiry_seconds: Some(600), ..no_options() }),
).await?;

// Conditional update: fails with `cas-mismatch` if anything wrote in between.
document::replace(
    key("user:42"),
    br#"{"name":"Ada Lovelace"}"#.to_vec(),
    Some(MutationOptions { cas: Some(written.cas), ..no_options() }),
).await?;

// A lookup reports absence as `none` rather than as an error.
if let Some(doc) = document::get(key("user:42"), Vec::new()).await? {
    let name = String::from_utf8_lossy(&doc.content);
}
```

Bind user input as query parameters rather than interpolating it into the
statement, so the cluster treats it as a value and never as SQL++ syntax:

```rust
use bindings::wasmcloud::couchbase::query::{self, QueryOptions};

let result = query::query(
    "SELECT name FROM users WHERE city = $1 LIMIT 10".to_string(),
    Some(QueryOptions {
        positional_parameters: vec![r#""Berlin""#.to_string()],
        named_parameters: Vec::new(),
        query_context: None,
        scan_consistency: None,
        timeout_ms: None,
        read_only: Some(true),
    }),
).await?;

for row in result.rows {
    // Each row is JSON text; decode it with whatever JSON library you already use.
}
```

These snippets are compiled against the generated bindings as part of developing
this plugin, so they reflect the real signatures rather than an idealized shape.

## How the interface maps onto the Data API

| WIT | Request |
|---|---|
| `document.get` | `GET …/documents/{key}`, `?project=` per field; `404` becomes `none` |
| `document.exists` | `GET …/documents/{key}`, body discarded |
| `document.insert` | `POST …/documents/{key}` |
| `document.upsert` | `PUT …/documents/{key}` |
| `document.replace` | `PUT …/documents/{key}` with `If-Match` — the caller's CAS, or `*` |
| `document.remove` | `DELETE …/documents/{key}` |
| `document.touch` | `POST …/documents/{key}/touch` |
| `binary.increment` / `decrement` | `POST …/documents/{key}/{op}` |
| `binary.append` / `prepend` | `POST …/documents/{key}/{op}` |
| `query.query` | `POST /_p/query/query/service` |

Options travel as headers: CAS as `If-Match`, TTL as `Expires` (a Go duration
string, so no calendar arithmetic is involved), durability as
`X-CB-DurabilityLevel`, and stored flags as `X-CB-Flags`. Responses report CAS
in `etag`.

Errors are classified from Couchbase's machine-readable `code` first and HTTP
status only as a fallback, because status alone is ambiguous — a `409` is both
"you inserted over an existing document" and "your CAS was stale", and callers
must handle those differently.

`document.get` returns `none` for an absent key, while operations that require
the document to exist (`replace`, `remove`, `touch`, `append`, `prepend`) return
the `not-found` error. That split is deliberate: absence is an ordinary outcome
of a lookup and an error for everything else.

## Design notes

- **Handle-free.** Every operation takes and returns plain values; there are no
  `resource` handles. Handles crossing a store boundary are relocated by the
  runtime rather than shared, so a handle-free interface stays on the fast path
  and avoids resource churn per call.
- **Buffered, not streamed.** The Query Service returns its whole result set as
  one JSON document, so a streaming signature would be streaming a payload that
  had already arrived in full. Bound result sets with `LIMIT` instead.
- **Cancellation is checked before a request is issued**, via
  `wasmcloud:host/cancel`. A teardown arriving while calls are queued returns
  those calls immediately rather than making each wait out the cluster; a
  request already in flight still runs to its own transport timeout.
- **No `wasi:cli/run`.** There is no background work between calls, and a pure
  capability plugin is allowed to omit it.

## What is and is not verified

Verified here:

- The component builds clean and its embedded WIT shows every capability
  function and both lifecycle hooks as `async func` — the plugin contract's hard
  requirement.
- Its import surface is `wasi:http/client@0.3.0`, `wasi:clocks/system-clock`,
  `wasmcloud:host/identity` and `wasmcloud:host/cancel`; its exports are the
  four `wasmcloud:couchbase` interfaces plus
  `wasmcloud:host/workload-lifecycle@0.1.1`.
- 24 unit tests cover config validation, endpoint parsing, error classification,
  CAS parsing, URL segment encoding, and the ISO 8601 conversion.
- A separate consumer component compiles against `interface/couchbase.wit`, so
  the interface is importable from a workload and the README's snippets match
  the generated signatures.
- A clean checkout builds: with `wit/deps/` and `target/` removed, `wash build`
  re-fetches dependencies and produces the component with no warnings.

**Not** verified — no live cluster was available:

- Every request the plugin builds is untested end-to-end against Couchbase.
- Two Data API details were taken from the published reference rather than
  observed, and are the most likely things to need adjusting: whether `PUT`
  creates a document that does not yet exist (which is what makes `upsert`
  upsert), and whether the Data API honours `If-Match: *` (which is what makes
  `replace` fail on an absent document rather than behaving like `upsert`). Pass
  an explicit CAS to `replace` if you need its precondition guaranteed today.
- The `project` query parameter is sent as a repeated `project=` pair; the
  reference documents the parameter but not its repetition syntax.

Anyone with a cluster to point this at: those are the first three things worth
checking, and the mapping table above is where to look.

## License

Apache-2.0. See [LICENSE](LICENSE). Vendored WIT and its provenance are
documented in [`wit-deps/README.md`](wit-deps/README.md).
