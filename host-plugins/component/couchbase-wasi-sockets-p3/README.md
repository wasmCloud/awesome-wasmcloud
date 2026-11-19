# Couchbase KV Component Host Plugin (wasi:sockets)

The recommended implementation of `wasmcloud:couchbase@0.2.0`, speaking Couchbase's KV
binary protocol directly over `wasi:sockets@0.3.0`, with optional verified TLS
through `wasi:tls@0.3.0-draft`. **No SDK and no gateway** — the protocol is
implemented in `proto.rs` and `conn.rs`, with the bindings glue in `plugin.rs`.

This is the one to reach for when the Data API is not available: it needs
nothing in front of the cluster, and talks to a stock Couchbase Server.

The interface is not copied. [`.wash/config.yaml`](.wash/config.yaml) resolves
`wasmcloud:couchbase` from `../couchbase/interface`, so this plugin and the
Data API plugin cannot drift.

SQL++ HTTP replies are read incrementally with backpressure and a 32 MiB body limit.
The reader also checks transport completion before returning a query result.
KV replies retain their separate 32 MiB framing limit.

**Status: working.** 40/40 on the
[shared verification scenario](../couchbase/verification/), the same score as
the Data API plugin, against a real cluster. Also run as a host plugin on a wasmCloud
2.12.0 cluster in Kubernetes.

**Requires wasmCloud 2.12.0 or newer**, built with the `host-component-plugins`
feature, such as `ghcr.io/wasmcloud/wash:2.12.0-all-features`. This plugin
imports `wasmcloud:host@0.1.6` to read plugin-backed secrets, which older hosts
do not provide.

## Why it exists

| | this plugin | [`couchbase`](../couchbase/) |
|---|---|---|
| Transport | KV protocol, `wasi:sockets@0.3.0` | Data API, `wasi:http@0.3.0` |
| Needs a gateway | no | **yes** (CNG, or Capella) |
| Concurrent calls | interleave | interleave |
| Dependencies | `serde_json` | `serde_json` |

The Data API is served by the Cloud Native Gateway. Where that is not deployed,
this plugin is the answer.

An earlier plugin embedded the official Couchbase Rust SDK instead. It was
removed: the SDK runs on Tokio under `block_on`, so its calls serialized, and its
TLS stack pulls in `aws-lc-sys`, which needs a C toolchain to build for wasm.

## It awaits rather than blocks

Every socket operation is an `await` on `wasi:sockets@0.3.0`, driven by the same
executor that drives the plugin's exported functions — so a call waiting on the
cluster yields to the plugin's other callers.

Measured on one cluster, eight concurrent ~600ms queries:

| | wall |
|---|---|
| no plugin at all (control) | 4,904 ms |
| this plugin | **4,933 ms** |
| `couchbase` (Data API) | 4,942 ms |
| the removed SDK plugin (`block_on`) | 10,221 ms |

Being within 29 ms of the control is the point: the plugin is transparent under
concurrency, where an implementation that blocks costs 2×.

Note that the p3 socket imports come from `wit-bindgen`, not from the target —
this builds for `wasm32-wasip2` and still imports `wasi:sockets/types@0.3.0`.
The `wasm32-wasip3` target is neither necessary nor sufficient for p3 I/O.

## The protocol, and two rules that are easy to get wrong

The handshake is `HELLO` → `SASL_AUTH` → `SELECT_BUCKET` → cluster config →
collection id, then document opcodes. [`src/proto.rs`](src/proto.rs) is
bindings-free and unit-tested; [`src/conn.rs`](src/conn.rs) moves the bytes.

**Hash the bare key, send the prefixed key.** With `COLLECTIONS` negotiated, a
request's key is the collection id as leb128 followed by the document id — but
the vbucket comes from the document id *alone*. Hashing the prefixed form sends
the request to the wrong vbucket.

**A wrong vbucket does not always announce itself.** On a single node the server
owns every vbucket, so a misrouted read is answered from the wrong one and comes
back `not-found` rather than `not-my-vbucket`. A routing bug therefore looks
exactly like a missing document. `vbucket_for("bench-doc", 1024) == 868` is a
unit test for precisely that, checked against what a live cluster serves.

## What it does not do

- **One node.** The cluster map is read for its vbucket count, not its topology:
  there is no per-node connection and no rebalance following. A `not-my-vbucket`
  is reported rather than retried elsewhere. Use a single-node cluster; a generic
  TCP load balancer does not supply vbucket routing.
- **No replica reads**, for the same reason.
- **`preserve-expiry` is refused**, not silently dropped.
- **Projections support dot-separated object fields.** Array indexes and escaped
  field names return `unsupported`; they are never silently omitted.
- **Durable KV writes are refused.** `durability-level` values other than
  `unknown` and `none`, and non-zero `persist-to`/`replicate-to`, return
  `unsupported`. Use the Data API plugin when a write needs durability.
- **SASL PLAIN only.** Use `couchbases://` with the TLS build to encrypt the
  credentials and verify the cluster's identity.

Each of these returns `unsupported` with a message naming the reason, rather
than a wrong answer.

TLS does not add multi-node routing, replica reads or cluster discovery.
Capella deployments require those features and remain unsupported.

Expiry reads compare the document and metadata CAS values and retry both reads
when they disagree. The call deadline also bounds a continuously changing
document; metadata failures are surfaced rather than reported as no expiry.
Keys are checked in bytes before encoding (250 bytes in the default collection,
246 in named collections). Malformed reply lengths, unexpected opcodes, and
responses over 32 MiB fail before an unbounded allocation or slice. Couchbase
[document values are limited to 20 MiB](https://docs.couchbase.com/server/current/learn/data/data.html).
The [protocol suite](../../../tests/README.md) exercises these failure paths
without a database account.

## Configuration

| Key | Required | Default | Meaning |
|---|---|---|---|
| `endpoint` | yes | — | `couchbase://host[:port]` (default `11210`), or `couchbases://host[:port]` (default `11207`) with the TLS build |
| `bucket` | yes | — | The one bucket this binding may reach |
| `username` / `password` | yes | — | Cluster credential. Source them from `secretFrom` |
| `scope` / `collection` | no | `_default` | Keyspace this binding operates in |
| `query-port` | no | `8093` / `18093` | SQL++ port, HTTP for `couchbase://` and HTTPS for `couchbases://` |
| `timeout-ms` | no | `30000` | Time limit on a call, document or SQL++, overridden by the call's own `timeout-ns`. Also bounds the connection attempt at bind |

SQL++ is an HTTP service, so it goes out over `wasi:http` to `query-port` rather
than over the KV socket. `allowedHosts` needs both ports. A host named there may
also be resolved, so `allowedIpNameLookups` is only for names it does not list.

### Who sets it

On a `host.plugins` entry, or a host group's `plugins` in the chart, the operator
owns the binding and the workload's manifest names none of it:

```yaml
- id: couchbase
  image: registry.example.com/couchbase-wasi-sockets-p3:0.1.0
  config:
    endpoint: couchbase://couchbase.example.com
    bucket: orders
  secretFrom: [couchbase-credentials]   # a Secret with `username` and `password`
  hostOwnedKeys: [endpoint, bucket, username, password, scope, collection, query-port]
  allowedHosts:
    - couchbase.example.com:11210
    - couchbase.example.com:8093
```

**List every key in `hostOwnedKeys`, including the ones `config` sets.** This
plugin claims no keys in code, so on 2.12.0 a key that is only in `config` can be
overridden from the workload's manifest, which would point the operator's
credential at a bucket of the workload's choosing.

A plugin-backed secret (`pluginSecrets`, served by another host plugin) cannot
fill the entry itself, only its named `bindings`. The workload then imports the
capability by `(implements ..)` label, and each label is a binding with its own
cluster and keyspace. When a bind carrying such a secret is refused, the host
withholds the reason from the workload's status. The plugin leaves error
reporting to the host and never copies the rejection text to stderr. Configuration
validation identifies the failing key without quoting the supplied value.

Idle KV connections are shared only while a bound workload owns the same
credentials and keyspace. Unbinding the last owner releases its cached connection;
an in-flight call finishing afterward cannot put it back. Connections opened
during bind validation are published only after every binding succeeds, and a
cancelled bind cannot publish state after unbind or a newer bind.


## Building

Targets `wasm32-wasip2`; CI uses stable Rust.

```console
wash build
```

Host component plugins are opt-in: the host must be built with the
`host-component-plugins` feature. The released `-all-features` images carry it.

### TLS

The TLS API is an experimental WASI draft.
The default build supports plaintext endpoints on older hosts.
Build the TLS variant with:

```sh
wash wit fetch
cargo build --locked --target wasm32-wasip2 --release --features tls --target-dir target/tls
```

This produces `target/tls/wasm32-wasip2/release/couchbase_wasi_sockets_p3_plugin.wasm`,
with the additional TLS imports.
It requires a host with both `host-component-plugins` and `wasi-tls`, including
the fixes merged in [wasmCloud PR #6014](https://github.com/wasmCloud/wasmCloud/pull/6014).
Use a release containing those fixes once available, or the upstream source
revision documented in the [TLS example](../tls-client/README.md#build-and-verify).
Host and secrets WIT are fetched normally by wash.
Only the TLS draft is [vendored at the repository root](../../../wit-deps/README.md).

Set `endpoint: couchbases://db.example.com` on the Couchbase binding.
The KV port defaults to `11207`, and SQL++ uses HTTPS on `18093`.
Grant the plugin both ports through `allowedHosts`.
The TLS handshake verifies the original endpoint hostname before any KV
authentication or application data is sent.
There is no plaintext fallback or certificate-verification bypass.
A build without the `tls` feature rejects `couchbases://` with rebuild instructions.

The host controls trust: raw KV TLS uses its `SharedTlsProvider`; SQL++ uses
the HTTP client's trust configuration, including `http_client_ca_paths` for
private CAs. Configuring that HTTP setting alone does not change raw TLS trust.
The [native verifier](../tls-client/verification/tls.rs) shows a provider with a
custom root certificate, and the
[Couchbase harness](verification/tls.rs) configures both trust paths.

Persistent connections retain their TLS streams and completion futures.
A bounded ciphertext bridge runs while a handshake or KV operation is active;
operation deadlines cover DNS, TCP, TLS and the protocol exchange.
The connection cache includes transport mode and the original certificate identity.

To run the shared forty-step scenario against real Couchbase KV TLS and SQL++
HTTPS, use Node.js 22+, Docker Compose and OpenSSL:

```sh
node ../tls-client/verification/run.mjs --runtime-source /path/to/wasmCloud --couchbase
```

The verifier builds the TLS plugin and caller, creates an isolated single-node
Couchbase fixture with fresh certificates and loopback-only dynamic ports,
and removes its containers and certificates afterward.
The native host installs the test CA separately for KV TLS and SQL++ HTTPS.
The local fixture routes the HTTP authority to `localhost`, another verified
identity on its test certificate; raw sockets use `host.wasmcloud.internal`.
It also checks that a mismatched KV certificate identity and denied egress
reject a workload during binding.

## License

Apache-2.0. See [LICENSE](LICENSE).
