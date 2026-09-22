# Couchbase KV Component Host Plugin (wasi:sockets)

A third implementation of `wasmcloud:couchbase@0.2.0`, speaking Couchbase's KV
binary protocol directly over `wasi:sockets@0.3.0`. **No SDK and no gateway** —
the protocol is implemented here: ~340 lines of wire format in `proto.rs` and
~215 of connection handling in `conn.rs`, plus the bindings glue.

This is the one to reach for when the Data API is not available: it needs
nothing in front of the cluster, and talks to a stock Couchbase Server.

The interface is not copied. [`.wash/config.yaml`](.wash/config.yaml) resolves
`wasmcloud:couchbase` from `../couchbase/interface`, so the three
implementations cannot drift.

**Status: working.** 34/34 on the
[shared verification scenario](../couchbase/verification/), the same score as
the other two, against a real cluster.

## Why it exists

| | this plugin | [`couchbase`](../couchbase/) | [`couchbase-kv-sdk`](../couchbase-kv-sdk/) |
|---|---|---|---|
| Transport | KV protocol, `wasi:sockets@0.3.0` | Data API, `wasi:http@0.3.0` | KV protocol, Couchbase Rust SDK |
| Needs a gateway | no | **yes** (CNG, or Capella) | no |
| Concurrent calls | interleave | interleave | **serialize** |
| Dependencies | `serde_json` | `serde_json` | the SDK, tokio, rustls |

The Data API is served by the Cloud Native Gateway. Where that is not deployed,
this plugin is the answer, and unlike the SDK-based sibling it neither blocks
the plugin's store nor carries a vendored patch to compile.

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
| `couchbase-kv-sdk` (SDK, `block_on`) | 10,221 ms |

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

- **No TLS**, so no `couchbases://` and no Capella. `couchbases://` is refused
  rather than quietly downgraded. Adding it means `wasi:tls@0.3.0-draft`, whose
  WIT is not in a registry and would need vendoring.
- **One node.** The cluster map is read for its vbucket count, not its topology:
  there is no per-node connection and no rebalance following. A `not-my-vbucket`
  is reported rather than retried elsewhere. Fine for a single node or behind a
  balancer; not yet a multi-node client.
- **No replica reads**, for the same reason.
- **`preserve-expiry` is refused**, not silently dropped.
- **SASL PLAIN only.** The server also offers SCRAM-SHA1/256/512. PLAIN over an
  untrusted network sends the password in the clear, which is the other reason
  TLS matters here.

Each of these returns `unsupported` with a message naming the reason, rather
than a wrong answer.

## Configuration

| Key | Required | Default | Meaning |
|---|---|---|---|
| `endpoint` | yes | — | `couchbase://host[:port]`, port defaulting to `11210` |
| `bucket` | yes | — | The one bucket this binding may reach |
| `username` / `password` | yes | — | Cluster credential. Source the password from `secretFrom` |
| `scope` / `collection` | no | `_default` | Keyspace this binding operates in |
| `query-port` | no | `8093` | Query service port, for SQL++ |
| `timeout-ms` | no | `30000` | Server-side limit for a SQL++ query |

SQL++ is an HTTP service, so it goes out over `wasi:http` to `query-port` rather
than over the KV socket. `allowedHosts` needs both ports, and
`allowedIpNameLookups` needs to cover the host unless `endpoint` is an address.

## Building

Tested with `wash` 2.9.0 and Rust 1.94, targeting `wasm32-wasip2`.

```console
wash build
```

Host component plugins are opt-in: the host must be built with the
`host-component-plugins` feature, which released builds do not carry.

## License

Apache-2.0. See [LICENSE](LICENSE).
