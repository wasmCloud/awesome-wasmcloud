# Component verification

Run native tests for the bindings-free logic:

```sh
cargo test --locked --manifest-path components/grpc-client/Cargo.toml
cargo test --locked --manifest-path host-plugins/component/couchbase/Cargo.toml
cargo test --locked --manifest-path host-plugins/component/couchbase-wasi-sockets-p3/Cargo.toml
```

The sockets tests also verify shared-connection ownership, teardown during an
in-flight call or bind, credential rotation, failed-bind cleanup, and diagnostics
that identify invalid configuration keys without exposing their values.

Run the live protocol checks from the repository root:

```sh
python3 tests/verify_components.py
```

Requires Python 3, OpenSSL, Docker, Rust with `wasm32-wasip2`, and `wash`.
`--skip-build` uses existing artifacts. `--host-image` selects an alternate
all-features host image; the default is `ghcr.io/wasmcloud/wash:2.12.1-all-features`.
The synthetic peer uses `node:22-alpine`. No cloud account or Couchbase credentials
are needed. Containers receive temporary copies of the compiled components,
public WIT, test certificates, and fixtures. The checkout is never mounted.
Each run uses its own network and removes its containers afterward.

The suite covers:

| Area | Cases |
|---|---|
| gRPC over plaintext and TLS | Unary, empty and multiple response messages; trailers-only service errors; authorization forwarding and `grpc-timeout` |
| gRPC framing | Truncated headers/messages, trailing garbage, unsupported compression, invalid flags and overflowing frame lengths |
| gRPC protocol | Missing, invalid and contradictory status; HTTP failures; wrong content type |
| Frontend | POST-only dispatch, `Allow: POST`, invalid paths/configuration, incomplete incoming bodies never forwarded |
| Deadlines | A peer trickling bytes cannot extend the whole-call deadline |
| Egress and TLS | Denied host, untrusted CA and wrong certificate hostname fail before an RPC is dispatched |
| Data API | Complete, truncated, stalled and trickling document bodies |
| KV metadata | A concurrent replacement retries to a matching CAS; continuous changes time out; metadata denial and malformed metadata return errors; absent TTL remains absent |
| KV framing and keys | Invalid lengths, oversized replies, wrong opcode/opaque, empty and oversized document IDs are refused without trapping or sending a write under a different ID |
| Query contexts | Bucket names containing dots retain their identifier boundaries on both transports |
| Query Service deadlines | Error `1080` becomes `timeout` on both transports, whether carried by HTTP 200 or HTTP 500 |
| Expiry range | Insert, upsert, replace, touch and get-and-touch reject out-of-range TTLs before dispatch |
| Gateway path limits | Dot-only IDs are explicitly refused for reads, writes and locking before any Data API request |

The real Couchbase Server / Cloud Native Gateway scenario remains in
[`host-plugins/component/couchbase/verification`](../host-plugins/component/couchbase/verification/).
It checks the actual service contract, including CRUD, binary flags, CAS,
locking, expiry, SQL++ parameters, consistency tokens and durability. Run both
suites when changing a transport.

The separate [TLS host component plugin example](../host-plugins/component/tls-client/)
has its own native verifier. It runs a caller through a real plugin store with
trusted and rejected certificates, denied egress, handshake deadlines and reply
bounds. Its README pins the runtime revision and documents the compatibility patch.

## Support boundaries

These are capability examples, not complete Couchbase or gRPC SDKs.

| Capability | Data API plugin | Sockets plugin |
|---|---|---|
| CRUD, binary values, CAS | Supported | Supported |
| Document IDs `.` and `..` | Explicitly refused; gateway normalizes these paths | Supported |
| Relative TTL and expiry reads | Supported | Supported; document and metadata CAS must agree |
| SQL++, positional parameters, read-only and consistency | Supported | Supported |
| SQL++ preserve-expiry | Forwarded | Forwarded |
| KV preserve-expiry | Explicitly refused | Explicitly refused |
| Modern durability | Forwarded to gateway | Explicitly refused |
| Legacy persist-to / replicate-to | Explicitly refused | Explicitly refused |
| Lock / unlock | Gateway alpha endpoints required | Supported |
| Replica reads | Explicitly refused | Explicitly refused |
| TLS | Supported | Explicitly refused |
| Multi-node topology / rebalance | Gateway manages routing | Not implemented; misrouted operations return an error |
| Projection | Delegated to gateway | Dot-separated object fields; arrays and escaped fields are explicitly refused |
| Plugin-backed secret handles | Not imported; host refuses such a binding | Supported |

Retry strategies, tracing spans, and remaining query tuning fields are not
implemented. The Data API forwards query metrics and client context IDs;
the sockets transport does not. Query results contain rows, not metrics or
profiling data. Database RBAC still determines which SQL++ keyspaces a credential
may access; `query_context` supplies a default keyspace rather than an access rule.
Query context identifiers are quoted so a dot in a bucket name remains part of
that name. Both transports round TTLs up to whole seconds and refuse expiries
beyond Couchbase's unsigned 32-bit Unix timestamp range, which ends in 2106.

The gRPC transport handles uncompressed response messages and buffers a complete
response. Compression and malformed protocol messages are rejected. Its Rust
transport returns all server-streamed frames; the HTTP frontend returns the first
message and reports the total in `grpc-frames`. Client and bidirectional streaming
are not implemented. TLS validates the certificate and hostname against the host's
trust configuration. The deadline covers the complete outgoing RPC, and is also
sent to the server through `grpc-timeout`.
