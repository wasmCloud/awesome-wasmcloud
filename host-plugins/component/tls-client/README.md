# TLS from a host component plugin

A runnable example of `wasi:tls@0.3.0-draft` wrapping a
`wasi:sockets@0.3.0` TCP connection inside a host component plugin. A caller
workload invokes `example:tls/client.ping`; the plugin completes a verified TLS
handshake, sends `PING\r\n`, and returns the echo peer's reply.

**Status: experimental reference example.** The components build with `wash`
2.7.0 and Rust 1.96, targeting `wasm32-wasip2`. The verifier uses wasmCloud
source revision `86e98479cf8336f563e68f66d6ee6c0b8924b2c9` (2.12.2), Wasmtime
48.0.8, and the included [runtime compatibility patch](verification/runtime.patch).
The TLS WIT is a draft, pinned locally to the host implementation.

## How it works

```mermaid
flowchart LR
    workload[Calling workload] --> plugin[TLS host component plugin]
    plugin <-->|Plaintext streams| tls[Host wasi:tls]
    tls <-->|Encrypted streams| tcp[WASI TCP socket]
    tcp <-->|TLS| peer[Echo peer]
```

The important code is in [`src/plugin.rs`](src/plugin.rs):

1. Connect the TCP socket using the host's egress policy.
2. Forward its encrypted receive stream through a bounded bridge into
   `Connector.receive`.
3. Pass an application stream to `Connector.send` and send the resulting
   encrypted stream through the socket.
4. Call `Connector.connect` **after wiring both directions**, passing the
   original hostname for certificate verification and SNI.
5. Send application bytes only after the handshake succeeds.
6. At plaintext EOF, stop the ciphertext bridge and check both TLS and socket
   completion futures before reporting success.

The address is an IP literal and port, such as `192.0.2.10:7443` or
`[2001:db8::1]:7443`. The separate `server-name` is the certificate identity,
usually the DNS hostname that resolved to that IP. Resolving a hostname does
not change the identity the certificate must prove.

This one-shot example has a five-second overall deadline and a 1024-byte reply
limit. Closing its application output emits TLS `close_notify`; the echo peer
also closes its TLS output. Stream EOF alone does not prove a clean exchange:
completion futures carry failures such as truncated TLS records.
The ciphertext bridge stops at TLS EOF so a peer that sends `close_notify`
without TCP FIN does not hold the call open. Ending that bridge does not bypass
TLS validation: both TLS completion futures must still succeed, including the
check for a missing `close_notify`.

## Host requirements and certificate trust

The host needs both `host-component-plugins` and `wasi-tls` enabled.
On the tested source revision, two runtime changes are also required:

- Classify the linked TLS draft as a host builtin, so the component-plugin
  loader does not search for another plugin to provide it.
- Forward the engine's configured TLS provider into component-plugin stores,
  so operator-supplied certificate authorities are actually used there.

The patch changes those two behaviors. It does not disable certificate or
hostname verification. An unpatched host fails these tests; enabling a Cargo
feature alone is insufficient on that revision.

The verifier's native host installs a `SharedTlsProvider` backed by rustls and
trusts a freshly generated test certificate. See the complete provider in
[`verification/tls.rs`](verification/tls.rs) and its use in
[`verification/harness.rs`](verification/harness.rs). Real deployments should
populate their host-controlled trust store with the appropriate CAs.
`http_client_ca_paths` configures the HTTP client's trust separately; it does
not configure this raw socket's TLS provider. Mutual TLS is outside this example.

TLS retains the socket egress restrictions. The verifier enables host loopback
and grants the plugin only its test peer's port; the caller has no network grant.
It also tests denial when that plugin grant is absent.

## Build and verify

Requires Rust with `wasm32-wasip2`, `wash`, Git, Node.js 22+, and a C toolchain
for the native host's TLS dependencies. Run from this directory:

```sh
# Stop at the first failed command without changing the calling shell's options.
(
set -eu
wash build
(cd verification/caller && wash build)
cargo test --locked

# Use a fresh checkout so the compatibility patch has a known base.
TLS_RUNTIME_DIR="$(mktemp -d)/wasmCloud"
git clone https://github.com/wasmCloud/wasmCloud.git "$TLS_RUNTIME_DIR"
git -C "$TLS_RUNTIME_DIR" checkout 86e98479cf8336f563e68f66d6ee6c0b8924b2c9
git -C "$TLS_RUNTIME_DIR" apply --check "$PWD/verification/runtime.patch"
git -C "$TLS_RUNTIME_DIR" apply "$PWD/verification/runtime.patch"
node verification/run.mjs --runtime-source "$TLS_RUNTIME_DIR" --skip-build
)
```

`verification/run.mjs` builds both components unless `--skip-build` is supplied.
It creates a temporary native harness, starts real component-plugin and caller
stores, and uses local TLS peers on dynamically allocated ports. No Docker,
database, public endpoint, or real credentials are required.
`--target-dir /path/to/cache` reuses native build artifacts; `--offline` uses
already cached dependencies. The verifier and its lockfile are pinned to
Wasmtime 48.0.8; validate newer runtime revisions separately.

The ten native integration checks cover:

- A verified round trip through the actual host component plugin.
- A verified reply and TLS `close_notify` while the peer keeps TCP open past
  the call deadline; the call must succeed without waiting for TCP FIN.
- Untrusted, expired, and wrong-host certificates; no application bytes reach
  the peer after a rejected handshake.
- Egress denied before TLS starts.
- A stalled handshake hitting the overall deadline.
- An oversized reply hitting the response bound.
- A complete plaintext reply followed by an unclean TLS close, which must still
  fail through the TLS completion future.
- Recognition of the exact TLS draft version linked by the host.

The caller in [`verification/caller`](verification/caller/) exposes an HTTP
handler that invokes the TLS capability. The harness sends it requests such as
`/?address=<test-peer-ip>:<port>&name=localhost` and checks the returned body
and failure reason, rather than treating any TLS error as a successful negative
test. In particular, a wrong-host test must report a hostname mismatch rather
than an unrelated CA failure.

## Applying the pattern to Couchbase

Reuse the stream wiring and verified handshake in the
[Couchbase sockets plugin](../couchbase-wasi-sockets-p3/src/conn.rs), then run
its existing KV framing and authentication over the plaintext TLS streams.
A persistent database connection keeps its streams and completion futures
alive and reads protocol frames per operation. The echo example closes each
connection after one exchange.

For `couchbases://`, Couchbase's default encrypted ports are `11207` for KV
and `18093` for SQL++. SQL++ uses HTTPS through `wasi:http` and its own trust
configuration. See [Couchbase's port reference](https://docs.couchbase.com/server/current/install/install-ports.html).
Include transport mode and TLS identity in a connection cache key, and keep
the entire connection/handshake inside the operation's deadline.

This example demonstrates the TLS transport. The existing Couchbase sockets
plugin still explicitly refuses `couchbases://`; its routing, durability, and
TLS feature work are separate from this tutorial.
