# gRPC Client Component

An HTTP front door for a gRPC service. `POST /<package>.<Service>/<Method>`
with an encoded protobuf message in the body becomes one gRPC call to the
configured endpoint.

gRPC is HTTP/2 with three conventions on top, so a component needs no transport
beyond the `wasi:http/client@0.3.0` it already has. The conventions are the
work, and [`src/grpc.rs`](src/grpc.rs), [`src/protocol.rs`](src/protocol.rs), and
[`src/deadline.rs`](src/deadline.rs) form the reusable transport. They know
nothing about this HTTP frontend, or about protobuf.
They reuse the bounded HTTP stream reader in
[`transport.rs`](../../host-plugins/component/couchbase/src/transport.rs).

## The part that is easy to get wrong

**A failed RPC is still `HTTP 200`.** The outcome is `grpc-status`, not the
HTTP status.

**`grpc-status` arrives in two different places.** A call that returns a
message puts it in real trailers, after the body. A call that returns none —
most errors — is sent as a *trailers-only* response, which puts it in the
HEADERS frame. A client reading only trailers sees nothing on exactly the
responses it most needs to understand; one reading only headers misses every
success. `unary` reads headers first, then trailers.

Both paths are exercised in [Verifying it](#verifying-it) below.

## Interface

```
POST /<package>.<Service>/<Method>
     body: the request message, unframed protobuf
     Authorization: forwarded to the service if present

200  body: the response message, unframed protobuf
     grpc-status:      the validated RPC status code; 0 is success
     grpc-message:     the failure reason, absent on success
     grpc-frames:      how many messages came back; >1 for server streaming
     grpc-http-status: the transport's status, 200 even for a failed RPC
400  the path is not a gRPC method path
405  method is not POST; Allow: POST
408  incoming upload or its completion exceeded the overall deadline
413  incoming request exceeded 4 MiB
500  GRPC_AUTHORITY is not set
502  transport/deadline failure, malformed gRPC response, or unsupported compression
```

The body is the first frame. A server-streaming call reports its count in
`grpc-frames`; use `protocol::split_frames` directly to read them all.

Bodies are read in chunks with backpressure.
The gRPC decoder handles partial headers and messages as bytes arrive,
with a 4 MiB limit per message, 32 MiB per response body, and 1024 response frames.
It validates declared lengths before retaining payload bytes.
The frontend reads at most 4 MiB of input before framing it;
fixed-length and chunked uploads use the same limit.
Its deadline starts before reading the upload and includes body completion.
The outgoing RPC receives only the remaining time.
No response is returned as successful until its body completion and gRPC status are validated.

Encoding and decoding protobuf is the caller's job — with `prost`, or by hand
for a message this small. Nothing here depends on a protobuf crate.

## Configuration

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `GRPC_AUTHORITY` | yes | — | The endpoint, as `host:port` |
| `GRPC_PLAINTEXT` | no | unset | `1` for `http://`. Plaintext is opt-in so a missing variable cannot silently downgrade TLS |
| `GRPC_TIMEOUT_MS` | no | `30000` | Positive integer; deadline for the incoming upload and complete RPC response. Remaining time is sent as `grpc-timeout` |

The endpoint also has to be in the workload's `allowedHosts`.

Only POST invokes the backend. Incomplete incoming bodies are rejected before
dispatch. Response framing, HTTP status, content type, and the final gRPC status
are validated; malformed responses cannot become successful empty messages.
The transport is uncompressed and buffers response messages. Client and
bidirectional streaming are not implemented.

`cargo test --locked` runs the protocol and deadline tests without generating
WIT bindings on the native host. The repository's
[protocol verification suite](../../tests/README.md) also covers plaintext and
TLS, service errors, malformed peers, deadlines, method rejection, authorization
forwarding and certificate validation.

## Building

Verified with the `wash:2.12.1-all-features` host, targeting `wasm32-wasip2`.

```console
wash build
```

## Verifying it

Any gRPC service will do. This uses Couchbase's Cloud Native Gateway, because
the [`couchbase` host plugin](../../host-plugins/component/couchbase/)'s
harness already runs one — `docker compose up -d` in its
[`verification/`](../../host-plugins/component/couchbase/verification/)
directory brings up a cluster with Protostellar gRPC on `18098`.

`.wash/config.yaml`:

```yaml
version: 2.0.0
workload:
  allowedHosts: ["<LAN>:18098"]
  environment:
    config:
      GRPC_AUTHORITY: <LAN>:18098
build:
  command: cargo build --target wasm32-wasip2 --release
  component_path: target/wasm32-wasip2/release/grpc_client.wasm
dev:
  # The harness's gateway uses a CA it generates locally.
  http_client_ca_paths:
    - /path/to/verification/tls/ca.crt
```

`<LAN>` is the machine's LAN address: a guest's `127.0.0.1` is its own virtual
network, not the machine.

Build a `couchbase.kv.v1.GetRequest` — four length-delimited string fields — and
call it:

```console
node --input-type=module > get.bin <<'JS'
// These example fields all fit in a one-byte protobuf length.
const field = (number, value) => {
  const bytes = Buffer.from(value);
  return Buffer.concat([Buffer.from([(number << 3) | 2, bytes.length]), bytes]);
};
process.stdout.write(Buffer.concat([
  field(1, 'testbucket'), field(2, '_default'),
  field(3, '_default'), field(4, 'my-doc'),
]));
JS

wash dev
curl -sD - --data-binary @get.bin -u user:pass \
  http://127.0.0.1:8000/couchbase.kv.v1.KvService/Get
```

A document that exists returns its content with `grpc-status: 0` and
`grpc-frames: 1`. One that does not returns `grpc-status: 5` with a
`grpc-message` and no body — the trailers-only path.

## Credit

The approach — driving Couchbase's protostellar gRPC straight from a component
over `wasi:http` p3, with no SDK — is Laurent Doguin's, from
[`ldoguin/wasmcloud-couchbase-cng-conduit`](https://github.com/ldoguin/wasmcloud-couchbase-cng-conduit).
That project is worth reading: it composes four components into a full
RealWorld backend, generates the entire protostellar surface with `prost`, and
scopes each component's Couchbase access with its own secrets and network
grant.

This is an independent implementation rather than a port, because that
repository carries no license and so cannot be copied from. The one behavioural
difference worth naming: it resolves `grpc-status` from trailers only, which is
why it treats an unreadable trailer as "not found".

## License

Apache-2.0. See [LICENSE](LICENSE).
