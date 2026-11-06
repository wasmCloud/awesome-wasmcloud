# gRPC Client Component

An HTTP front door for a gRPC service. `POST /<package>.<Service>/<Method>`
with an encoded protobuf message in the body becomes one gRPC call to the
configured endpoint.

gRPC is HTTP/2 with three conventions on top, so a component needs no transport
beyond the `wasi:http/client@0.3.0` it already has. The conventions are the
work, and [`src/grpc.rs`](src/grpc.rs) is the part worth copying: it knows
nothing about this component, or about protobuf.

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
     grpc-status:      the RPC's status code; 0 is success, `unknown` if the
                       server sent none
     grpc-message:     the failure reason, absent on success
     grpc-frames:      how many messages came back; >1 for server streaming
     grpc-http-status: the transport's status, 200 even for a failed RPC
400  the path is not a gRPC method path
500  GRPC_AUTHORITY is not set
502  the call never reached the service
```

The body is the first frame. A server-streaming call reports its count in
`grpc-frames`; use `grpc::split_frames` directly to read them all.

Encoding and decoding protobuf is the caller's job — with `prost`, or by hand
for a message this small. Nothing here depends on a protobuf crate.

## Configuration

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `GRPC_AUTHORITY` | yes | — | The endpoint, as `host:port` |
| `GRPC_PLAINTEXT` | no | unset | `1` for `http://`. Plaintext is opt-in so a missing variable cannot silently downgrade TLS |
| `GRPC_TIMEOUT_MS` | no | `30000` | Per-call deadline |

The endpoint also has to be in the workload's `allowedHosts`.

## Building

Tested with `wash` 2.9.0 and Rust 1.94, targeting `wasm32-wasip2`.

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
python3 -c '
def f(n, v):
    b = v.encode(); return bytes([(n << 3) | 2, len(b)]) + b
import sys
sys.stdout.buffer.write(f(1,"testbucket")+f(2,"_default")+f(3,"_default")+f(4,"my-doc"))
' > get.bin

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
