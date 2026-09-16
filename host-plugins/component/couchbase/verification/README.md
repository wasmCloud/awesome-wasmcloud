# Verification harness

What was used to run the plugin against a real Couchbase cluster in a real
wasmCloud host. The results are summarized in the project
[README](../README.md#run-live); this is how to reproduce them.

- `docker-compose.yml` — the whole environment: a real Couchbase Server, an init
  step that configures it, and the Data API server. One command, no manual setup.
- `dataapi/` — a **Data API server**, implementing the documented endpoints over
  the official Couchbase SDK's native KV operations. Since no Couchbase release
  ships the Data API, this is what makes end-to-end testing possible at all.
- `scenario/` — a workload component importing `wasmcloud:couchbase`. One
  `GET /` runs 32 steps and returns a line per step, so a single request
  exercises the whole capability across the store boundary.

The scenario drives **both** implementations — the Data API plugin here and the
[KV plugin](../../couchbase-kv/) — because they export the same interface, and
both score 32/32. Where the two transports genuinely differ it asserts on
coherence rather than on one fixed answer: `get-and-lock`/`unlock` and
`preserve-expiry` either report `unsupported`, or work *and* are checked for
having actually worked — the lock must refuse a wrong CAS and accept its own,
and a preserved TTL must still be there on read-back. A step that only checked
"returned ok" would pass on an implementation that silently dropped the option.

Point the KV plugin at the cluster directly (`couchbase://<lan-host>`, port
11210) rather than at the gateway; it does not need `dataapi/` at all.

Point the plugin at a real Capella endpoint instead and `dataapi/` is
unnecessary — that is the run that would close the [open
questions](../README.md#still-unverified).

## What the Data API server is, and what it proves

It performs **native KV operations** through the SDK rather than translating to
SQL++, so the semantics under test are the cluster's own:

- CAS is the real 64-bit value the cluster mints. A `remove` reports one, which
  a SQL++ translation cannot.
- Binary documents are stored as bytes with their real common flags, through a
  pass-through transcoder. Couchbase itself describes the scenario's binary
  document as `<binary (6 b)>` — it is not JSON wrapped in anything.
- Expiry, `touch`, `get-and-touch`, counters and append/prepend are the real KV
  operations, so a TTL is a real TTL.
- Errors are real SDK exceptions (`DocumentExistsException`,
  `CasMismatchException`, ...) mapped onto the documented codes, rather than
  inferred from a status.

It serves **only** the documented endpoint set. Inventing routes the real Data
API lacks — a lock/unlock, a replica read — would produce a harness that passes
against fiction, and would hide exactly the `unsupported` results the plugin is
supposed to return.

**The limit of it:** the store beneath is real Couchbase, so anything depending
on cluster behaviour is genuine. The mapping from HTTP onto those operations is
this server's reading of the published reference — the same reading the plugin
holds. It cannot confirm that reading is correct. Only a real Capella endpoint
can, which is why the [open questions](../README.md#still-unverified) stay
open.

## Why there is no pure-Docker option

The Data API is a Capella service. No Couchbase Server release ships it —
verified by probing every listening HTTP port on both **7.6.4** and **8.0.2**
(the newest published image):

```console
$ curl -u Administrator:... http://127.0.0.1:8091/v1/callerIdentity
Not found.
```

Every `/v1/buckets/.../documents/...` path 404s on 8091, 8092, 8093 and their
TLS counterparts. There is no `couchbase/data-api` image on Docker Hub either;
the only adjacent one is `couchbase/sync-gateway`, which serves the App
Services API, a different contract. Port 11280 on 8.0.x is the gRPC
(`couchbase2://`) endpoint, not REST.

Hence `dataapi/`. Everything underneath it is a real cluster.

## Running it

**1. The stack.**

```console
docker compose up -d
```

That starts Couchbase, waits for it to be healthy, configures the node, creates
`testbucket`, adds `appuser` / `apppass123`, builds the Data API server and
starts it on <http://127.0.0.1:9000>. It is re-runnable against an existing
volume and prints `READY:` when usable. `docker compose down -v` removes it.

**2. A host with the feature.** Component host plugins are opt-in and absent
from released `wash` builds:

```console
git clone https://github.com/wasmCloud/wasmCloud && cd wasmCloud
cargo build --bin wash --features host-component-plugins
```

**3. The scenario.** Build the plugin (`wash build` in the project root), copy
`../interface/couchbase.wit` into `scenario/wit/deps/wasmcloud-couchbase/`
along with the p3 WASI deps, and give `scenario/.wash/config.yaml` the plugin
and its interface config:

```yaml
version: 2.0.0
build:
  command: cargo build --target wasm32-wasip2 --release
  component_path: target/wasm32-wasip2/release/cbtest.wasm
dev:
  host_plugins:
    - id: couchbase
      file: ../../target/wasm32-wasip2/release/couchbase_plugin.wasm
      allowedHosts: ["127.0.0.1:9000"]
  host_interfaces:
    - namespace: wasmcloud
      package: couchbase
      interfaces: [types, sqlpp-types, document, sqlpp]
      version: "0.2.0"
      config:
        endpoint: http://127.0.0.1:9000
        bucket: testbucket
        username: appuser
        password: apppass123
```

### Address the gateway by LAN IP, not `127.0.0.1`

`127.0.0.1` inside a guest means the **virtual** network, so a plugin dialling
`127.0.0.1:9000` gets `ConnectionRefused` rather than the gateway published on
the machine. `host.wasmcloud.internal` is the name for the machine's loopback.

For the **KV plugin**, which uses raw `wasi:sockets`, that name is all it takes
— see [its README](../../couchbase-kv/README.md#reaching-the-machines-loopback);
`allowedHostLoopbackPorts` on the plugin entry is enough and `dataapi/` is not
needed at all.

The **Data API plugin** cannot use it: `wasi:http` does not resolve the
sentinel (`DnsError: address not available`) and does not read
`allowedHostLoopbackPorts`. Use the LAN address for this one.

Use the machine's LAN address instead. Docker publishes on `0.0.0.0`, so the
gateway answers there, and a LAN address is an ordinary external address that
plain egress already covers:

```console
LAN=$(ipconfig getifaddr en0)      # macOS; `hostname -I | awk '{print $1}'` on Linux
```

A LAN *hostname* works too, and is worth preferring on a laptop whose address
moves between networks — this one changed twice mid-session. The KV plugin
resolves it through `wasi:sockets/ip-name-lookup`, so the declaration also needs
`allowedIpNameLookups`:

```yaml
    - id: couchbase-kv
      allowedHosts: ["macbookpro.lan:11210", "macbookpro.lan:8093"]
      allowedIpNameLookups: ["*"]
```

```yaml
dev:
  plugins:
    - id: couchbase
      allowedHosts: ["<LAN>:9000"]
  host_interfaces:
    - namespace: wasmcloud
      package: couchbase
      config:
        endpoint: http://<LAN>:9000
```

Verified: 32/32 against wash 2.9.0 this way, where `127.0.0.1` scores 9/31.

Then, from `scenario/`:

```console
/path/to/wasmCloud/target/debug/wash dev
curl -s http://127.0.0.1:8000/
```

Every line should read `OK`.

## Seeing what went on the wire

The server serves `GET http://127.0.0.1:9000/__log`, reporting the method,
framing headers and options of every request the plugin made:

```console
curl -s http://127.0.0.1:9000/__log | python3 -m json.tool
```

That is how the missing `Content-Length` was found — every request was going out
`Transfer-Encoding: chunked`, including body-less `DELETE`s. Worth a glance
after any change to the request path.

## Checking the cluster directly

The scenario's claims are worth confirming against Couchbase rather than
against the plugin's own report:

```console
curl -s -u appuser:apppass123 http://127.0.0.1:8093/query/service \
  -H 'Content-Type: application/json' \
  -d '{"statement":"SELECT META(d).id AS id, META(d).expiration AS ttl, TOSTRING(META(d).cas) AS cas, d AS doc FROM testbucket._default._default AS d USE KEYS [\"scenario:ttl\"]"}'
```

`scenario:ttl` should carry an expiration 600 seconds after its write.
`TOSTRING` matters: a Couchbase CAS is around 1.7×10¹⁸ and would lose precision
as a JSON number.

## Two negative paths worth re-running

Both have specific, engineered messages, and both are easy to regress:

- Drop `bucket` from the interface config. The **deploy** must fail with
  `wasmcloud:couchbase missing required config key 'bucket'` — validation
  happens in `on-workload-bind`, not on first use.
- Set `allowedHosts` to something that does not cover the endpoint. Calls must
  fail as `not-configured` naming the policy, never as `unauthorized`, which
  would send someone off to rotate working credentials.
