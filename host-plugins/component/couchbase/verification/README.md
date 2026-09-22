# Verification harness

Runs the plugin against a real Couchbase cluster in a real wasmCloud host. The
results are summarized in the project [README](../README.md#run-live).

- `demo.sh` — the whole thing end to end, for showing someone. Brings the
  stack up, builds both plugins, and runs the *same* workload against each in
  turn, then prints what was identical and where the two transports genuinely
  differ. `--keep` leaves the stack running; `KV_TRANSPORT=loopback` points
  the KV plugin at `host.wasmcloud.internal` instead of the LAN address.
- `docker-compose.yml` — the whole environment: a real Couchbase Server, an init
  step that configures it, and the **Cloud Native Gateway** in front of it,
  serving the Data API. One command, no manual setup.
- `scenario/` — a workload component importing `wasmcloud:couchbase`. One
  `GET /` runs 34 steps and returns a line per step, so a single request
  exercises the whole capability across the store boundary.

The scenario drives **both** implementations — the Data API plugin here and the
[KV plugin](../../couchbase-kv/) — because they export the same interface, and
both score 34/34. Where the two transports genuinely differ it asserts on
coherence rather than on one fixed answer: `get-and-lock`/`unlock` and
`preserve-expiry` either report `unsupported`, or work *and* are checked for
having actually worked — the lock must refuse a wrong CAS and accept its own,
and a preserved TTL must still be there on read-back. A step that only checked
"returned ok" would pass on an implementation that silently dropped the option.

The KV plugin talks to the cluster directly (`couchbase://<lan-host>`, ports
11210 and 8093) and does not go through the gateway at all.

## The Data API here is the real one

The Data API is served by the
[Cloud Native Gateway](https://docs.couchbase.com/cloud-native-gateway/current/intro/about-cng.html)
(CNG, [`couchbase/stellar-gateway`](https://github.com/couchbase/stellar-gateway)),
which fronts Capella and runs self-hosted in front of any cluster. It also
serves Protostellar gRPC. Couchbase Server does not serve the Data API itself.

The public `couchbase/cloud-native-gateway` image runs standalone, so this stack
needs neither Kubernetes nor the operator.

So this tests the actual implementation, not a reading of its documentation.
The formats are the ones live Capella sends: a bare 16-digit hex ETag
(`18d67903e6ef0000`), and a `bucket:vbid:vbuuid:seqno` mutation token
(`testbucket:663:6268d18e82e2:4`).

Locking lives under `/v1.alpha` and needs CNG's `--alpha-endpoints`, which
this stack passes. A read reports a document's absolute expiry in an `Expires`
header, checked by step 31. CNG writes that header's zone as `UTC` where HTTP-date requires `GMT`,
so a strict HTTP-date parser rejects every value it sends; the plugin's accepts
both.

## Running it

`./demo.sh` does all of the below. By hand:

**1. The stack.** The Data API is HTTPS only, under a CA the stack generates
into `tls/` (gitignored, per machine). The leaf certificate must name the
address the plugin will dial, so pass it in:

```console
LAN=$(ipconfig getifaddr en0)      # macOS; `hostname -I | awk '{print $1}'` on Linux
CNG_SAN="IP:$LAN" docker compose up -d
```

That starts Couchbase, configures the node, creates `testbucket`, adds
`appuser` / `apppass123`, issues the certificates, and starts CNG with the Data
API on `https://$LAN:18008` and Protostellar gRPC on `18098`. It is re-runnable
against an existing volume; `docker compose down -v` removes it.

Give CNG about half a minute. Document reads answer almost at once, but the
SQL++ passthrough (`/_p/query/...`) fails with `failed to select query endpoint`
until CNG has loaded the cluster map. Readiness means *that* answers:

```console
curl --cacert tls/ca.crt -u appuser:apppass123 -H 'content-type: application/json' \
  -d '{"statement":"SELECT 1"}' "https://$LAN:18008/_p/query/query/service"
```

**2. A host with the feature.** Component host plugins are opt-in and absent
from released `wash` builds:

```console
git clone https://github.com/wasmCloud/wasmCloud && cd wasmCloud
cargo build --bin wash --features host-component-plugins
```

**3. The scenario.** Build the plugin (`wash build --skip-fetch` in the project
root), then give `scenario/.wash/config.yaml` the WIT source, the CA, the plugin
and its interface config. `wasmcloud:couchbase@0.2.0` is not published, so it
resolves from `../../interface`; run `wash wit fetch` once in `scenario/`,
because `wash dev` deliberately does not fetch.

```yaml
version: 2.0.0
wit:
  sources:
    "wasmcloud:couchbase": ../../interface
build:
  command: cargo build --target wasm32-wasip2 --release
  component_path: target/wasm32-wasip2/release/cbtest.wasm
dev:
  # Trust the stack's CA for outbound HTTPS. This reaches a host plugin's
  # wasi:http too, so the plugin verifies CNG rather than skipping verification.
  http_client_ca_paths:
    - /absolute/path/to/verification/tls/ca.crt
  host_plugins:
    - id: couchbase
      file: ../../target/wasm32-wasip2/release/couchbase_plugin.wasm
      allowedHosts: ["<LAN>:18008"]
  host_interfaces:
    - namespace: wasmcloud
      package: couchbase
      interfaces: [types, sqlpp-types, document, sqlpp]
      version: "0.2.0"
      config:
        endpoint: https://<LAN>:18008
        bucket: testbucket
        username: appuser
        password: apppass123
```

Then, from `scenario/`:

```console
/path/to/wasmCloud/target/debug/wash dev
curl -s http://127.0.0.1:8000/
```

Every line should read `OK`.

### Address the stack by LAN address, not `127.0.0.1`

`127.0.0.1` inside a guest means the **virtual** network, so a plugin dialling
`127.0.0.1:18008` gets `ConnectionRefused` rather than the gateway published on
the machine. `host.wasmcloud.internal` is the name for the machine's loopback.

For the **KV plugin**, which uses raw `wasi:sockets`, that name is all it takes
— see [its README](../../couchbase-kv/README.md#reaching-the-machines-loopback);
`allowedHostLoopbackPorts` on the plugin entry is enough.

The **Data API plugin** cannot use it: `wasi:http` does not resolve the
sentinel (`DnsError: address not available`) and does not read
`allowedHostLoopbackPorts`. Use the LAN address. Docker publishes on `0.0.0.0`,
so the gateway answers there, and a LAN address is an ordinary external address
that plain egress already covers.

A LAN *hostname* works too, and is worth preferring on a laptop whose address
moves between networks. For the Data API plugin, remember the certificate: pass
it to `CNG_SAN` as `DNS:<name>`. The KV plugin resolves the name through
`wasi:sockets/ip-name-lookup`, so its declaration also needs
`allowedIpNameLookups`:

```yaml
    - id: couchbase-kv
      allowedHosts: ["macbookpro.lan:11210", "macbookpro.lan:8093"]
      allowedIpNameLookups: ["*"]
```

## Seeing what went on the wire

CNG does not log requests, and the Data API is HTTPS, so checking the plugin's
framing means capturing its traffic on the host side of TLS. Worth doing after
any change to the request path: request framing is easy to break without failing
a single scenario step.

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
