# Couchbase KV Component Host Plugin

A second implementation of `wasmcloud:couchbase@0.2.0` — the *same* interface
the sibling plugin exports, so a workload cannot tell which is serving it —
reaching Couchbase over the binary KV protocol with `couchbases://` instead of
over the Data API's HTTPS surface.

The interface is not copied. [`.wash/config.yaml`](.wash/config.yaml) resolves
`wasmcloud:couchbase` from `../couchbase/interface`, so the two implementations
cannot drift.

**Status: working.** It embeds the official Couchbase Rust SDK, compiles to
`wasm32-wasip2`, loads as a host component plugin, and scores 34/34 on the
shared verification scenario against a real cluster — the same scenario the
sibling plugin also scores 34/34 on. See
[`../couchbase/verification/`](../couchbase/verification/).

## Why this exists

The Data API is served by Couchbase's Cloud Native Gateway — in Capella, or
self-hosted in front of any cluster — and has a fixed endpoint set. This
transport:

- needs **no gateway** in front of the cluster: it talks to the data and query
  services directly;
- serves **`get-and-lock` / `unlock`** on the stable protocol, where the Data
  API has them only under `/v1.alpha`, behind a gateway flag Capella need not
  set;
- honours **`preserve-expiry`**, which the Data API cannot express;
- avoids an HTTP hop for what is natively a binary protocol.

## Getting the SDK into a component

Four constraints. The first two are build-time; the last two present as a hang
or a panic far from their cause.

1. **Tokio does not build for `wasm32` by default.** Its socket support is gated
   behind `--cfg tokio_unstable`, set for this target in
   [`.cargo/config.toml`](.cargo/config.toml), so a plain
   `cargo build --target wasm32-wasip2` works.

2. **`couchbase-connstr` calls `read_system_conf`**, which reads
   `/etc/resolv.conf` and is not compiled for wasm. A two-line vendored patch
   gates it; see [`vendor/couchbase-connstr/WASM-PATCH.md`](vendor/couchbase-connstr/WASM-PATCH.md).

3. **Some SDK methods need a Tokio context without ever being awaited.**
   `Cluster::bucket` reads like a plain accessor but spawns the task that
   resolves the bucket's agent, so calling it outside a runtime context panics:

   ```
   there is no reactor running, must be called from the context of a Tokio 1.x runtime
   ```

   `with_collection` and `with_scope` enter a context for exactly those calls
   and drop it again before `block_on` — which panics in turn if called from
   *inside* a runtime context, so the guard cannot simply wrap everything.

4. **The SDK must never be handed a hostname.** Tokio resolves names on its
   blocking pool; wasm has no threads, so it aborts with

   ```
   OS can't spawn worker thread: Not supported (os error 58)
   ```

   The failure is the *thread spawn*, before any lookup happens — so no DNS
   grant can fix it. `std`'s resolver has no such problem: it calls
   `wasi:sockets/ip-name-lookup` directly, which the host grants through
   `allowedIpNameLookups`. `resolve_endpoint` resolves the name in the plugin
   and hands the SDK an address literal. That is what lets `endpoint` name a
   host at all.

### Timeouts are the plugin's to enforce

`couchbase` 1.0.1 takes no timeout on a document operation — only a query does,
as `server_timeout`. So a document call runs under `tokio::time::timeout` here.
That matters more for this transport than for the sibling: these calls hold the
plugin's only store, so one call waiting forever stops every workload on the
host.

Build the timer inside `block_on`. A `Sleep` registers with the timer driver
when it is constructed, so constructing one outside a runtime context panics
with `CONTEXT_MISSING_ERROR` before anything is awaited — the same trap as
`Cluster::bucket` above.

### The cost: calls serialize

The SDK's futures want a Tokio context and the component-model executor is
unrelated to it, so SDK work runs on a current-thread Tokio runtime via
`block_on`. `block_on` does not yield to the component executor, and a host
component plugin is a single pinned instance shared by every workload on the
host, so calls through this plugin **serialize**.

The sibling Data API plugin does not have this property: it awaits `wasi:http`
through the same executor that drives its exports, so its calls interleave.
Removing this would mean driving the transport's futures from the component
executor — i.e. a native `wasi:sockets` p3 implementation of the KV protocol
with no SDK in the graph.

## Configuration

Same keys as the sibling plugin, except `endpoint` takes a connection string:

| Key | Required | Default | Meaning |
|---|---|---|---|
| `endpoint` | yes | — | `couchbases://host` (TLS) or `couchbase://host`. A bare host is promoted to `couchbases://` rather than downgraded to plaintext. An `http(s)://` URL is rejected with a pointer to the sibling plugin. |
| `bucket` | yes | — | The one bucket this binding may reach. |
| `username` / `password` | yes | — | Cluster access credential. Source the password from `secretFrom`. |
| `scope` / `collection` | no | `_default` | Keyspace this binding operates in. |
| `timeout-ms` | no | `30000` | Per-request time limit, overridden by a call's own `timeout-ns`. |

A hostname `endpoint` needs `allowedIpNameLookups` to cover it, and
`allowedHosts` to cover the resolved address and the cluster's ports (`11210`
for KV, `8093` for the query service, or their TLS counterparts).

## Host requirements

Host component plugins are opt-in: the host must be built with the
`host-component-plugins` feature, which released builds do not carry.

```console
$ wash build
```

Two of the packages this world uses are not in a registry:
`wasmcloud:couchbase@0.2.0`, which lives in
[`../couchbase/interface`](../couchbase/interface/), and `wasmcloud:host@0.1.1`,
vendored under the sibling plugin's `wit-deps/`. Both are mapped in
[`.wash/config.yaml`](.wash/config.yaml), so `wash build` resolves them from the
checkout and fetches only the `wasi:*` packages. `--skip-fetch` additionally
skips that registry round-trip, which is useful offline once `wit/deps/` exists.

## Egress

Measured from inside a plugin export with an awaited p3 connect:

| Target | Result |
|---|---|
| `127.0.0.1:11210` | `ConnectionRefused` — the **virtual** network, which is what `127.0.0.1` means for a guest |
| `127.255.255.254:11210` (the `host.wasmcloud.internal` sentinel) | `AccessDenied` — gated, and see the gap below |
| a public address | `RemoteUnreachable` — the policy permitted it; the address genuinely was not reachable |

### Reaching the machine's loopback

`allowedHostLoopbackPorts` is a field on `HostPluginConfig` as of
[wasmCloud#5577](https://github.com/wasmCloud/wasmCloud/pull/5577), which is
what lets this plugin talk to a cluster on the developer's own machine:

```yaml
    - id: couchbase-kv
      allowedHostLoopbackPorts: ["11210", "8093"]
```

```yaml
        endpoint: couchbase://host.wasmcloud.internal
```

It needs **neither `allowedHosts` nor `allowedIpNameLookups`**: the `*.wasmcloud.internal` zone is resolved inside the
host, ahead of the name allowlist, and the grant is checked at connect where it
belongs. `wash dev` enables the host-wide gate already, so the per-plugin list
is the only declaration needed.

`127.0.0.1` still means the **virtual** network, so the sentinel name is the
spelling that reaches the machine.

**This does not help the sibling Data API plugin**, which reaches Couchbase
over `wasi:http`: that path never resolves the sentinel
(`DnsError: address not available`) and does not consult
`allowedHostLoopbackPorts` at all — with only that list set it is refused by
`allowedHosts` as deny-all. For an HTTP-based plugin, address the host service
by the machine's **LAN IP or LAN hostname** instead. Docker publishes on
`0.0.0.0`, and a LAN address is an ordinary external address that plain egress
already permits.

Two smaller notes:

- **A p3 socket import forces the whole `wasi:sockets` package to 0.3.0.**
  Declaring `wasi:sockets/...@0.2.0` alongside it fails to resolve. The p2
  imports a built component carries come from the wasm32-wasip2 target itself
  (they appear as `@0.2.12`), so they must not be declared in the world.
- **TLS needs nothing from the host** when the SDK terminates it in-guest with
  rustls — the built component imports no `wasi:tls`. A native p3
  implementation would instead want `wasi:tls`, which is *not* a default
  wash-runtime feature.

## What is implemented

Every `wasmcloud:couchbase/document` and `sqlpp` operation: insert, upsert,
replace, get (including **projection** and **`with-expiry`**, both delegated to
the SDK's own `GetOptions`), remove, touch, get-and-touch, **get-and-lock,
unlock**, and SQL++ queries with positional parameters, `scan-consistency`, and
**`consistent-with`**.

SQL++ runs at *scope* level, not cluster level. A cluster-level query resolves
an unqualified keyspace against the cluster, so `FROM _default` fails with `No
bucket named _default` — the binding's bucket is never consulted. Scope level
makes the binding's keyspace implicit, which is both what the statement means
and the boundary the workload was granted.

`config.rs` is bindings-free and unit-tested on the host.

Replica reads are **not in the interface at all**. No implementation can serve
them — the Data API has no endpoint and the Couchbase Rust SDK 1.0.1 exposes no
`get_any_replica`/`get_all_replicas` — so exposing a call that could only ever
fail was dead surface, and it has been removed from
[`../couchbase/interface/couchbase.wit`](../couchbase/interface/couchbase.wit).

## License

Apache-2.0. See [LICENSE](LICENSE). The vendored `couchbase-connstr` keeps its
own Apache-2.0 licence and attribution; see
[`vendor/couchbase-connstr/WASM-PATCH.md`](vendor/couchbase-connstr/WASM-PATCH.md).
