# amqp

A [host component plugin](https://wasmcloud.com/docs/runtime/creating-component-host-plugins) that gives workloads an AMQP 0-9-1 client, for RabbitMQ and other brokers that speak the protocol. The plugin holds the connection to the broker, publishes messages for workloads, and delivers messages from queues to the workloads that consume them.

It is written in Rust with [amqprs](https://github.com/gftea/amqprs) and runs as one long-lived Wasm component inside the host.

> **Status: experimental, version 0.1.** It speaks plain AMQP only. **AMQPS (TLS) is not supported yet**; see [TLS](#tls-is-not-supported-yet). It has been run with `wash dev` and `wash host` against RabbitMQ in Docker, and not on Kubernetes.

## What workloads get

Workloads use an AMQP-specific interface, `wasmcloud:amqp@0.1.0`, instead of `wasmcloud:messaging`, so they keep what AMQP offers: exchanges and routing keys, message properties and headers, and explicit settlement of each message. The WIT is in [`wit/`](wit/), and every type in it is documented.

| Interface | Served by | What it does |
|---|---|---|
| `publisher` | the plugin | `publish(exchange, routing-key, message)` puts a message on the broker. |
| `handler` | the workload | `handle-delivery(delivery)` receives each message from the workload's queues, and returns what to do with it. |

**Publishing.** The plugin publishes with publisher confirms, so `publish` returns once the broker has taken responsibility for the message, or fails with a reason: `unavailable` while the plugin is not connected, `rejected` when the broker refuses the message (the error carries the broker's own text, such as `404 NOT_FOUND - no exchange 'x'`), `invalid` for an argument AMQP cannot carry. An empty `exchange` is the default exchange, which routes by queue name.

**Consuming.** A workload exports `handler` and names its queues in its config (see [Configuration](#configuration)). Each message from those queues is delivered to `handle-delivery`, and the `outcome` it returns settles the message:

| Outcome | AMQP | Effect |
|---|---|---|
| `ack` | `basic.ack` | The message is done. |
| `nack(requeue)` | `basic.nack` | The message was not handled. `requeue: true` puts it back on the queue. |
| `reject(requeue)` | `basic.reject` | The message can never be handled. `requeue: false` discards it, or dead-letters it if the queue has a dead-letter exchange. |

Settling by return value means a message is settled exactly once, on the channel it arrived on, and a late settlement can never reach a different message after a reconnect. If the call itself fails, the plugin settles for the workload: it requeues the message when the workload could not be reached, and rejects it without requeueing when the workload was reached and failed, so a message that keeps crashing a handler cannot loop. Delivery is at least once.

In Rust, with the bindings `wit-bindgen` generates from the `handler-service` world:

```rust
async fn handle_delivery(delivery: Delivery) -> Result<Outcome, CallError> {
    Ok(match process(&delivery.message.body) {
        Ok(()) => Outcome::Ack,
        Err(Failure::Transient) => Outcome::Nack(true),
        Err(Failure::Permanent) => Outcome::Reject(false),
    })
}
```

[`example/`](example/) is a complete workload: an HTTP endpoint that publishes, and a handler that consumes.

## Try it

You need:

- [Docker](https://docs.docker.com/get-docker/), for RabbitMQ.
- Rust with the `wasm32-wasip2` target: `rustup target add wasm32-wasip2`.
- `wash` 2.10.3 **built with the `host-component-plugins` feature**. The release binaries do not include it. Either build it, which takes a while:

  ```console
  cargo install --locked --git https://github.com/wasmCloud/wasmCloud --tag v2.10.3 wash \
    --no-default-features --features wasm_component_model_implements,host-component-plugins
  ```

  or run the published all-features image, `ghcr.io/wasmcloud/wash:2.10.3-all-features`, for `wash host`. A `wash` that was built without the feature refuses to start a plugin.

Start RabbitMQ with a user other than `guest`, which RabbitMQ only lets connect from the machine it runs on:

```console
docker run -d --name rabbitmq -p 5672:5672 \
  -e RABBITMQ_DEFAULT_USER=wasmcloud -e RABBITMQ_DEFAULT_PASS=wasmcloud \
  rabbitmq:4
```

Build the plugin, then run the example, which loads it:

```console
wash build            # in this directory
cd example
wash dev
```

In another terminal, publish a message to the queue the example consumes:

```console
curl -X POST 'http://localhost:8000/publish?key=wasmcloud-example' -d 'hello'
```

`wash dev` prints what the handler received:

```text
received `hello` from queue `wasmcloud-example` (routing key `wasmcloud-example`, redelivered: false, content type: Some("application/x-www-form-urlencoded"), headers: [])
```

Message properties and headers pass through. Set a content type, and add headers with `header.<name>` query parameters:

```console
curl -X POST 'http://localhost:8000/publish?key=wasmcloud-example&header.source=billing' \
  -H 'content-type: text/plain' -d 'hello'
```

The handler settles by what the body says. Publish `retry` and it nacks the message once with requeue, so you see it twice, the second time with `redelivered: true`. Publish `reject` and it rejects the message without requeueing, so the message is dropped.

Stop the broker with `docker stop rabbitmq` and publishing fails with `unavailable`. Start it again and the plugin reconnects by itself, and consuming resumes.

## Configuration

Two parties configure the plugin, and they cannot touch each other's settings: the operator who runs the host sets where the broker is and who to connect as, and each workload sets which queues it consumes.

### The broker: set by the operator

These keys go in the plugin's own entry in the host configuration (`config`, `configFrom` or `secretFrom`, which merge, the last source winning). The plugin reads them only from there, so nothing in a workload's config can change them.

| Key | Required | Meaning |
|---|---|---|
| `host` | yes | The broker's host name or IP address. |
| `port` | no | The port. Default `5672`. |
| `username` | yes | The user to connect as. |
| `password` | yes | The user's password. It may be empty, but the key must be set, so a secret that failed to resolve is not mistaken for one. |
| `vhost` | no | The virtual host. Default `/`. |

Put the credentials in `secretFrom` and the rest in `config`. A plugin is declared in `host.plugins` of the host's configuration file. This is the form `wash host` takes, and it was run as written, with the broker at `172.17.0.2`:

```yaml
secrets:
  rabbitmq-credentials:
    dir: /etc/rabbitmq-credentials   # files named `username` and `password`

host:
  plugins:
    - id: amqp
      file: ./amqp_plugin.wasm       # or: image: ghcr.io/you/amqp-plugin:0.1.0
      config:
        host: 172.17.0.2
        port: "5672"
      secretFrom:
        - rabbitmq-credentials
      allowedHosts:
        - 172.17.0.2:5672
```

Run it with `wash host --user-config host.yaml`, plus the NATS flags a host needs. The host's `workloadConfig` policy is `deny` by default, which also stops a workload from setting a key that the operator has set.

### Network access

A plugin may only reach what its entry grants.

- **A broker on the network:** list it in `allowedHosts`, as above. For a host name, the entry both lets the plugin resolve the name and connect to it: `rabbitmq.example.com:5672`.
- **A broker on the machine that runs the host**, such as the Docker container above: `127.0.0.1` inside a plugin is the plugin's own virtual network, not the machine. Use `host.wasmcloud.internal` as the `host`, and list the port in `allowedHostLoopbackPorts: [5672]`. `wash dev` allows this. For `wash host`, start the host with `--allow-host-loopback` as well. The example does this.

### The workload: set by the workload

A workload names its queues in the `config` of its `wasmcloud:amqp` entry under `hostInterfaces`. The `handler` interface has to be listed, or the workload is refused at deploy.

| Key | Required | Meaning |
|---|---|---|
| `queues` | to consume | Comma-separated names of the queues to consume. |
| `prefetch` | no | How many unsettled messages the broker may have outstanding per queue, from 1 to 65535. Default `16`. The plugin calls the handler for that many at once, so messages finish in any order. Use `1` to handle them one at a time, in queue order. |
| `declare-queues` | no | `true` declares each queue as a durable queue before consuming it. Default `false`. Convenient for trying things out. In production, create queues with your usual tooling, so a typo cannot create one. |

A workload without `queues` only publishes. In `wash dev`, this is `dev.host_interfaces` in `.wash/config.yaml`; see [`example/.wash/config.yaml`](example/.wash/config.yaml). On Kubernetes it is the same entry in a `WorkloadDeployment`:

```yaml
hostInterfaces:
  - namespace: wasmcloud
    package: amqp
    version: "0.1.0"
    interfaces: [publisher, handler]
    config:
      queues: orders,audit
      prefetch: "8"
```

A bad value fails the deploy with the reason, for example ``invalid wasmcloud:amqp config: `prefetch` must be a number from 1 to 65535, not `0` ``.

## Behavior to know

- **Reconnecting.** When the connection is lost, the plugin reconnects with a delay that doubles from half a second to 30 seconds. Until it does, `publish` fails at once with `unavailable`. Consumers restart on their own, and the broker redelivers what was unsettled.
- **When a queue is consumed.** A workload's queues are consumed from the moment it is running until it stops, and never while it cannot receive. A message is not taken from the broker with no one to give it to.
- **Isolation.** Each workload that publishes gets its own channel, and each queue a workload consumes gets its own. When a workload makes the broker close its channel, for instance by publishing to an exchange that does not exist, the other workloads are not disturbed.
- **Limits.** AMQP names, such as exchange, routing key and queue, and the short properties, such as content type and message id, hold at most 255 bytes. Headers take at most 16 KiB. A body is at most 16 MiB, which is RabbitMQ's default. A larger value comes back as `invalid`.
- **Headers.** A header is a boolean, an integer, a float, a string, bytes or a timestamp. On a delivery, a header of any other AMQP type, such as a nested table or a decimal, is left out.
- **Changing the plugin's config** needs a host restart. Plugins load once, when the host starts.

## Limitations

### TLS (AMQPS) is not supported yet

Today, TLS would have to run inside the plugin: amqprs's `tls` feature compiles for `wasm32-wasip2`, but it brings no rustls crypto provider, and the provider would have to build for Wasm too. The certificates and keys would also live inside the guest. wasmCloud is adding TLS for plugins on the host side, where the operator names the CA and client certificate on the plugin's `allowedHosts` entry, and the host makes the TLS connection, so the plugin never sees a key. That needs a change in how the plugin opens its connection, which is in one place, `connect` in [`src/client.rs`](src/client.rs), and amqprs takes a connector, not a stream from the caller.

Until then, connect over a network you trust, or put the broker behind something that terminates TLS.

### Other limitations

- **One broker identity.** Every workload bound to the plugin uses the plugin's credentials, and the plugin does not limit which exchanges or queues a workload names. Use the broker's own permissions and virtual hosts to bound what the plugin's user can reach.
- **No `mandatory` publishes.** The broker drops a message that no binding routes, and `publish` still succeeds.
- **No topology management.** The plugin does not declare exchanges or bindings, and `declare-queues` declares a durable queue with default arguments, which fails against a queue that exists with other arguments.
- **Latency.** Calls into the plugin run between short slices of tokio, which is how the plugin serves workloads while it also drives the broker connection, so a call waits up to about 20 ms to start. The loop also costs about 3% of one core while idle in testing.
- **Back-to-back publishes are slower on the shipped host.** `wash-runtime` sets `TCP_NODELAY` on HTTP connections, but not on the sockets a guest opens, and WASI gives a guest no way to set it. Nagle's algorithm then holds a small write behind the broker's delayed acknowledgement of the last one. In testing on the published 2.10.3 image, publishes spaced apart took about 14 ms and publishes sent back to back took about 37 ms. With `set_nodelay(true)` added where the host connects a guest's socket, back to back took about 20 ms. Settling messages is affected the same way.

## Troubleshooting

The plugin logs through the host, so look for `wasmcloud-amqp` in the host's output.

| You see | Likely cause |
|---|---|
| `could not connect to the broker ...: Permission denied` | The broker is not in the plugin's `allowedHosts`, or the port is not in `allowedHostLoopbackPorts`. |
| `could not resolve ...: Name does not resolve` | The name is not resolvable from the host, or is not in `allowedHosts`. |
| A deploy fails with `the wasmcloud:amqp plugin is not configured: the plugin's config needs a ...` | A required key is missing from the plugin's entry. |
| `the broker closed the channel of the consumer of queue ...: 404 NOT_FOUND - no queue`, then `could not consume queue ...` | The queue does not exist. Create it, or set `declare-queues`. The plugin retries with a growing delay, up to 30 seconds, and starts consuming once the queue exists. |
| `the broker closed the channel ... 406 PRECONDITION_FAILED` | `declare-queues` met a queue that exists with different arguments. |
| The broker refuses the login | Check `username`, `password` and `vhost`. |

## How it works

The plugin is one long-lived instance, and a WASI component has no threads, so everything shares one thread, in two halves. **tokio** runs the client, which owns the connection to the broker. The **component-model executor** runs everything that talks to the host: the functions workloads call, and the code that routes each delivery to the right workload's handler. They talk through channels.

tokio cannot simply be left running, because `block_on` would hold the thread for ever and no call from a workload would be served. So the plugin's `wasi:cli/run` task runs tokio in short slices and yields to the executor between them. Reading [`src/lib.rs`](src/lib.rs) first is the quickest way in:

| File | What it holds |
|---|---|
| [`src/lib.rs`](src/lib.rs) | The exported functions and the `run` loop. |
| [`src/client.rs`](src/client.rs) | The tokio half: connect and reconnect, publisher channels with confirms, consumers. |
| [`src/dispatch.rs`](src/dispatch.rs) | The executor half: which workloads can be called, and routing each delivery to one. |
| [`src/config.rs`](src/config.rs) | The plugin's settings and a workload's subscription. |
| [`src/convert.rs`](src/convert.rs) | Messages between the WIT types and amqprs, with the checks that keep amqprs from panicking. |
| [`wit/`](wit/) | The `wasmcloud:amqp` interface and the plugin's world. |

## Development

```console
wash build        # builds target/wasm32-wasip2/release/amqp_plugin.wasm
cargo test        # unit tests, which run natively
```

`wash build` fetches the WIT dependencies from the registries, and `.cargo/config.toml` turns on the tokio flag the plugin needs. To build without `wash`: `cargo build --release --target wasm32-wasip2`.

### Tested with

| | |
|---|---|
| `wash` | 2.10.3, built with `host-component-plugins`. `wash host` ran on the published `ghcr.io/wasmcloud/wash:2.10.3-all-features` image, and so did `wash dev`, from prebuilt artifacts. |
| RabbitMQ | 4.3.6 (`rabbitmq:4`) |
| Rust | 1.96.0 |
| amqprs | 2.1.5 |
| tokio | 1.53.3 |
| wit-bindgen | 0.60.0 |

## License

Apache-2.0. See [LICENSE](LICENSE).
