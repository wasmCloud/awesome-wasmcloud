# temporal

A [host component plugin](https://wasmcloud.com/docs/runtime/creating-host-component-plugins) that turns wasmCloud workloads into [Temporal](https://temporal.io) activity workers. The plugin polls Temporal's task queues, hands each activity task to a workload, and reports the result, failure or cancellation back to Temporal. Workflows stay in Temporal's own SDKs (Go, TypeScript, Python, Java and others), and the activities they call run as wasmCloud components.

It is written in Rust and runs as one long-lived Wasm component inside the host. It talks to Temporal's gRPC API with a [tonic](https://github.com/hyperium/tonic) client generated from Temporal's protos, over a small `wasi:http` transport, so it needs no sockets, TLS code or async runtime of its own.

> **Status: experimental, version 0.1.** It has been run end to end with `wash dev` against a local Temporal dev server; see [Tested with](#tested-with). Payloads are passed through without decoding, client certificates (mutual TLS) are not supported, and API keys for Temporal Cloud are untested.

## What workloads get

Workloads use `wasmcloud:temporal@0.1.0`. The WIT is in [`wit/`](wit/): [`types.wit`](wit/types.wit) holds the interfaces and documents every type, and [`world.wit`](wit/world.wit) holds the worlds. A workload targets the `activity-worker` world:

| Interface | Served by | What it does |
|---|---|---|
| `activity` | the workload | `execute(info, input)` runs one activity attempt and returns its result payloads, or an `execute-error`. |
| `context` | the plugin | `heartbeat(task-token, details)` records a heartbeat for a running attempt, and returns `true` when Temporal has asked to cancel it. |
| `types` | the plugin | The records the other two share: `payload`, `activity-info`, `failure`, `execute-error`, `heartbeat-error`. |

**Running an activity.** For each task, the plugin calls `execute` with an `activity-info` and the input payloads. `activity-info.activity-type` is the name the workflow called, such as `greet`, and the workload dispatches on it. `attempt` starts at 1 and goes up on each retry. `task-token` identifies the attempt, for heartbeats. `heartbeat-details` carries the details of the last heartbeat of an earlier attempt, so a retried activity can resume.

**Payloads.** A `payload` is Temporal's own: `data` plus `metadata`, whose `encoding` entry names the format, for example `json/plain`. The plugin passes payloads through in both directions without decoding them, so the workload encodes and decodes them itself. Temporal's SDKs send `json/plain` by default.

**Returning.** `execute` returns a list of payloads on success, or one of:

| `execute-error` | Reported to Temporal as |
|---|---|
| `failed(failure)` | `RespondActivityTaskFailed` with the workload's `message`, `error-type` and `details`. `non-retryable: true` stops Temporal retrying, whatever the workflow's retry policy. `false` leaves the decision to the retry policy. |
| `cancelled` | `RespondActivityTaskCanceled`. Return it after `heartbeat` returns `true`. Temporal refuses a cancellation it did not ask for, so the plugin then reports a retryable failure instead. |
| `host(detail)` | A retryable failure, with the message `the wasmCloud host could not call the workload: <detail>`. Set by the host, not the workload, when the call could not be completed, for example when it ran past the host's call limit (see [Limitations](#limitations)). |

A workload that is asked for an `activity-type` it does not implement should return a non-retryable `failed`.

**Heartbeats.** A long activity calls `context.heartbeat` with its `task-token` now and then. `Ok(true)` means Temporal has asked to cancel the activity, so the workload should stop and return `cancelled`. `Err(not-found)` means Temporal no longer knows the attempt, because it timed out or already finished, so the workload should stop. `Err(unavailable)` means the plugin could not reach Temporal, and the workload may carry on and try again. The `details` are kept by Temporal and handed to the next attempt.

In Rust, with the bindings `wit-bindgen` generates from the `activity-worker` world:

```rust
async fn execute(info: ActivityInfo, input: Vec<Payload>) -> Result<Vec<Payload>, ExecuteError> {
    match info.activity_type.as_str() {
        "greet" => Ok(vec![greet(&input)?]),
        other => Err(ExecuteError::Failed(Failure {
            message: format!("unknown activity type `{other}`"),
            error_type: "UnknownActivity".into(),
            non_retryable: true,
            details: vec![],
        })),
    }
}
```

[`example/`](example/) is a complete workload with four activities, and a Python workflow that calls them.

## Try it

You need:

- Rust with the `wasm32-wasip2` target: `rustup target add wasm32-wasip2`.
- The [Temporal CLI](https://docs.temporal.io/cli), for `temporal server start-dev`.
- Python 3, for the example workflow, which uses Temporal's Python SDK, [`temporalio`](https://pypi.org/project/temporalio/).
- `wash` 2.10.3 **built with the `host-component-plugins` feature**. The release binaries do not include it. Either build it:

  ```console
  cargo install --locked --git https://github.com/wasmCloud/wasmCloud --tag v2.10.3 wash \
    --no-default-features --features wasm_component_model_implements,host-component-plugins
  ```

  or run the published all-features image, `ghcr.io/wasmcloud/wash:2.10.3-all-features`. A `wash` that was built without the feature refuses to start a plugin.

Start a local Temporal server. It listens for gRPC on port 7233 and serves its web UI on <http://localhost:8233>:

```console
temporal server start-dev
```

The example points the plugin at `http://127.0.0.1:7233` (see [Network access and TLS](#network-access-and-tls)). Then build the plugin and run the example workload, which loads it:

```console
wash build            # in this directory
cd example
wash dev
```

The plugin starts polling the task queue `wasmcloud-example`. In another terminal, set up the workflow starter:

```console
cd example/workflow
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

Then run each scenario with `python run.py <scenario>`. `run.py` connects to `localhost:7233`, runs the workflows in a Python worker on the task queue `wasmcloud-example-workflows`, and the workflows send their activities to `wasmcloud-example`:

| Scenario | Activity | What happens | Printed |
|---|---|---|---|
| `greet` | `greet` | The workload returns a greeting, and the workflow completes with it. | `Hello, wasmCloud!` |
| `fail` | `fail` | The workload returns a non-retryable failure. The workflow sees it as an `ApplicationError` with the workload's message, and Temporal does not retry. | `ApplicationError: this activity always fails (type ExampleError, non_retryable=True)` |
| `flaky` | `flaky` | The workload returns a retryable failure on attempts 1 and 2, Temporal retries it, and attempt 3 succeeds. | `succeeded on attempt 3` |
| `cancel` | `slow` | The workload heartbeats once a second. After 5 seconds the workflow is cancelled, the next heartbeat returns `true`, and the workload stops and returns `cancelled`. | `workflow ended: CancelledError('Workflow cancelled')`, then `activity stopped: ActivityTaskCanceled` |

Each run also prints a link to it in Temporal's web UI, which shows the activity's attempts, heartbeats and result. The workload logs each attempt (``running `greet` (attempt 1) ...``) in `wash dev`'s output.

## Configuration

Temporal's address, namespace, task queues and credentials are set by the operator in the plugin's own entry in the host configuration (`config`, `configFrom` or `secretFrom`). The plugin reads them only from there, through `wasi:config`, so nothing in a workload's config can change them.

| Key | Required | Meaning |
|---|---|---|
| `address` | yes | Temporal's frontend gRPC endpoint. Must start with `http://` or `https://`. |
| `namespace` | no | The Temporal namespace. Default `default`. |
| `task-queues` | yes | Comma-separated names of the task queues to poll. Empty names are skipped and repeats dropped. At least one is needed. |
| `identity` | no | The worker identity Temporal records for this plugin. Default `wasmcloud-temporal`. |
| `api-key` | no | Sent as `Authorization: Bearer <key>` on every call, as Temporal Cloud expects. Untested against Temporal Cloud. |
| `max-concurrent` | no | How many activities may run at once, across all task queues. A whole number above 0. Default `16`. |

Values are trimmed of surrounding whitespace. An empty `address`, `namespace`, `identity` or `api-key` counts as unset, so an empty `api-key` sends no key. When a key appears more than once, the last entry wins. If the config is invalid, the plugin logs `the plugin is not configured: <reason>` and does not poll. The plugin never logs the API key.

Put `api-key` in `secretFrom` and the rest in `config`. A plugin is declared in `host.plugins` of the host's configuration file, for `wash host --user-config host.yaml`:

```yaml
host:
  plugins:
    - id: temporal
      file: ./temporal_plugin.wasm   # or: image: ghcr.io/you/temporal-plugin:0.1.0
      config:
        address: http://temporal.example.com:7233
        namespace: default
        task-queues: orders,billing
      allowedHosts:
        - temporal.example.com:7233
```

In `wash dev`, the same entry goes under `dev.plugins` in `.wash/config.yaml`, as in [`example/.wash/config.yaml`](example/.wash/config.yaml).

### Network access and TLS

A plugin may only reach what its entry grants, so list Temporal's frontend in `allowedHosts`. The host makes the plugin's outgoing HTTP requests, so `127.0.0.1` in `address` is the loopback of the machine the host runs on, and `http://127.0.0.1:7233` reaches a local `temporal server start-dev` (checked with wash 2.10.3). When `wash` itself runs in a container, run that container with `--network host`, or use an address the container can reach. [`example/.wash/config.yaml`](example/.wash/config.yaml) lists the address in `allowedHosts` through a YAML anchor, so there is one place to change it.

TLS is the host's job. With an `https://` address, the host makes the TLS connection, HTTP/2 included, and the plugin holds no certificates or keys. For a server with a private CA, the operator adds the CA to the host's trust roots for outbound HTTPS.

### Workloads

A workload is called only if it exports `wasmcloud:temporal/activity` and declares `wasmcloud:temporal` under `hostInterfaces`. In `wash dev`, this is `dev.host_interfaces` in `.wash/config.yaml`. On Kubernetes it is the same entry in a `WorkloadDeployment`:

```yaml
hostInterfaces:
  - namespace: wasmcloud
    package: temporal
    version: "0.1.0"
    interfaces: [activity, context]
```

Workloads set no keys: which task queues to poll is the operator's choice, not the workload's.

## Behavior to know

- **Polling.** The plugin long-polls each configured task queue with `PollActivityTaskQueue`, with a 70-second deadline, like Temporal's SDKs. Temporal ends an empty poll after about a minute, and the plugin starts the next one. A waiting poll does not block the plugin's other calls, such as heartbeats.
- **Routing.** For each task, the plugin asks the host which workloads it can call that export `wasmcloud:temporal/activity@0.1.0`, and picks the next one round-robin. If that workload stopped in the meantime, it tries the next. The plugin does not look at the activity type when it picks, so **every workload bound to the plugin should implement the same activity types**. To run different activities in different workloads, run them on separate hosts or plugin instances with separate task queues.
- **No workload.** A poll loop does not poll while no callable workload exports `wasmcloud:temporal/activity`: it logs `no workload exports ... yet, so task queue ... waits for one` and checks again every second, so the plugin takes no task it cannot run, and other workers on the same queue can take it. If the last workload stops between a poll and the call, the task is reported as a retryable failure, `no workload exports wasmcloud:temporal/activity`.
- **Retries.** Temporal owns retries. A retryable failure, or a `host` error, goes back to Temporal, which schedules the next attempt by the workflow's retry policy, and the plugin delivers that attempt like any other task, with `attempt` raised by one. A non-retryable failure ends the activity.
- **Heartbeats and cancellation.** `context.heartbeat` makes a `RecordActivityTaskHeartbeat` call to Temporal for each call from a workload. Temporal learns of a cancellation request only through a heartbeat, so an activity that never heartbeats runs to the end even when cancelled. Set a heartbeat timeout on long activities, so Temporal retries an attempt whose workload stopped heartbeating.
- **Deadlines.** Polls carry a 70-second gRPC deadline, and the plugin's other calls (results, failures, cancellations, heartbeats) 10 seconds. The plugin also gives the host's HTTP client connect and read timeouts of the deadline plus 10 seconds, so a dropped connection cannot stall a poll loop or a heartbeat for the host's default of 10 minutes.
- **Reaching Temporal.** When a poll fails, the plugin logs the error and polls that queue again after a delay: 1 second, doubling with each failure in a row up to 30 seconds, and back to 1 second after any successful poll.
- **Concurrency.** There is one poll loop per task queue. At most `max-concurrent` activities run at once, across all task queues. A loop takes a free slot before it polls, and the slot is given back once the activity's result has been reported, so the plugin never takes a task from Temporal that it has no room to run. When all slots are taken, every loop waits.
- **Reporting.** If reporting a result, failure or cancellation to Temporal fails, the plugin logs it and does not retry. Temporal then retries the attempt once its start-to-close or heartbeat timeout passes.
- **Shutdown.** The host gives the plugin no signal when it stops. It drops the plugin, so open polls and running activities are abandoned where they wait, and nothing is reported for them. Temporal retries each unreported attempt once its start-to-close or heartbeat timeout passes, as when a worker process dies.
- **Changing the plugin's config** needs a host restart. Plugins load once, when the host starts.

## Limitations

- **Activities only.** The plugin is an activity worker. Workflows run in Temporal's SDKs, not in wasmCloud.
- **One routing pool.** All workloads bound to the plugin share its task queues, and tasks go round-robin among them, so they must implement the same activity types.
- **Payloads are not decoded.** Workloads handle encodings themselves. There is no payload codec or data converter, so encrypted or compressed payloads reach the workload as they are.
- **No mutual TLS.** Client certificates for one plugin are draft work upstream ([wasmCloud#5626](https://github.com/wasmCloud/wasmCloud/pull/5626)).
- **API keys are untested** against Temporal Cloud.
- **One identity.** Every workload bound to the plugin acts with the plugin's namespace and credentials.
- **Calls into a workload are capped by the host**, at 600 seconds by default (`WASH_EPHEMERAL_CALL_TIMEOUT_SECS`). An activity that runs longer comes back as a `host` error and is reported as a retryable failure, so Temporal runs it again from the start, or from its last heartbeat details if the workload resumes from them. Split longer work into shorter activities, or raise the limit on the host.
- **Retries have no jitter.** Poll loops that fail together retry together.
- **Message size.** Responses from Temporal are accepted up to 128 MiB, and the plugin reads each response whole before decoding it.
- **Polling needs gRPC.** Temporal's HTTP API has no route for polling task queues as of server 1.32, so `address` must be the gRPC frontend (port 7233 on a dev server), not the HTTP API.

## Troubleshooting

The plugin logs through the host, so look for `wasmcloud-temporal` in the host's output.

| You see | Likely cause |
|---|---|
| `the plugin is not configured: ...` | A required key is missing, or a value is invalid: for example an `address` with a path, or an `api-key` with an `http://` address, which would send the key in cleartext. The message names the key. |
| `polling task queue ... failed, retrying in ...s: Unavailable("could not reach Temporal: ...")` | Temporal's address is not in the plugin's `allowedHosts`, or the server is not listening on that address. With `wash` in a container, `127.0.0.1` is the container's loopback unless it runs with `--network host`. |
| `polling task queue ... failed, retrying in ...s: NotFound` | Most likely `namespace` does not exist on the server. |
| `no workload exports ... yet, so task queue ... waits for one` | No workload that exports `wasmcloud:temporal/activity` and declares `wasmcloud:temporal` in its `hostInterfaces` is running. |
| A workflow's activity times out with no attempt in the workload's log | The workflow uses a task queue that is not in `task-queues`, or the plugin is not polling (see the lines above). |
| An activity fails with `the wasmCloud host could not call the workload: ...` | The host could not complete the call, for example because the activity ran past the host's call limit. |
| `could not report activity ... to Temporal: ...` | The result did not reach Temporal. Temporal retries the attempt after its timeout. |
| An activity fails with `unknown activity type` (in the example) | The workload that received it does not implement that activity type. Every bound workload must implement all of them. |

## Development

```console
wash build        # builds target/wasm32-wasip2/release/temporal_plugin.wasm
```

To build without `wash`: `cargo build --release --target wasm32-wasip2`. That needs the WIT dependencies in `wit/deps`, which are not committed (they are listed in `.gitignore`, and [`wkg.lock`](wkg.lock) pins them). `wash build` fetches them itself. To fetch them with [`wkg`](https://github.com/bytecodealliance/wasm-pkg-tools), point it at the registries for the `wasmcloud` and `wasi` namespaces:

```console
cat > /tmp/wkg.toml <<'EOF'
[namespace_registries]
wasmcloud = "wasmcloud.com"
wasi = "wasi.dev"
EOF
WKG_CONFIG_FILE=/tmp/wkg.toml wkg wit fetch
```

Run the same in `example/` for the example workload's dependencies.

| File | What it holds |
|---|---|
| [`src/lib.rs`](src/lib.rs) | The exported `context.heartbeat` and the `wasi:cli/run` task that starts the worker. |
| [`src/config.rs`](src/config.rs) | The plugin's settings, read from `wasi:config` and checked. |
| [`src/worker.rs`](src/worker.rs) | The poll loops, the `max-concurrent` slots, backoff, and routing tasks to workloads through `wasmcloud:host/workload-call`. |
| [`src/temporal.rs`](src/temporal.rs) | The five gRPC calls to Temporal: `PollActivityTaskQueue`, `RespondActivityTaskCompleted`, `RespondActivityTaskFailed`, `RespondActivityTaskCanceled` and `RecordActivityTaskHeartbeat`. |
| [`src/transport.rs`](src/transport.rs) | Carries tonic's requests over `wasi:http`. Unary calls only: it collects the request and response bodies, and hands the response trailers, which hold the gRPC status, back to tonic. |
| [`src/convert.rs`](src/convert.rs) | Converts between Temporal's proto types and the WIT types. |
| [`src/log.rs`](src/log.rs) | Logging through the host's `wasi:logging`. |
| [`build.rs`](build.rs) | Generates the tonic client for those five calls with `tonic-prost-build`, from the descriptor set that `temporalio-protos` (feature `vendored-protox`) builds with protox, so no `protoc` is needed. The message types come from `temporalio-protos`. tonic pulls in `tokio`, but the plugin never runs it. |
| [`wit/`](wit/) | The `wasmcloud:temporal` interfaces and the plugin's world. |

## Tested with

Run end to end with `wash dev` and `temporal server start-dev` on one machine: all four example scenarios, heartbeats with details carried into a retried attempt, and stopping the host in the middle of an activity. Not tested: TLS, API keys against Temporal Cloud, Kubernetes.

| | |
|---|---|
| `wash` | 2.10.3 (`ghcr.io/wasmcloud/wash:2.10.3-all-features`) |
| Temporal | Server 1.32.0, CLI 1.9.1 (`temporal server start-dev`) |
| `temporalio` (Python, example workflow) | 1.34.0 |
| Rust | 1.96.0 (cargo 1.96.0) |

## License

Apache-2.0. See [LICENSE](LICENSE).
