# Awesome wasmCloud

> Community-maintained components, host plugins, workload examples, and tools for [wasmCloud](https://wasmcloud.com).

[wasmCloud](https://github.com/wasmCloud/wasmCloud) runs WebAssembly components across clouds, Kubernetes, and edge devices, wiring them to capabilities at runtime instead of at build time. This repository collects what the community has built on top of it.

**Source may be hosted here or linked.** A project can live in this repository as a directory with its own README, license, and build instructions, or stay in its own repository and be listed here with a link. Entries are tagged `(hosted)` or `(linked)` so you know which you are getting. See [CONTRIBUTING.md](CONTRIBUTING.md) to add yours.

For a project hosted here, clone and build it directly:

```console
git clone https://github.com/wasmCloud/awesome-wasmcloud.git
cd awesome-wasmcloud/workload-examples/<project>
wash build
```

## Contents

- [Components](#components)
- [Host Plugins](#host-plugins)
  - [Native Host Plugins](#native-host-plugins)
  - [Host Component Plugins](#host-component-plugins)
- [Workload Examples](#workload-examples)
- [Tools](#tools)
- [Contributing](#contributing)

## Components

Reusable WebAssembly components that implement a WIT interface. Hosted projects live in [`components/`](components/).

_Nothing here yet. [Add the first one](CONTRIBUTING.md)._

## Host Plugins

[Host plugins](https://wasmcloud.com/docs/overview/hosts/plugins) extend a wasmCloud host with an implementation of a WIT world, which is linked to workloads at runtime. They come in two flavors, and a workload cannot tell which one is serving a capability it imports.

### Native Host Plugins

Rust implementations of the [`HostPlugin` trait](https://wasmcloud.com/docs/runtime/creating-host-plugins), linked into the host binary. The right choice when a capability needs direct host resources (filesystem, network, hardware) or has to run with the host's privileges. Hosted projects live in [`host-plugins/native/`](host-plugins/native/).

_Nothing here yet. [Add the first one](CONTRIBUTING.md)._

### Host Component Plugins

Capabilities built as [WebAssembly components](https://wasmcloud.com/docs/runtime/creating-host-component-plugins) and deployed into a host at runtime as trigger services with a capability ingress, so you ship, version, and sandbox them like any other component. Currently opt-in via the `host-component-plugins` feature, so check the docs for the state of play before depending on one. Hosted projects live in [`host-plugins/component/`](host-plugins/component/).

- [couchbase](host-plugins/component/couchbase/) (hosted): Serves a `wasmcloud:couchbase` capability — document key-value operations, atomic counters, and SQL++ queries — over the Couchbase Capella Data API using `wasi:http/client@0.3.0`. Written in Rust with an all-async WIT interface, and takes per-workload cluster credentials through the `wasmcloud:host/workload-lifecycle` bind hook.

## Workload Examples

End-to-end applications demonstrating how components compose into a running system. Hosted projects live in [`workload-examples/`](workload-examples/).

- [wasi-ai-app](https://github.com/bharattech/wasi-ai-app) (linked): Local AI meeting-notes pipeline built from three Rust components, a transcriber (oxiwhisper + `ggml-tiny`), a summarizer (Candle + Qwen3-0.6B), and a web UI. Runs inference entirely on-device and ships a Kubernetes deployment guide.

## Tools

CLIs, libraries, editor integrations, and developer tooling. Hosted projects live in [`tools/`](tools/).

_Nothing here yet. [Add the first one](CONTRIBUTING.md)._

## Contributing

Contributions are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) first for the two contribution routes, licensing rules, and what a project needs to be accepted.

Projects here are contributed by the community and maintained by their authors. Inclusion is not an endorsement, security review, or statement of production readiness. Read the code before running it.

## License

The repository is [Apache-2.0](LICENSE). Projects hosted here carry their own `LICENSE` file in their directory, which governs that project.
