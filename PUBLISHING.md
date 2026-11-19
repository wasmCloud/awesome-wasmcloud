# Publishing Couchbase

The [Couchbase release workflow](.github/workflows/couchbase-release.yml) builds
the WIT contract, Data API plugin, KV socket plugin and KV socket plugin with TLS.
Pull requests build and validate every artifact without publishing.
The publish jobs use the repository's GitHub token, sign OCI manifest digests
with GitHub build provenance, and verify anonymous pulls from GHCR.
The WIT check also exercises `wash wit fetch` from a fresh consumer project.

The authoritative contract remains in
[`host-plugins/component/couchbase/interface/`](host-plugins/component/couchbase/interface/).
Its package declaration determines the WIT version, independently of the plugins.
It publishes to `ghcr.io/wasmcloud/interfaces/wasmcloud/couchbase:<version>`,
matching wash's default `wasmcloud` namespace registry.
An existing version with different WIT fails publication; bump the WIT version
and update its consumers before releasing a changed contract.
Unchanged WIT is reused without replacing its original metadata or provenance.

On pushes to `main`, components publish under
`ghcr.io/wasmcloud/components/<name>:sha-<full-commit-sha>`.
Versioned releases use `couchbase-v<version>` Git tags:

```console
git tag -a couchbase-v0.1.0 -m 'Release Couchbase plugins 0.1.0'
git push origin couchbase-v0.1.0
```

Tag the reviewed commit you intend to release.
The tag version must match both plugins' Cargo package versions.
The three component names are `couchbase`, `couchbase-wasi-sockets-p3`, and
`couchbase-wasi-sockets-p3-tls`.
Existing component tags are immutable: different bytes require a new version.
All publish jobs wait for native tests and all four artifact builds to pass.
Publishing is restricted to this repository's `main` and release tags;
manual runs on feature branches build without publishing.

GHCR creates new packages as private by default.
After the first push, an organization package administrator must make each new
package public in its package settings and retain this repository's Actions access.
If anonymous verification fails for that reason, make it public and rerun the
failed publish job; matching artifacts are reused and attestation can resume.

To inspect the same build locally, install stable Rust with `wasm32-wasip2`,
wash and wasm-tools, then run, for example:

```console
node .github/scripts/build-couchbase-release.mjs wit
node .github/scripts/build-couchbase-release.mjs tls
node --test .github/scripts/*.test.mjs
```

TLS releases require the host's upstream component plugin TLS support;
publishing a TLS component does not add that support to older host binaries.
