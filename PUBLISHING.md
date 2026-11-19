# Publishing WIT packages and components

The [artifact workflow](.github/workflows/artifacts.yml) builds and publishes
entries from [`.github/artifacts.json`](.github/artifacts.json).
Add a catalog entry to publish another WIT package, Rust component, or Rust
host component plugin; the workflows and scripts contain no project-specific logic.
Pull requests build and validate every registered artifact without publishing.
Rust builds use stable, run native tests with their configured features,
and compile the component for `wasm32-wasip2` with committed dependency locks.

## Register artifacts

Each entry names its source directory, license, and release group.
WIT packages declare their package identity and interfaces for a consumer smoke test.
Rust components name the Cargo library binary, optional Cargo features,
and any required imports, exports, or forbidden imports.
For example, a new package and plugin could be registered as:

```json
[
  {
    "id": "weather-wit",
    "kind": "wit",
    "releaseGroup": "weather",
    "directory": "host-plugins/component/weather/interface",
    "package": "example:weather",
    "smokeImports": ["forecast"],
    "license": "Apache-2.0"
  },
  {
    "id": "weather",
    "kind": "component",
    "releaseGroup": "weather",
    "directory": "host-plugins/component/weather",
    "binary": "weather_plugin",
    "contracts": { "weather-wit": ["forecast"] },
    "license": "Apache-2.0"
  }
]
```

WIT files can be split across files in the source directory.
Keep one authoritative source; the build stages it without committing a second copy.
If dependencies need fetching, set `fetchFrom` to the wash project directory
whose `wash wit fetch` populates that source directory's `deps/`.
Commit its `wkg.lock` and any project wash configuration.

A component's `contracts` map names registered WIT packages and the interfaces
it must export at their source versions.
Shared WIT dependencies are included when selecting a release, even when
another release group owns that contract.
Use `features` for variants and `imports`, `exports`, and `forbiddenImports`
for their fully versioned interface requirements.
Each variant has its own artifact id and image repository.
A new entry's directory must contain the appropriate license file.

## Versions and destinations

A WIT declaration `package <namespace>:<name>@<version>;` publishes to
`ghcr.io/wasmcloud/interfaces/<namespace>/<name>:<version>`.
Its version is independent of the components implementing it.
Different WIT at an existing version fails the preflight and authenticated publish checks.
Unchanged WIT retains its original metadata and provenance.
The consumer test fetches the published package through a remote OCI source.
Set `defaultRegistry: true` when the namespace has a default wash registry resolver;
that additionally proves `wash wit fetch` works without a source override.

Rust components publish to `ghcr.io/wasmcloud/components/<id>`.
On `main`, their tag is `sha-<full-commit-sha>`.
Versions come from Cargo metadata, including inherited workspace versions.
Release tags use `<releaseGroup>-v<version>`, for example:

```console
git tag -a weather-v0.1.0 -m 'Release weather 0.1.0'
git push origin weather-v0.1.0
```

Tag the reviewed commit you intend to release.
Only that group's artifacts and shared WIT dependencies are selected.
Each selected component's Cargo version must match the tag.
WIT keeps its own version; a release group containing only WIT uses its WIT version.
All versions must be OCI-compatible SemVer, without build metadata.
Unregistered release groups publish nothing.
Manual runs on feature branches build without publishing.
Existing version and commit tags are immutable: different bytes require a new version or commit.

## Attestations and access

The [reusable publisher](.github/workflows/publish-artifact.yml) handles every artifact.
All builds must pass before publication, and WIT packages must publish and verify
successfully before components publish.
The jobs use the repository's GitHub token, attest OCI manifest digests,
push provenance bundles to GHCR, and verify anonymous pulls.
They also verify attestations from both GitHub and GHCR.
New attestations must match the reusable workflow and source commit;
reused WIT retains its original, verified repository provenance.
Publication runs are serialized to protect immutable version checks.

For an existing package, grant this repository's Actions write access in the
package's settings before publishing from it.
If GHCR creates a new package as private, a package administrator must make it public.
Then rerun the failed publish jobs: matching artifacts are reused and attestation can resume.

Run the same helpers locally with an id from the catalog:

```console
node .github/scripts/plan-artifacts.mjs
node .github/scripts/build-artifact.mjs weather-wit
node .github/scripts/build-artifact.mjs weather
node --test .github/scripts/*.test.mjs
```

Publishing a component does not change its runtime requirements.
See each project's README for host features, TLS support, and deployment configuration.
