# Vendored WIT

## `wasmcloud-host/`

`wasmcloud:host@0.1.1`, copied verbatim from
[wasmCloud](https://github.com/wasmCloud/wasmCloud) at `wit/host/wit/`
(commit `b8e535692`). It is Apache-2.0, the same license as this project.

This package is defined and installed by the wasmCloud runtime itself rather
than being a normal registry dependency, so it is vendored here to keep a clean
checkout buildable against a known-good copy instead of whatever happens to be
published. `.wash/config.yaml` points `wasmcloud:host` at this directory.

To refresh it against a newer wasmCloud:

```console
git -C /path/to/wasmCloud show main:wit/host/wit/identity.wit > wasmcloud-host/identity.wit
git -C /path/to/wasmCloud show main:wit/host/wit/cancel.wit > wasmcloud-host/cancel.wit
git -C /path/to/wasmCloud show main:wit/host/wit/lifecycle.wit > wasmcloud-host/lifecycle.wit
```

If the package version changes, update the `@0.1.1` references in
`../wit/world.wit` to match.
