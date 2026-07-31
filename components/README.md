# Components

Reusable WebAssembly components that implement a WIT interface.

One directory per hosted component. Projects can also be listed as links
without a directory here. See [CONTRIBUTING.md](../CONTRIBUTING.md) for
requirements, and add a line to the [root README](../README.md#components)
either way.

## Using a hosted component

Scaffold a new local project from just one hosted component, without cloning
the whole repository:

```console
wash new https://github.com/wasmCloud/awesome-wasmcloud --subfolder components/<component>
```

This checks out `<component>` into a new local directory (named after it, or
pass `--name` to choose your own), ready to build on as your own project.
