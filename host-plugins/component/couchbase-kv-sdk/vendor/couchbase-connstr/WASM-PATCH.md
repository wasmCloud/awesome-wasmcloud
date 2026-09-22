# Vendored `couchbase-connstr`

A verbatim copy of [`couchbase-connstr`](https://crates.io/crates/couchbase-connstr)
1.0.1 (Apache-2.0, Couchbase) with **one change**, applied via
`[patch.crates-io]` in the parent `Cargo.toml`.

## The change

`lookup_srv` falls back to `hickory_resolver::system_conf::read_system_conf`
when the connection string names no DNS server. That reads `/etc/resolv.conf`,
which a Wasm sandbox has no equivalent of — hickory does not compile the
function for `wasm32` at all, so the crate fails to build with:

```
error[E0432]: unresolved import `hickory_resolver::system_conf::read_system_conf`
```

The import and its one call site are therefore `#[cfg]`-gated off for wasm, and
on wasm the fallback builds a resolver config pointing at a public nameserver
instead. Every other path, including the crate's own `Some(dns_config)` branch
for an explicitly configured resolver, is untouched.

The SRV lookup matters because a Capella connection string
(`couchbases://cb.<id>.cloud.couchbase.com`) resolves its nodes through
`_couchbases._tcp.<host>` records rather than a plain A record.

## Refreshing it

Re-vendor from the registry and re-apply:

```console
cp -R ~/.cargo/registry/src/*/couchbase-connstr-<version> vendor/couchbase-connstr
```

then gate `read_system_conf` as `git diff` here shows. If upstream makes the
system-conf fallback conditional itself, drop this vendored copy and the
`[patch.crates-io]` entry with it.
