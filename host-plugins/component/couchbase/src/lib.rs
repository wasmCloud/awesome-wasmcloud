//! A wasmCloud component host plugin providing `wasmcloud:couchbase`.
//!
//! The host pins one instance of this component and routes every workload's
//! `wasmcloud:couchbase` calls to it across a store boundary. Because it runs
//! inside the Wasm sandbox it cannot open the binary KV protocol's sockets, so
//! it reaches Couchbase over the Data API's HTTP surface through
//! `wasi:http/client` — the p3 asynchronous client, so one tenant's slow
//! round-trip yields the executor instead of blocking every other tenant behind
//! it.
//!
//! Layout: everything that can be reasoned about without the WIT bindings lives
//! in [`api`], [`config`] and [`timefmt`], which compile and test on the host.
//! [`plugin`] is the bindings glue and is compiled only for wasm.

pub mod api;
pub mod config;
pub mod timefmt;

#[cfg(target_family = "wasm")]
mod plugin;
