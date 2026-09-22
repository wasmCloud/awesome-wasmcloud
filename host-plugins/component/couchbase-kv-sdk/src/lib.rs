//! A wasmCloud component host plugin providing `wasmcloud:couchbase` over the
//! Couchbase KV protocol.
//!
//! The sibling `couchbase` plugin exports the same interface over the Capella
//! Data API's HTTPS surface. This one embeds the official Couchbase Rust SDK
//! and connects with `couchbases://` through `wasi:sockets` and `wasi:tls`,
//! which is what lets it serve the operations the Data API has no endpoint for
//! and work against self-hosted clusters.
//!
//! [`config`] is bindings-free and compiles and tests on the
//! host; [`plugin`] is the bindings glue and is compiled only for wasm.

pub mod config;


#[cfg(target_family = "wasm")]
mod plugin;
