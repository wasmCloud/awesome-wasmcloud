//! A wasmCloud component host plugin providing `wasmcloud:couchbase`.
//!
//! The host pins one instance of this component and routes every workload's
//! `wasmcloud:couchbase` calls to it across a store boundary. This transport
//! reaches Couchbase over the Data API's HTTP surface through
//! `wasi:http/client` — the p3 asynchronous client, so one tenant's slow
//! round-trip yields the executor instead of blocking every other tenant behind
//! it.
//!
//! Layout: everything that can be reasoned about without the WIT bindings lives
//! in [`api`], [`config`], [`mutation`] and [`timefmt`], which compile and test
//! on the host.
//! `plugin` is the bindings glue and is compiled only for wasm.

pub mod api;
pub mod config;
pub mod mutation;
pub mod query;
#[cfg(any(target_family = "wasm", test))]
pub(crate) mod query_rows;
pub mod timefmt;
pub mod transport;

#[cfg(any(target_family = "wasm", test))]
mod workload_bindings;

#[cfg(target_family = "wasm")]
mod plugin;

#[cfg(target_family = "wasm")]
mod query_stream;

#[cfg(target_family = "wasm")]
mod bindings {
    #![allow(unsafe_code)]
    wit_bindgen::generate!({ world: "couchbase-plugin", generate_all });
}
