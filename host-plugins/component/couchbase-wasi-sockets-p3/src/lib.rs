//! A `wasmcloud:couchbase` host plugin that speaks the KV binary protocol
//! directly over `wasi:sockets@0.3.0`.
//!
//! No SDK and no gateway: the protocol is implemented here, and every socket
//! operation is an `await` that yields to the component executor.
//!
//! [`proto`] is bindings-free and tests on the host; [`conn`] and [`plugin`]
//! are the bindings glue and compile only for wasm.

pub mod proto;

#[cfg(target_family = "wasm")]
mod bindings {
    #![allow(unsafe_code)]
    wit_bindgen::generate!({ world: "couchbase-wasi-sockets-p3-plugin", generate_all });
}

pub mod config;

#[cfg(target_family = "wasm")]
mod conn;

#[cfg(target_family = "wasm")]
mod plugin;
