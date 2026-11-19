//! A `wasmcloud:couchbase` host plugin that speaks the KV binary protocol
//! directly over `wasi:sockets@0.3.0`.
//!
//! No SDK and no gateway: the protocol is implemented here, and every socket
//! operation is an `await` that yields to the component executor.
//!
//! [`proto`] is bindings-free and tests on the host; `conn` and `plugin`
//! are the bindings glue and compile only for wasm.

pub mod projection;
pub mod proto;

#[path = "../../couchbase/src/query.rs"]
pub mod query;

#[path = "../../couchbase/src/query_rows.rs"]
#[cfg(any(target_family = "wasm", test))]
pub(crate) mod query_rows;

#[cfg(target_family = "wasm")]
#[path = "../../couchbase/src/query_stream.rs"]
mod query_stream;

#[path = "../../couchbase/src/transport.rs"]
pub mod transport;

#[cfg(target_family = "wasm")]
mod bindings {
    #![allow(unsafe_code)]
    #[cfg(not(feature = "tls"))]
    wit_bindgen::generate!({ world: "couchbase-wasi-sockets-p3-plugin", generate_all });
    #[cfg(feature = "tls")]
    wit_bindgen::generate!({ world: "couchbase-wasi-sockets-p3-tls-plugin", generate_all });
}

pub mod config;

#[cfg(any(target_family = "wasm", test))]
mod lifecycle;

#[cfg(target_family = "wasm")]
mod conn;

#[cfg(target_family = "wasm")]
mod plugin;
