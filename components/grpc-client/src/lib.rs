//! An HTTP front door for a gRPC service, as a WebAssembly component.
//!
//! A `POST /<package>.<Service>/<Method>` carrying an encoded protobuf message
//! becomes one gRPC call to the configured endpoint. The response message comes
//! back as the body, and the RPC's own outcome comes back in the `grpc-status`
//! and `grpc-message` response headers — which is where it belongs, since a
//! failed RPC is still `HTTP 200`.
//!
//! The transport is [`grpc`], which is the part worth copying: it knows nothing
//! about this component, or about protobuf.
//!
//! Approach credit: Laurent Doguin's `wasmcloud-couchbase-cng-conduit`
//! (<https://github.com/ldoguin/wasmcloud-couchbase-cng-conduit>) showed that
//! protostellar gRPC can be driven straight from a component over `wasi:http`
//! p3. This is an independent implementation; see the README.

pub mod deadline;
pub mod protocol;

#[cfg(target_family = "wasm")]
mod bindings {
    wit_bindgen::generate!({ world: "grpc-client", generate_all });
}

#[cfg(target_family = "wasm")]
mod grpc;
#[cfg(target_family = "wasm")]
mod handler;
