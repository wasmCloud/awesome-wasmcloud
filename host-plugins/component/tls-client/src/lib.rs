//! A host component plugin demonstrating host-provided TLS over WASI sockets.
pub mod deadline;
#[cfg(target_family = "wasm")]
mod plugin;
