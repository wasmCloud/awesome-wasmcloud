//! Logging through the host's `wasi:logging`, so the plugin's messages land in
//! the host's own log, tagged with their level.
//!
//! Nothing here may log a credential. Messages name Temporal's address and
//! never its API key.

use crate::bindings::wasi::logging::logging::{Level, log};

const CONTEXT: &str = "wasmcloud-temporal";

pub fn info(message: &str) {
    log(Level::Info, CONTEXT, message);
}

pub fn warn(message: &str) {
    log(Level::Warn, CONTEXT, message);
}

pub fn error(message: &str) {
    log(Level::Error, CONTEXT, message);
}
