# Pinned TLS WIT

`wasi-tls/` is the `wasi:tls@0.3.0-draft` stream-transform API copied from
wasmCloud revision `269ceabad0c8c3bc02111cc1d399563e6465acc1`, under
`crates/wash-runtime/tests/fixtures/p3-wit-deps/wasi-tls-0.3.0-draft/`.
That runtime uses Wasmtime 48.0.8. The API has to match the host; fetching the
unrelated P2 TLS proposal or a different draft does not provide these imports.

The upstream implementation is
[wasmtime-wasi-tls 48.0.8](https://github.com/bytecodealliance/wasmtime/tree/v48.0.8/crates/wasi-tls).
Its Apache-2.0 with LLVM exception license is retained in `wasi-tls/LICENSE`.
The native verifier's TLS provider/server helper is adapted from the same
wasmCloud revision's `tests/common/tls.rs`, under Apache-2.0.
