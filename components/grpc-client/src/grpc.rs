//! gRPC over `wasi:http/client@0.3.0`.
//!
//! gRPC is HTTP/2 with three conventions layered on top, and each one is a
//! place to go wrong:
//!
//! 1. **Framing.** A message is prefixed with five bytes: one compression flag
//!    and a big-endian `u32` length. A server-streaming call sends several of
//!    those frames back to back in one body.
//! 2. **`te: trailers`.** The request must announce that it can accept
//!    trailers, or a conforming server may refuse the call.
//! 3. **The status is not the HTTP status.** A failed RPC is usually `HTTP
//!    200` with `grpc-status` set to something other than `0`. Which is why
//!    [`Reply::status`] is what a caller checks, never `http_status`.
//!
//! The third convention has a wrinkle worth stating plainly, because it is the
//! one that bites: **`grpc-status` arrives in headers or in trailers depending
//! on whether there is a body.** A call that returns a message puts the status
//! in real trailers, after the body. A call that returns none — most errors —
//! is sent as a *trailers-only* response, which puts the status in the HEADERS
//! frame. A client that reads only trailers sees nothing at all on exactly the
//! responses it most needs to understand, and one that reads only headers
//! misses every success. [`unary`] reads headers first, then trailers.
//!
//! Nothing here knows about protobuf. Encode a message with `prost` (or by
//! hand) and hand the bytes over; decode what comes back the same way.

use crate::bindings;
use bindings::wasi::http::client;
use bindings::wasi::http::types::{Fields, Method, Request, RequestOptions, Response, Scheme};

/// One unary call.
pub struct Call<'a> {
    /// `host:port` of the gRPC endpoint.
    pub authority: &'a str,
    /// TLS. gRPC over plaintext is `false`.
    pub secure: bool,
    /// `/<package>.<Service>/<Method>`, e.g.
    /// `/couchbase.kv.v1.KvService/Get`.
    pub path: &'a str,
    /// The encoded request message, without gRPC framing.
    pub message: Vec<u8>,
    /// Extra headers, typically `authorization`.
    pub headers: Vec<(String, Vec<u8>)>,
    /// Per-call deadline applied to connect, first byte and between bytes.
    pub timeout_ns: Option<u64>,
}

/// What a call came back with.
pub struct Reply {
    /// The transport's status. Almost always `200`, including for a failed RPC.
    pub http_status: u16,
    /// The gRPC status code. `0` is success; `5` is `NOT_FOUND`, and so on.
    /// `None` when the server sent none, which is a protocol violation and
    /// should be treated as a failure rather than as success.
    pub status: Option<u32>,
    /// The server's `grpc-message`, empty on success.
    pub message: String,
    /// The response messages, unframed. A unary call yields one; a
    /// server-streaming call yields one per streamed message.
    pub frames: Vec<Vec<u8>>,
}

impl Reply {
    /// Whether the RPC itself succeeded. A missing status is not success.
    pub fn ok(&self) -> bool {
        self.status == Some(0)
    }
}

/// Wrap a message in gRPC's five-byte frame header.
fn frame(message: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(5 + message.len());
    framed.push(0); // not compressed
    framed.extend_from_slice(&(message.len() as u32).to_be_bytes());
    framed.extend_from_slice(message);
    framed
}

/// Split a response body into its messages.
///
/// A short or truncated trailing frame is dropped rather than guessed at: a
/// partial protobuf message decodes into plausible nonsense.
pub fn split_frames(body: &[u8]) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    let mut pos = 0;
    while pos + 5 <= body.len() {
        // Byte 0 is the compression flag. Nothing here negotiates compression
        // (no `grpc-accept-encoding` is sent), so a server setting it would be
        // violating the negotiation; the frame is skipped rather than handed
        // back as if it were readable.
        let compressed = body[pos] != 0;
        let len = u32::from_be_bytes([body[pos + 1], body[pos + 2], body[pos + 3], body[pos + 4]])
            as usize;
        pos += 5;
        let Some(message) = body.get(pos..pos + len) else {
            break;
        };
        if !compressed {
            frames.push(message.to_vec());
        }
        pos += len;
    }
    frames
}

/// Read `grpc-status` and `grpc-message` out of a field set.
fn read_status(fields: &[(String, Vec<u8>)]) -> (Option<u32>, Option<String>) {
    let get = |name: &str| -> Option<String> {
        fields
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .and_then(|(_, v)| String::from_utf8(v.clone()).ok())
    };
    let status = get("grpc-status").and_then(|v| v.trim().parse::<u32>().ok());
    (status, get("grpc-message"))
}

/// Send one unary gRPC call.
pub async fn unary(call: Call<'_>) -> Result<Reply, String> {
    let body = frame(&call.message);

    let fields = Fields::new();
    let set = |name: &str, value: &[u8]| -> Result<(), String> {
        fields
            .append(name, value)
            .map_err(|e| format!("header `{name}` was rejected: {e:?}"))
    };
    set("content-type", b"application/grpc")?;
    // Announce that trailers are acceptable. A conforming server may refuse
    // the call without it.
    set("te", b"trailers")?;
    // A `wasi:http` body is a stream, so without an explicit length the
    // request goes out chunked. Some proxies handle a chunked gRPC request
    // poorly.
    set("content-length", body.len().to_string().as_bytes())?;
    for (name, value) in &call.headers {
        set(name, value)?;
    }

    let (trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));
    let (mut tx, rx) = bindings::wit_stream::new();
    wit_bindgen::spawn_local(async move {
        tx.write_all(body).await;
        drop(tx);
        // The request carries no trailers of its own; the future still has to
        // resolve or the request body never completes.
        let _ = trailers_tx.write(Ok(None)).await;
    });

    let options = RequestOptions::new();
    if let Some(ns) = call.timeout_ns {
        let _ = options.set_connect_timeout(Some(ns));
        let _ = options.set_first_byte_timeout(Some(ns));
        let _ = options.set_between_bytes_timeout(Some(ns));
    }

    let (request, _sent) = Request::new(fields, Some(rx), trailers_rx, Some(options));
    request
        .set_method(&Method::Post)
        .map_err(|()| "wasi:http rejected POST".to_string())?;
    request
        .set_scheme(Some(&if call.secure {
            Scheme::Https
        } else {
            Scheme::Http
        }))
        .map_err(|()| "wasi:http rejected the scheme".to_string())?;
    request
        .set_authority(Some(call.authority))
        .map_err(|()| format!("`{}` is not a valid authority", call.authority))?;
    request
        .set_path_with_query(Some(call.path))
        .map_err(|()| format!("`{}` is not a valid path", call.path))?;

    let response = client::send(request)
        .await
        .map_err(|e| format!("transport error: {e:?}"))?;

    let http_status = response.get_status_code();
    // Copied before `consume_body` moves the response. For a trailers-only
    // response this is where the status lives.
    let headers = response.get_headers().copy_all();

    let (res_tx, res_rx) = bindings::wit_future::new(|| Ok(()));
    let (body_stream, trailers) = Response::consume_body(response, res_rx);
    let received = body_stream.collect().await;
    drop(res_tx);

    let (mut status, mut message) = read_status(&headers);
    if status.is_none() {
        // No status in the headers, so this response has a body and the status
        // is in the trailers that follow it.
        if let Ok(Some(trailing)) = trailers.await {
            let (s, m) = read_status(&trailing.copy_all());
            status = s;
            message = message.or(m);
        }
    }

    Ok(Reply {
        http_status,
        status,
        message: message.unwrap_or_default(),
        frames: split_frames(&received),
    })
}
