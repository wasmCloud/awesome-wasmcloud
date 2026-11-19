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

use crate::{bindings, deadline, protocol, transport};
use bindings::wasi::clocks::monotonic_clock;
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
    /// Overall RPC deadline, also applied to transport inactivity and sent to the server.
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

/// Send one unary gRPC call.
pub async fn unary(call: Call<'_>) -> Result<Reply, String> {
    match call.timeout_ns {
        Some(0) => Err("gRPC deadline exceeded".into()),
        Some(ns) => deadline::within(unary_inner(call), monotonic_clock::wait_for(ns))
            .await
            .unwrap_or_else(|| Err("gRPC deadline exceeded".into())),
        None => unary_inner(call).await,
    }
}

async fn unary_inner(call: Call<'_>) -> Result<Reply, String> {
    if call.message.len() > protocol::MAX_MESSAGE_BYTES {
        return Err("gRPC message exceeds the 4 MiB message limit".into());
    }
    let length = call.message.len() as u32;
    let mut prefix = vec![0];
    prefix.extend_from_slice(&length.to_be_bytes());

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
    set(
        "content-length",
        (call.message.len() + 5).to_string().as_bytes(),
    )?;
    if let Some(ns) = call.timeout_ns {
        set("grpc-timeout", protocol::timeout_header(ns).as_bytes())?;
    }
    for (name, value) in &call.headers {
        set(name, value)?;
    }

    let (trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));
    let (mut tx, rx) = bindings::wit_stream::new();
    wit_bindgen::spawn_local(async move {
        if tx.write_all(prefix).await.is_empty() {
            tx.write_all(call.message).await;
        }
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
    let (mut body_stream, trailers) = Response::consume_body(response, res_rx);
    let mut decoder = protocol::FrameDecoder::default();
    loop {
        let (_, chunk) = body_stream
            .read(Vec::with_capacity(transport::READ_CHUNK_BYTES))
            .await;
        if chunk.is_empty() {
            break;
        }
        decoder.push(&chunk)?;
    }
    drop(res_tx);
    // This future also carries body transport failures, even when a
    // trailers-only gRPC status was already present in the response headers.
    let trailing = trailers
        .await
        .map_err(|e| format!("response body error: {e:?}"))?;

    let trailers = trailing.map(|fields| fields.copy_all()).unwrap_or_default();
    let frames = decoder.finish()?;
    let (status, message) =
        protocol::response_status(http_status, &headers, &trailers, !frames.is_empty())?;
    Ok(Reply {
        http_status,
        status: Some(status),
        message,
        frames,
    })
}
