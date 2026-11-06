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

mod bindings {
    wit_bindgen::generate!({ world: "grpc-client", generate_all });
}

mod grpc;

use bindings::exports::wasi::http::handler::Guest as Handler;
use bindings::wasi::cli::environment;
use bindings::wasi::http::types::{ErrorCode, Fields, Request, Response};

struct Component;

/// Default per-call deadline, in nanoseconds.
const DEFAULT_TIMEOUT_NS: u64 = 30_000_000_000;

fn var(name: &str) -> Option<String> {
    environment::get_environment()
        .into_iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v)
}

/// A plain-text response. Used for anything that never became a gRPC call.
fn text(status: u16, body: String) -> Response {
    let fields = Fields::new();
    let _ = fields.append("content-type", b"text/plain");
    respond(status, fields, body.into_bytes())
}

fn respond(status: u16, fields: Fields, body: Vec<u8>) -> Response {
    let (mut tx, rx) = bindings::wit_stream::new();
    let (trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));
    wit_bindgen::spawn_local(async move {
        tx.write_all(body).await;
        drop(tx);
        let _ = trailers_tx.write(Ok(None)).await;
    });
    let (response, _result) = Response::new(fields, Some(rx), trailers_rx);
    let _ = response.set_status_code(status);
    response
}

async fn handle(request: Request) -> Response {
    let Some(authority) = var("GRPC_AUTHORITY") else {
        return text(
            500,
            "GRPC_AUTHORITY is not set: it names the gRPC endpoint, as host:port\n".to_string(),
        );
    };
    // Plaintext gRPC is opt-in, so a missing variable cannot silently downgrade
    // a deployment that meant to use TLS.
    let secure = var("GRPC_PLAINTEXT").as_deref() != Some("1");
    let timeout_ns = var("GRPC_TIMEOUT_MS")
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(DEFAULT_TIMEOUT_NS, |ms| ms.saturating_mul(1_000_000));

    let path = request.get_path_with_query().unwrap_or_default();
    // A gRPC method path is `/package.Service/Method`: two segments, no query.
    if path.matches('/').count() != 2 || path.contains('?') || path.ends_with('/') {
        return text(
            400,
            format!("`{path}` is not a gRPC method path; expected /<package>.<Service>/<Method>\n"),
        );
    }

    // The caller's credential is forwarded rather than held here, so this
    // component never needs one of its own.
    let mut headers = Vec::new();
    for (name, value) in request.get_headers().copy_all() {
        if name.eq_ignore_ascii_case("authorization") {
            headers.push((name, value));
        }
    }

    let (req_tx, req_rx) = bindings::wit_future::new(|| Ok(()));
    let (body_stream, _trailers) = Request::consume_body(request, req_rx);
    let message = body_stream.collect().await;
    drop(req_tx);

    let reply = match grpc::unary(grpc::Call {
        authority: &authority,
        secure,
        path: &path,
        message,
        headers,
        timeout_ns: Some(timeout_ns),
    })
    .await
    {
        Ok(reply) => reply,
        Err(e) => return text(502, format!("{e}\n")),
    };

    let fields = Fields::new();
    let _ = fields.append("content-type", b"application/octet-stream");
    let _ = fields.append("grpc-frames", reply.frames.len().to_string().as_bytes());
    // The transport's own status, which is 200 even for a failed RPC. Anything
    // else means the call never reached the service as a gRPC call.
    let _ = fields.append("grpc-http-status", reply.http_status.to_string().as_bytes());
    match reply.status {
        Some(code) => {
            let _ = fields.append("grpc-status", code.to_string().as_bytes());
        }
        // Say so rather than defaulting to 0, which would report a protocol
        // violation as a successful call.
        None => {
            let _ = fields.append("grpc-status", b"unknown");
        }
    }
    // `grpc-message` carries the failure reason, and is empty on success.
    if !reply.ok() && !reply.message.is_empty() {
        let _ = fields.append("grpc-message", reply.message.as_bytes());
    }

    // The first frame is the unary response. Further frames belong to a
    // server-streaming call, and `grpc-frames` says how many there were.
    let body = reply.frames.into_iter().next().unwrap_or_default();
    respond(200, fields, body)
}

impl Handler for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        Ok(handle(request).await)
    }
}

bindings::export!(Component with_types_in bindings);
