use crate::{bindings, deadline, grpc, protocol, transport};
use bindings::wasi::clocks::monotonic_clock;

use bindings::exports::wasi::http::handler::Guest as Handler;
use bindings::wasi::cli::environment;
use bindings::wasi::http::types::{ErrorCode, Fields, Method, Request, Response};

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
    if !matches!(request.get_method(), Method::Post) {
        let fields = Fields::new();
        let _ = fields.append("allow", b"POST");
        let _ = fields.append("content-type", b"text/plain");
        return respond(405, fields, b"only POST invokes an RPC\n".to_vec());
    }
    let Some(authority) = var("GRPC_AUTHORITY") else {
        return text(
            500,
            "GRPC_AUTHORITY is not set: it names the gRPC endpoint, as host:port\n".to_string(),
        );
    };
    // Plaintext gRPC is opt-in, so a missing variable cannot silently downgrade
    // a deployment that meant to use TLS.
    let secure = var("GRPC_PLAINTEXT").as_deref() != Some("1");
    let timeout_ns = match var("GRPC_TIMEOUT_MS") {
        None => DEFAULT_TIMEOUT_NS,
        Some(value) => match value.parse::<u64>() {
            Ok(ms) if ms > 0 => ms.saturating_mul(1_000_000),
            _ => return text(500, "GRPC_TIMEOUT_MS must be a positive integer\n".into()),
        },
    };
    let deadline_at = monotonic_clock::now().saturating_add(timeout_ns);

    let path = request.get_path_with_query().unwrap_or_default();
    // A gRPC method path is `/package.Service/Method`: two segments, no query.
    let valid_path = path
        .strip_prefix('/')
        .and_then(|p| p.split_once('/'))
        .is_some_and(|(service, method)| {
            !service.is_empty()
                && !method.is_empty()
                && !method.contains('/')
                && !path.contains('?')
        });
    if !valid_path {
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
    let (mut body_stream, trailers) = Request::consume_body(request, req_rx);
    let upload = async move {
        let message = transport::read_bounded(
            async |capacity| body_stream.read(Vec::with_capacity(capacity)).await.1,
            protocol::MAX_MESSAGE_BYTES,
        )
        .await
        .map_err(|_| text(413, "request exceeds the 4 MiB message limit\n".into()))?;
        drop(req_tx);
        trailers
            .await
            .map_err(|error| text(400, format!("request body error: {error:?}\n")))?;
        Ok::<_, Response>(message)
    };
    let message = match deadline::within(
        upload,
        monotonic_clock::wait_for(deadline_at.saturating_sub(monotonic_clock::now())),
    )
    .await
    {
        Some(Ok(message)) => message,
        Some(Err(response)) => return response,
        None => return text(408, "request upload deadline exceeded\n".into()),
    };

    let reply = match grpc::unary(grpc::Call {
        authority: &authority,
        secure,
        path: &path,
        message,
        headers,
        timeout_ns: Some(deadline_at.saturating_sub(monotonic_clock::now())),
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
