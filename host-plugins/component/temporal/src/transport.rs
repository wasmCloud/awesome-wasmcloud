//! Carries tonic's gRPC requests over `wasi:http`, so the generated client
//! runs without tokio or a socket of its own.
//!
//! Every call the plugin makes is unary, so the request body is collected
//! before it is sent, and the response body and trailers are read in full
//! before they go back to tonic. gRPC puts its status in the trailers, so they
//! must reach tonic too. The host makes the connection, HTTP/2 and TLS
//! included.

use std::convert::Infallible;
use std::future::{Future, Ready, ready};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue};
use http_body_util::combinators::WithTrailers;
use http_body_util::{BodyExt, Full};
use tonic::Status;

use crate::bindings::wasi::http::client;
use crate::bindings::wasi::http::types::{
    ErrorCode, Fields, Method, Request, RequestOptions, Response, Scheme,
};
use crate::bindings::{wit_future, wit_stream};

/// The response body tonic gets: the whole body, then the trailers.
pub type ResponseBody = WithTrailers<Full<Bytes>, Ready<Option<Result<HeaderMap, Infallible>>>>;

/// A call's gRPC deadline, put in the request's extensions so the transport
/// can bound its own waits. Without one, the host's defaults apply, which can
/// leave a dead connection unnoticed for minutes.
#[derive(Clone, Copy)]
pub struct Deadline(pub Duration);

/// How long past a call's deadline the transport waits on the connection, so
/// that Temporal's own `DEADLINE_EXCEEDED` normally arrives first.
const DEADLINE_MARGIN: Duration = Duration::from_secs(10);

/// Sends requests to one Temporal frontend.
#[derive(Clone)]
pub struct Transport {
    https: bool,
    authority: String,
}

impl Transport {
    /// `address` is `http://host:port` or `https://host:port`.
    pub fn new(address: &str) -> Self {
        let (scheme, authority) = address.split_once("://").unwrap_or(("http", address));
        Self {
            https: scheme == "https",
            authority: authority.trim_end_matches('/').to_string(),
        }
    }
}

impl tower_service::Service<http::Request<tonic::body::Body>> for Transport {
    type Response = http::Response<ResponseBody>;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Status>>>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        Box::pin(send(self.clone(), request))
    }
}

async fn send(
    transport: Transport,
    request: http::Request<tonic::body::Body>,
) -> Result<http::Response<ResponseBody>, Status> {
    let (parts, body) = request.into_parts();
    let body = body.collect().await?.to_bytes();

    let headers = Fields::from_list(&to_list(&parts.headers))
        .map_err(|e| Status::internal(format!("the request headers were refused: {e:?}")))?;
    let (mut body_writer, body_reader) = wit_stream::new::<u8>();
    let (trailers_writer, trailers) =
        wit_future::new::<Result<Option<Fields>, ErrorCode>>(|| Ok(None));
    let options = parts
        .extensions
        .get::<Deadline>()
        .map(|&deadline| options(deadline));
    let (outgoing, _) = Request::new(headers, Some(body_reader), trailers, options);

    let path = parts.uri.path_and_query().map(|p| p.as_str().to_string());
    let scheme = if transport.https {
        Scheme::Https
    } else {
        Scheme::Http
    };
    outgoing
        .set_method(&Method::Post)
        .and_then(|()| outgoing.set_scheme(Some(&scheme)))
        .and_then(|()| outgoing.set_authority(Some(&transport.authority)))
        .and_then(|()| outgoing.set_path_with_query(path.as_deref()))
        .map_err(|()| Status::internal("the request's address was refused"))?;

    // The host reads the body while `send` runs, and a stream write waits for
    // its reader, so the body is written from a task of its own. gRPC requests
    // have no trailers.
    wit_bindgen::spawn_local(async move {
        body_writer.write_all(body.to_vec()).await;
        drop(body_writer);
        let _ = trailers_writer.write(Ok(None)).await;
    });

    let response = client::send(outgoing)
        .await
        .map_err(|e| Status::unavailable(format!("could not reach Temporal: {e:?}")))?;
    receive(response).await
}

async fn receive(response: Response) -> Result<http::Response<ResponseBody>, Status> {
    let status = response.get_status_code();
    let headers = to_map(response.get_headers());

    // `res` reports how reading the response went, and reading it always
    // succeeds here, so it keeps its default, `Ok(())`, which dropping the
    // writer sends.
    let (res_writer, res) = wit_future::new::<Result<(), ErrorCode>>(|| Ok(()));
    let (body, trailers) = Response::consume_body(response, res);
    let body = body.collect().await;
    let trailers = trailers.await;
    drop(res_writer);
    let trailers = trailers
        .map_err(|e| Status::unavailable(format!("the response from Temporal broke off: {e:?}")))?
        .map(to_map);

    let mut builder = http::Response::builder().status(status);
    if let Some(map) = builder.headers_mut() {
        *map = headers;
    }
    builder
        .body(Full::new(Bytes::from(body)).with_trailers(ready(trailers.map(Ok))))
        .map_err(|e| Status::internal(format!("Temporal sent a response tonic cannot read: {e}")))
}

/// Bounds connecting, the wait for the response, and each gap in it. A host
/// that refuses a timeout keeps its default for it, so errors are ignored.
fn options(Deadline(deadline): Deadline) -> RequestOptions {
    let limit = u64::try_from((deadline + DEADLINE_MARGIN).as_nanos()).unwrap_or(u64::MAX);
    let options = RequestOptions::new();
    let _ = options.set_connect_timeout(Some(limit));
    let _ = options.set_first_byte_timeout(Some(limit));
    let _ = options.set_between_bytes_timeout(Some(limit));
    options
}

fn to_list(headers: &HeaderMap) -> Vec<(String, Vec<u8>)> {
    headers
        .iter()
        .map(|(name, value)| (name.as_str().to_string(), value.as_bytes().to_vec()))
        .collect()
}

/// Copies `wasi:http` fields into a header map. The host has already checked
/// them, so a field `http` would refuse is skipped.
fn to_map(fields: Fields) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (name, value) in fields.copy_all() {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_bytes(&value),
        ) {
            map.append(name, value);
        }
    }
    map
}
