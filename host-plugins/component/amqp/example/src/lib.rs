//! An example workload for the wasmCloud AMQP plugin.
//!
//! - `POST /publish?key=<routing key>` publishes the request body. An
//!   `exchange=<name>` query parameter picks the exchange; without it the
//!   message goes to the default exchange, which routes by queue name. The
//!   request's `content-type` becomes the message's, and each
//!   `header.<name>=<value>` query parameter becomes a message header.
//! - Each message the broker delivers from the workload's queue reaches
//!   `handle_delivery`, which prints it and settles it. A body of `reject`
//!   is rejected, and a body of `retry` is sent back once.

mod bindings {
    wit_bindgen::generate!({ world: "example", generate_all });
}

use bindings::exports::wasi::http::handler::Guest as HttpHandler;
use bindings::exports::wasmcloud::amqp::handler::{CallError, Guest as AmqpHandler};
use bindings::wasi::http::types::{ErrorCode, Fields, Request, Response};
use bindings::wasmcloud::amqp::publisher;
use bindings::wasmcloud::amqp::types::{Delivery, HeaderValue, Message, Outcome, Properties};

struct Component;

impl AmqpHandler for Component {
    async fn handle_delivery(delivery: Delivery) -> Result<Outcome, CallError> {
        let Delivery {
            queue,
            routing_key,
            redelivered,
            message,
            ..
        } = delivery;
        let body = String::from_utf8_lossy(&message.body).into_owned();
        println!(
            "received `{body}` from queue `{queue}` (routing key `{routing_key}`, \
             redelivered: {redelivered}, content type: {:?}, headers: {:?})",
            message.properties.content_type, message.properties.headers
        );
        Ok(match body.as_str() {
            "reject" => Outcome::Reject(false),
            "retry" if !redelivered => Outcome::Nack(true),
            _ => Outcome::Ack,
        })
    }
}

impl HttpHandler for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let target = request.get_path_with_query().unwrap_or_default();
        let (path, query) = target.split_once('?').unwrap_or((target.as_str(), ""));
        if path != "/publish" {
            return respond(404, "POST /publish?key=<routing key>\n");
        }

        let Some(key) = query_param(query, "key") else {
            return respond(400, "the `key` query parameter is the routing key\n");
        };
        let exchange = query_param(query, "exchange").unwrap_or_default();
        let headers = query
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .filter_map(|(name, value)| {
                let name = name.strip_prefix("header.")?;
                Some((name.to_string(), HeaderValue::Text(value.to_string())))
            })
            .collect();
        let content_type = request
            .get_headers()
            .get("content-type")
            .into_iter()
            .next()
            .and_then(|value| String::from_utf8(value).ok());

        // Reading the body moves the request. The first future is how the
        // handler reports how it fared, and nothing here has more to report.
        let (done, outcome) = bindings::wit_future::new(|| Ok(()));
        let (body, _trailers) = Request::consume_body(request, outcome);
        let body = body.collect().await;
        let _ = done.write(Ok(())).await;

        let message = Message {
            properties: Properties {
                content_type,
                content_encoding: None,
                headers,
                delivery_mode: None,
                priority: None,
                correlation_id: None,
                reply_to: None,
                expiration: None,
                message_id: None,
                timestamp: None,
                message_type: None,
                user_id: None,
                app_id: None,
            },
            body,
        };
        match publisher::publish(exchange, key, message).await {
            Ok(()) => respond(202, "published\n"),
            Err(err) => respond(502, &format!("could not publish: {err:?}\n")),
        }
    }
}

/// The value of query parameter `name`, as sent. The example does not decode
/// percent-escapes.
fn query_param(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then(|| value.to_string())
    })
}

fn respond(status: u16, body: &str) -> Result<Response, ErrorCode> {
    let headers = Fields::new();
    let (mut tx, rx) = bindings::wit_stream::new();
    let (trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));
    let body = body.as_bytes().to_vec();
    wit_bindgen::spawn_local(async move {
        tx.write_all(body).await;
        drop(tx);
        let _ = trailers_tx.write(Ok(None)).await;
    });
    let (response, _result) = Response::new(headers, Some(rx), trailers_rx);
    response
        .set_status_code(status)
        .map_err(|()| ErrorCode::InternalError(Some("could not set the status".to_string())))?;
    Ok(response)
}

bindings::export!(Component with_types_in bindings);
