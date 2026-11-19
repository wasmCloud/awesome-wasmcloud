mod bindings {
    wit_bindgen::generate!({ world:"caller",generate_all });
}
use bindings::wasi::http::types::{ErrorCode, Fields, Request, Response};
struct Component;
impl bindings::exports::wasi::http::handler::Guest for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let path = request.get_path_with_query().unwrap_or_default();
        let query = path.split_once('?').map(|p| p.1).unwrap_or_default();
        let get = |key: &str| {
            query
                .split('&')
                .find_map(|pair| {
                    pair.split_once('=')
                        .filter(|p| p.0 == key)
                        .map(|p| p.1.to_string())
                })
                .unwrap_or_default()
        };
        let result = bindings::example::tls::client::ping(get("address"), get("name")).await;
        let (status, body) = match result {
            Ok(value) => (200, value),
            Err(error) => (502, error),
        };
        let (mut tx, rx) = bindings::wit_stream::new();
        let (done_tx, done_rx) = bindings::wit_future::new(|| Ok(None));
        wit_bindgen::spawn_local(async move {
            tx.write_all(body.into_bytes()).await;
            drop(tx);
            let _ = done_tx.write(Ok(None)).await;
        });
        let (response, _) = Response::new(Fields::new(), Some(rx), done_rx);
        let _ = response.set_status_code(status);
        Ok(response)
    }
}
bindings::export!(Component with_types_in bindings);
