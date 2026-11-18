mod bindings {
    wit_bindgen::generate!({ world: "component-fault-probe", generate_all });
}

use bindings::exports::wasi::http::handler::Guest;
use bindings::wasi::http::types::{ErrorCode, Fields, Request, Response};
use bindings::wasmcloud::couchbase::{document, sqlpp};

struct Probe;

impl Guest for Probe {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let id = request
            .get_path_with_query()
            .unwrap_or_default()
            .trim_start_matches('/')
            .to_string();
        let body = if id == "oversized-id" || id == "empty-id" {
            let key = if id == "empty-id" { String::new() } else { "x".repeat(65_537) };
            match document::upsert(key, b"{}".to_vec(), None).await {
                Ok(_) => "WRITE OK".to_string(),
                Err(error) => format!("WRITE ERROR {error:?}"),
            }
        } else if id == "query-error" {
            match sqlpp::query("SELECT RAW 1".into(), vec![], None).await {
                Ok(value) => format!("QUERY OK {value:?}"),
                Err(error) => format!("QUERY ERROR {error:?}"),
            }
        } else {
            let options = document::DocumentGetOptions {
                with_expiry: id.starts_with("expiry-"),
                project: None,
                use_replica: None,
                timeout_ns: None,
                retry_strategy: None,
                parent_span: None,
            };
            match document::get(id, Some(options)).await {
                Ok(value) => format!(
                    "OK cas={} expiry={:?} document={}",
                    value.cas,
                    value.expires_at,
                    String::from_utf8_lossy(&value.document)
                ),
                Err(error) => format!("ERROR {error:?}"),
            }
        }
        .into_bytes();
        let (mut writer, reader) = bindings::wit_stream::new();
        let (trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));
        wit_bindgen::spawn_local(async move {
            writer.write_all(body).await;
            drop(writer);
            let _ = trailers_tx.write(Ok(None)).await;
        });
        let (response, _sent) = Response::new(Fields::new(), Some(reader), trailers_rx);
        Ok(response)
    }
}

bindings::export!(Probe with_types_in bindings);
