mod bindings {
    #[cfg(not(feature = "read-only"))]
    wit_bindgen::generate!({ world: "component-fault-probe", generate_all });
    #[cfg(feature = "read-only")]
    wit_bindgen::generate!({ world: "couchbase-read-only", generate_all });
}

use bindings::exports::wasi::http::handler::Guest;
use bindings::wasi::http::types::{ErrorCode, Fields, Request, Response};
#[cfg(not(feature = "read-only"))]
use bindings::wasmcloud::couchbase::{document_read as read, document_write as write};
#[cfg(not(feature = "read-only"))]
#[path = "../../query-consumer.rs"]
mod query_consumer;

struct Probe;

#[cfg(not(feature = "read-only"))]
impl Guest for Probe {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let id = request
            .get_path_with_query()
            .unwrap_or_default()
            .trim_start_matches('/')
            .to_string();
        if id.starts_with("query-stream-") {
            return stream_query(&id).await;
        }
        let body = if id.starts_with("large-") && id.ends_with("-ttl") {
            use bindings::wasmcloud::couchbase::types::DurabilityLevel;
            use bindings::wasmcloud::couchbase::types::*;
            let result = match id.as_str() {
                "large-touch-ttl" => write::touch(
                    "range-probe".into(),
                    Some(DocumentTouchOptions {
                        expires_in: u64::MAX,
                        timeout_ns: None,
                        retry_strategy: None,
                        parent_span: None,
                    }),
                )
                .await
                .map(|_| ()),
                "large-get-touch-ttl" => write::get_and_touch(
                    "range-probe".into(),
                    Some(DocumentGetAndTouchOptions {
                        expires_in: u64::MAX,
                        timeout_ns: None,
                        retry_strategy: None,
                        parent_span: None,
                    }),
                )
                .await
                .map(|_| ()),
                "large-insert-ttl" => write::insert(
                    "range-probe".into(),
                    b"{}".to_vec(),
                    Some(DocumentInsertOptions {
                        expires_in_ns: u64::MAX,
                        flags: None,
                        persist_to: 0,
                        replicate_to: 0,
                        durability_level: DurabilityLevel::Unknown,
                        timeout_ns: None,
                        retry_strategy: None,
                        parent_span: None,
                    }),
                )
                .await
                .map(|_| ()),
                "large-replace-ttl" => write::replace(
                    "range-probe".into(),
                    b"{}".to_vec(),
                    Some(DocumentReplaceOptions {
                        cas: 0,
                        expires_in_ns: u64::MAX,
                        preserve_expiry: false,
                        flags: None,
                        persist_to: 0,
                        replicate_to: 0,
                        durability_level: DurabilityLevel::Unknown,
                        timeout_ns: None,
                        retry_strategy: None,
                        parent_span: None,
                    }),
                )
                .await
                .map(|_| ()),
                _ => write::upsert(
                    "range-probe".into(),
                    b"{}".to_vec(),
                    Some(DocumentUpsertOptions {
                        expires_in_ns: u64::MAX,
                        preserve_expiry: false,
                        flags: None,
                        persist_to: 0,
                        replicate_to: 0,
                        durability_level: DurabilityLevel::Unknown,
                        timeout_ns: None,
                        retry_strategy: None,
                        parent_span: None,
                    }),
                )
                .await
                .map(|_| ()),
            };
            format!("TTL {result:?}")
        } else if id == "dot-lock-id" {
            format!("LOCK {:?}", write::get_and_lock("..".into(), None).await)
        } else if id == "dot-remove-id" {
            format!("REMOVE {:?}", write::remove(".".into(), None).await)
        } else if id == "oversized-id" || id == "empty-id" {
            let key = if id == "empty-id" {
                String::new()
            } else {
                "x".repeat(65_537)
            };
            match write::upsert(key, b"{}".to_vec(), None).await {
                Ok(_) => "WRITE OK".to_string(),
                Err(error) => format!("WRITE ERROR {error:?}"),
            }
        } else if id.starts_with("query-") {
            let statement = match id.as_str() {
                "query-context" => "SELECT RAW \"query-context\"",
                "query-timeout" => "SELECT RAW \"query-timeout\"",
                "query-timeout-http" => "SELECT RAW \"query-timeout-http\"",
                "query-oversized" => "SELECT RAW \"query-oversized\"",
                _ => "SELECT RAW 1",
            };
            match query_consumer::query(statement.into(), vec![], None).await {
                Ok(value) => format!("QUERY OK {value:?}"),
                Err(error) => format!("QUERY ERROR {error:?}"),
            }
        } else {
            let options = bindings::wasmcloud::couchbase::types::DocumentGetOptions {
                with_expiry: id.starts_with("expiry-"),
                project: None,
                use_replica: None,
                timeout_ns: None,
                retry_strategy: None,
                parent_span: None,
            };
            let key = match id.as_str() {
                "dot-id" => ".",
                "dotdot-id" => "..",
                _ => &id,
            };
            match read::get(key.into(), Some(options)).await {
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

#[cfg(feature = "read-only")]
impl Guest for Probe {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let id = request
            .get_path_with_query()
            .unwrap_or_default()
            .trim_start_matches('/')
            .to_string();
        let result = bindings::wasmcloud::couchbase::document_read::get(id, None).await;
        let text = match result {
            Ok(document) => format!(
                "READ OK cas={} bytes={}",
                document.cas,
                document.document.len()
            ),
            Err(error) => format!("READ ERROR {error:?}"),
        };
        text_response(text)
    }
}

fn text_response(text: String) -> Result<Response, ErrorCode> {
    let (mut writer, reader) = bindings::wit_stream::new();
    let (trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));
    wit_bindgen::spawn_local(async move {
        writer.write_all(text.into_bytes()).await;
        drop(writer);
        let _ = trailers_tx.write(Ok(None)).await;
    });
    let (response, _) = Response::new(Fields::new(), Some(reader), trailers_rx);
    Ok(response)
}

#[cfg(not(feature = "read-only"))]
async fn stream_query(mode: &str) -> Result<Response, ErrorCode> {
    use bindings::wasmcloud::couchbase::sqlpp::{self, QueryCompletion};
    let result = match sqlpp::query(format!("SELECT RAW \"{mode}\""), vec![], None).await {
        Ok(result) => result,
        Err(error) => return text_response(format!("QUERY ERROR {error:?}")),
    };
    let mode = mode.to_string();
    let (mut writer, reader) = bindings::wit_stream::new();
    let (trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));
    wit_bindgen::spawn_local(async move {
        if mode.ends_with("backpressure") {
            bindings::wasi::clocks::monotonic_clock::wait_for(300_000_000).await;
        }
        let mut rows = result.rows;
        let mut count = 0;
        let mut bytes = 0;
        loop {
            let (_, chunk) = rows.read(Vec::with_capacity(1)).await;
            if chunk.is_empty() {
                break;
            }
            for row in chunk {
                count += 1;
                bytes += row.len();
                if mode.ends_with("cancel") {
                    drop(result.completion);
                    drop(rows);
                    writer
                        .write_all(b"QUERY CANCELLED after one row".to_vec())
                        .await;
                    drop(writer);
                    let _ = trailers_tx.write(Ok(None)).await;
                    return;
                }
                if mode.ends_with("incremental")
                    && !writer
                        .write_all(format!("ROW {row}\n").into_bytes())
                        .await
                        .is_empty()
                {
                    return;
                }
            }
        }
        let message = match QueryCompletion::finish(result.completion).await {
            Ok(()) => format!("QUERY STREAM OK rows={count} bytes={bytes}"),
            Err(error) => format!("QUERY STREAM ERROR rows={count} {error:?}"),
        };
        writer.write_all(message.into_bytes()).await;
        drop(writer);
        let _ = trailers_tx.write(Ok(None)).await;
    });
    let (response, _) = Response::new(Fields::new(), Some(reader), trailers_rx);
    Ok(response)
}
