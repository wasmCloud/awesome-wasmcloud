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
        let body = if id.starts_with("large-") && id.ends_with("-ttl") {
            use bindings::wasmcloud::couchbase::types::DurabilityLevel;
            use document::*;
            let result = match id.as_str() {
                "large-touch-ttl" => touch(
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
                "large-get-touch-ttl" => get_and_touch(
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
                "large-insert-ttl" => insert(
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
                "large-replace-ttl" => replace(
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
                _ => upsert(
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
            format!("LOCK {:?}", document::get_and_lock("..".into(), None).await)
        } else if id == "dot-remove-id" {
            format!("REMOVE {:?}", document::remove(".".into(), None).await)
        } else if id == "oversized-id" || id == "empty-id" {
            let key = if id == "empty-id" {
                String::new()
            } else {
                "x".repeat(65_537)
            };
            match document::upsert(key, b"{}".to_vec(), None).await {
                Ok(_) => "WRITE OK".to_string(),
                Err(error) => format!("WRITE ERROR {error:?}"),
            }
        } else if id == "query-error" || id == "query-context" {
            let statement = if id == "query-context" {
                "SELECT RAW \"query-context\""
            } else {
                "SELECT RAW 1"
            };
            match sqlpp::query(statement.into(), vec![], None).await {
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
            let key = match id.as_str() {
                "dot-id" => ".",
                "dotdot-id" => "..",
                _ => &id,
            };
            match document::get(key.into(), Some(options)).await {
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
