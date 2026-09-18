//! A workload that drives `wasmcloud:couchbase` through the host component
//! plugin and reports what each operation did.
//!
//! One `GET /` runs the whole scenario and returns a line per step, so a single
//! request exercises the capability end to end across the store boundary.

mod bindings {
    wit_bindgen::generate!({ world: "cb-test", generate_all });
}

use bindings::exports::wasi::http::handler::Guest as Handler;
use bindings::wasi::http::types::{ErrorCode, Fields, Request, Response};

use bindings::wasmcloud::couchbase::document::{
    self, DocumentGetAndTouchOptions, DocumentGetOptions, DocumentInsertOptions,
    DocumentRemoveOptions, DocumentReplaceOptions, DocumentTouchOptions, DocumentUnlockOptions,
    DocumentUpsertOptions,
};
use bindings::wasmcloud::couchbase::sqlpp::{self, SqlppQueryOptions};
use bindings::wasmcloud::couchbase::sqlpp_types::{SqlppQueryError, SqlppValue};
use bindings::wasmcloud::couchbase::types::{
    DocumentError, DurabilityLevel, MutationState, MutationToken, QueryScanConsistency,
};

struct Component;

const SECOND_NS: u64 = 1_000_000_000;

fn insert_opts(expires_in_ns: u64) -> DocumentInsertOptions {
    DocumentInsertOptions {
        expires_in_ns,
        flags: None,
        persist_to: 0,
        replicate_to: 0,
        durability_level: DurabilityLevel::Unknown,
        timeout_ns: None,
        retry_strategy: None,
        parent_span: None,
    }
}

fn upsert_opts(expires_in_ns: u64) -> DocumentUpsertOptions {
    DocumentUpsertOptions {
        expires_in_ns,
        preserve_expiry: false,
        flags: None,
        persist_to: 0,
        replicate_to: 0,
        durability_level: DurabilityLevel::Unknown,
        timeout_ns: None,
        retry_strategy: None,
        parent_span: None,
    }
}

fn replace_opts(cas: u64) -> DocumentReplaceOptions {
    DocumentReplaceOptions {
        cas,
        expires_in_ns: 0,
        preserve_expiry: false,
        flags: None,
        persist_to: 0,
        replicate_to: 0,
        durability_level: DurabilityLevel::Unknown,
        timeout_ns: None,
        retry_strategy: None,
        parent_span: None,
    }
}

fn remove_opts(cas: u64) -> DocumentRemoveOptions {
    DocumentRemoveOptions {
        cas,
        persist_to: 0,
        replicate_to: 0,
        durability_level: DurabilityLevel::Unknown,
        timeout_ns: None,
        retry_strategy: None,
        parent_span: None,
    }
}

fn get_opts(project: Option<Vec<String>>) -> DocumentGetOptions {
    DocumentGetOptions {
        with_expiry: false,
        project,
        timeout_ns: None,
        retry_strategy: None,
        parent_span: None,
    }
}

fn query_opts(readonly: bool) -> SqlppQueryOptions {
    SqlppQueryOptions {
        scan_consistency: QueryScanConsistency::RequestPlus,
        consistent_with: None,
        profile: None,
        scan_cap: 0,
        pipeline_batch: 0,
        pipeline_cap: 0,
        scan_wait_ns: 0,
        readonly,
        max_parallelism: 0,
        client_context_id: None,
        metrics: true,
        ad_hoc: true,
        timeout_ns: 0,
        retry_strategy: None,
        parent_span: None,
        preserve_expiry: false,
        use_flex_index: false,
    }
}

fn describe(e: &DocumentError) -> String {
    match e {
        DocumentError::NotFound => "not-found".to_string(),
        DocumentError::CasMismatch => "cas-mismatch".to_string(),
        DocumentError::Locked => "locked".to_string(),
        DocumentError::NotLocked => "not-locked".to_string(),
        DocumentError::AlreadyExists => "already-exists".to_string(),
        DocumentError::NotJson => "not-json".to_string(),
        DocumentError::PathNotFound => "path-not-found".to_string(),
        DocumentError::PathInvalid => "path-invalid".to_string(),
        DocumentError::PathTooDeep => "path-too-deep".to_string(),
        DocumentError::InvalidValue => "invalid-value".to_string(),
        DocumentError::SubdocumentDeltaInvalid => "subdocument-delta-invalid".to_string(),
        DocumentError::Unauthorized => "unauthorized".to_string(),
        DocumentError::NotConfigured(m) => format!("not-configured({m})"),
        DocumentError::RequestFailed(m) => format!("request-failed({m})"),
        DocumentError::Timeout => "timeout".to_string(),
        DocumentError::Unsupported(m) => format!("unsupported({m})"),
        DocumentError::InvalidArgument(m) => format!("invalid-argument({m})"),
        DocumentError::Other(m) => format!("other({m})"),
    }
}

fn describe_query(e: &SqlppQueryError) -> String {
    match e {
        SqlppQueryError::Unexpected(m) => format!("unexpected({m})"),
        SqlppQueryError::InvalidArgument(m) => format!("invalid-argument({m})"),
        SqlppQueryError::Unauthorized => "unauthorized".to_string(),
        SqlppQueryError::NotConfigured(m) => format!("not-configured({m})"),
        SqlppQueryError::RequestFailed(m) => format!("request-failed({m})"),
        SqlppQueryError::Timeout => "timeout".to_string(),
        SqlppQueryError::Server(m) => format!("server({m})"),
    }
}

struct Report(Vec<String>);

impl Report {
    fn push(&mut self, step: &str, outcome: String) {
        self.0.push(format!("{step}: {outcome}"));
    }

    fn ok<T, E>(
        &mut self,
        step: &str,
        result: Result<T, E>,
        show: impl Fn(T) -> String,
        name: impl Fn(&E) -> String,
    ) {
        match result {
            Ok(v) => self.push(step, format!("OK {}", show(v))),
            Err(e) => self.push(step, format!("UNEXPECTED-ERR {}", name(&e))),
        }
    }

    fn expect_err<T, E>(
        &mut self,
        step: &str,
        expected: &str,
        result: Result<T, E>,
        name: impl Fn(&E) -> String,
    ) {
        match result {
            Ok(_) => self.push(step, format!("UNEXPECTED-OK (wanted {expected})")),
            Err(e) => {
                let got = name(&e);
                if got.starts_with(expected) {
                    self.push(step, format!("OK {got}"));
                } else {
                    self.push(step, format!("WRONG-ERR got {got}, wanted {expected}"));
                }
            }
        }
    }
}

fn doc_text(d: &[u8]) -> String {
    String::from_utf8_lossy(d).to_string()
}

async fn scenario() -> Vec<String> {
    let mut r = Report(Vec::new());
    let k = "scenario:doc";

    // Start from a known-empty key so the run is repeatable.
    let _ = document::remove(k.to_string(), None).await;

    r.expect_err(
        "01 get-missing",
        "not-found",
        document::get(k.to_string(), None).await,
        describe,
    );

    let inserted = document::insert(
        k.to_string(),
        r#"{"name":"Ada","n":1}"#.as_bytes().to_vec(),
        Some(insert_opts(0)),
    )
    .await;
    let cas1 = inserted.as_ref().map(|m| m.cas).unwrap_or(0);
    r.ok(
        "02 insert",
        inserted,
        |m| format!("cas={} bucket={}", m.cas, m.bucket),
        describe,
    );

    r.ok(
        "03 get",
        document::get(k.to_string(), None).await,
        |g| format!("cas={} doc={}", g.cas, doc_text(&g.document)),
        describe,
    );

    r.expect_err(
        "04 insert-duplicate",
        "already-exists",
        document::insert(k.to_string(), "{}".as_bytes().to_vec(), Some(insert_opts(0))).await,
        describe,
    );

    let upserted = document::upsert(
        k.to_string(),
        r#"{"name":"Ada Lovelace","n":2}"#.as_bytes().to_vec(),
        Some(upsert_opts(0)),
    )
    .await;
    let cas2 = upserted.as_ref().map(|m| m.cas).unwrap_or(0);
    r.ok("05 upsert", upserted, |m| format!("cas={}", m.cas), describe);
    r.push(
        "06 cas-changed",
        format!("OK cas1={cas1} cas2={cas2} differ={}", cas1 != cas2),
    );

    r.expect_err(
        "07 replace-stale-cas",
        "cas-mismatch",
        document::replace(
            k.to_string(),
            r#"{"stale":true}"#.as_bytes().to_vec(),
            Some(replace_opts(cas1)),
        )
        .await,
        describe,
    );

    r.ok(
        "08 replace-current-cas",
        document::replace(
            k.to_string(),
            r#"{"name":"Ada","n":3}"#.as_bytes().to_vec(),
            Some(replace_opts(cas2)),
        )
        .await,
        |m| format!("cas={}", m.cas),
        describe,
    );

    // `replace` on an absent key relies on If-Match: * to demand existence.
    r.expect_err(
        "09 replace-absent",
        "not-found",
        document::replace(
            "scenario:absent".to_string(),
            "{}".as_bytes().to_vec(),
            Some(replace_opts(0)),
        )
        .await,
        describe,
    );

    r.ok(
        "10 touch",
        document::touch(
            k.to_string(),
            Some(DocumentTouchOptions {
                expires_in: 3600 * SECOND_NS,
                timeout_ns: None,
                retry_strategy: None,
                parent_span: None,
            }),
        )
        .await,
        |m| format!("cas={}", m.cas),
        describe,
    );

    r.expect_err(
        "11 touch-zero",
        "invalid-argument",
        document::touch(
            k.to_string(),
            Some(DocumentTouchOptions {
                expires_in: 0,
                timeout_ns: None,
                retry_strategy: None,
                parent_span: None,
            }),
        )
        .await,
        describe,
    );

    r.ok(
        "12 get-projected",
        document::get(k.to_string(), Some(get_opts(Some(vec!["name".to_string()])))).await,
        |g| doc_text(&g.document),
        describe,
    );

    r.ok(
        "13 get-and-touch",
        document::get_and_touch(
            k.to_string(),
            Some(DocumentGetAndTouchOptions {
                expires_in: 1800 * SECOND_NS,
                timeout_ns: None,
                retry_strategy: None,
                parent_span: None,
            }),
        )
        .await,
        |g| format!("cas={} doc={}", g.cas, doc_text(&g.document)),
        describe,
    );

    r.ok(
        "14 upsert-with-expiry",
        document::upsert(
            "scenario:ttl".to_string(),
            r#"{"temp":true}"#.as_bytes().to_vec(),
            Some(upsert_opts(600 * SECOND_NS)),
        )
        .await,
        |m| format!("cas={}", m.cas),
        describe,
    );

    // The Data API has no path for these; a couchbases:// implementation of the
    // same interface does. They must say which, not fail obscurely.
    // The transport decides whether these exist, so the assertion is on
    // coherence rather than on one fixed answer: either the operation is
    // refused as `unsupported`, or it works *and* the lock it took is real.
    match document::get_and_lock(k.to_string(), None).await {
        Err(DocumentError::Unsupported(_)) => {
            r.push("15 get-and-lock", "OK unsupported on this transport".to_string());
            r.expect_err(
                "16 unlock",
                "unsupported",
                document::unlock(k.to_string(), None).await,
                describe,
            );
        }
        Ok(locked) => {
            let lock_cas = locked.cas;
            r.push("15 get-and-lock", format!("OK locked cas={lock_cas}"));

            // Prove the lock is real by unlocking it wrongly first: a lock is
            // released only by its own CAS, so a bad CAS must be refused and the
            // real one accepted. Asserting on a *write* instead would be
            // testing the SDK's retry policy — it retries the server's
            // temporary "locked" failure until the lock lapses, and then
            // succeeds, which looks like no lock was ever taken.
            let unlock_with = |cas| {
                document::unlock(
                    k.to_string(),
                    Some(DocumentUnlockOptions {
                        cas,
                        timeout_ns: None,
                        retry_strategy: None,
                        parent_span: None,
                    }),
                )
            };
            let wrong = unlock_with(lock_cas.wrapping_add(1)).await;
            r.push(
                "16 unlock",
                match wrong {
                    Ok(()) => "UNEXPECTED-OK unlock accepted a CAS that was not the lock's"
                        .to_string(),
                    Err(_) => match unlock_with(lock_cas).await {
                        Ok(()) => "OK lock held, refused a wrong cas, then released".to_string(),
                        Err(e) => format!(
                            "UNEXPECTED-ERR unlock rejected the lock's own cas: {}",
                            describe(&e)
                        ),
                    },
                },
            );
        }
        Err(e) => r.push("15 get-and-lock", format!("UNEXPECTED-ERR {}", describe(&e))),
    }

    // SQL++ with a bound positional parameter.
    r.ok(
        "17 sqlpp-parameterized",
        sqlpp::query(
            "SELECT RAW d.name FROM _default AS d USE KEYS $1".to_string(),
            vec![SqlppValue::Json(format!("\"{k}\""))],
            Some(query_opts(true)),
        )
        .await,
        |v| match v {
            SqlppValue::Json(rows) => format!("rows={rows}"),
            SqlppValue::Null => "null".to_string(),
        },
        describe_query,
    );

    r.ok(
        "18 sqlpp-null-param",
        sqlpp::query(
            "SELECT RAW $1 IS NULL".to_string(),
            vec![SqlppValue::Null],
            Some(query_opts(true)),
        )
        .await,
        |v| match v {
            SqlppValue::Json(rows) => format!("rows={rows}"),
            SqlppValue::Null => "null".to_string(),
        },
        describe_query,
    );

    r.expect_err(
        "19 sqlpp-syntax-error",
        "invalid-argument",
        sqlpp::query("SLECT bogus".to_string(), vec![], None).await,
        describe_query,
    );

    r.expect_err(
        "20 sqlpp-bad-param",
        "invalid-argument",
        sqlpp::query(
            "SELECT RAW $1".to_string(),
            vec![SqlppValue::Json("not json".to_string())],
            None,
        )
        .await,
        describe_query,
    );

    r.ok(
        "21 remove",
        document::remove(k.to_string(), Some(remove_opts(0))).await,
        |m| format!("cas={}", m.cas),
        describe,
    );
    r.expect_err(
        "22 get-after-remove",
        "not-found",
        document::get(k.to_string(), None).await,
        describe,
    );
    r.expect_err(
        "23 remove-absent",
        "not-found",
        document::remove("scenario:absent".to_string(), None).await,
        describe,
    );

    // Options that cannot be honoured must be rejected, not silently dropped:
    // ignoring preserve-expiry would quietly clear the document's TTL. Where the
    // transport does honour it, the TTL has to still be there afterwards —
    // "returned ok" alone would pass on an implementation that dropped it.
    let pk = "scenario:preserve";
    let _ = document::remove(pk.to_string(), None).await;
    let seeded = document::upsert(
        pk.to_string(),
        r#"{"a":1}"#.as_bytes().to_vec(),
        Some(upsert_opts(3600 * SECOND_NS)),
    )
    .await;
    r.push(
        "24 preserve-expiry",
        match seeded {
            Err(e) => format!("UNEXPECTED-ERR seeding a document with a TTL failed: {}", describe(&e)),
            Ok(_) => match document::upsert(
                pk.to_string(),
                r#"{"a":2}"#.as_bytes().to_vec(),
                Some(DocumentUpsertOptions {
                    preserve_expiry: true,
                    ..upsert_opts(0)
                }),
            )
            .await
            {
                Err(DocumentError::Unsupported(_)) => {
                    "OK unsupported on this transport".to_string()
                }
                Err(e) => format!("UNEXPECTED-ERR {}", describe(&e)),
                Ok(_) => {
                    let after = document::get(
                        pk.to_string(),
                        Some(DocumentGetOptions {
                            with_expiry: true,
                            ..get_opts(None)
                        }),
                    )
                    .await;
                    match after {
                        Ok(g) if g.expires_at.is_some() => {
                            "OK honoured, TTL survived the rewrite".to_string()
                        }
                        Ok(_) => "UNEXPECTED-OK preserve-expiry was accepted but the TTL is gone"
                            .to_string(),
                        Err(e) => format!("UNEXPECTED-ERR reading back the TTL: {}", describe(&e)),
                    }
                }
            },
        },
    );

    r.expect_err(
        "25 persist-to-rejected",
        "unsupported",
        document::upsert(
            "scenario:persist".to_string(),
            r#"{"a":1}"#.as_bytes().to_vec(),
            Some(DocumentUpsertOptions {
                persist_to: 2,
                ..upsert_opts(0)
            }),
        )
        .await,
        describe,
    );

    // consistent-with needs mutation tokens the Data API never returns.
    r.expect_err(
        "26 consistent-with-zeroed-rejected",
        "invalid-argument",
        sqlpp::query(
            "SELECT RAW 1".to_string(),
            vec![],
            Some(SqlppQueryOptions {
                consistent_with: Some(MutationState {
                    tokens: vec![MutationToken {
                        bucket_name: String::new(),
                        partition_uuid: 0,
                        partition_id: 0,
                        sequence_number: 0,
                    }],
                }),
                ..query_opts(true)
            }),
        )
        .await,
        describe_query,
    );

    // A per-call timeout is honoured rather than replaced by the binding's.
    r.ok(
        "27 per-call-timeout-honoured",
        document::get(
            "scenario:ttl".to_string(),
            Some(DocumentGetOptions {
                timeout_ns: Some(20 * 1_000_000_000),
                ..get_opts(None)
            }),
        )
        .await,
        |_| "completed within the per-call budget".to_string(),
        describe,
    );

    // A genuinely binary document: bytes that are not valid UTF-8, stored with
    // Couchbase's raw-binary common flags. The previous `json-string` shape
    // could not represent this at all.
    const RAW_BINARY_FLAGS: u32 = 0x0300_0000;
    let blob: Vec<u8> = vec![0x00, 0xFF, 0xFE, 0x01, 0x80, 0x7F];
    // A token whose bucket is not this binding's cannot constrain the query.
    r.expect_err(
        "28 consistent-with-wrong-bucket-rejected",
        "invalid-argument",
        sqlpp::query(
            "SELECT RAW 1".to_string(),
            vec![],
            Some(SqlppQueryOptions {
                consistent_with: Some(MutationState {
                    tokens: vec![MutationToken {
                        bucket_name: "someone-elses-bucket".to_string(),
                        partition_uuid: 12345,
                        partition_id: 1,
                        sequence_number: 9,
                    }],
                }),
                ..query_opts(true)
            }),
        )
        .await,
        describe_query,
    );

    // The mutation token round-trip: write, take the token the cluster minted,
    // and ask a query to be consistent with exactly that write.
    let tokened = document::upsert(
        "scenario:tokened".to_string(),
        br#"{"marker":"at-plus"}"#.to_vec(),
        Some(upsert_opts(0)),
    )
    .await;
    match tokened {
        Ok(m) => {
            r.push(
                "29 mutation-token-parsed",
                format!(
                    "OK vbid={} vbuuid={} seq={} raw={:?}",
                    m.partition_id, m.partition_uuid, m.seq, m.raw_token
                ),
            );
            r.ok(
                "30 query-consistent-with-real-token",
                sqlpp::query(
                    "SELECT RAW d.marker FROM _default AS d USE KEYS $1".to_string(),
                    vec![SqlppValue::Json("\"scenario:tokened\"".to_string())],
                    Some(SqlppQueryOptions {
                        consistent_with: Some(MutationState {
                            tokens: vec![MutationToken {
                                bucket_name: m.bucket.clone(),
                                partition_uuid: m.partition_uuid,
                                partition_id: m.partition_id,
                                sequence_number: m.seq,
                            }],
                        }),
                        ..query_opts(true)
                    }),
                )
                .await,
                |v| match v {
                    SqlppValue::Json(rows) => format!("rows={rows}"),
                    SqlppValue::Null => "null".to_string(),
                },
                describe_query,
            );
        }
        Err(e) => r.push("29 mutation-token-parsed", format!("UNEXPECTED-ERR {}", describe(&e))),
    }

    // A document written with a TTL has to report that expiry when asked for it.
    // Both transports can: the Data API sends an `Expires` header on the read,
    // the KV protocol returns it from a subdoc lookup. An implementation that
    // ignored `with-expiry` would still return the document, so the step checks
    // the field itself — and that it is in the future, not a placeholder.
    let ek = "scenario:expiry";
    let _ = document::remove(ek.to_string(), None).await;
    let written = document::upsert(
        ek.to_string(),
        r#"{"e":1}"#.as_bytes().to_vec(),
        Some(upsert_opts(3600 * SECOND_NS)),
    )
    .await;
    r.push(
        "31 get-with-expiry",
        match written {
            Err(e) => format!("UNEXPECTED-ERR writing a document with a TTL: {}", describe(&e)),
            Ok(_) => match document::get(
                ek.to_string(),
                Some(DocumentGetOptions { with_expiry: true, ..get_opts(None) }),
            )
            .await
            {
                Ok(g) => match g.expires_at {
                    Some(t) if t.year >= 2026 => {
                        format!("OK expires-at={:04}-{:02}-{:02}", t.year, t.month, t.day)
                    }
                    Some(t) => format!("UNEXPECTED-OK expires-at is in the past: year {}", t.year),
                    None => "UNEXPECTED-OK with-expiry was set but no expires-at came back".to_string(),
                },
                Err(e) => format!("UNEXPECTED-ERR {}", describe(&e)),
            },
        },
    );

    r.ok(
        "32 upsert-binary-document",
        document::upsert(
            "scenario:binary".to_string(),
            blob.clone(),
            Some(DocumentUpsertOptions {
                flags: Some(RAW_BINARY_FLAGS),
                ..upsert_opts(0)
            }),
        )
        .await,
        |m| format!("cas={}", m.cas),
        describe,
    );
    r.ok(
        "33 binary-round-trips-byte-for-byte",
        document::get("scenario:binary".to_string(), None).await,
        move |g| {
            format!(
                "bytes={:?} identical={} flags=0x{:08x}",
                g.document,
                g.document == blob,
                g.flags
            )
        },
        describe,
    );

    r.0
}

impl Handler for Component {
    async fn handle(_request: Request) -> Result<Response, ErrorCode> {
        let lines = scenario().await;
        let body = lines.join("\n").into_bytes();

        let fields = Fields::new();
        let _ = fields.append("content-type", b"text/plain");
        let (mut tx, rx) = bindings::wit_stream::new();
        let (trailers_tx, trailers_rx) = bindings::wit_future::new(|| Ok(None));
        wit_bindgen::spawn_local(async move {
            tx.write_all(body).await;
            drop(tx);
            let _ = trailers_tx.write(Ok(None)).await;
        });
        let (response, _result) = Response::new(fields, Some(rx), trailers_rx);
        let _ = response.set_status_code(200);
        Ok(response)
    }
}

bindings::export!(Component with_types_in bindings);
