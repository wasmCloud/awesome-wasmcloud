//! Turning Data API responses into something the WIT error model can express,
//! plus the small amount of URL and header handling the request path needs.
//!
//! Bindings-free so it compiles and tests on the host. The plugin maps
//! [`Failure`] onto `wasmcloud:couchbase/types.error` at the boundary.

/// A classified failure, one-for-one with the named cases of the WIT `error`
/// variant so the mapping at the boundary is mechanical.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    NotFound,
    AlreadyExists,
    CasMismatch,
    InvalidArgument(String),
    Unauthorized,
    Timeout,
    /// Anything the cluster reported that does not map to a case above.
    Server {
        status: u16,
        code: Option<String>,
        message: String,
    },
}

/// The error body the Data API returns: `{"code": "...", "message": "..."}`.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct ErrorBody {
    code: Option<String>,
    message: Option<String>,
    /// The Query Service passthrough reports failures in an `errors` array
    /// instead of a top-level code/message pair.
    errors: Vec<QueryError>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct QueryError {
    code: Option<u32>,
    msg: Option<String>,
}

/// Classify a non-2xx Data API response.
///
/// Couchbase's machine-readable `code` decides wherever it is present, because
/// HTTP status alone is ambiguous: a 409 is both "you tried to insert over an
/// existing document" and "your CAS was stale", which callers must handle
/// differently. Status is the fallback when no code comes back.
pub fn classify(status: u16, body: &[u8]) -> Failure {
    let parsed: ErrorBody = serde_json::from_slice(body).unwrap_or_default();

    let message = parsed
        .message
        .clone()
        .or_else(|| parsed.errors.first().and_then(|e| e.msg.clone()))
        .unwrap_or_else(|| {
            // Fall back to the raw body so an unparseable error is still
            // diagnosable, but never let a huge payload become the message.
            let text = String::from_utf8_lossy(body).trim().to_string();
            if text.is_empty() {
                format!("Couchbase returned HTTP {status} with no body")
            } else {
                truncate(&text, 512)
            }
        });

    let code = parsed
        .code
        .clone()
        .or_else(|| parsed.errors.first().and_then(|e| e.code.map(|c| c.to_string())));

    if let Some(code) = code.as_deref() {
        match code {
            "DocumentNotFound" | "PathNotFound" | "CollectionNotFound" | "ScopeNotFound" => {
                return Failure::NotFound
            }
            "DocumentExists" => return Failure::AlreadyExists,
            "CasMismatch" => return Failure::CasMismatch,
            "InvalidArgument" => return Failure::InvalidArgument(message),
            "Unauthorized" | "Forbidden" | "AuthenticationFailure" => return Failure::Unauthorized,
            "Timeout" | "UnambiguousTimeout" | "AmbiguousTimeout" => return Failure::Timeout,
            _ => {}
        }
    }

    match status {
        400 | 422 => Failure::InvalidArgument(message),
        401 | 403 => Failure::Unauthorized,
        404 => Failure::NotFound,
        // Without a code to disambiguate, a bare conflict is far more often an
        // insert over an existing key than a stale CAS.
        409 => Failure::AlreadyExists,
        412 => Failure::CasMismatch,
        408 | 504 => Failure::Timeout,
        _ => Failure::Server {
            status,
            code,
            message,
        },
    }
}

/// Read a CAS out of a response's `etag`.
///
/// Couchbase renders the CAS as an ETag, which arrives quoted and may be
/// decimal or `0x`-prefixed hex depending on the endpoint. A value that parses
/// as neither yields `0`, which the interface documents as "no CAS" — better
/// than failing an otherwise successful operation over a header.
pub fn parse_cas(etag: &str) -> u64 {
    let trimmed = etag
        .trim()
        .trim_start_matches("W/")
        .trim_matches('"')
        .trim();

    if let Some(hex) = trimmed.strip_prefix("0x").or_else(|| trimmed.strip_prefix("0X")) {
        return u64::from_str_radix(hex, 16).unwrap_or(0);
    }
    trimmed.parse::<u64>().unwrap_or(0)
}

/// Percent-encode one path segment.
///
/// Document keys are arbitrary byte strings up to 250 bytes, so a key
/// containing `/`, `?`, `#`, or a space would otherwise change which resource
/// the URL addresses. Everything outside the RFC 3986 unreserved set is
/// escaped.
pub fn encode_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// Format a TTL as the Go duration string the Data API's `Expires` header
/// takes. `0` means "no expiry".
pub fn expiry_duration(seconds: u32) -> String {
    format!("{seconds}s")
}

/// The `X-CB-DurabilityLevel` value for a WIT durability level.
///
/// `MajorityAndPersistOnMaster` is the Data API's spelling of the level the
/// Couchbase SDKs call `MajorityAndPersistToActive`; the WIT uses the SDK name.
pub fn durability_header(level_index: u8) -> &'static str {
    match level_index {
        1 => "Majority",
        2 => "MajorityAndPersistOnMaster",
        3 => "PersistToMajority",
        _ => "None",
    }
}

fn truncate(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_string();
    }
    // Cut on a char boundary so the result is still valid UTF-8.
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_code_decides_over_an_ambiguous_status() {
        // 409 covers both conflicts; only the code tells them apart.
        assert_eq!(
            classify(409, br#"{"code":"CasMismatch","message":"stale"}"#),
            Failure::CasMismatch
        );
        assert_eq!(
            classify(409, br#"{"code":"DocumentExists","message":"taken"}"#),
            Failure::AlreadyExists
        );
        // With no code, a bare 409 is read as an insert conflict.
        assert_eq!(classify(409, b""), Failure::AlreadyExists);
    }

    #[test]
    fn maps_statuses_without_a_code() {
        assert_eq!(classify(404, b""), Failure::NotFound);
        assert_eq!(classify(401, b""), Failure::Unauthorized);
        assert_eq!(classify(403, b""), Failure::Unauthorized);
        assert_eq!(classify(412, b""), Failure::CasMismatch);
        assert_eq!(classify(504, b""), Failure::Timeout);
        assert!(matches!(classify(400, b""), Failure::InvalidArgument(_)));
    }

    #[test]
    fn unmapped_statuses_keep_the_cluster_detail() {
        let failure = classify(503, br#"{"code":"Overloaded","message":"try later"}"#);
        assert_eq!(
            failure,
            Failure::Server {
                status: 503,
                code: Some("Overloaded".to_string()),
                message: "try later".to_string(),
            }
        );
    }

    #[test]
    fn reads_query_service_error_arrays() {
        let body = br#"{"errors":[{"code":3000,"msg":"syntax error - line 1, column 7"}],"status":"fatal"}"#;
        assert_eq!(
            classify(500, body),
            Failure::Server {
                status: 500,
                code: Some("3000".to_string()),
                message: "syntax error - line 1, column 7".to_string(),
            }
        );
    }

    #[test]
    fn falls_back_to_the_raw_body_when_json_parsing_fails() {
        let failure = classify(500, b"upstream exploded");
        assert_eq!(
            failure,
            Failure::Server {
                status: 500,
                code: None,
                message: "upstream exploded".to_string(),
            }
        );
    }

    #[test]
    fn an_empty_body_still_produces_a_useful_message() {
        let Failure::Server { message, .. } = classify(599, b"") else {
            panic!("expected a server failure");
        };
        assert!(message.contains("599"), "got: {message}");
    }

    #[test]
    fn truncates_a_runaway_body_on_a_char_boundary() {
        // A multi-byte char straddling the cut must not produce invalid UTF-8;
        // reaching this assertion at all means the slice was valid.
        let body = "é".repeat(1000);
        let Failure::Server { message, .. } = classify(500, body.as_bytes()) else {
            panic!("expected a server failure");
        };
        assert!(message.ends_with('…'));
        assert!(message.len() <= 512 + 3);
    }

    #[test]
    fn parses_cas_in_every_shape_it_arrives_in() {
        assert_eq!(parse_cas("\"1234567890\""), 1_234_567_890);
        assert_eq!(parse_cas("1234567890"), 1_234_567_890);
        assert_eq!(parse_cas("\"0x1f\""), 31);
        assert_eq!(parse_cas("W/\"42\""), 42);
        assert_eq!(parse_cas("  \"7\"  "), 7);
        // Anything unparseable is "no CAS" rather than a failed operation.
        assert_eq!(parse_cas("not-a-cas"), 0);
        assert_eq!(parse_cas(""), 0);
    }

    #[test]
    fn escapes_characters_that_would_change_the_url() {
        assert_eq!(encode_segment("airline_10"), "airline_10");
        assert_eq!(encode_segment("a/b"), "a%2Fb");
        assert_eq!(encode_segment("with space"), "with%20space");
        assert_eq!(encode_segment("q?x=1#y"), "q%3Fx%3D1%23y");
        assert_eq!(encode_segment("safe-._~"), "safe-._~");
        // Non-ASCII is escaped per UTF-8 byte.
        assert_eq!(encode_segment("é"), "%C3%A9");
    }

    #[test]
    fn formats_expiry_and_durability_for_the_wire() {
        assert_eq!(expiry_duration(600), "600s");
        assert_eq!(expiry_duration(0), "0s");
        assert_eq!(durability_header(0), "None");
        assert_eq!(durability_header(2), "MajorityAndPersistOnMaster");
        assert_eq!(durability_header(3), "PersistToMajority");
    }
}
