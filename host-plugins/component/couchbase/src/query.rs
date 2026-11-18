//! Query consistency shared by the Data API and sockets transports.

use std::collections::BTreeMap;

use serde_json::Value;

/// Quote each path component independently: a dot in a bucket name is data,
/// not another element of the query context. SQL++ escapes a backtick by doubling it.
pub fn context(bucket: &str, scope: &str) -> String {
    format!(
        "default:`{}`.`{}`",
        bucket.replace('`', "``"),
        scope.replace('`', "``")
    )
}

#[derive(Debug, PartialEq, Eq)]
pub enum Failure {
    InvalidArgument(String),
    Unauthorized,
    Timeout,
    Server(String),
    Unexpected(String),
}

fn http_failure(status: u16, message: String) -> Failure {
    match status {
        400 | 422 => Failure::InvalidArgument(message),
        401 | 403 => Failure::Unauthorized,
        408 | 504 => Failure::Timeout,
        _ => Failure::Server(format!("HTTP {status}: {message}")),
    }
}

/// Decode a Query Service envelope, checking both its HTTP status and its
/// statement-level errors before treating absent results as an empty result.
pub fn parse_response(status: u16, payload: &[u8]) -> Result<Value, Failure> {
    let successful = (200..300).contains(&status);
    let parsed: Value = serde_json::from_slice(payload).map_err(|error| {
        if successful {
            Failure::Unexpected(format!("the query service did not answer JSON: {error}"))
        } else {
            http_failure(status, String::from_utf8_lossy(payload).into_owned())
        }
    })?;
    if let Some(error) = parsed
        .get("errors")
        .and_then(Value::as_array)
        .and_then(|e| e.first())
    {
        let message = error
            .get("msg")
            .and_then(Value::as_str)
            .unwrap_or("the query failed")
            .to_string();
        return Err(match error.get("code").and_then(Value::as_u64) {
            Some(3000) => Failure::InvalidArgument(message),
            Some(12003) | Some(12021) => Failure::InvalidArgument(format!(
                "the statement referenced a keyspace that does not exist: {message}"
            )),
            Some(13014) => Failure::Unauthorized,
            _ => http_failure(status, message),
        });
    }
    if !successful {
        let message = parsed
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| parsed.to_string());
        return Err(http_failure(status, message));
    }
    if !parsed.is_object() {
        return Err(Failure::Unexpected(
            "the query service did not return an object".to_string(),
        ));
    }
    match parsed.get("results") {
        Some(Value::Array(rows)) => Ok(Value::Array(rows.clone())),
        None => Ok(Value::Array(Vec::new())),
        Some(_) => Err(Failure::Unexpected(
            "query results are not an array".to_string(),
        )),
    }
}

/// Build a sparse scan vector from `(partition, uuid, sequence)` tokens.
///
/// A partition must wait for its highest sequence, independent of token order.
/// Different UUIDs describe different histories and cannot safely be merged.
pub fn scan_vector(tokens: impl IntoIterator<Item = (u64, u64, u64)>) -> Option<Value> {
    let mut partitions = BTreeMap::<u64, (u64, u64)>::new();
    for (partition, uuid, sequence) in tokens {
        if uuid == 0 {
            return None;
        }
        let entry = partitions.entry(partition).or_insert((uuid, sequence));
        if entry.0 != uuid {
            return None;
        }
        entry.1 = entry.1.max(sequence);
    }
    if partitions.is_empty() {
        return None;
    }
    Some(Value::Object(
        partitions
            .into_iter()
            .map(|(partition, (uuid, sequence))| {
                (
                    partition.to_string(),
                    serde_json::json!([sequence, uuid.to_string()]),
                )
            })
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_preserves_identifier_boundaries() {
        assert_eq!(
            context("test.bucket", "inventory"),
            "default:`test.bucket`.`inventory`"
        );
        assert_eq!(context("a`b", "c.d"), "default:`a``b`.`c.d`");
    }

    #[test]
    fn http_failures_without_a_query_error_array_cannot_become_empty_results() {
        let payload = br#"{"message":"query service temporarily unavailable"}"#;
        assert!(matches!(
            parse_response(503, payload),
            Err(Failure::Server(_))
        ));
        assert_eq!(parse_response(401, payload), Err(Failure::Unauthorized));
        assert_eq!(parse_response(504, payload), Err(Failure::Timeout));
        assert!(matches!(
            parse_response(503, b"unavailable"),
            Err(Failure::Server(_))
        ));
    }

    #[test]
    fn statement_errors_and_successful_empty_mutations_are_distinguished() {
        assert!(matches!(
            parse_response(200, br#"{"errors":[{"code":3000,"msg":"syntax"}]}"#),
            Err(Failure::InvalidArgument(_))
        ));
        assert!(matches!(
            parse_response(200, br#"{"errors":[{"code":5000,"msg":"failure"}]}"#),
            Err(Failure::Server(_))
        ));
        assert_eq!(
            parse_response(200, br#"{"results":[1,2]}"#),
            Ok(serde_json::json!([1, 2]))
        );
        assert_eq!(
            parse_response(200, br#"{"status":"success"}"#),
            Ok(serde_json::json!([]))
        );
        assert!(matches!(
            parse_response(200, b"[]"),
            Err(Failure::Unexpected(_))
        ));
    }

    #[test]
    fn repeated_partitions_keep_the_highest_sequence_in_any_order() {
        let expected = serde_json::json!({"5": [100, "9"], "6": [7, "10"]});
        for tokens in [
            [(5, 9, 100), (6, 10, 7), (5, 9, 90)],
            [(5, 9, 90), (6, 10, 7), (5, 9, 100)],
        ] {
            assert_eq!(scan_vector(tokens), Some(expected.clone()));
        }
    }

    #[test]
    fn incompatible_histories_and_missing_tokens_are_refused() {
        assert!(scan_vector([]).is_none());
        assert!(scan_vector([(5, 0, 100)]).is_none());
        assert!(scan_vector([(5, 9, 100), (5, 10, 90)]).is_none());
        assert!(scan_vector([(5, 10, 90), (5, 9, 100)]).is_none());
    }
}
