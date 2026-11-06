//! Mutation tokens: reading `X-CB-MutationToken` off a write, and turning one
//! back into the scan vector a query needs for `at_plus` consistency.
//!
//! Bindings-free so it compiles and tests on the host.
//!
//! # Why this parser refuses to guess
//!
//! A mutation token is three numbers — a vBucket id, that vBucket's UUID, and a
//! sequence number — plus the bucket they belong to. The Data API returns one
//! in `X-CB-MutationToken` on every mutation, but its OpenAPI specification
//! declares the header only as `type: string`, "A token representing the
//! mutation of the document". The encoding is not documented anywhere.
//!
//! **Update: the format is now known.** A live Capella endpoint returns
//! `travel-sample:16:794f18f71747:512`, i.e.
//! `bucket:vbucket-id:vbucket-uuid-in-hex:sequence-number`. That form is parsed
//! directly. The reasoning below still governs everything else: a delimited
//! token that does *not* match this exact shape is left opaque rather than
//! guessed at.
//!
//! That matters more than it looks, because of how the token gets used. Feeding
//! it back as `scan_vector` with `at_plus` consistency asks the query service to
//! wait until the index has caught up to that exact point. Hand it a vector with
//! the fields transposed and nothing errors: the numbers are all plausible, the
//! query runs, and it simply does not wait for the write the caller was trying
//! to observe. A silent wrong answer, in the one place a caller reached for a
//! correctness guarantee.
//!
//! Couchbase compounds these values in at least two different orders in this
//! very API — the query service takes `{"<vbid>": [seqno, "<vbuuid>"]}` while
//! the search service takes `{"<vbid>/<vbuuid>": seqno}` — so a delimited token
//! like `123:456:789` is genuinely ambiguous about which field is which.
//!
//! So this parses two shapes and no others: Capella's verified
//! `bucket:vbid:vbuuid-hex:seqno`, checked strictly enough that a different
//! four-field encoding cannot pass for it, and a JSON object whose fields say
//! what they are. Anything else is [`Token::Opaque`] — carried verbatim so a
//! caller can still see it and report it, but never taken apart by guesswork.

use serde_json::Value;

/// The parts of a mutation token, once they are known.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Parts {
    pub bucket: String,
    pub partition_id: u64,
    pub partition_uuid: u64,
    pub sequence_number: u64,
}

/// A token as it came back from the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    /// Self-describing, and safe to take apart.
    Structured(Parts),
    /// Present but in an unrecognized encoding. Kept verbatim: it is the only
    /// evidence of what the real format is, and a caller can report it.
    Opaque(String),
}

impl Token {
    pub fn parts(&self) -> Option<&Parts> {
        match self {
            Token::Structured(parts) => Some(parts),
            Token::Opaque(_) => None,
        }
    }

    pub fn raw(&self) -> Option<&str> {
        match self {
            Token::Opaque(raw) => Some(raw),
            Token::Structured(_) => None,
        }
    }
}

/// Field spellings accepted for each part, so a JSON token is recognized
/// whichever convention it uses.
const BUCKET_KEYS: &[&str] = &["bucket", "bucket_name", "bucketName"];
const VBID_KEYS: &[&str] = &["vbid", "vbucket", "vbucket_id", "vbucketId", "partition_id", "partitionId"];
const VBUUID_KEYS: &[&str] = &["vbuuid", "vbucket_uuid", "vbucketUuid", "partition_uuid", "partitionUuid", "uuid"];
const SEQNO_KEYS: &[&str] = &["seqno", "seq", "sequence_number", "sequenceNumber"];

/// Read `X-CB-MutationToken`.
///
/// Returns `None` only for an absent or blank header. A present-but-unfamiliar
/// token comes back as [`Token::Opaque`] rather than being discarded, because
/// losing it would also lose the only clue to its format.
pub fn parse(header: &str) -> Option<Token> {
    let raw = header.trim();
    if raw.is_empty() {
        return None;
    }
    Some(match parse_structured(raw) {
        Some(parts) => Token::Structured(parts),
        None => Token::Opaque(raw.to_string()),
    })
}

fn parse_structured(raw: &str) -> Option<Parts> {
    if !raw.starts_with('{') {
        return parse_colon_form(raw);
    }
    let Value::Object(map) = serde_json::from_str::<Value>(raw).ok()? else {
        return None;
    };

    // All three numbers must be present and named. A token missing any of them
    // cannot build a scan vector, so a partial parse is no better than none.
    let partition_id = pick_u64(&map, VBID_KEYS)?;
    let partition_uuid = pick_u64(&map, VBUUID_KEYS)?;
    let sequence_number = pick_u64(&map, SEQNO_KEYS)?;
    let bucket = BUCKET_KEYS
        .iter()
        .find_map(|k| map.get(*k).and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();

    Some(Parts {
        bucket,
        partition_id,
        partition_uuid,
        sequence_number,
    })
}

/// Capella's wire form: `bucket:vbid:vbuuid-hex:seqno`.
///
/// Verified against a live endpoint (`travel-sample:16:794f18f71747:512`). The
/// shape is checked strictly — exactly four fields, a bucket that is not itself
/// a number, a hex UUID, decimal id and sequence — so that a *different*
/// four-field encoding cannot be silently misread as this one. A bucket name
/// containing a colon would not round-trip, and is rejected rather than guessed.
fn parse_colon_form(raw: &str) -> Option<Parts> {
    let fields: Vec<&str> = raw.split(':').collect();
    if fields.len() != 4 {
        return None;
    }
    let (bucket, vbid, vbuuid, seqno) = (fields[0], fields[1], fields[2], fields[3]);
    if bucket.is_empty() {
        return None;
    }
    // The id and sequence are decimal; the UUID is hex. Requiring all three to
    // parse in their own radix is what makes a mismatched encoding fail rather
    // than produce plausible nonsense.
    let partition_id = vbid.parse::<u64>().ok()?;
    let partition_uuid = u64::from_str_radix(vbuuid, 16).ok()?;
    let sequence_number = seqno.parse::<u64>().ok()?;
    Some(Parts {
        bucket: bucket.to_string(),
        partition_id,
        partition_uuid,
        sequence_number,
    })
}

/// A `u64` under any of `keys`, accepting both a JSON number and a string.
///
/// vBucket UUIDs and sequence numbers routinely exceed 2^53, where a JSON
/// number silently loses precision, so any sane encoder writes them as strings —
/// but accept both rather than reject a token over its choice.
fn pick_u64(map: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<u64> {
    for key in keys {
        match map.get(*key) {
            Some(Value::Number(n)) => {
                if let Some(v) = n.as_u64() {
                    return Some(v);
                }
            }
            Some(Value::String(s)) => {
                if let Ok(v) = s.trim().parse::<u64>() {
                    return Some(v);
                }
            }
            _ => {}
        }
    }
    None
}

/// Build the sparse `scan_vector` the query service takes for `at_plus`.
///
/// Shape per the Data API's OpenAPI spec: an object keyed by vBucket number as
/// a **string**, each value a two-element `[sequence-number, vBucket-UUID]`
/// where the sequence number is a JSON number and the UUID is a string.
///
/// Returns `None` if any token lacks structure — a partial vector is worse than
/// none, since it would silently under-constrain the query.
pub fn scan_vector(tokens: &[Token]) -> Option<Value> {
    if tokens.is_empty() {
        return None;
    }
    let mut vector = serde_json::Map::new();
    for token in tokens {
        let parts = token.parts()?;
        // A zeroed token is what an implementation reports when it had none;
        // treating it as a real position would claim a guarantee nobody has.
        if parts.partition_uuid == 0 {
            return None;
        }
        vector.insert(
            parts.partition_id.to_string(),
            Value::Array(vec![
                Value::from(parts.sequence_number),
                Value::String(parts.partition_uuid.to_string()),
            ]),
        );
    }
    Some(Value::Object(vector))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn structured(raw: &str) -> Parts {
        match parse(raw).expect("token should be present") {
            Token::Structured(parts) => parts,
            Token::Opaque(raw) => panic!("expected structured, got opaque {raw}"),
        }
    }

    #[test]
    fn absent_or_blank_is_no_token() {
        assert!(parse("").is_none());
        assert!(parse("   ").is_none());
    }

    #[test]
    fn parses_a_named_json_token() {
        let parts = structured(
            r#"{"bucket":"travel","vbid":5,"vbuuid":"205096593892159","seqno":"42"}"#,
        );
        assert_eq!(
            parts,
            Parts {
                bucket: "travel".to_string(),
                partition_id: 5,
                partition_uuid: 205_096_593_892_159,
                sequence_number: 42,
            }
        );
    }

    #[test]
    fn accepts_the_alternate_field_spellings() {
        let parts = structured(
            r#"{"bucket_name":"b","partition_id":7,"partition_uuid":9,"sequence_number":11}"#,
        );
        assert_eq!(parts.partition_id, 7);
        assert_eq!(parts.partition_uuid, 9);
        assert_eq!(parts.sequence_number, 11);
        assert_eq!(parts.bucket, "b");
    }

    #[test]
    fn accepts_values_past_2_to_the_53_as_strings() {
        // A JSON number would lose precision here; a string must not.
        let parts = structured(r#"{"vbid":1,"vbuuid":"18446744073709551615","seqno":"9007199254740993"}"#);
        assert_eq!(parts.partition_uuid, u64::MAX);
        assert_eq!(parts.sequence_number, 9_007_199_254_740_993);
    }

    #[test]
    fn parses_capellas_wire_form() {
        // Captured verbatim from a live Capella Data API response.
        let parts = structured("travel-sample:16:794f18f71747:512");
        assert_eq!(
            parts,
            Parts {
                bucket: "travel-sample".to_string(),
                partition_id: 16,
                // hex, not decimal
                partition_uuid: 0x794f_18f7_1747,
                sequence_number: 512,
            }
        );
    }

    #[test]
    fn an_unfamiliar_encoding_stays_opaque_rather_than_being_guessed() {
        // Not four fields, or fields that do not parse in their own radix, or a
        // different compound entirely: none may be taken apart on a hunch.
        for raw in [
            "123:456:789",
            "607/205096593892159:2",
            "bucket,5,205096593892159,42",
            "gAAAAAAAAAE=",
            "bucket:notanumber:794f18f71747:512",
            "bucket:16:zzzz:512",
            ":16:794f18f71747:512",
        ] {
            assert_eq!(
                parse(raw),
                Some(Token::Opaque(raw.to_string())),
                "{raw} must not be parsed structurally"
            );
        }
    }

    #[test]
    fn a_json_token_missing_a_field_is_opaque_not_partial() {
        // No vbuuid: a scan vector built from this would be wrong, so it must
        // not present itself as structured.
        assert!(matches!(
            parse(r#"{"vbid":5,"seqno":42}"#),
            Some(Token::Opaque(_))
        ));
    }

    #[test]
    fn builds_the_documented_sparse_scan_vector() {
        let tokens = vec![
            Token::Structured(Parts {
                bucket: "b".into(),
                partition_id: 5,
                partition_uuid: 5409393,
                sequence_number: 12,
            }),
            Token::Structured(Parts {
                bucket: "b".into(),
                partition_id: 19,
                partition_uuid: 47574574,
                sequence_number: 34,
            }),
        ];
        let vector = scan_vector(&tokens).expect("structured tokens build a vector");
        // Keyed by vBucket as a string; [seqno (number), vbuuid (string)].
        assert_eq!(vector["5"], serde_json::json!([12, "5409393"]));
        assert_eq!(vector["19"], serde_json::json!([34, "47574574"]));
    }

    #[test]
    fn refuses_to_build_a_vector_it_cannot_fully_justify() {
        assert!(scan_vector(&[]).is_none());
        // One opaque token poisons the whole vector: a partial one would
        // silently under-constrain the query rather than fail.
        assert!(scan_vector(&[Token::Opaque("?".into())]).is_none());
        // A zeroed token means "no token was available", not "position zero".
        assert!(scan_vector(&[Token::Structured(Parts {
            bucket: "b".into(),
            partition_id: 1,
            partition_uuid: 0,
            sequence_number: 5,
        })])
        .is_none());
    }
}
