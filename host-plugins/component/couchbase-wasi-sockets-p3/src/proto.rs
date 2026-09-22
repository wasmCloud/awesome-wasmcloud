//! The Couchbase KV binary protocol: framing, key encoding, and routing.
//!
//! Bindings-free, so it compiles and tests on the host. [`crate::conn`] moves
//! these bytes over a socket; nothing here knows how.
//!
//! # The two rules that are easy to get wrong
//!
//! **Hash the bare key, send the prefixed key.** With `COLLECTIONS`
//! negotiated, a request's key is the collection id as leb128 followed by the
//! document id — but the vbucket is computed from the document id *alone*.
//! Hashing the prefixed form routes to the wrong vbucket.
//!
//! **A wrong vbucket does not always announce itself.** On a single-node
//! cluster the node owns every vbucket, so a misrouted read is answered from
//! the wrong one and comes back `not-found` rather than `not-my-vbucket`. A
//! routing bug therefore looks exactly like a missing document.

/// Request magic.
pub const MAGIC_REQUEST: u8 = 0x80;
/// Response magic.
pub const MAGIC_RESPONSE: u8 = 0x81;
/// Every request and response starts with a fixed 24-byte header.
pub const HEADER_LEN: usize = 24;

/// The opcodes this implementation sends.
pub mod op {
    pub const GET: u8 = 0x00;
    pub const SET: u8 = 0x01;
    pub const ADD: u8 = 0x02;
    pub const REPLACE: u8 = 0x03;
    pub const DELETE: u8 = 0x04;
    pub const HELLO: u8 = 0x1f;
    pub const SASL_LIST_MECHS: u8 = 0x20;
    pub const SASL_AUTH: u8 = 0x21;
    pub const TOUCH: u8 = 0x1c;
    pub const GET_AND_TOUCH: u8 = 0x1d;
    pub const GET_AND_LOCK: u8 = 0x94;
    pub const UNLOCK: u8 = 0x95;
    pub const SELECT_BUCKET: u8 = 0x89;
    pub const GET_CLUSTER_CONFIG: u8 = 0xb5;
    pub const GET_COLLECTION_ID: u8 = 0xbb;
    pub const SUBDOC_MULTI_LOOKUP: u8 = 0xd0;
    /// Metadata, including the expiry a plain `GET` does not report.
    pub const GET_META: u8 = 0xa0;
}

/// Protocol features worth negotiating in `HELLO`.
pub mod feature {
    /// Mutation tokens on every write, which `consistent-with` needs.
    pub const MUTATION_SEQNO: u16 = 0x0004;
    pub const XATTR: u16 = 0x0006;
    pub const SELECT_BUCKET: u16 = 0x0008;
    /// Datatype bit set on JSON values.
    pub const JSON: u16 = 0x000b;
    /// Changes the key encoding; see the module docs.
    pub const COLLECTIONS: u16 = 0x0012;
}

/// Status codes this implementation distinguishes.
pub mod status {
    pub const OK: u16 = 0x0000;
    pub const NOT_FOUND: u16 = 0x0001;
    pub const EXISTS: u16 = 0x0002;
    pub const TOO_LARGE: u16 = 0x0003;
    pub const INVALID: u16 = 0x0004;
    pub const NOT_STORED: u16 = 0x0005;
    pub const DELTA_BAD_VALUE: u16 = 0x0006;
    pub const NOT_MY_VBUCKET: u16 = 0x0007;
    pub const LOCKED: u16 = 0x0009;
    pub const AUTH_ERROR: u16 = 0x0020;
    pub const NOT_LOCKED: u16 = 0x0030;
    pub const UNKNOWN_COLLECTION: u16 = 0x0088;
    pub const UNKNOWN_COMMAND: u16 = 0x0081;
    pub const NOT_SUPPORTED: u16 = 0x0083;
}

/// The JSON common-flags value, matching what every Couchbase SDK writes.
pub const JSON_FLAGS: u32 = 0x0200_0006;

/// A parsed response header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseHeader {
    pub opcode: u8,
    pub key_len: usize,
    pub extras_len: usize,
    pub data_type: u8,
    pub status: u16,
    pub body_len: usize,
    pub opaque: u32,
    pub cas: u64,
}

impl ResponseHeader {
    /// Parse the fixed 24-byte header.
    pub fn parse(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < HEADER_LEN {
            return Err(format!("short header: {} bytes", bytes.len()));
        }
        if bytes[0] != MAGIC_RESPONSE {
            return Err(format!("not a response: magic 0x{:02x}", bytes[0]));
        }
        Ok(Self {
            opcode: bytes[1],
            key_len: u16::from_be_bytes([bytes[2], bytes[3]]) as usize,
            extras_len: bytes[4] as usize,
            data_type: bytes[5],
            status: u16::from_be_bytes([bytes[6], bytes[7]]),
            body_len: u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize,
            opaque: u32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
            cas: u64::from_be_bytes([
                bytes[16], bytes[17], bytes[18], bytes[19],
                bytes[20], bytes[21], bytes[22], bytes[23],
            ]),
        })
    }

    /// Where the value starts within the body.
    pub fn value_offset(&self) -> usize {
        self.extras_len + self.key_len
    }
}

/// Build one request frame.
pub fn request(
    opcode: u8,
    vbucket: u16,
    key: &[u8],
    extras: &[u8],
    value: &[u8],
    opaque: u32,
    cas: u64,
) -> Vec<u8> {
    let body_len = extras.len() + key.len() + value.len();
    let mut out = Vec::with_capacity(HEADER_LEN + body_len);
    out.push(MAGIC_REQUEST);
    out.push(opcode);
    out.extend_from_slice(&(key.len() as u16).to_be_bytes());
    out.push(extras.len() as u8);
    out.push(0); // data type: raw. JSON is advertised by the value's flags.
    out.extend_from_slice(&vbucket.to_be_bytes());
    out.extend_from_slice(&(body_len as u32).to_be_bytes());
    out.extend_from_slice(&opaque.to_be_bytes());
    out.extend_from_slice(&cas.to_be_bytes());
    out.extend_from_slice(extras);
    out.extend_from_slice(key);
    out.extend_from_slice(value);
    out
}

/// Encode an unsigned integer as leb128, the collection-id prefix format.
pub fn leb128(mut value: u32) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

/// The wire key: collection id, then the document id.
pub fn encode_key(collection_id: u32, key: &str) -> Vec<u8> {
    let mut out = leb128(collection_id);
    out.extend_from_slice(key.as_bytes());
    out
}

/// CRC32 as Couchbase computes it for vbucket routing.
///
/// This is the standard CRC-32 polynomial, and only the middle bits are used:
/// `(crc >> 16) & 0x7fff`.
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for byte in data {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Which vbucket a document id belongs to.
///
/// Hashes the document id alone — never the collection-prefixed form. See the
/// module docs for why that distinction bites.
pub fn vbucket_for(key: &str, total_vbuckets: u16) -> u16 {
    if total_vbuckets == 0 {
        return 0;
    }
    (((crc32(key.as_bytes()) >> 16) & 0x7FFF) % u32::from(total_vbuckets)) as u16
}

/// The `HELLO` payload: the features to request, as big-endian u16s.
pub fn hello_features() -> Vec<u8> {
    [
        feature::MUTATION_SEQNO,
        feature::XATTR,
        feature::SELECT_BUCKET,
        feature::JSON,
        feature::COLLECTIONS,
    ]
    .iter()
    .flat_map(|f| f.to_be_bytes())
    .collect()
}

/// The features a server agreed to, decoded from its `HELLO` response.
pub fn parse_hello_response(value: &[u8]) -> Vec<u16> {
    value
        .chunks_exact(2)
        .map(|c| u16::from_be_bytes([c[0], c[1]]))
        .collect()
}

/// The SASL PLAIN payload: `\0user\0password`.
pub fn sasl_plain(username: &str, password: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(username.len() + password.len() + 2);
    out.push(0);
    out.extend_from_slice(username.as_bytes());
    out.push(0);
    out.extend_from_slice(password.as_bytes());
    out
}

/// Extras for a write that carries flags and an expiry.
pub fn store_extras(flags: u32, expiry_seconds: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(8);
    out.extend_from_slice(&flags.to_be_bytes());
    out.extend_from_slice(&expiry_seconds.to_be_bytes());
    out
}

/// The collection id from a `GET_COLLECTION_ID` response's extras.
///
/// Layout: 8 bytes of manifest id, then the 4-byte collection id.
pub fn parse_collection_id(extras: &[u8]) -> Result<u32, String> {
    if extras.len() < 12 {
        return Err(format!("collection-id extras too short: {}", extras.len()));
    }
    Ok(u32::from_be_bytes([extras[8], extras[9], extras[10], extras[11]]))
}

/// How many vbuckets the cluster has, from a `GET_CLUSTER_CONFIG` body.
///
/// The map's length is the count. A config without one means the bucket is not
/// vbucket-mapped, which this implementation cannot route for.
pub fn parse_vbucket_count(config: &[u8]) -> Result<u16, String> {
    let parsed: serde_json::Value =
        serde_json::from_slice(config).map_err(|e| format!("cluster config is not JSON: {e}"))?;
    let map = parsed
        .get("vBucketServerMap")
        .and_then(|m| m.get("vBucketMap"))
        .and_then(|m| m.as_array())
        .ok_or_else(|| "cluster config has no vBucketMap".to_string())?;
    u16::try_from(map.len()).map_err(|_| format!("implausible vbucket count: {}", map.len()))
}

/// The mutation token a write returns in its extras, when `MUTATION_SEQNO`
/// was negotiated: the vbucket's uuid, then the sequence number.
///
/// Absent extras mean the feature was not agreed, and `consistent-with`
/// cannot be satisfied from this write.
pub fn parse_mutation_token(extras: &[u8]) -> Option<(u64, u64)> {
    if extras.len() < 16 {
        return None;
    }
    let uuid = u64::from_be_bytes([
        extras[0], extras[1], extras[2], extras[3],
        extras[4], extras[5], extras[6], extras[7],
    ]);
    let seqno = u64::from_be_bytes([
        extras[8], extras[9], extras[10], extras[11],
        extras[12], extras[13], extras[14], extras[15],
    ]);
    Some((uuid, seqno))
}

/// The absolute expiry a `GET_META` reports, as unix seconds.
///
/// Layout: deleted(4), flags(4), expiry(4), seqno(8). `0` means the document
/// has no expiry, which is not the epoch.
pub fn parse_meta_expiry(extras: &[u8]) -> Option<u32> {
    if extras.len() < 12 {
        return None;
    }
    match u32::from_be_bytes([extras[8], extras[9], extras[10], extras[11]]) {
        0 => None,
        seconds => Some(seconds),
    }
}

/// A civil date and time, broken out of a unix timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Civil {
    pub year: i32,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
}

/// Convert unix seconds to a civil UTC date.
///
/// Howard Hinnant's `civil_from_days`, which is exact for the whole range and
/// needs no table. The era arithmetic counts from March so a leap day lands at
/// the end of a 400-year era, where it does not disturb the month lengths.
pub fn civil_from_unix(unix_seconds: i64) -> Civil {
    let days = unix_seconds.div_euclid(86_400);
    let secs = unix_seconds.rem_euclid(86_400);

    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u8;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u8;
    let year = if month <= 2 { y + 1 } else { y } as i32;

    Civil {
        year,
        month,
        day,
        hour: (secs / 3_600) as u8,
        minute: ((secs % 3_600) / 60) as u8,
        second: (secs % 60) as u8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_frame_has_the_documented_layout() {
        let frame = request(op::GET, 0x0364, b"key", b"", b"", 7, 0);
        assert_eq!(frame[0], MAGIC_REQUEST);
        assert_eq!(frame[1], op::GET);
        assert_eq!(&frame[2..4], &3u16.to_be_bytes()); // key length
        assert_eq!(frame[4], 0); // extras length
        assert_eq!(&frame[6..8], &0x0364u16.to_be_bytes()); // vbucket
        assert_eq!(&frame[8..12], &3u32.to_be_bytes()); // body length
        assert_eq!(&frame[12..16], &7u32.to_be_bytes()); // opaque
        assert_eq!(&frame[HEADER_LEN..], b"key");
    }

    #[test]
    fn a_response_header_round_trips() {
        let mut bytes = vec![0u8; HEADER_LEN];
        bytes[0] = MAGIC_RESPONSE;
        bytes[1] = op::GET;
        bytes[4] = 4; // extras
        bytes[6..8].copy_from_slice(&status::NOT_FOUND.to_be_bytes());
        bytes[8..12].copy_from_slice(&10u32.to_be_bytes());
        bytes[16..24].copy_from_slice(&42u64.to_be_bytes());

        let header = ResponseHeader::parse(&bytes).unwrap();
        assert_eq!(header.status, status::NOT_FOUND);
        assert_eq!(header.body_len, 10);
        assert_eq!(header.cas, 42);
        assert_eq!(header.extras_len, 4);
        assert_eq!(header.value_offset(), 4);
    }

    #[test]
    fn a_request_magic_is_not_mistaken_for_a_response() {
        let mut bytes = vec![0u8; HEADER_LEN];
        bytes[0] = MAGIC_REQUEST;
        assert!(ResponseHeader::parse(&bytes).is_err());
        assert!(ResponseHeader::parse(&[MAGIC_RESPONSE; 4]).is_err());
    }

    #[test]
    fn leb128_matches_the_collection_prefix_format() {
        assert_eq!(leb128(0), vec![0x00]);
        assert_eq!(leb128(1), vec![0x01]);
        assert_eq!(leb128(127), vec![0x7f]);
        // 128 needs a continuation bit.
        assert_eq!(leb128(128), vec![0x80, 0x01]);
        assert_eq!(leb128(300), vec![0xac, 0x02]);
    }

    #[test]
    fn the_wire_key_carries_the_collection_prefix() {
        assert_eq!(encode_key(0, "doc"), b"\x00doc".to_vec());
        assert_eq!(encode_key(300, "doc"), b"\xac\x02doc".to_vec());
    }

    #[test]
    fn the_vbucket_comes_from_the_bare_key() {
        // Verified against a live cluster: `bench-doc` in a 1024-vbucket
        // bucket is served from vbucket 868. Hashing the collection-prefixed
        // key instead yields 743, which reads as `not-found` rather than as an
        // error -- so this constant is the regression test for that bug.
        assert_eq!(vbucket_for("bench-doc", 1024), 868);
    }

    #[test]
    fn vbucket_selection_stays_in_range() {
        for key in ["", "a", "scenario:doc", "a much longer document identifier"] {
            for total in [64u16, 128, 1024] {
                assert!(vbucket_for(key, total) < total, "{key} in {total}");
            }
        }
        // A cluster that reported no vbuckets must not divide by zero.
        assert_eq!(vbucket_for("k", 0), 0);
    }

    #[test]
    fn sasl_plain_is_nul_separated() {
        assert_eq!(sasl_plain("user", "pass"), b"\0user\0pass".to_vec());
    }

    #[test]
    fn hello_asks_for_the_features_the_plugin_depends_on() {
        let features = parse_hello_response(&hello_features());
        assert!(features.contains(&feature::COLLECTIONS));
        assert!(features.contains(&feature::MUTATION_SEQNO));
        assert!(features.contains(&feature::JSON));
    }

    #[test]
    fn reads_the_collection_id_out_of_extras() {
        let mut extras = vec![0u8; 12];
        extras[8..12].copy_from_slice(&9u32.to_be_bytes());
        assert_eq!(parse_collection_id(&extras).unwrap(), 9);
        assert!(parse_collection_id(&[0u8; 4]).is_err());
    }

    #[test]
    fn reads_the_vbucket_count_from_a_cluster_config() {
        let config = br#"{"vBucketServerMap":{"vBucketMap":[[0],[0],[0],[0]]}}"#;
        assert_eq!(parse_vbucket_count(config).unwrap(), 4);
        // A bucket with no vbucket map cannot be routed for, and says so.
        assert!(parse_vbucket_count(br#"{"nodesExt":[]}"#).is_err());
        assert!(parse_vbucket_count(b"not json").is_err());
    }

    #[test]
    fn reads_a_mutation_token_from_a_write() {
        let mut extras = Vec::new();
        extras.extend_from_slice(&0x1234_5678_9abc_def0u64.to_be_bytes());
        extras.extend_from_slice(&42u64.to_be_bytes());
        assert_eq!(parse_mutation_token(&extras), Some((0x1234_5678_9abc_def0, 42)));
        // Without MUTATION_SEQNO the server sends nothing, which is not the
        // same as a token of zeroes.
        assert_eq!(parse_mutation_token(&[]), None);
        assert_eq!(parse_mutation_token(&[0u8; 8]), None);
    }

    #[test]
    fn reads_the_expiry_out_of_meta_extras() {
        let mut extras = vec![0u8; 20];
        extras[8..12].copy_from_slice(&1_700_000_000u32.to_be_bytes());
        assert_eq!(parse_meta_expiry(&extras), Some(1_700_000_000));
        // A document with no expiry reports zero, which is absence rather than
        // the epoch.
        assert_eq!(parse_meta_expiry(&vec![0u8; 20]), None);
        assert_eq!(parse_meta_expiry(&[0u8; 4]), None);
    }

    #[test]
    fn converts_unix_seconds_to_a_civil_date() {
        assert_eq!(
            civil_from_unix(0),
            Civil { year: 1970, month: 1, day: 1, hour: 0, minute: 0, second: 0 }
        );
        // 2026-09-22T17:04:05Z
        assert_eq!(
            civil_from_unix(1_790_096_645),
            Civil { year: 2026, month: 9, day: 22, hour: 17, minute: 4, second: 5 }
        );
        // A leap day, which the era arithmetic has to place correctly.
        assert_eq!(
            civil_from_unix(1_709_164_800),
            Civil { year: 2024, month: 2, day: 29, hour: 0, minute: 0, second: 0 }
        );
        // The last second before a year rolls over.
        assert_eq!(
            civil_from_unix(1_735_689_599),
            Civil { year: 2024, month: 12, day: 31, hour: 23, minute: 59, second: 59 }
        );
    }

    #[test]
    fn store_extras_carry_flags_then_expiry() {
        let extras = store_extras(JSON_FLAGS, 600);
        assert_eq!(&extras[0..4], &JSON_FLAGS.to_be_bytes());
        assert_eq!(&extras[4..8], &600u32.to_be_bytes());
    }
}
