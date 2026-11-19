//! Incremental Query Service envelopes: one bounded JSON row at a time.
use crate::query::{self, Failure};
use serde_json::{Map, Value};
use std::collections::BTreeSet;

pub const MAX_ROW_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_METADATA_BYTES: usize = 64 * 1024;
pub const MAX_RESPONSE_BYTES: usize = 256 * 1024 * 1024;
const MAX_ROWS: usize = 1_000_000;

#[derive(Clone, Copy)]
enum State {
    Start,
    Key(bool),
    KeyValue,
    Colon,
    Field,
    FieldValue,
    Row(bool),
    RowValue,
    RowDelimiter,
    FieldDelimiter,
    Done,
}

#[derive(Default)]
struct Token {
    bytes: Vec<u8>,
    depth: usize,
    quoted: bool,
    escaped: bool,
    compound: bool,
    started: bool,
}

impl Token {
    // Returns (consumed, complete). A primitive's delimiter is reprocessed by
    // the envelope state machine, rather than becoming part of its JSON value.
    fn push(&mut self, byte: u8, limit: usize) -> Result<(bool, bool), Failure> {
        if self.started
            && !self.quoted
            && !self.compound
            && (whitespace(byte) || matches!(byte, b',' | b']' | b'}'))
        {
            return Ok((false, true));
        }
        if self.bytes.len() == limit {
            return Err(Failure::RequestFailed(if limit == MAX_ROW_BYTES {
                "SQL++ row exceeds the 32 MiB row limit".into()
            } else {
                "SQL++ metadata exceeds the 64 KiB metadata limit".into()
            }));
        }
        if self.bytes.len() == self.bytes.capacity() {
            let capacity = self.bytes.capacity().saturating_mul(2).max(256).min(limit);
            self.bytes.reserve_exact(capacity - self.bytes.len());
        }
        self.bytes.push(byte);
        if !self.started {
            self.started = true;
            match byte {
                b'"' => self.quoted = true,
                b'[' | b'{' => {
                    self.compound = true;
                    self.depth = 1;
                }
                b',' | b']' | b'}' => return Err(malformed()),
                _ => {}
            }
            return Ok((true, false));
        }
        if self.quoted {
            if self.escaped {
                self.escaped = false;
            } else if byte == b'\\' {
                self.escaped = true;
            } else if byte == b'"' {
                self.quoted = false;
                if !self.compound {
                    return Ok((true, true));
                }
            }
        } else {
            match byte {
                b'"' => self.quoted = true,
                b'[' | b'{' => self.depth += 1,
                b']' | b'}' if self.compound => {
                    self.depth -= 1;
                    if self.depth == 0 {
                        return Ok((true, true));
                    }
                }
                _ => {}
            }
        }
        Ok((true, false))
    }

    fn take(&mut self) -> Vec<u8> {
        std::mem::take(self).bytes
    }
}

fn whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\r' | b'\n')
}

fn malformed() -> Failure {
    Failure::Unexpected("the Query Service returned an invalid or truncated JSON envelope".into())
}

pub struct Decoder {
    state: State,
    token: Token,
    key: String,
    keys: BTreeSet<String>,
    metadata: Map<String, Value>,
    metadata_bytes: usize,
    response_bytes: usize,
    rows: usize,
}

impl Default for Decoder {
    fn default() -> Self {
        Self {
            state: State::Start,
            token: Token::default(),
            key: String::new(),
            keys: BTreeSet::new(),
            metadata: Map::new(),
            metadata_bytes: 0,
            response_bytes: 0,
            rows: 0,
        }
    }
}

impl Decoder {
    /// Consume at most one row. The caller must await its downstream write
    /// before resuming this chunk, so neither rows nor chunks queue up here.
    pub fn next(&mut self, bytes: &[u8]) -> Result<(usize, Option<String>), Failure> {
        let mut consumed = 0;
        while let Some(&byte) = bytes.get(consumed) {
            let mut advance = true;
            let mut row = None;
            match self.state {
                State::Start if whitespace(byte) => {}
                State::Start if byte == b'{' => self.state = State::Key(true),
                State::Key(_) if whitespace(byte) => {}
                State::Key(true) if byte == b'}' => self.state = State::Done,
                State::Key(_) if byte == b'"' => {
                    self.token.push(byte, MAX_METADATA_BYTES)?;
                    self.state = State::KeyValue;
                }
                State::KeyValue => {
                    let (used, done) = self.token.push(byte, MAX_METADATA_BYTES)?;
                    advance = used;
                    if done {
                        let key = self.token.take();
                        self.add_metadata_bytes(key.len())?;
                        self.key = serde_json::from_slice(&key).map_err(|_| malformed())?;
                        if !self.keys.insert(self.key.clone()) {
                            return Err(malformed());
                        }
                        self.state = State::Colon;
                    }
                }
                State::Colon if whitespace(byte) => {}
                State::Colon if byte == b':' => self.state = State::Field,
                State::Field if whitespace(byte) => {}
                State::Field if self.key == "results" && byte == b'[' => {
                    self.state = State::Row(true)
                }
                State::Field if self.key == "results" => return Err(malformed()),
                State::Field => {
                    self.state = State::FieldValue;
                    advance = false;
                }
                State::Row(_) if whitespace(byte) => {}
                State::Row(true) if byte == b']' => self.state = State::FieldDelimiter,
                State::Row(_) => {
                    self.state = State::RowValue;
                    advance = false;
                }
                State::RowValue | State::FieldValue => {
                    let is_row = matches!(self.state, State::RowValue);
                    let (used, done) = self.token.push(
                        byte,
                        if is_row {
                            MAX_ROW_BYTES
                        } else {
                            MAX_METADATA_BYTES
                        },
                    )?;
                    advance = used;
                    if done {
                        let value = self.token.take();
                        if is_row {
                            // Validate without allocating a JSON tree. The same
                            // bytes then move directly into the row's string.
                            serde_json::from_slice::<serde::de::IgnoredAny>(&value)
                                .map_err(|_| malformed())?;
                            self.rows += 1;
                            if self.rows > MAX_ROWS {
                                return Err(Failure::RequestFailed(
                                    "SQL++ response exceeds the one million row limit".into(),
                                ));
                            }
                            row = Some(String::from_utf8(value).map_err(|_| malformed())?);
                            self.state = State::RowDelimiter;
                        } else {
                            self.add_metadata_bytes(value.len())?;
                            let value: Value =
                                serde_json::from_slice(&value).map_err(|_| malformed())?;
                            if matches!(self.key.as_str(), "errors" | "status" | "message") {
                                self.metadata.insert(self.key.clone(), value);
                            }
                            self.state = State::FieldDelimiter;
                        }
                    }
                }
                State::RowDelimiter if whitespace(byte) => {}
                State::RowDelimiter if byte == b',' => self.state = State::Row(false),
                State::RowDelimiter if byte == b']' => self.state = State::FieldDelimiter,
                State::FieldDelimiter if whitespace(byte) => {}
                State::FieldDelimiter if byte == b',' => self.state = State::Key(false),
                State::FieldDelimiter if byte == b'}' => self.state = State::Done,
                State::Done if whitespace(byte) => {}
                _ => return Err(malformed()),
            }
            if advance {
                consumed += 1;
                self.response_bytes += 1;
                if self.response_bytes > MAX_RESPONSE_BYTES {
                    return Err(Failure::RequestFailed(
                        "SQL++ response exceeds the 256 MiB response limit".into(),
                    ));
                }
            }
            if row.is_some() {
                return Ok((consumed, row));
            }
        }
        Ok((consumed, None))
    }

    fn add_metadata_bytes(&mut self, amount: usize) -> Result<(), Failure> {
        self.metadata_bytes += amount;
        if self.metadata_bytes > MAX_METADATA_BYTES {
            return Err(Failure::RequestFailed(
                "SQL++ metadata exceeds the 64 KiB metadata limit".into(),
            ));
        }
        Ok(())
    }

    /// Only EOF, a valid closing envelope and successful service metadata can
    /// complete a query. The transport completion must be checked separately.
    pub fn finish(self, status: u16) -> Result<(), Failure> {
        if !matches!(self.state, State::Done) {
            return Err(malformed());
        }
        let metadata = Value::Object(self.metadata);
        if metadata
            .get("errors")
            .is_some_and(|value| !value.is_array())
            || metadata
                .get("status")
                .is_some_and(|value| !value.is_string())
        {
            return Err(malformed());
        }
        query::check_metadata(status, &metadata)?;
        if !self.keys.contains("results") && metadata.get("status").is_none() {
            return Err(malformed());
        }
        if let Some(value) = metadata.get("status").and_then(Value::as_str) {
            if !matches!(value, "success" | "completed")
                && metadata
                    .get("errors")
                    .and_then(Value::as_array)
                    .is_none_or(Vec::is_empty)
            {
                return Err(if value == "timeout" {
                    Failure::Timeout
                } else {
                    Failure::Server(format!("query finished with status {value}"))
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(parts: &[&[u8]]) -> (Vec<String>, Result<(), Failure>) {
        let mut decoder = Decoder::default();
        let mut rows = Vec::new();
        for part in parts {
            let mut offset = 0;
            while offset < part.len() {
                match decoder.next(&part[offset..]) {
                    Ok((used, row)) => {
                        offset += used;
                        rows.extend(row);
                    }
                    Err(error) => return (rows, Err(error)),
                }
            }
        }
        (rows, decoder.finish(200))
    }

    #[test]
    fn every_byte_boundary_preserves_rows_and_late_errors() {
        let input = br#"{"requestID":"x","results":[null,1,true,"a\\\"b",{"a":[1,2]},["x"]],"status":"success"}"#;
        let expected = decode(&[input]);
        assert!(expected.1.is_ok());
        assert_eq!(
            expected.0,
            [
                "null",
                "1",
                "true",
                r#""a\\\"b""#,
                r#"{"a":[1,2]}"#,
                r#"["x"]"#
            ]
        );
        for boundary in 0..=input.len() {
            assert_eq!(decode(&[&input[..boundary], &input[boundary..]]), expected);
        }
        let bytes: Vec<&[u8]> = input.chunks(1).collect();
        assert_eq!(decode(&bytes), expected);
        let (rows, error) = decode(&[
            br#"{"results":[1],"errors":[{"code":1080,"msg":"deadline"}],"status":"timeout"}"#,
        ]);
        assert_eq!(rows, ["1"]);
        assert_eq!(error, Err(Failure::Timeout));
    }

    #[test]
    fn rows_are_available_before_the_envelope_finishes() {
        let mut decoder = Decoder::default();
        assert_eq!(
            decoder.next(br#"{"results":[{"id":1},"#).unwrap().1,
            Some("{\"id\":1}".into())
        );
        assert!(decoder.finish(200).is_err());
    }

    #[test]
    fn malformed_truncated_duplicate_and_trailing_data_are_refused() {
        for input in [
            "[]",
            "{}",
            "{\u{000b}\"results\":[]}",
            "{} trailing",
            "{\"results\":null}",
            "{\"results\":[1,]}",
            "{\"status\":42}",
            "{\"errors\":\"broken\",\"results\":[]}",
            "{\"results\":[1]",
            "{\"results\":[1],\"results\":[]}",
            "{\"results\":[{]}",
            "{\"results\":[01]}",
            "{\"results\":[tru]}",
            "{\"status\":\"success\",}",
        ] {
            assert!(decode(&[input.as_bytes()]).1.is_err(), "{input}");
        }
        assert!(decode(&[br#"{"status":"fatal"}"#]).1.is_err());
        assert!(decode(&[br#"{"status":"success"}"#]).1.is_ok());
    }

    #[test]
    fn utf8_split_across_reads_is_preserved_and_invalid_utf8_is_refused() {
        let input = "{\"results\":[\"雪\"]}".as_bytes();
        for boundary in 0..input.len() {
            assert_eq!(
                decode(&[&input[..boundary], &input[boundary..]]).0,
                ["\"雪\""]
            );
        }
        assert!(decode(&[b"{\"results\":[\"\xff\"]}"]).1.is_err());
    }

    #[test]
    fn metadata_and_total_budgets_are_enforced_across_chunks() {
        let mut decoder = Decoder::default();
        decoder.next(b"{\"signature\":\"").unwrap();
        assert!(decoder.next(&vec![b'x'; MAX_METADATA_BYTES]).is_err());
        let mut decoder = Decoder::default();
        decoder.response_bytes = MAX_RESPONSE_BYTES;
        assert!(decoder.next(b"{").is_err());
        let mut decoder = Decoder::default();
        decoder.rows = MAX_ROWS;
        assert!(decoder.next(b"{\"results\":[0,").is_err());
    }

    #[test]
    fn row_bound_is_checked_before_growing_the_buffer() {
        let mut token = Token::default();
        token.push(b'"', 4).unwrap();
        for byte in b"abc" {
            token.push(*byte, 4).unwrap();
        }
        assert!(token.push(b'd', 4).is_err());
        assert_eq!(token.bytes.len(), 4);
        assert!(token.bytes.capacity() <= 4);
    }

    #[test]
    fn http_failures_without_success_fields_keep_their_classification() {
        for (status, expected) in [(401, "unauthorized"), (503, "server")] {
            let mut decoder = Decoder::default();
            decoder
                .next(br#"{"message":"temporarily unavailable"}"#)
                .unwrap();
            match (decoder.finish(status), expected) {
                (Err(Failure::Unauthorized), "unauthorized")
                | (Err(Failure::Server(_)), "server") => {}
                other => panic!("wrong HTTP error classification: {other:?}"),
            }
        }
    }

    #[test]
    fn results_larger_than_the_old_body_limit_retain_only_one_row() {
        let mut decoder = Decoder::default();
        decoder.next(b"{\"results\":[").unwrap();
        let row = format!("\"{}\"", "x".repeat(1024 * 1024));
        for index in 0..33 {
            let (used, value) = decoder.next(row.as_bytes()).unwrap();
            assert_eq!(used, row.len());
            assert_eq!(value.unwrap().len(), row.len());
            assert_eq!(decoder.token.bytes.capacity(), 0);
            decoder
                .next(if index == 32 { b"]}" } else { b"," })
                .unwrap();
        }
        assert!(decoder.response_bytes > 32 * 1024 * 1024);
        assert!(decoder.finish(200).is_ok());
    }
}
