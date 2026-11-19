//! Bindings-free gRPC framing and response validation.

/// Maximum protobuf bytes in one request or response message.
pub const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
/// Maximum wire bytes across all frames in a response.
pub const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
/// Bound metadata overhead even when response messages are empty.
pub const MAX_RESPONSE_FRAMES: usize = 1024;

pub fn frame(message: &[u8]) -> Result<Vec<u8>, String> {
    if message.len() > MAX_MESSAGE_BYTES {
        return Err("gRPC message exceeds the 4 MiB message limit".into());
    }
    let length = u32::try_from(message.len()).map_err(|_| "gRPC message exceeds u32 length")?;
    let capacity = message
        .len()
        .checked_add(5)
        .ok_or("gRPC frame length overflow")?;
    let mut framed = Vec::with_capacity(capacity);
    framed.push(0);
    framed.extend_from_slice(&length.to_be_bytes());
    framed.extend_from_slice(message);
    Ok(framed)
}

pub fn split_frames(body: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut decoder = FrameDecoder::default();
    decoder.push(body)?;
    decoder.finish()
}

/// Decode across arbitrary stream boundaries without a second whole-body copy.
/// Declared message lengths and frame counts are checked before retaining data.
#[derive(Default)]
pub(crate) struct FrameDecoder {
    header: [u8; 5],
    header_len: usize,
    remaining: Option<usize>,
    message: Vec<u8>,
    frames: Vec<Vec<u8>>,
    wire_bytes: usize,
}

impl FrameDecoder {
    pub(crate) fn push(&mut self, mut bytes: &[u8]) -> Result<(), String> {
        if bytes.len() > MAX_RESPONSE_BYTES - self.wire_bytes {
            return Err("gRPC response exceeds the 32 MiB body limit".into());
        }
        self.wire_bytes += bytes.len();
        while !bytes.is_empty() {
            if let Some(remaining) = self.remaining {
                let take = remaining.min(bytes.len());
                self.message.extend_from_slice(&bytes[..take]);
                bytes = &bytes[take..];
                self.remaining = Some(remaining - take);
            } else {
                let take = (5 - self.header_len).min(bytes.len());
                self.header[self.header_len..self.header_len + take]
                    .copy_from_slice(&bytes[..take]);
                self.header_len += take;
                bytes = &bytes[take..];
                if self.header_len < 5 {
                    continue;
                }
                match self.header[0] {
                    0 => {}
                    1 => return Err("compressed gRPC messages are not supported".into()),
                    _ => return Err("invalid gRPC compression flag".into()),
                }
                let length = u32::from_be_bytes([
                    self.header[1],
                    self.header[2],
                    self.header[3],
                    self.header[4],
                ]) as usize;
                if length > MAX_MESSAGE_BYTES {
                    return Err("gRPC message exceeds the 4 MiB message limit".into());
                }
                if self.frames.len() >= MAX_RESPONSE_FRAMES {
                    return Err("gRPC response exceeds the 1024-frame limit".into());
                }
                // The validated length caps capacity, avoiding geometric
                // growth beyond the per-message budget on split frames.
                self.message = Vec::with_capacity(length);
                self.remaining = Some(length);
            }
            if self.remaining == Some(0) {
                self.frames.push(std::mem::take(&mut self.message));
                self.remaining = None;
                self.header_len = 0;
            }
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Vec<Vec<u8>>, String> {
        if self.remaining.is_some() {
            return Err("truncated gRPC message".into());
        }
        if self.header_len != 0 {
            return Err("truncated gRPC frame header".into());
        }
        Ok(self.frames)
    }
}

/// The wire timeout has at most eight decimal digits. Round up when changing
/// units so the server never receives a shorter timeout than the local one.
pub fn timeout_header(ns: u64) -> String {
    for (unit, divisor) in [
        ("n", 1),
        ("u", 1_000),
        ("m", 1_000_000),
        ("S", 1_000_000_000),
        ("M", 60_000_000_000),
        ("H", 3_600_000_000_000),
    ] {
        let value = ns.div_ceil(divisor).max(1);
        if value <= 99_999_999 {
            return format!("{value}{unit}");
        }
    }
    unreachable!("u64 nanoseconds fit in eight digits of hours")
}

fn field<'a>(fields: &'a [(String, Vec<u8>)], name: &str) -> Result<Option<&'a [u8]>, String> {
    let mut values = fields
        .iter()
        .filter(|(key, _)| key.eq_ignore_ascii_case(name));
    let first = values.next().map(|(_, value)| value.as_slice());
    if values.next().is_some() {
        return Err(format!("duplicate {name} field"));
    }
    Ok(first)
}

fn status(fields: &[(String, Vec<u8>)]) -> Result<Option<u32>, String> {
    field(fields, "grpc-status")?
        .map(|value| {
            let text = std::str::from_utf8(value).map_err(|_| "invalid grpc-status encoding")?;
            let code = text.parse::<u32>().map_err(|_| "invalid grpc-status")?;
            if code > 16 || text != code.to_string() {
                return Err("invalid grpc-status".into());
            }
            Ok(code)
        })
        .transpose()
}

pub fn response_status(
    http_status: u16,
    headers: &[(String, Vec<u8>)],
    trailers: &[(String, Vec<u8>)],
    has_body: bool,
) -> Result<(u32, String), String> {
    if http_status != 200 {
        return Err(format!("gRPC endpoint returned HTTP {http_status}"));
    }
    let content_type = field(headers, "content-type")?.ok_or("missing gRPC content-type")?;
    let content_type = std::str::from_utf8(content_type)
        .map_err(|_| "invalid gRPC content-type")?
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if content_type != "application/grpc"
        && content_type
            .strip_prefix("application/grpc+")
            .is_none_or(|suffix| suffix.is_empty())
    {
        return Err("response is not a gRPC content-type".into());
    }
    let head_status = status(headers)?;
    let tail_status = status(trailers)?;
    if head_status.is_some() && (has_body || tail_status.is_some()) {
        return Err(
            "grpc-status in response headers is only valid for a trailers-only response".into(),
        );
    }
    let code = tail_status.or(head_status).ok_or("missing grpc-status")?;
    let source = if tail_status.is_some() {
        trailers
    } else {
        headers
    };
    let message = field(source, "grpc-message")?
        .map(String::from_utf8_lossy)
        .map(|message| message.into_owned())
        .unwrap_or_default();
    Ok((code, message))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn incremental_frames_handle_every_boundary_and_empty_messages() -> Result<(), String> {
        let wire = [frame(b"one")?, frame(b"")?, frame(b"two")?].concat();
        for size in 1..=wire.len() {
            let mut decoder = FrameDecoder::default();
            for chunk in wire.chunks(size) {
                decoder.push(chunk)?;
            }
            assert_eq!(
                decoder.finish()?,
                vec![b"one".to_vec(), vec![], b"two".to_vec()]
            );
        }
        Ok(())
    }

    #[test]
    fn stream_limits_reject_headers_and_empty_frame_floods_before_buffering() -> Result<(), String>
    {
        let mut oversized = vec![0];
        oversized.extend_from_slice(&((MAX_MESSAGE_BYTES + 1) as u32).to_be_bytes());
        assert!(FrameDecoder::default()
            .push(&oversized)
            .is_err_and(|error| error.contains("4 MiB")));
        let mut decoder = FrameDecoder::default();
        for _ in 0..MAX_RESPONSE_FRAMES {
            decoder.push(&[0; 5])?;
        }
        assert!(decoder
            .push(&[0; 5])
            .is_err_and(|error| error.contains("1024-frame")));
        assert!(frame(&vec![0; MAX_MESSAGE_BYTES + 1]).is_err());
        let mut decoder = FrameDecoder::default();
        let message = frame(&vec![0; MAX_MESSAGE_BYTES])?;
        for _ in 0..7 {
            decoder.push(&message)?;
        }
        assert!(decoder
            .push(&message)
            .is_err_and(|error| error.contains("32 MiB")));
        Ok(())
    }
    fn fields(pairs: &[(&str, &str)]) -> Vec<(String, Vec<u8>)> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.as_bytes().to_vec()))
            .collect()
    }

    #[test]
    fn valid_empty_unary_and_multiple_messages_round_trip() {
        for value in [vec![], vec![10, 2, 111, 107]] {
            assert_eq!(split_frames(&frame(&value).unwrap()).unwrap(), vec![value]);
        }
        let body = [frame(b"one").unwrap(), frame(b"two").unwrap()].concat();
        assert_eq!(
            split_frames(&body).unwrap(),
            vec![b"one".to_vec(), b"two".to_vec()]
        );
        assert!(split_frames(&[]).unwrap().is_empty());
    }

    #[test]
    fn incomplete_compressed_and_overflowing_frames_fail() {
        for body in [
            vec![0],
            vec![0, 0, 0, 0, 5, 10, 0],
            vec![0, 255, 255, 255, 255],
            vec![1, 0, 0, 0, 0],
            vec![2, 0, 0, 0, 0],
            [frame(b"valid").unwrap(), vec![0, 0]].concat(),
        ] {
            assert!(split_frames(&body).is_err(), "{body:?}");
        }
    }

    #[test]
    fn timeout_is_positive_and_fits_the_wire_format() {
        assert_eq!(timeout_header(1), "1n");
        assert_eq!(timeout_header(100_000_001), "100001u");
        for ns in [0, 1, 99_999_999, 100_000_000, 30_000_000_000, u64::MAX] {
            let value = timeout_header(ns);
            let digits = &value[..value.len() - 1];
            assert!(digits.len() <= 8);
            assert!(digits.parse::<u64>().unwrap() > 0);
        }
    }

    #[test]
    fn success_trailers_and_trailers_only_errors_are_supported() {
        let headers = fields(&[("content-type", "application/grpc+proto")]);
        assert_eq!(
            response_status(200, &headers, &fields(&[("grpc-status", "0")]), true),
            Ok((0, String::new()))
        );
        let headers = fields(&[
            ("content-type", "application/grpc"),
            ("grpc-status", "5"),
            ("grpc-message", "missing"),
        ]);
        assert_eq!(
            response_status(200, &headers, &[], false),
            Ok((5, "missing".into()))
        );
    }

    #[test]
    fn malformed_or_contradictory_status_cannot_report_success() {
        let headers = fields(&[("content-type", "application/grpc")]);
        assert!(response_status(503, &headers, &[], false).is_err());
        assert!(response_status(
            200,
            &fields(&[("content-type", "text/html")]),
            &fields(&[("grpc-status", "0")]),
            true
        )
        .is_err());
        assert!(response_status(200, &headers, &[], false).is_err());
        for code in ["00", "17", "bad", "-1"] {
            assert!(
                response_status(200, &headers, &fields(&[("grpc-status", code)]), true).is_err()
            );
        }
        let duplicate = fields(&[("grpc-status", "0"), ("grpc-status", "13")]);
        assert!(response_status(200, &headers, &duplicate, true).is_err());
        let header_status = fields(&[("content-type", "application/grpc"), ("grpc-status", "0")]);
        assert!(
            response_status(200, &header_status, &fields(&[("grpc-status", "13")]), true).is_err()
        );
    }
}
