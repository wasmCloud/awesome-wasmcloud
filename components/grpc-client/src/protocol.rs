//! Bindings-free gRPC framing and response validation.

pub fn frame(message: &[u8]) -> Result<Vec<u8>, String> {
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
    let mut frames = Vec::new();
    let mut pos = 0;
    while pos < body.len() {
        let header = body[pos..].get(..5).ok_or("truncated gRPC frame header")?;
        match header[0] {
            0 => {}
            1 => return Err("compressed gRPC messages are not supported".into()),
            _ => return Err("invalid gRPC compression flag".into()),
        }
        let length = u32::from_be_bytes(header[1..5].try_into().expect("four bytes")) as usize;
        pos = pos.checked_add(5).ok_or("gRPC frame offset overflow")?;
        let end = pos
            .checked_add(length)
            .ok_or("gRPC frame length overflow")?;
        let message = body.get(pos..end).ok_or("truncated gRPC message")?;
        frames.push(message.to_vec());
        pos = end;
    }
    Ok(frames)
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
        && !content_type
            .strip_prefix("application/grpc+")
            .is_some_and(|suffix| !suffix.is_empty())
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
