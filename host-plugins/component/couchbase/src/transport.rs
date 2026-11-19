//! Body completion and overall deadlines, shared by the HTTP transports.

use std::future::{poll_fn, Future, IntoFuture};
use std::pin::pin;
use std::task::Poll;

/// Fits a 20 MiB Couchbase document, including a base64 JSON envelope.
pub const MAX_HTTP_BODY_BYTES: usize = 32 * 1024 * 1024;
pub const READ_CHUNK_BYTES: usize = 16 * 1024;

/// Read with backpressure and a hard byte budget, including chunked bodies.
/// The caller supplies a stream read, which must honor the requested capacity.
/// One extra byte distinguishes an exact-limit body from an oversized one.
pub async fn read_bounded(
    mut read: impl AsyncFnMut(usize) -> Vec<u8>,
    limit: usize,
) -> Result<Vec<u8>, &'static str> {
    let mut body = Vec::new();
    loop {
        let remaining = limit - body.len();
        let chunk = read(READ_CHUNK_BYTES.min(remaining.saturating_add(1))).await;
        if chunk.len() > remaining {
            return Err("body exceeds configured byte limit");
        }
        if chunk.is_empty() {
            return Ok(body);
        }
        // Clamp growth as well as length: geometric Vec growth can otherwise
        // reserve beyond the byte budget for irregular stream chunk sizes.
        let needed = body.len() + chunk.len();
        if needed > body.capacity() {
            let capacity = body
                .capacity()
                .saturating_mul(2)
                .max(READ_CHUNK_BYTES)
                .max(needed)
                .min(limit);
            body.reserve_exact(capacity - body.len());
        }
        body.extend_from_slice(&chunk);
    }
}

/// A byte stream ending is not evidence that its transport completed: p3
/// reports a truncated body or read timeout through the completion future.
pub async fn receive_body<T, R, E>(
    body: impl Future<Output = Result<T, E>>,
    completion: impl IntoFuture<Output = Result<R, E>>,
) -> Result<T, E> {
    let received = body.await?;
    completion.await?;
    Ok(received)
}

/// Bound the whole operation with one timer. Transport inactivity timeouts
/// alone permit a peer to keep a call alive indefinitely by trickling bytes.
pub async fn within<F: Future>(operation: F, timer: impl Future<Output = ()>) -> Option<F::Output> {
    let mut operation = pin!(operation);
    let mut timer = pin!(timer);
    poll_fn(|cx| {
        if let Poll::Ready(output) = operation.as_mut().poll(cx) {
            return Poll::Ready(Some(output));
        }
        if timer.as_mut().poll(cx).is_ready() {
            return Poll::Ready(None);
        }
        Poll::Pending
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::{pending, ready};
    use std::task::{Context, Waker};

    fn finish<F: Future>(future: F) -> F::Output {
        let mut future = pin!(future);
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("expected an immediately completed future"),
        }
    }

    #[test]
    fn partial_bytes_are_discarded_on_transport_failure() {
        for error in ["connection terminated", "read timeout"] {
            assert_eq!(
                finish(receive_body(
                    ready(Ok(b"abc".to_vec())),
                    ready(Err::<(), _>(error))
                )),
                Err(error),
            );
        }
        assert_eq!(
            finish(receive_body(
                ready(Ok(b"complete".to_vec())),
                ready(Ok::<_, ()>(()))
            )),
            Ok(b"complete".to_vec()),
        );
    }

    #[test]
    fn bytes_do_not_become_success_before_completion_is_known() {
        let mut received = pin!(receive_body(ready(Ok(b"abc")), pending::<Result<(), ()>>()));
        assert!(received
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
    }

    #[test]
    fn progressing_body_does_not_reset_the_overall_deadline() {
        let mut bytes = 0;
        let body = poll_fn(|_| {
            bytes += 1;
            if bytes == 10 {
                Poll::Ready(bytes)
            } else {
                Poll::Pending
            }
        });
        let mut ticks = 0;
        let timer = poll_fn(|_| {
            ticks += 1;
            if ticks == 3 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        });
        let mut call = pin!(within(body, timer));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(call.as_mut().poll(&mut cx).is_pending());
        assert!(call.as_mut().poll(&mut cx).is_pending());
        assert_eq!(call.as_mut().poll(&mut cx), Poll::Ready(None));
    }

    #[test]
    fn completion_on_the_deadline_wins() {
        assert_eq!(finish(within(ready(42), ready(()))), Some(42));
        assert_eq!(finish(within(pending::<()>(), ready(()))), None);
    }

    #[test]
    fn bounded_reads_accept_exact_limits_and_stop_on_the_first_extra_byte() {
        for length in [0, 17, 18] {
            let mut remaining = length;
            let mut requested = Vec::new();
            let result = finish(read_bounded(
                async |capacity| {
                    requested.push(capacity);
                    let length = remaining.min(capacity);
                    remaining -= length;
                    vec![b'x'; length]
                },
                17,
            ));
            assert_eq!(result.is_ok(), length <= 17);
            if let Ok(body) = result {
                assert!(body.capacity() <= 17);
            }
            assert!(requested.iter().all(|&n| n <= 18));
            assert_eq!(remaining, 0);
        }
        assert_eq!(
            finish(receive_body(
                ready(Err::<(), _>("size limit")),
                pending::<Result<(), _>>()
            )),
            Err("size limit")
        );
    }
}
