//! An overall RPC deadline, independent of transport inactivity timers.
use std::future::{poll_fn, Future};
use std::pin::pin;
use std::task::Poll;

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
    #[test]
    fn pending_io_is_cancelled_at_the_deadline() {
        let mut future = pin!(within(pending::<()>(), ready(())));
        assert_eq!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(None)
        );
    }
    #[test]
    fn completion_on_the_deadline_wins() {
        let mut future = pin!(within(ready(42), ready(())));
        assert_eq!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Some(42))
        );
    }
}
