//! Backpressured rows and a typed, consuming final-status resource.
use crate::{bindings, query, query_rows, transport};
use bindings::exports::wasmcloud::couchbase::{
    sqlpp::{GuestQueryCompletion, QueryCompletion, QueryResult},
    sqlpp_types::SqlppQueryError,
};
use bindings::wasi::{
    clocks::monotonic_clock,
    http::types::{ErrorCode, Response},
};
use std::{
    cell::RefCell,
    future::poll_fn,
    rc::Rc,
    task::{Poll, Waker},
};

#[derive(Default)]
struct State {
    done: bool,
    cancelled: bool,
    result: Option<Result<(), SqlppQueryError>>,
    waiter: Option<Waker>,
    cancellation: Option<Waker>,
}

pub(crate) struct Completion(Rc<RefCell<State>>);

impl Drop for Completion {
    fn drop(&mut self) {
        let mut state = self.0.borrow_mut();
        if !state.done {
            state.cancelled = true;
            if let Some(waker) = state.cancellation.take() {
                waker.wake();
            }
        }
    }
}

impl GuestQueryCompletion for Completion {
    async fn finish(this: QueryCompletion) -> Result<(), SqlppQueryError> {
        let completion = this.into_inner::<Self>();
        poll_fn(|cx| {
            let mut state = completion.0.borrow_mut();
            if let Some(result) = state.result.take() {
                Poll::Ready(result)
            } else {
                state.waiter = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
}

pub(crate) fn transport_error(error: ErrorCode) -> SqlppQueryError {
    match error {
        ErrorCode::DnsTimeout
        | ErrorCode::ConnectionTimeout
        | ErrorCode::ConnectionReadTimeout
        | ErrorCode::ConnectionWriteTimeout
        | ErrorCode::HttpResponseTimeout => SqlppQueryError::Timeout,
        other => SqlppQueryError::RequestFailed(format!("{other:?}")),
    }
}

fn query_error(error: query::Failure) -> SqlppQueryError {
    match error {
        query::Failure::InvalidArgument(message) => SqlppQueryError::InvalidArgument(message),
        query::Failure::Unauthorized => SqlppQueryError::Unauthorized,
        query::Failure::Timeout => SqlppQueryError::Timeout,
        query::Failure::Server(message) => SqlppQueryError::Server(message),
        query::Failure::Unexpected(message) => SqlppQueryError::Unexpected(message),
        query::Failure::RequestFailed(message) => SqlppQueryError::RequestFailed(message),
    }
}

/// Capture the response and deadline before the export returns. Background
/// work never re-reads ambient caller identity or another binding's config.
pub(crate) fn start(response: Response, deadline: u64) -> QueryResult {
    let state = Rc::new(RefCell::new(State::default()));
    let producer_state = state.clone();
    let (mut writer, rows) = bindings::wit_stream::new();
    wit_bindgen::spawn_local(async move {
        let cancelled = poll_fn(|cx| {
            let mut state = producer_state.borrow_mut();
            if state.cancelled {
                Poll::Ready(())
            } else {
                state.cancellation = Some(cx.waker().clone());
                Poll::Pending
            }
        });
        let operation = async {
            transport::within(
                pump(response, &mut writer),
                monotonic_clock::wait_until(deadline),
            )
            .await
            .unwrap_or(Err(SqlppQueryError::Timeout))
        };
        let result = transport::within(operation, cancelled)
            .await
            .unwrap_or_else(|| {
                Err(SqlppQueryError::RequestFailed(
                    "query result was cancelled".into(),
                ))
            });
        // Wake a finishing caller only after the row producer has closed.
        drop(writer);
        let mut state = producer_state.borrow_mut();
        state.done = true;
        state.result = Some(result);
        if let Some(waiter) = state.waiter.take() {
            waiter.wake();
        }
    });
    QueryResult {
        rows,
        completion: QueryCompletion::new(Completion(state)),
    }
}

async fn pump(
    response: Response,
    writer: &mut wit_bindgen::StreamWriter<String>,
) -> Result<(), SqlppQueryError> {
    let status = response.get_status_code();
    let (completion_tx, completion_rx) = bindings::wit_future::new(|| Ok(()));
    let (mut body, trailers) = Response::consume_body(response, completion_rx);
    let mut decoder = query_rows::Decoder::default();
    loop {
        let (_, chunk) = body
            .read(Vec::with_capacity(transport::READ_CHUNK_BYTES))
            .await;
        if chunk.is_empty() {
            break;
        }
        let mut offset = 0;
        while offset < chunk.len() {
            let (used, row) = decoder.next(&chunk[offset..]).map_err(query_error)?;
            offset += used;
            if let Some(row) = row.filter(|_| (200..300).contains(&status)) {
                if !writer.write_all(vec![row]).await.is_empty() {
                    return Err(SqlppQueryError::RequestFailed(
                        "query row reader was dropped".into(),
                    ));
                }
            }
        }
    }
    drop(completion_tx);
    trailers.await.map_err(transport_error)?;
    decoder.finish(status).map_err(query_error)
}
