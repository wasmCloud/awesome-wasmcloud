//! Bounded formatting for the small query results in the verification callers.
use crate::bindings::wasmcloud::couchbase::{
    sqlpp::{self, QueryCompletion, SqlppQueryOptions},
    sqlpp_types::{SqlppQueryError, SqlppValue},
};

pub async fn query(
    statement: String,
    params: Vec<SqlppValue>,
    options: Option<SqlppQueryOptions>,
) -> Result<SqlppValue, SqlppQueryError> {
    let result = sqlpp::query(statement, params, options).await?;
    let mut rows = result.rows;
    let mut json = String::from("[");
    let mut first = true;
    loop {
        let (_, chunk) = rows.read(Vec::with_capacity(1)).await;
        if chunk.is_empty() {
            break;
        }
        for row in chunk {
            if json.len() + row.len() + 2 > 128 * 1024 {
                return Err(SqlppQueryError::Unexpected(
                    "verification result exceeds 128 KiB".into(),
                ));
            }
            if !first {
                json.push(',');
            }
            first = false;
            json.push_str(&row);
        }
    }
    QueryCompletion::finish(result.completion).await?;
    json.push(']');
    Ok(SqlppValue::Json(json))
}
