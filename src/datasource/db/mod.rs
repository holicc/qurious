use std::sync::Arc;

use crate::error::{Error, Result};
use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
};
use connectorx::{prelude::CXQuery, source_router::SourceConn};

#[cfg(feature = "postgres")]
pub mod postgres;

pub(crate) fn get_record_batch(
    conn: &SourceConn,
    schema: Option<SchemaRef>,
    sql: &str,
) -> Result<RecordBatch> {
    let queries = &[CXQuery::from(sql)];
    let data = connectorx::get_arrow::get_arrow(conn, None, queries)
        .map_err(|e| Error::InternalError(e.to_string()))?;

    let mut batch = data
        .arrow()
        .map_err(|e| Error::InternalError(e.to_string()))?;

    if batch.len() == 0 {
        return Ok(RecordBatch::new_empty(
            schema.unwrap_or(Arc::new(Schema::empty())),
        ));
    }

    Ok(batch.remove(0))
}
