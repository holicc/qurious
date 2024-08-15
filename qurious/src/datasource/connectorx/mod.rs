use crate::error::{Error, Result};
use arrow::record_batch::RecordBatch;
use connectorx::prelude::{get_arrow, ArrowDestination, CXQuery, SourceConn};

pub mod postgres;

fn query_batchs(source: &SourceConn, sql: &str) -> Result<Vec<RecordBatch>> {
    query(source, sql).and_then(|dst| dst.arrow().map_err(|e| Error::InternalError(e.to_string())))
}

fn query(source: &SourceConn, sql: &str) -> Result<ArrowDestination> {
    let queries = &[CXQuery::from(sql)];
    get_arrow(source, None, queries).map_err(|e| Error::InternalError(e.to_string()))
}
