use std::sync::Arc;

use crate::{
    error::{Error, Result},
    logical::expr::{BinaryExpr, LogicalExpr},
};
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

pub(crate) fn expr_to_sql(expr: &LogicalExpr) -> String {
    match expr {
        LogicalExpr::Alias(_) => todo!(),
        LogicalExpr::Column(c) => c.name.clone(),
        LogicalExpr::Literal(v) => v.to_value_string(),
        LogicalExpr::BinaryExpr(BinaryExpr { left, op, right }) => {
            format!("{} {} {}", expr_to_sql(left), op, expr_to_sql(right))
        }
        LogicalExpr::AggregateExpr(_) => todo!(),
    }
}
