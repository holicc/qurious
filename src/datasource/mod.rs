pub mod adbc;
pub mod file;
pub mod memory;

use crate::{error::Result, logical::expr::LogicalExpr};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use std::fmt::Debug;

pub trait DataSource: Debug {
    fn schema(&self) -> SchemaRef;

    /// Perform a scan of the data source and return the results as RecordBatch
    fn scan(&self, projection: Option<Vec<String>>, filters: &[LogicalExpr]) -> Result<Vec<RecordBatch>>;
}
