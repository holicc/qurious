pub mod file;
pub mod memory;

use crate::{datatypes::scalar::ScalarValue, error::Result, logical::expr::LogicalExpr};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use std::fmt::Debug;

pub trait DataSource: Debug + Sync + Send {
    fn schema(&self) -> SchemaRef;

    /// Perform a scan of the data source and return the results as RecordBatch
    fn scan(&self, projection: Option<Vec<String>>, filters: &[LogicalExpr]) -> Result<Vec<RecordBatch>>;

    /// Get the default value for a column, if available.
    fn get_column_default(&self, _column: &str) -> Option<&ScalarValue> {
        None
    }
}
