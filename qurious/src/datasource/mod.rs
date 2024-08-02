pub mod file;
pub mod memory;

use crate::{datatypes::scalar::ScalarValue, error::Result, logical::expr::LogicalExpr, physical::plan::PhysicalPlan};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use std::{fmt::Debug, sync::Arc};

pub trait DataSource: Debug + Sync + Send {
    fn schema(&self) -> SchemaRef;

    /// Perform a scan of the data source and return the results as RecordBatch
    fn scan(&self, projection: Option<Vec<String>>, filters: &[LogicalExpr]) -> Result<Vec<RecordBatch>>;

    /// Get the default value for a column, if available.
    fn get_column_default(&self, _column: &str) -> Option<&ScalarValue> {
        None
    }

    /// Insert a new record batch into the data source
    fn insert_into(&self, _input: Arc<dyn PhysicalPlan>) -> Result<Arc<dyn PhysicalPlan>> {
        unimplemented!("insert_into not implemented for {:?}", self)
    }

    /// Delete records from the data source
    /// The input plan is the filter expression to apply to the data source
    fn delete(&self, _input: Arc<dyn PhysicalPlan>) -> Result<Arc<dyn PhysicalPlan>> {
        unimplemented!("delete not implemented for {:?}", self)
    }

    /// Update records in the data source
    /// The input plan is the filter expression to apply to the data source
    fn update(&self, _input: Arc<dyn PhysicalPlan>) -> Result<Arc<dyn PhysicalPlan>> {
        unimplemented!("update not implemented for {:?}", self)
    }
}
