use std::fmt;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::datatypes::scalar::ScalarValue;
use crate::error::Result;
use crate::physical::expr::PhysicalExpr;
use crate::physical::plan::PhysicalPlan;
use std::fmt::Debug;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableType {
    Base,
    View,
    Temporary,
    External,
}

impl fmt::Display for TableType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableType::Base => write!(f, "BASE TABLE"),
            TableType::View => write!(f, "VIEW"),
            TableType::Temporary => write!(f, "LOCAL TEMPORARY"),
            TableType::External => write!(f, "EXTERNAL"),
        }
    }
}

pub trait TableProvider: Debug + Send + Sync {
    fn schema(&self) -> SchemaRef;

    /// Perform a scan of the data source and return the results as RecordBatch
    fn scan(
        &self,
        projection: Option<Vec<String>>,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Vec<RecordBatch>>;

    /// Get the default value for a column, if available.
    fn get_column_default(&self, _column: &str) -> Option<ScalarValue> {
        None
    }

    fn insert(&self, _input: Arc<dyn PhysicalPlan>) -> Result<u64> {
        unimplemented!("insert_into not implemented")
    }

    /// Delete records from the data source
    /// The input plan is the filter expression to apply to the data source
    fn delete(&self, _filter: Option<Arc<dyn PhysicalExpr>>) -> Result<u64> {
        unimplemented!("delete not implemented")
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}
