use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::datasource::DataSource;
use crate::error::Result;

use super::PhysicalPlan;

pub struct Scan {
    schema: SchemaRef,
    datasource: Arc<dyn DataSource>,
    projections: Option<Vec<String>>,
}

impl Scan {
    pub fn new(schema: SchemaRef, datasource: Arc<dyn DataSource>, projections: Option<Vec<String>>) -> Self {
        Self {
            schema,
            datasource,
            projections,
        }
    }
}

impl PhysicalPlan for Scan {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        self.datasource.scan(self.projections.clone(), &vec![])
    }

    /// Scan is a leaf node and has no child plans
    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        None
    }
}
