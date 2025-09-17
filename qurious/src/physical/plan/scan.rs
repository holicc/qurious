use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::physical::expr::PhysicalExpr;
use crate::provider::table::TableProvider;

use super::PhysicalPlan;

pub struct Scan {
    schema: SchemaRef,
    datasource: Arc<dyn TableProvider>,
    filter: Option<Arc<dyn PhysicalExpr>>,
    projections: Option<Vec<String>>,
}

impl Scan {
    pub fn new(
        schema: SchemaRef,
        datasource: Arc<dyn TableProvider>,
        projections: Option<Vec<String>>,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self {
            schema,
            datasource,
            projections,
            filter,
        }
    }
}

impl PhysicalPlan for Scan {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        self.datasource.scan(self.projections.clone(), self.filter.as_ref())
    }

    /// Scan is a leaf node and has no child plans
    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        None
    }
}
