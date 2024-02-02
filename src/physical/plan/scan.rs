use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::{datasource::DataSource, types::schema::Schema};

use super::PhysicalPlan;

pub struct Scan {
    datasource: Arc<dyn DataSource>,
    projections: Option<Vec<String>>,
}

impl PhysicalPlan for Scan {
    fn schema(&self) -> &Schema {
        self.datasource
            .schema()
            .select(self.projections.clone().unwrap_or_default())
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        self.datasource.scan(self.projections.clone())
    }

    /// Scan is a leaf node and has no child plans
    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        None
    }
}
