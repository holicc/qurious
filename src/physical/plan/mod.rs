mod aggregate;
mod filter;
mod projection;
mod scan;

use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

use crate::error::Result;

pub trait PhysicalPlan {
    fn schema(&self) -> SchemaRef;
    fn execute(&self) -> Result<Vec<RecordBatch>>;
    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>>;
}
