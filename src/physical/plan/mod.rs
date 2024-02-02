mod filter;
mod projection;
mod scan;

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::types::schema::Schema;

pub trait PhysicalPlan {
    fn schema(&self) -> &Schema;
    fn execute(&self) -> Result<Vec<RecordBatch>>;
    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>>;
}
