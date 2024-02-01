mod scan;
mod projection;
mod filter;

use std::sync::Arc;

use crate::error::Result;
use crate::types::batch::RecordBatch;
use crate::types::schema::Schema;

pub trait PhysicalPlan {
    fn schema(&self) -> &Schema;
    fn execute(&self) -> Result<Vec<RecordBatch>>;
    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>>;
}
