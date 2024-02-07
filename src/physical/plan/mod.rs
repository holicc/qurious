mod aggregate;
mod filter;
mod projection;
mod scan;

pub use aggregate::HashAggregate;
pub use filter::Filter;
pub use projection::Projection;
pub use scan::Scan;

use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

use crate::error::Result;

pub trait PhysicalPlan {
    fn schema(&self) -> SchemaRef;
    fn execute(&self) -> Result<Vec<RecordBatch>>;
    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>>;
}
