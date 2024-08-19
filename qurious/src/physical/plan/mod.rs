mod aggregate;
mod empty;
mod filter;
mod join;
mod limit;
mod projection;
mod scan;
mod sort;
mod values;

pub use aggregate::HashAggregate;
pub use empty::EmptyRelation;
pub use filter::Filter;
pub use join::{join_schema, ColumnIndex, CrossJoin, Join, JoinFilter, JoinSide};
pub use limit::Limit;
pub use projection::Projection;
pub use scan::Scan;
pub use sort::*;
pub use values::*;

use crate::error::Result;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use std::sync::Arc;

pub trait PhysicalPlan {
    fn schema(&self) -> SchemaRef;
    fn execute(&self) -> Result<Vec<RecordBatch>>;
    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>>;
}
