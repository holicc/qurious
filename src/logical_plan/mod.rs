mod aggregate;
mod filter;
mod projection;
mod scan;

pub use aggregate::Aggregate;
pub use filter::Filter;
pub use projection::Projection;
pub use scan::TableScan;

use crate::types::schema::Schema;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Projection(Projection),
    Filter(Filter),
    Aggregate(Aggregate),
    TableScan(TableScan),
}

impl LogicalPlan {
    pub fn schema(&self) -> &Schema {
        match self {
            LogicalPlan::Projection(p) => p.schema(),
            LogicalPlan::Filter(f) => f.schema(),
            LogicalPlan::Aggregate(a) => a.schema(),
            LogicalPlan::TableScan(t) => t.schema(),
        }
    }
}
