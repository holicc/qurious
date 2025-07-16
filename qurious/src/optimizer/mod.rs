pub mod rule;

use crate::{error::Result, logical::plan::LogicalPlan};

pub trait Optimizer: Sync + Send {
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan>;
}
