mod push_down_projections;

use std::fmt::Display;

use crate::{error::Result, logical::plan::LogicalPlan};

pub trait OptimizerRule {
    fn name(&self) -> &str;

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>>;
}
