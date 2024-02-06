use std::sync::Arc;

use crate::{error::Result, logical::plan::LogicalPlan, physical::plan::PhysicalPlan};

pub trait QueryPlanner {
    fn create_physical_plan(&self, logical_plan: &LogicalPlan) -> Result<Arc<dyn PhysicalPlan>>;
}

struct DefaultQueryPlanner;

impl QueryPlanner for DefaultQueryPlanner {
    fn create_physical_plan(&self, logical_plan: &LogicalPlan) -> Result<Arc<dyn PhysicalPlan>> {
        unimplemented!()
    }
}

