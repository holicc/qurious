mod push_down_projections;

use push_down_projections::ProjectionPushDownRule;

use crate::{error::Result, logical::plan::LogicalPlan};

pub trait OptimizerRule {
    fn name(&self) -> &str;

    fn optimize(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>>;
}

pub struct Optimzier {
    rules: Vec<Box<dyn OptimizerRule + Sync + Send>>,
}

impl Optimzier {
    pub fn new() -> Self {
        Self {
            rules: vec![Box::new(ProjectionPushDownRule)],
        }
    }

    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let mut plan = plan.clone();
        for rule in &self.rules {
            match rule.optimize(&plan)? {
                Some(new_plan) => {
                    plan = new_plan;
                }
                None => {}
            }
        }
        Ok(plan)
    }
}
