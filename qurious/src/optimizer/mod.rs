mod count_wildcard_rule;
mod optimize_projections;
mod push_down_projections;
mod type_coercion;
mod egg;

use count_wildcard_rule::CountWildcardRule;
use type_coercion::TypeCoercion;

use crate::{error::Result, logical::plan::LogicalPlan};

pub trait OptimizerRule {
    fn name(&self) -> &str;

    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan>;
}

pub struct Optimizer {
    rules: Vec<Box<dyn OptimizerRule + Sync + Send>>,
}

impl Optimizer {
    pub fn new() -> Self {
        Self {
            rules: vec![Box::new(CountWildcardRule), Box::new(TypeCoercion::default())],
        }
    }

    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let mut current_plan = plan.clone();
        for rule in &self.rules {
            current_plan = rule.optimize(current_plan)?;
        }
        Ok(current_plan)
    }
}
