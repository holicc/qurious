mod count_wildcard_rule;
mod extract_equijoin_predicate;
mod pushdown_filter_inner_join;
// mod scalar_subquery_to_join;
mod type_coercion;

use crate::{error::Result, logical::plan::LogicalPlan, optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate};
use count_wildcard_rule::CountWildcardRule;
use pushdown_filter_inner_join::PushdownFilterInnerJoin;
use type_coercion::TypeCoercion;

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
            rules: vec![
                Box::new(CountWildcardRule),
                Box::new(TypeCoercion),
                Box::new(ExtractEquijoinPredicate),
                Box::new(PushdownFilterInnerJoin),
            ],
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
