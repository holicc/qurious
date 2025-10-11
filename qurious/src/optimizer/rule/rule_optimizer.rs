use log::debug;

use crate::common::transformed::{TransformNode, Transformed, TransformedResult};
use crate::error::Result;
use crate::logical::plan::LogicalPlan;
use crate::optimizer::rule::count_wildcard_rule::CountWildcardRule;
use crate::optimizer::rule::eliminate_cross_join::EliminateCrossJoin;
use crate::optimizer::rule::extract_equijoin_predicate::ExtractEquijoinPredicate;
use crate::optimizer::rule::pushdown_filter::PushdownFilter;
use crate::optimizer::rule::scalar_subquery_to_join::ScalarSubqueryToJoin;
use crate::optimizer::rule::simplify_exprs::SimplifyExprs;
use crate::optimizer::rule::type_coercion::TypeCoercion;
use crate::optimizer::Optimizer;

pub trait OptimizerRule: Sync + Send {
    fn name(&self) -> &str;

    fn rewrite(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>>;
}

pub struct RuleBaseOptimizer {
    rules: Vec<Box<dyn OptimizerRule>>,
}

impl RuleBaseOptimizer {
    pub fn new() -> Self {
        Self {
            rules: vec![
                Box::new(CountWildcardRule),
                Box::new(TypeCoercion),
                Box::new(SimplifyExprs),
                Box::new(ScalarSubqueryToJoin::default()),
                Box::new(EliminateCrossJoin),
                Box::new(ExtractEquijoinPredicate),
                Box::new(PushdownFilter),
            ],
        }
    }

    pub fn with_rules(rules: Vec<Box<dyn OptimizerRule>>) -> Self {
        Self { rules }
    }
}

impl Optimizer for RuleBaseOptimizer {
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let mut current_plan = plan.clone();
        for rule in &self.rules {
            debug!("Applying rule: {}", rule.name());
            current_plan = current_plan.transform(|plan| rule.rewrite(plan)).data()?;
        }
        Ok(current_plan)
    }
}
