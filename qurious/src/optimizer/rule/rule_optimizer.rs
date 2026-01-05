use log::debug;

use crate::common::transformed::{TransformNode, Transformed, TransformedResult};
use crate::error::Result;
use crate::logical::plan::LogicalPlan;
use crate::optimizer::rule::count_wildcard_rule::CountWildcardRule;
use crate::optimizer::rule::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
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
                Box::new(SimplifyExprs),
                Box::new(ScalarSubqueryToJoin::default()),
                Box::new(DecorrelatePredicateSubquery::default()),
                Box::new(EliminateCrossJoin),
                Box::new(ExtractEquijoinPredicate),
                Box::new(PushdownFilter),
                // Run type coercion late so correlated subqueries have been rewritten/decorrelated
                // (avoids trying to type-check outer-ref columns inside subquery schemas).
                Box::new(TypeCoercion),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{test_utils::sql_to_plan, utils};

    #[test]
    fn tpch_q2_should_keep_part_table_scan() {
        // Regression test: optimizer must not drop the `part` relation for TPC-H Q2.
        let sql = r#"
select
  s_acctbal,
  s_name,
  n_name,
  p_partkey,
  p_mfgr,
  s_address,
  s_phone,
  s_comment
from
  part,
  supplier,
  partsupp,
  nation,
  region
where
  p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 15
  and p_type like '%BRASS'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'EUROPE'
  and ps_supplycost = (
  select
  min(ps_supplycost)
  from
  partsupp,
  supplier,
  nation,
  region
  where
  p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'EUROPE'
)
order by
  s_acctbal desc,
  n_name,
  s_name,
  p_partkey
limit 10;
"#;

        let plan = sql_to_plan(sql);
        let original = utils::format(&plan, 0);
        assert!(
            original.contains("TableScan: part"),
            "original plan lost part scan:\n{original}"
        );

        let optimizer = RuleBaseOptimizer::new();
        let optimized = optimizer.optimize(&plan).unwrap();
        let formatted = utils::format(&optimized, 0);
        assert!(
            formatted.contains("TableScan: part"),
            "optimized plan lost part scan:\n{formatted}"
        );
    }
}
