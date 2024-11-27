use super::OptimizerRule;
use crate::common::transformed::{TransformNode, Transformed, TransformedResult, TreeNodeRecursion};
use crate::error::Result;
use crate::logical::expr::LogicalExpr;
use crate::logical::plan::LogicalPlan;

/// Convert scalar subquery to join
///
/// ```sql
/// SELECT a FROM t1 WHERE t1.a = (SELECT b FROM t2 WHERE t2.a = t1.a);
/// ```
///
/// After the rule is applied, the plan will look like this:
/// ```text
/// SELECT a FROM t1 LEFT JOIN (SELECT b FROM t2 WHERE t2.a = t1.a) AS t2 ON t1.a = t2.a;
/// ```
#[derive(Debug, Default, Clone)]
pub struct ScalarSubqueryToJoin;

impl OptimizerRule for ScalarSubqueryToJoin {
    fn name(&self) -> &str {
        "scalar_subquery_to_join"
    }

    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        plan.transform(|plan| match plan {
            LogicalPlan::Filter(filter) => {
                if !contains_scalar_subquery(&filter.expr) {
                    return Ok(Transformed::no(LogicalPlan::Filter(filter)));
                }

                todo!()
            }
            _ => Ok(Transformed::no(plan)),
        })
        .data()
    }
}

fn contains_scalar_subquery(expr: &LogicalExpr) -> bool {
    let mut contains = false;
    expr.apply(|expr| {
        if let LogicalExpr::SubQuery(_) = expr {
            contains = true;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("[SHOULD NOT HAPPEN] contains scalar subquery");

    contains
}
