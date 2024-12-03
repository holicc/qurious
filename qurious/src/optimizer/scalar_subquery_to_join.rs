use super::OptimizerRule;
use crate::common::transformed::{TransformNode, Transformed, TransformedResult, TreeNodeRecursion};
use crate::error::Result;
use crate::logical::expr::LogicalExpr;
use crate::logical::plan::LogicalPlan;
use crate::logical::LogicalPlanBuilder;

/// Convert scalar subquery to join
///
/// ```sql
/// SELECT a FROM t1 WHERE t1.a = (SELECT MIN(b) FROM t2 WHERE t2.a = t1.a);
/// ```
///
/// After the rule is applied, the plan will look like this:
/// ```text
/// SELECT a FROM t1 LEFT JOIN (SELECT MIN(b) FROM t2 WHERE t2.a = t1.a) AS t2 ON t1.a = t2.a WHERE t1.a = t2.b;
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
                    return Ok(Transformed::no(plan));
                }

                let (aliases, rewritten_subquery) = extract_scalar_subquery(&filter.expr)?;

                let alias_subquery = LogicalPlanBuilder::from(*filter.input)
                    .join_on(aliases)
                    .build()?;

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

fn extract_scalar_subquery(expr: &LogicalExpr) -> Result<(Vec<(LogicalExpr, String)>, LogicalExpr)> {
    todo!()
}
#[cfg(test)]
mod tests {

    #[test]
    fn test_contains_scalar_subquery() {}
}
