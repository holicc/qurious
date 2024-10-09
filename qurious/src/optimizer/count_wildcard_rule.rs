use super::OptimizerRule;
use crate::common::transformed::{TransformNode, Transformed, TransformedResult};
use crate::datatypes::scalar::ScalarValue;
use crate::error::Result;
use crate::logical::expr::{AggregateExpr, AggregateOperator, LogicalExpr};
use crate::logical::plan::LogicalPlan;

pub struct CountWildcardRule;

impl OptimizerRule for CountWildcardRule {
    fn name(&self) -> &str {
        "count_wildcard_rule"
    }

    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        plan.transform(|input| input.map_exprs(count_wildcard_rule)).data()
    }
}

fn count_wildcard_rule(expr: LogicalExpr) -> Result<Transformed<LogicalExpr>> {
    if let LogicalExpr::AggregateExpr(agg) = &expr {
        if AggregateOperator::Count == agg.op && LogicalExpr::Wildcard == *agg.expr {
            return Ok(Transformed::yes(LogicalExpr::AggregateExpr(AggregateExpr {
                op: AggregateOperator::Count,
                expr: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(1)))),
            })));
        }
    }

    Ok(Transformed::no(expr))
}
