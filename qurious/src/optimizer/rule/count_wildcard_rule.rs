use crate::common::transformed::{TransformNode, Transformed};
use crate::datatypes::scalar::ScalarValue;
use crate::error::Result;
use crate::logical::expr::{AggregateExpr, AggregateOperator, LogicalExpr};
use crate::logical::plan::LogicalPlan;
use crate::optimizer::rule::rule_optimizer::OptimizerRule;

pub struct CountWildcardRule;

impl OptimizerRule for CountWildcardRule {
    fn name(&self) -> &str {
        "count_wildcard_rule"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform(|input| input.map_exprs(count_wildcard_rule))
    }
}

fn count_wildcard_rule(expr: LogicalExpr) -> Result<Transformed<LogicalExpr>> {
    if let LogicalExpr::AggregateExpr(agg) = &expr {
        if AggregateOperator::Count == agg.op && LogicalExpr::Wildcard == *agg.expr {
            return Ok(Transformed::yes(LogicalExpr::AggregateExpr(AggregateExpr {
                op: AggregateOperator::Count,
                expr: Box::new(LogicalExpr::Literal(ScalarValue::from(1))),
            })));
        }
    }

    Ok(Transformed::no(expr))
}
