use super::OptimizerRule;
use crate::error::Result;
use crate::logical::expr::{BinaryExpr, LogicalExpr};
use crate::logical::plan::{LogicalPlan, Transformed};

#[derive(Default)]
pub struct TypeCoercion {}

impl OptimizerRule for TypeCoercion {
    fn name(&self) -> &str {
        "type_coercion"
    }

    fn optimize(&self, base_plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        base_plan
            .clone()
            .map_expr(|expr| type_coercion(base_plan, expr))
            .map(Some)
    }
}

fn type_coercion(plan: &LogicalPlan, expr: &LogicalExpr) -> Result<Transformed<LogicalExpr>> {
    match expr {
        LogicalExpr::BinaryExpr(binary_op) => coerce_binary_op(plan, binary_op)
            .map(LogicalExpr::BinaryExpr)
            .map(Transformed::Yes),
        _ => todo!(),
    }
}

fn coerce_binary_op(plan: &LogicalPlan, expr: &BinaryExpr) -> Result<BinaryExpr> {
    let final_type = expr.get_result_type(plan)?;

    Ok(BinaryExpr {
        left: Box::new(expr.left.cast_to(&final_type)),
        op: expr.op.clone(),
        right: Box::new(expr.right.cast_to(&final_type)),
    })
}
