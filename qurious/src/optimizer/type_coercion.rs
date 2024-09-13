use super::OptimizerRule;
use crate::error::Result;
use crate::logical::expr::{BinaryExpr, LogicalExpr};
use crate::logical::plan::{base_plan, LogicalPlan, Transformed};
use crate::utils;

#[derive(Default)]
pub struct TypeCoercion {}

impl OptimizerRule for TypeCoercion {
    fn name(&self) -> &str {
        "type_coercion"
    }

    fn optimize(&self, base_plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let plan = base_plan.clone();
        let plan = if let LogicalPlan::Filter(mut filter) = plan {
            if let Transformed::Yes(new_expr) = type_coercion(base_plan, &filter.expr)? {
                filter.expr = new_expr;
            }
            LogicalPlan::Filter(filter)
        } else {
            plan
        };

        plan.map_expr(type_coercion).map(Some)
    }
}

fn type_coercion(plan: &LogicalPlan, expr: &LogicalExpr) -> Result<Transformed<LogicalExpr>> {
    match expr {
        LogicalExpr::BinaryExpr(binary_op) => coerce_binary_op(base_plan(plan), binary_op)
            .map(LogicalExpr::BinaryExpr)
            .map(Transformed::Yes),
        _ => Ok(Transformed::No),
    }
}

fn coerce_binary_op(plan: &LogicalPlan, expr: &BinaryExpr) -> Result<BinaryExpr> {
    let ll = expr.left.field(plan)?;
    let rr = expr.right.field(plan)?;
    let left_type = ll.data_type();
    let right_type = rr.data_type();

    let (l, r) = if left_type == right_type {
        (expr.left.as_ref().clone(), expr.right.as_ref().clone())
    } else {
        let final_type = utils::get_input_types(left_type, right_type);
        (expr.left.cast_to(&final_type), expr.right.cast_to(&final_type))
    };

    Ok(BinaryExpr {
        left: Box::new(l),
        op: expr.op.clone(),
        right: Box::new(r),
    })
}
