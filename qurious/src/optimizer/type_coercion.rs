use std::sync::Arc;

use arrow::datatypes::Schema;

use super::OptimizerRule;
use crate::common::transformed::{TransformNode, Transformed, TransformedResult};
use crate::error::Result;
use crate::logical::expr::{BinaryExpr, LogicalExpr};
use crate::logical::plan::LogicalPlan;
use crate::utils::{self, merge_schema};

#[derive(Default)]
pub struct TypeCoercion;

impl OptimizerRule for TypeCoercion {
    fn name(&self) -> &str {
        "type_coercion"
    }

    fn optimize(&self, base_plan: LogicalPlan) -> Result<LogicalPlan> {
        let mut merged_schema = Arc::new(Schema::empty());
        let schema = base_plan.schema();

        for input in base_plan.children().into_iter().flat_map(|x| x) {
            merged_schema = merge_schema(&schema, &input.schema()).map(Arc::new)?;
        }

        base_plan
            .transform(|plan| plan.map_exprs(|expr| type_coercion(&merged_schema, expr)))
            .data()
    }
}

fn type_coercion(plan: &Arc<Schema>, expr: LogicalExpr) -> Result<Transformed<LogicalExpr>> {
    match expr {
        LogicalExpr::BinaryExpr(binary_op) => coerce_binary_op(plan, binary_op)
            .map(LogicalExpr::BinaryExpr)
            .map(Transformed::yes),
        _ => Ok(Transformed::no(expr.clone())),
    }
}

fn coerce_binary_op(schema: &Arc<Schema>, expr: BinaryExpr) -> Result<BinaryExpr> {
    let left_type = expr.left.data_type(schema)?;
    let right_type = expr.right.data_type(schema)?;

    if left_type != right_type {
        let final_type = utils::get_input_types(&left_type, &right_type);
        return Ok(BinaryExpr {
            left: Box::new(expr.left.cast_to(&final_type)),
            op: expr.op,
            right: Box::new(expr.right.cast_to(&final_type)),
        });
    }

    Ok(expr)
}
