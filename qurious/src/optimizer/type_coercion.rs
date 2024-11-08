use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};

use super::OptimizerRule;
use crate::common::transformed::{TransformNode, Transformed, TransformedResult};
use crate::error::Result;
use crate::logical::expr::alias::Alias;
use crate::logical::expr::{AggregateExpr, BinaryExpr, LogicalExpr};
use crate::logical::plan::LogicalPlan;
use crate::utils::merge_schema;
use crate::utils::type_coercion::get_input_types;

#[derive(Default)]
pub struct TypeCoercion;

impl OptimizerRule for TypeCoercion {
    fn name(&self) -> &str {
        "type_coercion"
    }

    fn optimize(&self, base_plan: LogicalPlan) -> Result<LogicalPlan> {
        base_plan
            .transform(|plan| {
                if matches!(plan, LogicalPlan::TableScan(_)) {
                    return Ok(Transformed::no(plan));
                }
                let mut merged_schema = Arc::new(Schema::empty());
                let schema = plan.schema();

                for input in plan.children().into_iter().flat_map(|x| x) {
                    merged_schema = merge_schema(&schema, &input.schema()).map(Arc::new)?;
                }

                plan.map_exprs(|expr| type_coercion(&merged_schema, expr))
            })
            .data()
    }
}

fn type_coercion(schema: &Arc<Schema>, expr: LogicalExpr) -> Result<Transformed<LogicalExpr>> {
    match expr {
        LogicalExpr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left = type_coercion(schema, *left).data().map(Box::new)?;
            let right = type_coercion(schema, *right).data().map(Box::new)?;

            coerce_binary_op(schema, BinaryExpr { left, op, right })
                .map(LogicalExpr::BinaryExpr)
                .map(Transformed::yes)
        }
        LogicalExpr::AggregateExpr(AggregateExpr { op, expr }) => type_coercion(schema, *expr)?.map_data(|expr| {
            Ok(LogicalExpr::AggregateExpr(AggregateExpr {
                op,
                expr: Box::new(expr),
            }))
        }),
        LogicalExpr::Alias(Alias { expr, name }) => {
            let expr = type_coercion(schema, *expr).data().map(Box::new)?;
            Ok(Transformed::yes(LogicalExpr::Alias(Alias { expr, name })))
        }
        _ => Ok(Transformed::no(expr)),
    }
}

fn coerce_binary_op(schema: &Arc<Schema>, expr: BinaryExpr) -> Result<BinaryExpr> {
    let left_type = expr.left.data_type(schema)?;
    let right_type = expr.right.data_type(schema)?;

    let (lhs, rhs) = get_input_types(&left_type, &expr.op, &right_type)?;

    Ok(BinaryExpr {
        left: cast_if_needed(expr.left, &left_type, &lhs),
        op: expr.op,
        right: cast_if_needed(expr.right, &right_type, &rhs),
    })
}

fn cast_if_needed(expr: Box<LogicalExpr>, current_type: &DataType, target_type: &DataType) -> Box<LogicalExpr> {
    if current_type != target_type {
        expr.cast_to(target_type).into()
    } else {
        expr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        common::table_relation::TableRelation,
        datatypes::{operator::Operator, scalar::ScalarValue},
        logical::{
            expr::{AggregateExpr, AggregateOperator, Column},
            plan::{EmptyRelation, Projection},
        },
        utils,
    };
    use arrow::datatypes::{DataType, Field};

    fn assert_analyzed_plan_eq(plan: LogicalPlan, expected: &str) {
        let optimizer = TypeCoercion::default();
        let optimized_plan = optimizer.optimize(plan).unwrap();
        assert_eq!(utils::format(&optimized_plan, 0), expected);
    }

    #[test]
    fn test_coerce_binary_expression() -> Result<()> {
        // Int32(int_col) + Float64(float_col)
        let schema = Arc::new(Schema::new(vec![
            Field::new("int_col", DataType::Int32, false),
            Field::new("float_col", DataType::Float64, false),
        ]));
        let expr = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(LogicalExpr::Column(Column::new("int_col", None::<TableRelation>))),
            op: Operator::Add,
            right: Box::new(LogicalExpr::Column(Column::new("float_col", None::<TableRelation>))),
        });
        let plan = LogicalPlan::Projection(Projection {
            exprs: vec![expr],
            input: Box::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(Schema::empty()),
            })),
            schema: schema.clone(),
        });

        assert_analyzed_plan_eq(
            plan,
            "Projection: (CAST(int_col AS Float64) + float_col)\n  Empty Relation\n",
        );

        Ok(())
    }

    #[test]
    fn test_no_coercion_needed() -> Result<()> {
        // Test case where no type coercion is needed for same types
        let schema = Arc::new(Schema::new(vec![
            Field::new("int1", DataType::Int32, false),
            Field::new("int2", DataType::Int32, false),
        ]));
        let expr = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(LogicalExpr::Column(Column::new("int1", None::<TableRelation>))),
            op: Operator::Add,
            right: Box::new(LogicalExpr::Column(Column::new("int2", None::<TableRelation>))),
        });
        let plan = LogicalPlan::Projection(Projection {
            exprs: vec![expr],
            input: Box::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(Schema::empty()),
            })),
            schema: schema.clone(),
        });

        assert_analyzed_plan_eq(plan, "Projection: (int1 + int2)\n  Empty Relation\n");

        Ok(())
    }

    #[test]
    fn test_nested_binary_expression() -> Result<()> {
        // Test nested binary expression: (int_col + float_col) * double_col
        let schema = Arc::new(Schema::new(vec![
            Field::new("int_col", DataType::Int32, false),
            Field::new("float_col", DataType::Float64, false),
            Field::new("double_col", DataType::Float64, false),
        ]));

        let inner_expr = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(LogicalExpr::Column(Column::new("int_col", None::<TableRelation>))),
            op: Operator::Add,
            right: Box::new(LogicalExpr::Column(Column::new("float_col", None::<TableRelation>))),
        });

        let outer_expr = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(inner_expr),
            op: Operator::Mul,
            right: Box::new(LogicalExpr::Column(Column::new("double_col", None::<TableRelation>))),
        });

        let plan = LogicalPlan::Projection(Projection {
            exprs: vec![outer_expr],
            input: Box::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(Schema::empty()),
            })),
            schema: schema.clone(),
        });

        assert_analyzed_plan_eq(
            plan,
            "Projection: (CAST(int_col AS Float64) + float_col * double_col)\n  Empty Relation\n",
        );

        Ok(())
    }

    #[test]
    fn test_mixed_numeric_types() -> Result<()> {
        // Test mixing different numeric types: int16 + int32 + float32 + float64
        let schema = Arc::new(Schema::new(vec![
            Field::new("int16_col", DataType::Int16, false),
            Field::new("int32_col", DataType::Int32, false),
            Field::new("float32_col", DataType::Float32, false),
            Field::new("float64_col", DataType::Float64, false),
        ]));

        let expr1 = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(LogicalExpr::Column(Column::new("int16_col", None::<TableRelation>))),
            op: Operator::Add,
            right: Box::new(LogicalExpr::Column(Column::new("float64_col", None::<TableRelation>))),
        });

        let expr2 = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(expr1),
            op: Operator::Add,
            right: Box::new(LogicalExpr::Column(Column::new("float32_col", None::<TableRelation>))),
        });

        let plan = LogicalPlan::Projection(Projection {
            exprs: vec![expr2],
            input: Box::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(Schema::empty()),
            })),
            schema: schema.clone(),
        });

        assert_analyzed_plan_eq(
            plan,
            "Projection: (CAST(int16_col AS Float64) + float64_col + CAST(float32_col AS Float64))\n  Empty Relation\n",
        );

        Ok(())
    }

    #[test]
    fn test_nested_aggregate_expression() -> Result<()> {
        // Test nested aggregate expression with type coercion:
        // sum(int_col) + avg(float32_col) * count(double_col)
        let schema = Arc::new(Schema::new(vec![
            Field::new("int_col", DataType::Int32, false),
            Field::new("float32_col", DataType::Float32, false),
            Field::new("double_col", DataType::Float64, false),
        ]));

        // Create sum(int_col)
        let sum_expr = LogicalExpr::AggregateExpr(AggregateExpr {
            op: AggregateOperator::Sum,
            expr: Box::new(LogicalExpr::Column(Column::new("int_col", None::<TableRelation>))),
        });

        // Create avg(float32_col)
        let avg_expr = LogicalExpr::AggregateExpr(AggregateExpr {
            op: AggregateOperator::Avg,
            expr: Box::new(LogicalExpr::Column(Column::new("float32_col", None::<TableRelation>))),
        });

        // Create count(double_col)
        let count_expr = LogicalExpr::AggregateExpr(AggregateExpr {
            op: AggregateOperator::Count,
            expr: Box::new(LogicalExpr::Column(Column::new("double_col", None::<TableRelation>))),
        });

        // Create avg * count
        let mul_expr = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(avg_expr),
            op: Operator::Mul,
            right: Box::new(count_expr),
        });

        // Create sum + (avg * count)
        let final_expr = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(sum_expr),
            op: Operator::Add,
            right: Box::new(mul_expr),
        });

        let plan = LogicalPlan::Projection(Projection {
            exprs: vec![final_expr],
            input: Box::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(Schema::empty()),
            })),
            schema: schema.clone(),
        });

        assert_analyzed_plan_eq(
            plan,
            "Projection: (CAST(SUM(int_col) AS Float64) + AVG(float32_col) * CAST(COUNT(double_col) AS Float64))\n  Empty Relation\n",
        );

        Ok(())
    }

    #[test]
    fn test_aggregate_function_coercion() -> Result<()> {
        // Test type coercion in aggregate functions:
        // - avg(int32_col) -> avg(cast(int32_col as float64))
        // - sum(float32_col) -> sum(cast(float32_col as float64))
        let schema = Arc::new(Schema::new(vec![
            Field::new("int32_col", DataType::Int32, false),
            Field::new("float32_col", DataType::Float32, false),
        ]));

        // Create avg(int32_col)
        let avg_expr = LogicalExpr::AggregateExpr(AggregateExpr {
            op: AggregateOperator::Avg,
            expr: Box::new(LogicalExpr::Column(Column::new("int32_col", None::<TableRelation>))),
        });

        // Create sum(float32_col)
        let sum_expr = LogicalExpr::AggregateExpr(AggregateExpr {
            op: AggregateOperator::Sum,
            expr: Box::new(LogicalExpr::Column(Column::new("float32_col", None::<TableRelation>))),
        });

        // Create avg + sum
        let final_expr = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(avg_expr),
            op: Operator::Add,
            right: Box::new(sum_expr),
        });

        let plan = LogicalPlan::Projection(Projection {
            exprs: vec![final_expr],
            input: Box::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(Schema::empty()),
            })),
            schema: schema.clone(),
        });

        assert_analyzed_plan_eq(
            plan,
            "Projection: (AVG(int32_col) + CAST(SUM(float32_col) AS Float64))\n  Empty Relation\n",
        );

        Ok(())
    }

    #[test]
    fn test_literal_coercion() -> Result<()> {
        // Test type coercion with literals:
        // - float_col + 42 (int literal)
        // - int_col * 3.14 (float literal)
        let schema = Arc::new(Schema::new(vec![
            Field::new("float_col", DataType::Float64, false),
            Field::new("int_col", DataType::Int32, false),
        ]));

        // Create float_col + 42
        let add_expr = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(LogicalExpr::Column(Column::new("float_col", None::<TableRelation>))),
            op: Operator::Add,
            right: Box::new(LogicalExpr::Literal(ScalarValue::from(42i32))),
        });

        // Create int_col * 3.14
        let mul_expr = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(LogicalExpr::Column(Column::new("int_col", None::<TableRelation>))),
            op: Operator::Mul,
            right: Box::new(LogicalExpr::Literal(ScalarValue::from(3.14))),
        });

        // Combine both expressions: (float_col + 42) + (int_col * 3.14)
        let final_expr = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(add_expr),
            op: Operator::Add,
            right: Box::new(mul_expr),
        });

        let plan = LogicalPlan::Projection(Projection {
            exprs: vec![final_expr],
            input: Box::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(Schema::empty()),
            })),
            schema: schema.clone(),
        });

        assert_analyzed_plan_eq(
            plan,
            "Projection: (float_col + CAST(Int32(42) AS Float64) + CAST(int_col AS Float64) * Float64(3.14))\n  Empty Relation\n",
        );

        Ok(())
    }

    #[test]
    fn test_literal_in_aggregate() -> Result<()> {
        // Test literal coercion in aggregate expressions:
        // sum(int_col + 1.5) -> sum(cast(int_col as float64) + 1.5)
        let schema = Arc::new(Schema::new(vec![Field::new("int_col", DataType::Int32, false)]));

        // Create int_col + 1.5
        let add_expr = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(LogicalExpr::Column(Column::new("int_col", None::<TableRelation>))),
            op: Operator::Add,
            right: Box::new(LogicalExpr::Literal(ScalarValue::from(1.5f64))),
        });

        // Create sum(int_col + 1.5)
        let sum_expr = LogicalExpr::AggregateExpr(AggregateExpr {
            op: AggregateOperator::Sum,
            expr: Box::new(add_expr),
        });

        let plan = LogicalPlan::Projection(Projection {
            exprs: vec![sum_expr],
            input: Box::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(Schema::empty()),
            })),
            schema: schema.clone(),
        });

        assert_analyzed_plan_eq(
            plan,
            "Projection: (SUM(CAST(int_col AS Float64) + Float64(1.5)))\n  Empty Relation\n",
        );

        Ok(())
    }
}
