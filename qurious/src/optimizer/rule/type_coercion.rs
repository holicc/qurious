use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};

use crate::common::table_schema::TableSchema;
use crate::common::transformed::{Transformed, TransformedResult};
use crate::error::Result;
use crate::logical::expr::alias::Alias;
use crate::logical::expr::{AggregateExpr, BinaryExpr, CaseExpr, LogicalExpr};
use crate::logical::plan::{Aggregate, EmptyRelation, Filter, LogicalPlan, Projection};
use crate::optimizer::rule::rule_optimizer::OptimizerRule;
use crate::utils::expr::exprs_to_fields;
use crate::utils::merge_schema;
use crate::utils::type_coercion::get_input_types;

pub struct TypeCoercion;

impl OptimizerRule for TypeCoercion {
    fn name(&self) -> &str {
        "type_coercion"
    }

    fn rewrite(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        // NOTE: PushdownFilter can move predicates into TableScan.filter. We must coerce those too
        // (e.g. Date32 >= '1993-07-01' in TPC-H Q4).
        if let LogicalPlan::TableScan(mut scan) = plan {
            let Some(filter) = scan.filter.take() else {
                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
            };
            let schema = scan.schema();
            let filter = type_coercion(&schema, filter).data()?;
            scan.filter = Some(filter);
            return Ok(Transformed::yes(LogicalPlan::TableScan(scan)));
        }
        let mut merged_schema = Arc::new(Schema::empty());
        let schema = plan.schema();

        for input in plan.children().into_iter().flat_map(|x| x) {
            merged_schema = merge_schema(&schema, &input.schema()).map(Arc::new)?;
        }

        // IMPORTANT: if we change expressions, we must rebuild the node schema so output field
        // names/types stay consistent with the new expressions (otherwise physical planning fails
        // when looking up fields by name).
        match plan {
            LogicalPlan::Projection(Projection {
                schema: _,
                input,
                exprs,
            }) => {
                let exprs = exprs
                    .into_iter()
                    .map(|expr| type_coercion(&merged_schema, expr).data())
                    .collect::<Result<Vec<_>>>()?;

                // Build a "reference" plan for expression typing using the merged schema
                // (important for tests and for cases where the immediate input schema may not
                // contain all fields needed for type inference).
                let ref_plan = LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: true,
                    schema: merged_schema.clone(),
                });

                let mut field_qualifiers = vec![];
                let mut fields = vec![];
                for expr in &exprs {
                    field_qualifiers.push(expr.qualified_name());
                    fields.push(expr.field(&ref_plan)?);
                }
                let schema = Arc::new(TableSchema::new(field_qualifiers, Arc::new(Schema::new(fields))));

                Ok(Transformed::yes(LogicalPlan::Projection(
                    Projection::try_new_with_schema(*input, exprs, schema)?,
                )))
            }
            LogicalPlan::Aggregate(Aggregate {
                schema: _,
                input,
                group_expr,
                aggr_expr,
            }) => {
                let group_expr = group_expr
                    .into_iter()
                    .map(|expr| type_coercion(&merged_schema, expr).data())
                    .collect::<Result<Vec<_>>>()?;
                let aggr_expr = aggr_expr
                    .into_iter()
                    .map(|expr| type_coercion(&merged_schema, expr).data())
                    .collect::<Result<Vec<_>>>()?;

                let ref_plan = LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: true,
                    schema: merged_schema.clone(),
                });

                let mut qualified_fields = exprs_to_fields(&group_expr, &ref_plan)?;
                qualified_fields.extend(exprs_to_fields(&aggr_expr, &ref_plan)?);
                let schema = TableSchema::try_new(qualified_fields).map(Arc::new)?;

                Ok(Transformed::yes(LogicalPlan::Aggregate(Aggregate {
                    schema,
                    input,
                    group_expr,
                    aggr_expr,
                })))
            }
            LogicalPlan::Filter(Filter { input, expr }) => {
                let expr = type_coercion(&merged_schema, expr).data()?;
                Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(*input, expr)?)))
            }
            _ => Ok(Transformed::no(plan)),
        }
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
        LogicalExpr::Case(CaseExpr {
            operand,
            when_then,
            else_expr,
        }) => {
            let operand = operand
                .map(|op| type_coercion(schema, *op).data().map(Box::new))
                .transpose()?;
            let when_then = when_then
                .into_iter()
                .map(|(w, t)| Ok((type_coercion(schema, w).data()?, type_coercion(schema, t).data()?)))
                .collect::<Result<Vec<_>>>()?;
            let else_expr = type_coercion(schema, *else_expr).data()?;

            // Coerce THEN/ELSE value expressions to a common type.
            let mut value_types = vec![];
            for (_, t) in &when_then {
                value_types.push(t.data_type(schema)?);
            }
            value_types.push(else_expr.data_type(schema)?);

            let target = coerce_case_value_type(&value_types);
            let when_then = when_then
                .into_iter()
                .map(|(w, t)| {
                    let cur = t.data_type(schema)?;
                    let t = if cur != target { t.cast_to(&target) } else { t };
                    Ok((w, t))
                })
                .collect::<Result<Vec<_>>>()?;
            let else_cur = else_expr.data_type(schema)?;
            let else_expr = if else_cur != target {
                else_expr.cast_to(&target)
            } else {
                else_expr
            };

            Ok(Transformed::yes(LogicalExpr::Case(CaseExpr {
                operand,
                when_then,
                else_expr: Box::new(else_expr),
            })))
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

fn coerce_case_value_type(types: &[DataType]) -> DataType {
    use arrow::datatypes::DataType::*;

    let mut has_decimal256 = false;
    let mut dec_p: u8 = 0;
    let mut dec_s: i8 = 0;
    let mut has_f64 = false;
    let mut has_f32 = false;
    let mut has_i64 = false;
    let mut has_int = false;

    for t in types {
        match t {
            Decimal256(p, s) => {
                has_decimal256 = true;
                dec_p = dec_p.max(*p);
                dec_s = dec_s.max(*s);
            }
            Decimal128(p, s) => {
                dec_p = dec_p.max(*p);
                dec_s = dec_s.max(*s);
            }
            Float64 => has_f64 = true,
            Float32 => has_f32 = true,
            Int64 => has_i64 = true,
            Int8 | Int16 | Int32 | UInt8 | UInt16 | UInt32 | UInt64 => has_int = true,
            Null => {}
            other => return other.clone(),
        }
    }

    if has_decimal256 {
        return Decimal256(dec_p.max(1), dec_s);
    }
    if dec_p > 0 {
        return Decimal128(dec_p, dec_s);
    }
    if has_f64 {
        return Float64;
    }
    if has_f32 {
        return Float32;
    }
    if has_i64 || has_int {
        return Int64;
    }
    DataType::Null
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
        common::{table_relation::TableRelation, table_schema::TableSchema},
        datatypes::{operator::Operator, scalar::ScalarValue},
        logical::{
            expr::{AggregateExpr, AggregateOperator, Column},
            plan::{EmptyRelation, Projection},
        },
        utils,
    };
    use arrow::datatypes::{DataType, Field};

    fn assert_analyzed_plan_eq(plan: LogicalPlan, expected: &str) {
        let optimizer = TypeCoercion;
        let optimized_plan = optimizer.rewrite(plan).data().unwrap();
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
            left: Box::new(LogicalExpr::Column(Column::new(
                "int_col",
                None::<TableRelation>,
                false,
            ))),
            op: Operator::Add,
            right: Box::new(LogicalExpr::Column(Column::new(
                "float_col",
                None::<TableRelation>,
                false,
            ))),
        });
        let plan = LogicalPlan::Projection(Projection {
            exprs: vec![expr],
            input: Box::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(Schema::empty()),
            })),
            schema: Arc::new(TableSchema::new(vec![], schema)),
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
            left: Box::new(LogicalExpr::Column(Column::new("int1", None::<TableRelation>, false))),
            op: Operator::Add,
            right: Box::new(LogicalExpr::Column(Column::new("int2", None::<TableRelation>, false))),
        });
        let plan = LogicalPlan::Projection(Projection {
            exprs: vec![expr],
            input: Box::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(Schema::empty()),
            })),
            schema: Arc::new(TableSchema::new(vec![], schema)),
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
            left: Box::new(LogicalExpr::Column(Column::new(
                "int_col",
                None::<TableRelation>,
                false,
            ))),
            op: Operator::Add,
            right: Box::new(LogicalExpr::Column(Column::new(
                "float_col",
                None::<TableRelation>,
                false,
            ))),
        });

        let outer_expr = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(inner_expr),
            op: Operator::Mul,
            right: Box::new(LogicalExpr::Column(Column::new(
                "double_col",
                None::<TableRelation>,
                false,
            ))),
        });

        let plan = LogicalPlan::Projection(Projection {
            exprs: vec![outer_expr],
            input: Box::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(Schema::empty()),
            })),
            schema: Arc::new(TableSchema::new(vec![], schema)),
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
            left: Box::new(LogicalExpr::Column(Column::new(
                "int16_col",
                None::<TableRelation>,
                false,
            ))),
            op: Operator::Add,
            right: Box::new(LogicalExpr::Column(Column::new(
                "float64_col",
                None::<TableRelation>,
                false,
            ))),
        });

        let expr2 = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(expr1),
            op: Operator::Add,
            right: Box::new(LogicalExpr::Column(Column::new(
                "float32_col",
                None::<TableRelation>,
                false,
            ))),
        });

        let plan = LogicalPlan::Projection(Projection {
            exprs: vec![expr2],
            input: Box::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(Schema::empty()),
            })),
            schema: Arc::new(TableSchema::new(vec![], schema)),
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
            expr: Box::new(LogicalExpr::Column(Column::new(
                "int_col",
                None::<TableRelation>,
                false,
            ))),
        });

        // Create avg(float32_col)
        let avg_expr = LogicalExpr::AggregateExpr(AggregateExpr {
            op: AggregateOperator::Avg,
            expr: Box::new(LogicalExpr::Column(Column::new(
                "float32_col",
                None::<TableRelation>,
                false,
            ))),
        });

        // Create count(double_col)
        let count_expr = LogicalExpr::AggregateExpr(AggregateExpr {
            op: AggregateOperator::Count,
            expr: Box::new(LogicalExpr::Column(Column::new(
                "double_col",
                None::<TableRelation>,
                false,
            ))),
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
            schema: Arc::new(TableSchema::new(vec![], schema)),
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
            expr: Box::new(LogicalExpr::Column(Column::new(
                "int32_col",
                None::<TableRelation>,
                false,
            ))),
        });

        // Create sum(float32_col)
        let sum_expr = LogicalExpr::AggregateExpr(AggregateExpr {
            op: AggregateOperator::Sum,
            expr: Box::new(LogicalExpr::Column(Column::new(
                "float32_col",
                None::<TableRelation>,
                false,
            ))),
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
            schema: Arc::new(TableSchema::new(vec![], schema)),
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
            left: Box::new(LogicalExpr::Column(Column::new(
                "float_col",
                None::<TableRelation>,
                false,
            ))),
            op: Operator::Add,
            right: Box::new(LogicalExpr::Literal(ScalarValue::from(42i32))),
        });

        // Create int_col * 3.14
        let mul_expr = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(LogicalExpr::Column(Column::new(
                "int_col",
                None::<TableRelation>,
                false,
            ))),
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
            schema: Arc::new(TableSchema::new(vec![], schema)),
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
            left: Box::new(LogicalExpr::Column(Column::new(
                "int_col",
                None::<TableRelation>,
                false,
            ))),
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
            schema: Arc::new(TableSchema::new(vec![], schema)),
        });

        assert_analyzed_plan_eq(
            plan,
            "Projection: (SUM(CAST(int_col AS Float64) + Float64(1.5)))\n  Empty Relation\n",
        );

        Ok(())
    }

    #[test]
    fn test_date_comparison_with_string_literal_is_cast() -> Result<()> {
        // date_col >= '1993-07-01' should become date_col >= CAST('1993-07-01' AS Date32)
        let schema = Arc::new(Schema::new(vec![Field::new("date_col", DataType::Date32, false)]));
        let expr = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(LogicalExpr::Column(Column::new(
                "date_col",
                None::<TableRelation>,
                false,
            ))),
            op: Operator::GtEq,
            right: Box::new(LogicalExpr::Literal(ScalarValue::Utf8(Some("1993-07-01".to_string())))),
        });
        let plan = LogicalPlan::Projection(Projection {
            exprs: vec![expr],
            input: Box::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(Schema::empty()),
            })),
            schema: Arc::new(TableSchema::new(vec![], schema)),
        });

        assert_analyzed_plan_eq(
            plan,
            "Projection: (date_col >= CAST(Utf8('1993-07-01') AS Date32))\n  Empty Relation\n",
        );

        Ok(())
    }
}
