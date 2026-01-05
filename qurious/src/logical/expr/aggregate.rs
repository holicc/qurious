use arrow::datatypes::{
    DataType, Field, FieldRef, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION,
    DECIMAL256_MAX_SCALE,
};

use crate::error::{Error, Result};
use crate::internal_err;
use crate::logical::expr::LogicalExpr;
use crate::logical::plan::LogicalPlan;
use std::convert::TryFrom;
use std::fmt::Display;
use std::sync::Arc;

use super::Column;
use crate::datatypes::scalar::ScalarValue;

/// Format an expression for *naming* purposes, stripping out CAST(...) (and nested alias) wrappers.
///
/// Type coercion may insert casts without changing the logical meaning of an expression; we don't
/// want those casts to affect output field names, otherwise downstream column lookups can break
/// (e.g. SUM(a*b) vs SUM(a*CAST(b AS ...))).
fn fmt_expr_for_name(expr: &LogicalExpr) -> String {
    match expr {
        LogicalExpr::Cast(c) => fmt_expr_for_name(&c.expr),
        LogicalExpr::Alias(a) => fmt_expr_for_name(&a.expr),
        LogicalExpr::Column(c) => c.to_string(),
        LogicalExpr::Literal(v) => v.to_string(),
        LogicalExpr::Negative(e) => format!("- {}", fmt_expr_for_name(e)),
        LogicalExpr::BinaryExpr(b) => format!(
            "{} {} {}",
            fmt_expr_for_name(&b.left),
            b.op,
            fmt_expr_for_name(&b.right)
        ),
        LogicalExpr::Case(case) => {
            let mut s = String::from("CASE");
            if let Some(op) = &case.operand {
                s.push(' ');
                s.push_str(&fmt_expr_for_name(op));
            }
            for (w, t) in &case.when_then {
                s.push_str(" WHEN ");
                s.push_str(&fmt_expr_for_name(w));
                s.push_str(" THEN ");
                s.push_str(&fmt_expr_for_name(t));
            }
            s.push_str(" ELSE ");
            s.push_str(&fmt_expr_for_name(&case.else_expr));
            s.push_str(" END");
            s
        }
        other => other.to_string(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggregateOperator {
    Sum,
    Min,
    Max,
    Avg,
    Count,
}

impl AggregateOperator {
    pub fn infer_type(&self, expr_data_type: &DataType) -> Result<DataType> {
        match self {
            AggregateOperator::Count => Ok(DataType::Int64),
            AggregateOperator::Avg => avg_return_type(expr_data_type),
            _ => Ok(expr_data_type.clone()),
        }
    }
}

fn avg_return_type(expr_data_type: &DataType) -> Result<DataType> {
    match expr_data_type {
        DataType::Decimal128(precision, scale) => {
            let new_precision = DECIMAL128_MAX_PRECISION.min(*precision + 4);
            let new_scale = DECIMAL128_MAX_SCALE.min(*scale + 4);
            Ok(DataType::Decimal128(new_precision, new_scale))
        }
        DataType::Decimal256(precision, scale) => {
            let new_precision = DECIMAL256_MAX_PRECISION.min(*precision + 4);
            let new_scale = DECIMAL256_MAX_SCALE.min(*scale + 4);
            Ok(DataType::Decimal256(new_precision, new_scale))
        }
        arg_type if arg_type.is_integer() || arg_type.is_floating() => Ok(DataType::Float64),
        other => internal_err!("avg does not support {other:?}"),
    }
}

impl Display for AggregateOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateOperator::Sum => write!(f, "SUM"),
            AggregateOperator::Min => write!(f, "MIN"),
            AggregateOperator::Max => write!(f, "MAX"),
            AggregateOperator::Avg => write!(f, "AVG"),
            AggregateOperator::Count => write!(f, "COUNT"),
        }
    }
}

impl TryFrom<String> for AggregateOperator {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        match value.to_lowercase().as_str() {
            "sum" => Ok(AggregateOperator::Sum),
            "min" => Ok(AggregateOperator::Min),
            "max" => Ok(AggregateOperator::Max),
            "avg" => Ok(AggregateOperator::Avg),
            "count" => Ok(AggregateOperator::Count),
            _ => Err(Error::InternalError(format!(
                "{} is not a valid aggregate operator",
                value
            ))),
        }
    }
}

impl TryFrom<&str> for AggregateOperator {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        Self::try_from(value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AggregateExpr {
    pub op: AggregateOperator,
    pub expr: Box<LogicalExpr>,
}

impl AggregateExpr {
    pub fn field(&self, plan: &LogicalPlan) -> Result<FieldRef> {
        self.expr.field(plan).and_then(|field| {
            // Use the *expression string* for non-column arguments, otherwise we may generate
            // names like COUNT(i32) from Arrow field names which won't match expression display.
            //
            // Special case: COUNT(*) is rewritten to COUNT(1) by `CountWildcardRule`, but the
            // output column name must remain COUNT(*) for SQL compatibility / tests.
            let col_name = match (self.op.clone(), self.expr.as_ref()) {
                (AggregateOperator::Count, LogicalExpr::Literal(ScalarValue::Int32(Some(1))))
                | (AggregateOperator::Count, LogicalExpr::Literal(ScalarValue::Int64(Some(1)))) => "*".to_string(),
                (_, LogicalExpr::Column(inner)) => inner.qualified_name(),
                (_, other) => fmt_expr_for_name(other),
            };

            Ok(Arc::new(Field::new(
                format!("{}({})", self.op, col_name),
                self.op.infer_type(field.data_type())?,
                true,
            )))
        })
    }

    pub(crate) fn as_column(&self) -> Result<LogicalExpr> {
        // Keep COUNT(*) naming stable even if it was rewritten to COUNT(1) internally.
        if self.op == AggregateOperator::Count {
            if matches!(
                self.expr.as_ref(),
                LogicalExpr::Literal(ScalarValue::Int32(Some(1))) | LogicalExpr::Literal(ScalarValue::Int64(Some(1)))
            ) {
                return Ok(LogicalExpr::Column(Column {
                    name: "COUNT(*)".to_string(),
                    relation: None,
                    is_outer_ref: false,
                }));
            }
        }

        let arg_name = match self.expr.as_ref() {
            LogicalExpr::Column(c) => c.to_string(),
            other => fmt_expr_for_name(other),
        };

        Ok(LogicalExpr::Column(Column {
            name: format!("{}({})", self.op, arg_name),
            relation: None,
            is_outer_ref: false,
        }))
    }
}

impl Display for AggregateExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.op, self.expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::operator::Operator;
    use crate::logical::expr::{BinaryExpr, CaseExpr, CastExpr, Column, LogicalExpr};
    use crate::logical::plan::{EmptyRelation, LogicalPlan};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn empty_plan_with_schema(fields: Vec<Field>) -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            schema: Arc::new(Schema::new(fields)),
        })
    }

    #[test]
    fn count_star_keeps_output_name_after_rewrite_to_count_1() {
        // Optimizer rule rewrites COUNT(*) -> COUNT(1) for execution.
        // However, the output column name must remain COUNT(*) to match SQL surface semantics
        // and sqllogictest expectations.
        let plan = empty_plan_with_schema(vec![]);
        let agg = AggregateExpr {
            op: AggregateOperator::Count,
            expr: Box::new(LogicalExpr::Literal(ScalarValue::Int32(Some(1)))),
        };

        let field = agg.field(&plan).unwrap();
        assert_eq!(field.name(), "COUNT(*)");

        let col = agg.as_column().unwrap();
        assert_eq!(col.to_string(), "COUNT(*)");
    }

    #[test]
    fn aggregate_naming_ignores_casts_in_argument_expression() {
        // TypeCoercion may insert CASTs (e.g. to make DECIMAL * INT valid),
        // but we don't want those CASTs to affect the aggregate output column name.
        let plan = empty_plan_with_schema(vec![
            Field::new("a", DataType::Decimal128(15, 2), false),
            Field::new("b", DataType::Int64, false),
        ]);

        let expr = LogicalExpr::BinaryExpr(BinaryExpr::new(
            LogicalExpr::Column(Column::new(
                "a",
                None::<crate::common::table_relation::TableRelation>,
                false,
            )),
            Operator::Mul,
            LogicalExpr::Cast(CastExpr::new(
                LogicalExpr::Column(Column::new(
                    "b",
                    None::<crate::common::table_relation::TableRelation>,
                    false,
                )),
                DataType::Decimal128(20, 0),
            )),
        ));

        let agg = AggregateExpr {
            op: AggregateOperator::Sum,
            expr: Box::new(expr),
        };

        let field = agg.field(&plan).unwrap();
        // cast is ignored for naming: b (not CAST(b AS ...))
        assert_eq!(field.name(), "SUM(a * b)");
        assert_eq!(agg.as_column().unwrap().to_string(), "SUM(a * b)");
    }

    #[test]
    fn aggregate_naming_ignores_casts_inside_case_expression() {
        // Similar to TPCH Q8: CASE branch literals may get casted by type coercion,
        // but the aggregate output name should stay stable.
        let plan = empty_plan_with_schema(vec![
            Field::new("cond", DataType::Boolean, false),
            Field::new("v", DataType::Decimal128(38, 4), false),
        ]);

        let case = CaseExpr {
            operand: None,
            when_then: vec![(
                LogicalExpr::Column(Column::new(
                    "cond",
                    None::<crate::common::table_relation::TableRelation>,
                    false,
                )),
                LogicalExpr::Column(Column::new(
                    "v",
                    None::<crate::common::table_relation::TableRelation>,
                    false,
                )),
            )],
            else_expr: Box::new(LogicalExpr::Cast(CastExpr::new(
                LogicalExpr::Literal(ScalarValue::Int64(Some(0))),
                DataType::Decimal128(38, 4),
            ))),
        };

        let agg = AggregateExpr {
            op: AggregateOperator::Sum,
            expr: Box::new(LogicalExpr::Case(case)),
        };

        let field = agg.field(&plan).unwrap();
        assert_eq!(field.name(), "SUM(CASE WHEN cond THEN v ELSE Int64(0) END)");
        assert_eq!(
            agg.as_column().unwrap().to_string(),
            "SUM(CASE WHEN cond THEN v ELSE Int64(0) END)"
        );
    }
}
