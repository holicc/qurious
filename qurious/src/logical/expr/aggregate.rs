use arrow::datatypes::{Field, FieldRef};

use crate::error::Result;
use crate::logical::expr::LogicalExpr;
use crate::logical::plan::LogicalPlan;
use std::fmt::Display;
use std::sync::Arc;

use super::Column;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggregateOperator {
    Sum,
    Min,
    Max,
    Avg,
    Count,
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

impl From<String> for AggregateOperator {
    fn from(value: String) -> Self {
        match value.to_lowercase().as_str() {
            "sum" => AggregateOperator::Sum,
            "min" => AggregateOperator::Min,
            "max" => AggregateOperator::Max,
            "avg" => AggregateOperator::Avg,
            "count" => AggregateOperator::Count,
            _ => unimplemented!("{} is not a valid aggregate operator", value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AggregateExpr {
    pub op: AggregateOperator,
    pub expr: Box<LogicalExpr>,
}

impl AggregateExpr {
    pub fn field(&self, plan: &LogicalPlan) -> Result<FieldRef> {
        self.expr.field(plan).map(|field| {
            let col_name = if let LogicalExpr::Column(inner) = self.expr.as_ref() {
                &inner.quanlified_name()
            } else {
                field.name()
            };

            Arc::new(Field::new(
                format!("{}({})", self.op, col_name),
                field.data_type().clone(),
                false,
            ))
        })
    }

    pub fn as_column(&self) -> Result<LogicalExpr> {
        self.expr.as_column().map(|inner_col| {
            LogicalExpr::Column(Column {
                name: format!("{}({})", self.op, inner_col),
                relation: None,
            })
        })
    }
}

impl Display for AggregateExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.op, self.expr)
    }
}

// macro_rules! make_aggregate_expr_fn {
//     ($name: ident, $op: expr, $re: ident) => {
//         pub fn $name(expr: LogicalExpr) -> $re {
//             $re {
//                 op: $op,
//                 expr: Box::new(expr),
//             }
//         }
//     };
// }

// make_aggregate_expr_fn!(sum, AggregateOperator::Sum, AggregateExpr);
// make_aggregate_expr_fn!(min, AggregateOperator::Min, AggregateExpr);
// make_aggregate_expr_fn!(max, AggregateOperator::Max, AggregateExpr);
// make_aggregate_expr_fn!(avg, AggregateOperator::Avg, AggregateExpr);
// make_aggregate_expr_fn!(count, AggregateOperator::Count, AggregateExpr);
