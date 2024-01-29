use crate::error::Result;
use crate::expr::LogicalExpr;
use crate::types::datatype::DataType;
use crate::{logical_plan::LogicalPlan, types::field::Field};
use std::fmt::Display;

#[derive(Debug, Clone, PartialEq)]
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
            AggregateOperator::Sum => write!(f, "sum"),
            AggregateOperator::Min => write!(f, "min"),
            AggregateOperator::Max => write!(f, "max"),
            AggregateOperator::Avg => write!(f, "avg"),
            AggregateOperator::Count => write!(f, "count"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AggregateExpr {
    op: AggregateOperator,
    expr: Box<LogicalExpr>,
}

impl AggregateExpr {
    pub fn to_field(&self, plan: &LogicalPlan) -> Result<Field> {
        Ok(Field {
            name: self.op.to_string(),
            datatype: if self.op == AggregateOperator::Count {
                DataType::Int64
            } else {
                self.expr.to_field(plan)?.datatype
            },
        })
    }
}

impl Display for AggregateExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.op, self.expr)
    }
}

macro_rules! make_aggregate_expr_fn {
    ($name: ident, $op: expr, $re: ident) => {
        pub fn $name(expr: LogicalExpr) -> $re {
            $re {
                op: $op,
                expr: Box::new(expr),
            }
        }
    };
}

make_aggregate_expr_fn!(sum, AggregateOperator::Sum, AggregateExpr);
make_aggregate_expr_fn!(min, AggregateOperator::Min, AggregateExpr);
make_aggregate_expr_fn!(max, AggregateOperator::Max, AggregateExpr);
make_aggregate_expr_fn!(avg, AggregateOperator::Avg, AggregateExpr);
make_aggregate_expr_fn!(count, AggregateOperator::Count, AggregateExpr);
