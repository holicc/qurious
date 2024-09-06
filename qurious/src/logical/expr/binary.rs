use arrow::datatypes::{DataType, Field, FieldRef};

use crate::datatypes::operator::Operator;
use crate::error::Result;
use crate::logical::plan::LogicalPlan;
use std::fmt::Display;
use std::sync::Arc;

use super::cast::CastExpr;
use super::LogicalExpr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BinaryExpr {
    pub left: Box<LogicalExpr>,
    pub op: Operator,
    pub right: Box<LogicalExpr>,
}

impl BinaryExpr {
    pub fn new(left: LogicalExpr, op: Operator, right: LogicalExpr) -> Self {
        Self {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    pub fn field(&self, plan: &LogicalPlan) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            format!("{} {} {}", self.left, self.op, self.right),
            match self.op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::And
                | Operator::Or => DataType::Boolean,
                Operator::Add | Operator::Sub | Operator::Mul | Operator::Div | Operator::Mod => {
                    self.left.field(plan)?.data_type().clone()
                }
            },
            false,
        )))
    }

    pub fn coerce_types(self, plan: &LogicalPlan) -> Result<BinaryExpr> {
        let left = self.left.field(plan)?;
        let right = self.right.field(plan)?;

        let (l, r) = match (left.data_type(), right.data_type()) {
            (_, DataType::LargeUtf8) => (
                Box::new(LogicalExpr::Cast(CastExpr::new(*self.left, DataType::LargeUtf8))),
                self.right,
            ),
            (DataType::LargeUtf8, _) => (
                self.left,
                Box::new(LogicalExpr::Cast(CastExpr::new(*self.right, DataType::LargeUtf8))),
            ),
            (DataType::Float64, _) => (
                Box::new(LogicalExpr::Cast(CastExpr::new(*self.left, DataType::Float64))),
                self.right,
            ),
            (_, DataType::Float64) => (
                self.left,
                Box::new(LogicalExpr::Cast(CastExpr::new(*self.right, DataType::Float64))),
            ),
            _ => (self.left, self.right),
        };

        Ok(BinaryExpr {
            left: l,
            op: self.op,
            right: r,
        })
    }
}

impl Display for BinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

macro_rules! make_binary_expr_fn {
    ($name: ident, $op: expr) => {
        pub fn $name(left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
            LogicalExpr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op: $op,
                right: Box::new(right),
            })
        }
    };
}

make_binary_expr_fn!(eq, Operator::Eq);
make_binary_expr_fn!(not_eq, Operator::NotEq);
make_binary_expr_fn!(gt, Operator::Gt);
make_binary_expr_fn!(gt_eq, Operator::GtEq);
make_binary_expr_fn!(lt, Operator::Lt);
make_binary_expr_fn!(lt_eq, Operator::LtEq);
make_binary_expr_fn!(and, Operator::And);
make_binary_expr_fn!(or, Operator::Or);
make_binary_expr_fn!(add, Operator::Add);
make_binary_expr_fn!(sub, Operator::Sub);
make_binary_expr_fn!(mul, Operator::Mul);
make_binary_expr_fn!(div, Operator::Div);
make_binary_expr_fn!(r#mod, Operator::Mod);
