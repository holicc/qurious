use arrow::datatypes::{DataType, Field, FieldRef};

use crate::datatypes::operator::Operator;
use crate::error::Result;
use crate::logical::plan::LogicalPlan;
use std::fmt::Display;
use std::sync::Arc;

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
            format!("({} {} {})", self.left, self.op, self.right),
            self.get_result_type(plan)?,
            false,
        )))
    }

    pub fn get_result_type(&self, plan: &LogicalPlan) -> Result<DataType> {
        let ll = self.left.field(plan)?;
        let rr = self.right.field(plan)?;
        let left_type = ll.data_type();
        let right_type = rr.data_type();

        let final_type = match (left_type, right_type) {
            (_, DataType::LargeUtf8) | (DataType::LargeUtf8, _) => DataType::LargeUtf8,
            (DataType::Float64, _) | (_, DataType::Float64) => DataType::Float64,
            (DataType::Int64, _) | (_, DataType::Int64) => DataType::Int64,
            (DataType::Int32, _) | (_, DataType::Int32) => DataType::Int32,
            (DataType::Int16, _) | (_, DataType::Int16) => DataType::Int16,
            (DataType::Int8, _) | (_, DataType::Int8) => DataType::Int8,
            (DataType::UInt64, _) | (_, DataType::UInt64) => DataType::UInt64,
            (DataType::UInt32, _) | (_, DataType::UInt32) => DataType::UInt32,
            (DataType::UInt16, _) | (_, DataType::UInt16) => DataType::UInt16,
            (DataType::UInt8, _) | (_, DataType::UInt8) => DataType::UInt8,
            _ => unimplemented!("Type coercion not supported for {:?} and {:?}", left_type, right_type),
        };

        Ok(final_type)
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
