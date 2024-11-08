use arrow::datatypes::{DataType, Field, FieldRef, Schema};

use crate::datatypes::operator::Operator;
use crate::error::Result;
use crate::logical::plan::LogicalPlan;
use crate::utils::type_coercion::get_result_type;
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
            self.to_string(),
            self.get_result_type(&plan.schema())?,
            true,
        )))
    }

    pub fn get_result_type(&self, schema: &Arc<Schema>) -> Result<DataType> {
        let lhs = self.left.data_type(schema)?;
        let rhs = self.right.data_type(schema)?;
        get_result_type(&lhs, &self.op, &rhs)
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
