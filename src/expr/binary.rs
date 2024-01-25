use crate::error::Result;
use std::fmt::Display;

use crate::types::datatype::DataType;
use crate::{
    expr::{LogicalExpr, Operator},
    logical_plan::LogicalPlan,
    types::field::Field,
};

pub struct BooleanExpr {
    left: Box<dyn LogicalExpr>,
    op: Operator,
    right: Box<dyn LogicalExpr>,
}

impl LogicalExpr for BooleanExpr {
    fn to_field(&self, _plan: &dyn LogicalPlan) -> Result<Field> {
        Ok(Field {
            name: self.op.to_string(),
            datatype: DataType::Bool,
        })
    }
}

impl Display for BooleanExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

pub struct MathExpr {
    left: Box<dyn LogicalExpr>,
    op: Operator,
    right: Box<dyn LogicalExpr>,
}

impl LogicalExpr for MathExpr {
    fn to_field(&self, plan: &dyn LogicalPlan) -> Result<Field> {
        Ok(Field {
            name: self.op.to_string(),
            datatype: self.left.to_field(plan)?.datatype,
        })
    }
}

impl Display for MathExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

macro_rules! make_bool_expr_fn {
    ($name: ident, $op: expr, $re: ident) => {
        pub fn $name(left: Box<dyn LogicalExpr>, right: Box<dyn LogicalExpr>) -> $re {
            $re {
                left,
                op: $op,
                right,
            }
        }
    };
}

make_bool_expr_fn!(eq, Operator::Eq, BooleanExpr);
make_bool_expr_fn!(not_eq, Operator::NotEq, BooleanExpr);
make_bool_expr_fn!(gt, Operator::Gt, BooleanExpr);
make_bool_expr_fn!(gt_eq, Operator::GtEq, BooleanExpr);
make_bool_expr_fn!(lt, Operator::Lt, BooleanExpr);
make_bool_expr_fn!(lt_eq, Operator::LtEq, BooleanExpr);
make_bool_expr_fn!(and, Operator::And, BooleanExpr);
make_bool_expr_fn!(or, Operator::Or, BooleanExpr);

make_bool_expr_fn!(add, Operator::Add, MathExpr);
make_bool_expr_fn!(sub, Operator::Sub, MathExpr);
make_bool_expr_fn!(mul, Operator::Mul, MathExpr);
make_bool_expr_fn!(div, Operator::Div, MathExpr);
make_bool_expr_fn!(r#mod, Operator::Mod, MathExpr);