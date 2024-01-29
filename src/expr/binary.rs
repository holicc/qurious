use crate::error::Result;
use std::fmt::Display;

use crate::types::datatype::DataType;
use crate::{logical_plan::LogicalPlan, types::field::Field};

use super::LogicalExpr;

#[derive(Debug, Clone)]
pub enum Operator {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
    And,
    Or,

    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operator::Eq => write!(f, "=="),
            Operator::NotEq => write!(f, "!="),
            Operator::Gt => write!(f, ">"),
            Operator::GtEq => write!(f, ">="),
            Operator::Lt => write!(f, "<"),
            Operator::LtEq => write!(f, "<="),
            Operator::And => write!(f, "&&"),
            Operator::Or => write!(f, "||"),
            Operator::Add => write!(f, "+"),
            Operator::Sub => write!(f, "-"),
            Operator::Mul => write!(f, "*"),
            Operator::Div => write!(f, "/"),
            Operator::Mod => write!(f, "%"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BinaryExpr {
    left: Box<LogicalExpr>,
    op: Operator,
    right: Box<LogicalExpr>,
}

impl BinaryExpr {
    pub fn to_field(&self, plan: &LogicalPlan) -> Result<Field> {
        Ok(Field {
            name: self.op.to_string(),
            datatype: match self.op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::And
                | Operator::Or => DataType::Boolean,
                Operator::Add | Operator::Sub | Operator::Mul | Operator::Div | Operator::Mod => {
                    self.left.to_field(plan)?.datatype
                }
            },
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
