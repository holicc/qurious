mod aggregate;
mod binary;
mod column;
mod literal;

use std::fmt::Display;

pub use aggregate::{AggregateExpr, AggregateOperator};

pub use binary::*;
pub use column::*;
pub use literal::*;

use crate::datatypes::scalar::ScalarValue;

#[derive(Debug, Clone)]
pub enum LogicalExpr {
    Column(Column),
    Literal(ScalarValue),
    BinaryExpr(BinaryExpr),
    AggregateExpr(AggregateExpr),
}

impl Display for LogicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalExpr::Column(c) => write!(f, "{}", c),
            LogicalExpr::Literal(v) => write!(f, "{}", v),
            LogicalExpr::BinaryExpr(e) => write!(f, "{}", e),
            LogicalExpr::AggregateExpr(e) => write!(f, "{}", e),
        }
    }
}
