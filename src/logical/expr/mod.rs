mod aggregate;
mod binary;
mod column;
mod literal;

use std::fmt::Display;

pub use aggregate::{AggregateExpr, AggregateOperator};

pub use binary::*;
pub use column::*;
pub use literal::*;

use crate::error::Result;
use crate::types::scalar::ScalarValue;
use crate::{logical::plan::LogicalPlan, types::field::Field};

#[derive(Debug, Clone)]
pub enum LogicalExpr {
    Column(Column),
    Literal(ScalarValue),
    BinaryExpr(BinaryExpr),
    AggregateExpr(AggregateExpr),
}

impl LogicalExpr {
    pub fn to_field(&self, plan: &LogicalPlan) -> Result<Field> {
        match self {
            LogicalExpr::Column(c) => c.to_field(plan),
            LogicalExpr::Literal(v) => Ok(v.to_field()),
            LogicalExpr::BinaryExpr(e) => e.to_field(plan),
            LogicalExpr::AggregateExpr(e) => e.to_field(plan),
        }
    }
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
