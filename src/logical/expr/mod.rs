mod aggregate;
mod alias;
mod binary;
mod column;
mod literal;

use std::fmt::Display;
use std::sync::Arc;

pub use aggregate::{AggregateExpr, AggregateOperator};

use arrow::datatypes::{FieldRef, Schema};
pub use binary::*;
pub use column::*;
pub use literal::*;

use crate::datatypes::scalar::ScalarValue;
use crate::error::{Error, Result};

use self::alias::Alias;

use super::plan::LogicalPlan;

#[derive(Debug, Clone)]
pub enum LogicalExpr {
    Alias(Alias),
    Column(Column),
    Literal(ScalarValue),
    BinaryExpr(BinaryExpr),
    AggregateExpr(AggregateExpr),
}

impl LogicalExpr {
    pub fn field(&self, plan: &LogicalPlan) -> Result<FieldRef> {
        match self {
            LogicalExpr::Column(c) => c.field(plan),
            LogicalExpr::BinaryExpr(b) => b.field(plan),
            LogicalExpr::AggregateExpr(a) => a.field(plan),
            LogicalExpr::Literal(v) => Ok(Arc::new(v.to_field())),
            LogicalExpr::Alias(a) => Err(Error::InternalError(format!(
                "Alias expression should have been resolved: {}",
                a
            ))),
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
            LogicalExpr::Alias(a) => write!(f, "{}", a),
        }
    }
}
