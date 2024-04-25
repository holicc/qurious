pub mod alias;

mod aggregate;
mod binary;
mod column;
mod literal;

use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;

pub use aggregate::{AggregateExpr, AggregateOperator};

use arrow::datatypes::{Field, FieldRef};
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
    Wildcard,
}

impl LogicalExpr {
    pub fn field(&self, plan: &LogicalPlan) -> Result<FieldRef> {
        match self {
            LogicalExpr::Column(c) => c.field(plan),
            LogicalExpr::BinaryExpr(b) => b.field(plan),
            LogicalExpr::AggregateExpr(a) => a.field(plan),
            LogicalExpr::Literal(v) => Ok(Arc::new(v.to_field())),
            LogicalExpr::Alias(a) => a.expr.field(plan),
            _ => Err(Error::InternalError(format!(
                "Cannot determine schema for expression: {:?}",
                self
            ))),
        }
    }

    pub fn using_columns(&self) -> HashSet<Column> {
        let mut columns = HashSet::new();
        let mut stack = vec![self];

        while let Some(expr) = stack.pop() {
            match expr {
                LogicalExpr::Column(a) => {
                    columns.insert(a.clone());
                }
                LogicalExpr::Alias(a) => {
                    stack.push(&a.expr);
                }
                LogicalExpr::BinaryExpr(binary_op) => {
                    stack.push(&binary_op.left);
                    stack.push(&binary_op.right);
                }
                LogicalExpr::AggregateExpr(ag) => {
                    stack.push(&ag.expr);
                }
                _ => {}
            }
        }

        columns
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
            LogicalExpr::Wildcard => write!(f, "*"),
        }
    }
}
