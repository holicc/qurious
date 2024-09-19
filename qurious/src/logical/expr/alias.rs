use std::fmt::Display;
use std::sync::Arc;

use arrow::datatypes::{Field, FieldRef};

use super::LogicalExpr;
use crate::error::Result;
use crate::logical::plan::LogicalPlan;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Alias {
    pub expr: Box<LogicalExpr>,
    pub name: String,
}

impl Alias {
    pub fn new(name: String, expr: LogicalExpr) -> Self {
        Alias {
            expr: Box::new(expr),
            name,
        }
    }

    pub fn field(&self, plan: &LogicalPlan) -> Result<FieldRef> {
        self.expr
            .field(plan)
            .map(|f| Arc::new(Field::new(self.name.clone(), f.data_type().clone(), f.is_nullable())))
    }
}

impl Display for Alias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} AS {}", self.expr, self.name)
    }
}
