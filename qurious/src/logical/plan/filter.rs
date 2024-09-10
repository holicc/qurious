use std::fmt::Display;

use crate::error::Result;
use crate::logical::expr::LogicalExpr;
use crate::logical::plan::LogicalPlan;
use arrow::datatypes::SchemaRef;

#[derive(Debug, Clone)]
pub struct Filter {
    pub input: Box<LogicalPlan>,
    pub expr: LogicalExpr,
}

impl Filter {
    pub fn try_new(input: LogicalPlan, expr: LogicalExpr) -> Result<Self> {
        Ok(Self {
            input: Box::new(input),
            expr,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        Some(vec![&self.input])
    }
}

impl Display for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Filter: {}", self.expr)
    }
}
