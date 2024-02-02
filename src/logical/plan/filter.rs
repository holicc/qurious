use std::fmt::Display;

use arrow::datatypes::SchemaRef;

use crate::logical::expr::LogicalExpr;
use crate::logical::plan::LogicalPlan;

#[derive(Debug, Clone)]
pub struct Filter {
    input: Box<LogicalPlan>,
    expr: LogicalExpr,
}

impl Filter {
    pub fn new(input: LogicalPlan, expr: LogicalExpr) -> Self {
        Self {
            input: Box::new(input),
            expr,
        }
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
