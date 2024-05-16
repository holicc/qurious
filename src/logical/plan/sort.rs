use std::fmt::Display;

use arrow::datatypes::SchemaRef;

use crate::logical::expr::SortExpr;
use crate::logical::plan::LogicalPlan;

#[derive(Debug, Clone)]
pub struct Sort {
    pub exprs: Vec<SortExpr>,
    pub input: Box<LogicalPlan>,
}

impl Sort {
    pub fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        Some(vec![&self.input])
    }
}

impl Display for Sort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Sort: {}",
            self.exprs
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}
