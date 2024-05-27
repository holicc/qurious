use std::fmt::Display;

use arrow::datatypes::SchemaRef;

use crate::logical::plan::LogicalPlan;

#[derive(Debug, Clone)]
pub struct Limit {
    pub input: Box<LogicalPlan>,
    pub fetch: usize,
    pub offset: usize,
}

impl Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Limit: fetch={}, offset={}", self.fetch, self.offset)
    }
}

impl Limit {
    pub fn new(input: LogicalPlan, fetch: usize, offset: usize) -> Self {
        Self {
            input: Box::new(input),
            fetch,
            offset,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        self.input.children()
    }
}
