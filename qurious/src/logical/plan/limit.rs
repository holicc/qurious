use std::fmt::Display;

use arrow::datatypes::SchemaRef;

use crate::logical::plan::LogicalPlan;

#[derive(Debug, Clone)]
pub struct Limit {
    pub input: Box<LogicalPlan>,
    pub fetch: Option<usize>,
    pub skip: usize,
}

impl Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Limit: fetch={}, skip={}",
            self.fetch.map(|x| x.to_string()).unwrap_or("None".to_owned()),
            self.skip
        )
    }
}

impl Limit {
    pub fn new(input: LogicalPlan, fetch: Option<usize>, skip: usize) -> Self {
        Self {
            input: Box::new(input),
            fetch,
            skip,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        Some(vec![&self.input])
    }
}
