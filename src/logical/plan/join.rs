use crate::logical::plan::LogicalPlan;
use arrow::datatypes::SchemaRef;
use std::{fmt::Display, sync::Arc};

#[derive(Debug, Clone)]
pub struct CrossJoin {
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub schema: SchemaRef,
}

impl CrossJoin {
    pub fn new(left: Arc<LogicalPlan>, right: Arc<LogicalPlan>, schema: SchemaRef) -> Self {
        Self {
            left,
            right,
            schema,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        self.left.children().map(|left| {
            let mut children = left.clone();
            children.extend(self.right.children().unwrap());
            children
        })
    }
}

impl Display for CrossJoin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CrossJoin: {} {}\n", self.left, self.right)
    }
}
