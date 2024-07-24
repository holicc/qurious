use crate::{
    common::join_type::JoinType,
    logical::{expr::LogicalExpr, plan::LogicalPlan},
};
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
        Self { left, right, schema }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        Some(vec![&self.left, &self.right])
    }
}

impl Display for CrossJoin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CrossJoin")
    }
}

#[derive(Debug, Clone)]
pub struct Join {
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub join_type: JoinType,
    pub filter: LogicalExpr,
    pub schema: SchemaRef,
}

impl Join {
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        Some(vec![&self.left, &self.right])
    }
}

impl Display for Join {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: Filter: {}", self.join_type, self.filter)
    }
}
