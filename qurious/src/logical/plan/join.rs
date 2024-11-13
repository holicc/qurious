use crate::{
    common::join_type::JoinType,
    logical::{expr::LogicalExpr, plan::LogicalPlan},
};
use arrow::datatypes::SchemaRef;
use std::{fmt::Display, sync::Arc};

#[derive(Debug, Clone)]
pub struct Join {
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub join_type: JoinType,
    pub filter: Option<LogicalExpr>,
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
        if let Some(filter) = &self.filter {
            write!(f, "{}: Filter: {}", self.join_type, filter)
        } else {
            write!(f, "{}", self.join_type)
        }
    }
}
