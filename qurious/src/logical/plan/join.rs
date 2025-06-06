use crate::{
    common::{join_type::JoinType, table_schema::TableSchemaRef},
    logical::{expr::LogicalExpr, plan::LogicalPlan},
};
use arrow::datatypes::SchemaRef;
use std::{fmt::Display, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CrossJoin {
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub schema: TableSchemaRef,
}

impl CrossJoin {
    pub fn schema(&self) -> SchemaRef {
        self.schema.arrow_schema()
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Join {
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub join_type: JoinType,
    /// Equijoin clause expressed as pairs of (left, right) join expressions
    pub on: Vec<(LogicalExpr, LogicalExpr)>,
    /// Filters applied during join (non-equi conditions)
    pub filter: Option<LogicalExpr>,
    pub schema: TableSchemaRef,
}

impl Join {
    pub fn schema(&self) -> SchemaRef {
        self.schema.arrow_schema()
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        Some(vec![&self.left, &self.right])
    }
}

impl Display for Join {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:", self.join_type)?;
        if let Some(filter) = &self.filter {
            write!(f, " Filter: {}", filter)?;
        }
        if !self.on.is_empty() {
            write!(
                f,
                " On: {}",
                self.on
                    .iter()
                    .map(|(l, r)| format!("({}, {})", l, r))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }
        Ok(())
    }
}
