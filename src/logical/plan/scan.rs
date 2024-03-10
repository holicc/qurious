use std::{fmt::Display, sync::Arc};

use arrow::datatypes::SchemaRef;

use crate::{common::OwnedTableRelation, datasource::DataSource, logical::expr::LogicalExpr};

use super::LogicalPlan;

#[derive(Debug, Clone)]
pub struct TableScan {
    pub relation: OwnedTableRelation,
    pub source: Arc<dyn DataSource>,
    pub projections: Option<Vec<String>>,
    pub filter: Option<LogicalExpr>,
}

impl TableScan {
    pub fn new(
        relation: impl Into<OwnedTableRelation>,
        source: Arc<dyn DataSource>,
        projections: Option<Vec<String>>,
        filter: Option<LogicalExpr>,
    ) -> Self {
        Self {
            relation: relation.into(),
            source,
            projections,
            filter,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.source.schema()
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        None
    }
}

impl Display for TableScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.projections.is_some() {
            write!(
                f,
                "TableScan: {} Projection: {:?}",
                self.relation.to_quanlify_name(),
                self.projections
            )
        } else {
            write!(f, "TableScan: {}", self.relation.to_quanlify_name())
        }
    }
}
