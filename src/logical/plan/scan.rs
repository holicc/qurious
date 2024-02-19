use std::{fmt::Display, sync::Arc};

use arrow::datatypes::SchemaRef;

use crate::datasource::DataSource;

use super::LogicalPlan;

#[derive(Debug, Clone)]
pub struct TableScan {
    pub name: String,
    pub source: Arc<dyn DataSource>,
    pub projections: Option<Vec<String>>,
}

impl TableScan {
    pub fn new(path: &str, source: Arc<dyn DataSource>, projections: Option<Vec<String>>) -> Self {
        Self {
            name: path.to_string(),
            source,
            projections,
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
            write!(f, "TableScan: {} projection: {:?}", self.name, self.projections)
        } else {
            write!(f, "TableScan: {}", self.name)
        }
    }
}
