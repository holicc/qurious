use std::{fmt::Display, sync::Arc};

use crate::{datasource::DataSource, types::schema::Schema};

use super::LogicalPlan;

pub struct Scan {
    path: String,
    source: Arc<dyn DataSource>,
    projections: Option<Vec<String>>,
}

impl LogicalPlan for Scan {
    fn schema(&self) -> &Schema {
        self.source.schema()
    }

    fn children(&self) -> Option<Vec<&dyn LogicalPlan>> {
        None
    }
}

impl Display for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.projections.is_some() {
            write!(f, "Scan: {} projection: {:?}", self.path, self.projections)
        } else {
            write!(f, "Scan: {}", self.path)
        }
    }
}
