use arrow::datatypes::SchemaRef;

use super::LogicalPlan;
use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

#[derive(Debug, Clone)]
pub struct SubqueryAlias {
    pub input: Arc<LogicalPlan>,
    pub alias: String,
    pub schema: SchemaRef,
}

impl SubqueryAlias {
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        Some(vec![&self.input])
    }
}

impl Display for SubqueryAlias {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SubqueryAlias: {}", self.alias)
    }
}
