use arrow::datatypes::SchemaRef;

use crate::common::table_relation::TableRelation;
use crate::error::Result;

use super::LogicalPlan;
use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubqueryAlias {
    pub input: Arc<LogicalPlan>,
    pub alias: TableRelation,
    pub schema: SchemaRef,
}

impl SubqueryAlias {
    pub fn try_new(input: LogicalPlan, alias: &str) -> Result<Self> {
        let schema = input.schema();
        Ok(Self {
            input: Arc::new(input),
            alias: alias.into(),
            schema,
        })
    }

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
