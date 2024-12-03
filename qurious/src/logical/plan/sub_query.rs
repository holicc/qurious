use arrow::datatypes::SchemaRef;

use crate::common::table_schema::TableSchema;
use crate::common::{table_relation::TableRelation, table_schema::TableSchemaRef};
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
    pub schema: TableSchemaRef,
}

impl SubqueryAlias {
    pub fn try_new(input: LogicalPlan, alias: &str) -> Result<Self> {
        Ok(Self {
            schema: TableSchema::try_from_qualified_schema(alias, input.schema())?.into(),
            input: Arc::new(input),
            alias: alias.into(),
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.arrow_schema()
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
