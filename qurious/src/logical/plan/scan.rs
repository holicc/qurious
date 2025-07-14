use std::{
    fmt::Display,
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::datatypes::SchemaRef;

use crate::common::table_schema::TableSchemaRef;
use crate::common::{table_relation::TableRelation, table_schema::TableSchema};
use crate::error::Result;
use crate::logical::expr::LogicalExpr;
use crate::provider::table::TableProvider;

use super::LogicalPlan;

#[derive(Debug, Clone)]
pub struct TableScan {
    pub table_name: TableRelation,
    pub source: Arc<dyn TableProvider>,
    pub filter: Option<LogicalExpr>,
    pub schema: TableSchemaRef,
}

impl TableScan {
    pub fn try_new(
        relation: impl Into<TableRelation>,
        source: Arc<dyn TableProvider>,
        filter: Option<LogicalExpr>,
    ) -> Result<Self> {
        let table_name = relation.into();
        Ok(Self {
            filter,
            schema: TableSchema::try_from_qualified_schema(table_name.clone(), source.schema()).map(Arc::new)?,
            source,
            table_name,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.arrow_schema()
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        None
    }
}

impl Display for TableScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableScan: {}", self.table_name.to_qualified_name(),)
    }
}

impl PartialEq for TableScan {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name && Arc::ptr_eq(&self.source, &other.source)
    }
}

impl Eq for TableScan {}

impl Hash for TableScan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.schema.hash(state);
        self.filter.hash(state);
    }
}
