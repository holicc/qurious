use arrow::datatypes::{Field, SchemaRef};

use crate::common::table_schema::TableSchema;
use crate::common::{table_relation::TableRelation, table_schema::TableSchemaRef};
use crate::error::{Error, Result};
use crate::internal_err;

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
        Self::try_new_with_columns(input, alias, vec![])
    }

    /// Alias a relation, optionally renaming its output columns (`AS alias (c1, c2, ...)`).
    ///
    /// Renaming here rather than in a projection above keeps the rename in a single node, and the
    /// existing rewrites that map through a `SubqueryAlias` positionally -- filter pushdown, and
    /// the physical plan, which reads its input by index -- then handle it without further work.
    pub fn try_new_with_columns(input: LogicalPlan, alias: &str, columns: Vec<String>) -> Result<Self> {
        let input_schema = input.schema();
        if !columns.is_empty() && columns.len() != input_schema.fields().len() {
            return internal_err!(
                "relation `{alias}` has {} columns available but {} column aliases were given",
                input_schema.fields().len(),
                columns.len()
            );
        }

        let relation: TableRelation = alias.into();
        let qualified_fields = input_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let name = columns.get(index).cloned().unwrap_or_else(|| field.name().clone());
                let field = Field::new(name, field.data_type().clone(), field.is_nullable());

                (Some(relation.clone()), Arc::new(field))
            })
            .collect();

        Ok(Self {
            schema: TableSchema::try_new(qualified_fields).map(Arc::new)?,
            input: Arc::new(input),
            alias: relation,
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
