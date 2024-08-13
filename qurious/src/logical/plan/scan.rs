use std::{fmt::Display, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};

use crate::common::table_relation::TableRelation;
use crate::error::{Error, Result};
use crate::logical::expr::LogicalExpr;
use crate::provider::table::TableProvider;

use super::LogicalPlan;

#[derive(Debug, Clone)]
pub struct TableScan {
    pub relation: TableRelation,
    pub source: Arc<dyn TableProvider>,
    pub projections: Option<Vec<String>>,
    pub projected_schema: SchemaRef,
    pub filter: Option<LogicalExpr>,
}

impl TableScan {
    pub fn try_new(
        relation: impl Into<TableRelation>,
        source: Arc<dyn TableProvider>,
        projections: Option<Vec<String>>,
        filter: Option<LogicalExpr>,
    ) -> Result<Self> {
        let relation = relation.into();

        let projected_schema = projections
            .as_ref()
            .map(|pj| {
                pj.iter()
                    .map(|name| {
                        source
                            .schema()
                            .field_with_name(name)
                            .map_err(|err| Error::ArrowError(err))
                            .cloned()
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .unwrap_or(Ok(source
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect()))
            .map(|fields| Arc::new(Schema::new(fields)))?;

        Ok(Self {
            relation,
            source,
            projections,
            projected_schema,
            filter,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        None
    }

    pub fn set_metadata(&mut self, k: &str, v: &str) {
        let schema = self.projected_schema.as_ref().clone();
        let mut metadata = schema.metadata;
        metadata.insert(k.to_owned(), v.to_owned());

        self.projected_schema = Arc::new(Schema::new_with_metadata(schema.fields, metadata));
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
