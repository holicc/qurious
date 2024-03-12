use std::{fmt::Display, sync::Arc};

use arrow::datatypes::{Field, FieldRef, Fields, Schema, SchemaRef};

use crate::error::{Error, Result};
use crate::{common::OwnedTableRelation, datasource::DataSource, logical::expr::LogicalExpr};

use super::LogicalPlan;

#[derive(Debug, Clone)]
pub struct TableScan {
    pub relation: OwnedTableRelation,
    pub source: Arc<dyn DataSource>,
    pub projections: Option<Vec<String>>,
    pub projected_schema: SchemaRef,
    pub filter: Option<LogicalExpr>,
}

impl TableScan {
    pub fn try_new(
        relation: impl Into<OwnedTableRelation>,
        source: Arc<dyn DataSource>,
        projections: Option<Vec<String>>,
        filter: Option<LogicalExpr>,
    ) -> Result<Self> {
        let relation = relation.into();
        let projected_schema = projections
            .as_ref()
            .map(|pj| {
                let fields = pj
                    .iter()
                    .map(|name| {
                        source
                            .schema()
                            .field_with_name(name)
                            .map_err(|err| Error::ArrowError(err))
                            .cloned()
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(Schema::new(Fields::from(fields))))
            })
            .unwrap_or_else(|| {
                let quanlified_fields = source
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| {
                        Field::new(
                            format!("{}.{}", relation.to_quanlify_name(), f.name()),
                            f.data_type().clone(),
                            f.is_nullable(),
                        )
                    })
                    .collect::<Vec<Field>>();

                Ok(Arc::new(Schema::new(Fields::from(quanlified_fields))))
            })?;

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
