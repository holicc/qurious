use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::FieldRef;

use crate::common::table_relation::TableRelation;
use crate::common::table_schema::qualified_field_index;
use crate::error::{Error, Result};
use crate::logical::plan::LogicalPlan;

use super::LogicalExpr;

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Column {
    pub name: String,
    pub relation: Option<TableRelation>,
    pub is_outer_ref: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, relation: Option<impl Into<TableRelation>>, is_outer_ref: bool) -> Self {
        Self {
            name: name.into(),
            relation: relation.map(|r| r.into()),
            is_outer_ref,
        }
    }

    pub fn field(&self, plan: &LogicalPlan) -> Result<FieldRef> {
        let schema = plan.schema();

        qualified_field_index(&schema, self.relation.as_ref(), &self.name)
            .map(|index| Arc::new(schema.field(index).clone()))
            .ok_or_else(|| {
                Error::InternalError(format!(
                    "column [{self}] not found in schema: [{}]",
                    schema
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })
    }

    pub fn qualified_name(&self) -> String {
        if let Some(relation) = &self.relation {
            format!("{}.{}", relation, self.name)
        } else {
            self.name.clone()
        }
    }

    pub fn with_relation(self, relation: impl Into<TableRelation>) -> Self {
        Self {
            relation: Some(relation.into()),
            ..self
        }
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(relation) = &self.relation {
            write!(f, "{}.{}", relation, self.name)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

impl FromStr for Column {
    type Err = Error;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        Ok(Self {
            name: s.to_string(),
            relation: None,
            is_outer_ref: false,
        })
    }
}

impl From<&str> for Column {
    fn from(s: &str) -> Self {
        Self {
            name: s.to_string(),
            relation: None,
            is_outer_ref: false,
        }
    }
}

pub fn column(name: &str) -> LogicalExpr {
    LogicalExpr::Column(Column {
        name: name.to_string(),
        relation: None,
        is_outer_ref: false,
    })
}
