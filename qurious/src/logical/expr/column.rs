use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::{FieldRef, SchemaRef};

use crate::arrow_err;
use crate::common::table_relation::TableRelation;
use crate::error::{Error, Result};
use crate::logical::plan::LogicalPlan;

use super::LogicalExpr;

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Column {
    pub name: String,
    pub relation: Option<TableRelation>,
}

impl Column {
    pub fn new(name: impl Into<String>, relation: Option<impl Into<TableRelation>>) -> Self {
        Self {
            name: name.into(),
            relation: relation.map(|r| r.into()),
        }
    }

    pub fn field(&self, plan: &LogicalPlan) -> Result<FieldRef> {
        plan.schema()
            .field_with_name(&self.name)
            .map(|f| Arc::new(f.clone()))
            .map_err(|e| arrow_err!(e))
    }

    pub fn quanlified_name(&self) -> String {
        if let Some(relation) = &self.relation {
            format!("{}.{}", relation, self.name)
        } else {
            self.name.clone()
        }
    }

    pub fn normalize_col_with_schemas_and_ambiguity_check(
        self,
        schemas: &[&[(&TableRelation, SchemaRef)]],
    ) -> Result<Self> {
        if self.relation.is_some() {
            return Ok(self);
        }


        for schema_level in schemas {
            let mut matched = schema_level
                .iter()
                .filter_map(|(relation, schema)| schema.field_with_name(&self.name).map(|f| (relation, f)).ok())
                .collect::<Vec<_>>();

            if matched.len() > 1 {
                return Err(Error::InternalError(format!("Column \"{}\" is ambiguous", self.name)));
            }

            if let Some((relation, _)) = matched.pop() {
                return Ok(Self {
                    name: self.name,
                    relation: Some((*relation).clone()),
                });
            }
        }

        Err(Error::InternalError(format!(
            "Column \"{}\" not found in any table",
            self.name
        )))
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
        })
    }
}

pub fn column(name: &str) -> LogicalExpr {
    LogicalExpr::Column(Column {
        name: name.to_string(),
        relation: None,
    })
}
