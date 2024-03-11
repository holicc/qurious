use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::FieldRef;

use crate::common::OwnedTableRelation;
use crate::error::{Error, Result};
use crate::logical::plan::LogicalPlan;

use super::LogicalExpr;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Column {
    pub name: String,
    pub relation: Option<OwnedTableRelation>,
}

impl Column {
    pub fn new(name: impl Into<String>, relation: Option<impl Into<OwnedTableRelation>>) -> Self {
        Self {
            name: name.into(),
            relation: relation.map(|r| r.into()),
        }
    }

    pub fn field(&self, plan: &LogicalPlan) -> Result<FieldRef> {
        let quanlified_name = if let Some(relation) = &self.relation {
            format!("{}.{}", relation, self.name)
        } else {
            self.name.clone()
        };

        plan.schema()
            .field_with_name(&quanlified_name)
            .map(|f| Arc::new(f.clone()))
            .map_err(|e| Error::ArrowError(e))
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
