use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::FieldRef;

use crate::error::{Error, Result};
use crate::logical::plan::LogicalPlan;

use super::LogicalExpr;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
}

impl Column {
    pub fn field(&self, plan: &LogicalPlan) -> Result<FieldRef> {
        plan.schema()
            .field_with_name(&self.name)
            .map(|f| Arc::new(f.clone()))
            .map_err(|e| Error::ArrowError(e))
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.name)
    }
}

impl FromStr for Column {
    type Err = Error;

    fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
        Ok(Self {
            name: s.to_string(),
        })
    }
}

pub fn column(name: &str) -> LogicalExpr {
    LogicalExpr::Column(Column {
        name: name.to_string(),
    })
}
