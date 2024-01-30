use std::fmt::Display;
use std::str::FromStr;

use crate::error::{Error, Result};
use crate::{logical::plan::LogicalPlan, types::field::Field};

use super::LogicalExpr;

#[derive(Debug, Clone)]
pub struct Column {
    name: String,
}

impl Column {
    pub fn to_field(&self, plan: &LogicalPlan) -> Result<Field> {
        plan.schema()
            .fields
            .iter()
            .filter(|t| t.name.as_str() == self.name.as_str())
            .next()
            .cloned()
            .ok_or(Error::ColumnNotFound(self.name.clone()))
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
