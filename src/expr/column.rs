use std::fmt::Display;

use crate::error::{Error, Result};
use crate::{expr::LogicalExpr, logical_plan::LogicalPlan, types::field::Field};

pub struct Column {
    name: String,
}

impl LogicalExpr for Column {
    fn to_field(&self, plan: &dyn LogicalPlan) -> Result<Field> {
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
