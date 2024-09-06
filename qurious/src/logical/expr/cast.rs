use std::{fmt::Display, sync::Arc};

use crate::error::Result;
use crate::logical::expr::LogicalExpr;
use crate::logical::plan::LogicalPlan;
use arrow::datatypes::{DataType, Field, FieldRef};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CastExpr {
    pub expr: Box<LogicalExpr>,
    pub data_type: DataType,
}

impl CastExpr {
    pub fn new(expr: LogicalExpr, data_type: DataType) -> Self {
        Self {
            expr: Box::new(expr),
            data_type,
        }
    }

    pub fn field(&self, _plan: &LogicalPlan) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.data_type.to_string(),
            self.data_type.clone(),
            false,
        )))
    }
}

impl Display for CastExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CAST({} AS {})", self.expr, self.data_type)
    }
}
