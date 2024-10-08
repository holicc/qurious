use arrow::datatypes::{FieldRef, Schema, SchemaRef};

use crate::error::Result;
use crate::{logical::expr::LogicalExpr, logical::plan::LogicalPlan};
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Projection {
    pub schema: SchemaRef,
    pub input: Box<LogicalPlan>,
    pub exprs: Vec<LogicalExpr>,
}

impl Projection {
    pub fn try_new(input: LogicalPlan, exprs: Vec<LogicalExpr>) -> Result<Self> {
        Ok(Self {
            schema: exprs
                .iter()
                .map(|f| f.field(&input))
                .collect::<Result<Vec<FieldRef>>>()
                .map(|fields| Arc::new(Schema::new(fields)))?,
            input: Box::new(input),
            exprs,
        })
    }

    pub fn try_new_with_schema(input: LogicalPlan, exprs: Vec<LogicalExpr>, schema: SchemaRef) -> Result<Self> {
        Ok(Self {
            schema,
            input: Box::new(input),
            exprs,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        Some(vec![&self.input])
    }
}

impl Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Projection: ({})",
            self.exprs
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}
