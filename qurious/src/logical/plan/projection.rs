use arrow::datatypes::{Schema, SchemaRef};

use crate::common::table_schema::{TableSchema, TableSchemaRef};
use crate::error::Result;
use crate::{logical::expr::LogicalExpr, logical::plan::LogicalPlan};
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Projection {
    pub schema: TableSchemaRef,
    pub input: Box<LogicalPlan>,
    pub exprs: Vec<LogicalExpr>,
}

impl Projection {
    pub fn try_new(input: LogicalPlan, exprs: Vec<LogicalExpr>) -> Result<Self> {
        let mut field_qualifiers = vec![];
        let mut fields = vec![];

        for expr in &exprs {
            field_qualifiers.push(expr.qualified_name());
            fields.push(expr.field(&input)?);
        }

        let schema = TableSchema::new(field_qualifiers, Arc::new(Schema::new(fields)));

        Ok(Self {
            schema: Arc::new(schema),
            input: Box::new(input),
            exprs,
        })
    }

    pub fn try_new_with_schema(input: LogicalPlan, exprs: Vec<LogicalExpr>, schema: TableSchemaRef) -> Result<Self> {
        Ok(Self {
            schema,
            input: Box::new(input),
            exprs,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.arrow_schema()
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
