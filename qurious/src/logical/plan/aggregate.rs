use super::LogicalPlan;
use crate::common::table_schema::{TableSchema, TableSchemaRef};
use crate::error::Result;
use crate::logical::expr::LogicalExpr;
use crate::utils::expr::exprs_to_fields;
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Aggregate {
    pub schema: TableSchemaRef,
    pub input: Box<LogicalPlan>,
    pub group_expr: Vec<LogicalExpr>,
    pub aggr_expr: Vec<LogicalExpr>,
}

impl Aggregate {
    pub fn try_new(input: LogicalPlan, group_expr: Vec<LogicalExpr>, aggr_expr: Vec<LogicalExpr>) -> Result<Self> {
        let mut qualified_fields = exprs_to_fields(&group_expr, &input)?;
        qualified_fields.extend(exprs_to_fields(&aggr_expr, &input)?);

        Ok(Self {
            schema: TableSchema::try_new(qualified_fields).map(Arc::new)?,
            input: Box::new(input),
            group_expr,
            aggr_expr,
        })
    }

    pub fn schema(&self) -> TableSchemaRef {
        self.schema.clone()
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        Some(vec![&self.input])
    }
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Aggregate: group_expr=[{}], aggregat_expr=[{}]",
            self.group_expr
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<String>>()
                .join(","),
            self.aggr_expr
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<String>>()
                .join(",")
        )
    }
}
