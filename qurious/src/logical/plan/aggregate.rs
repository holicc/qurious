use arrow::datatypes::{Schema, SchemaRef};
use itertools::Itertools;

use super::LogicalPlan;
use crate::error::Result;
use crate::logical::expr::{AggregateExpr, LogicalExpr};
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Aggregate {
    /// The schema description of the aggregate output
    pub schema: SchemaRef,
    pub input: Box<LogicalPlan>,
    pub group_expr: Vec<LogicalExpr>,
    pub aggr_expr: Vec<AggregateExpr>,
}

impl Aggregate {
    pub fn try_new(input: LogicalPlan, group_expr: Vec<LogicalExpr>, aggr_expr: Vec<AggregateExpr>) -> Result<Self> {
        let group_expr = group_expr.into_iter().unique().collect::<Vec<_>>();
        let aggr_expr = aggr_expr.into_iter().unique().collect::<Vec<_>>();

        let group_fields = group_expr.iter().map(|f| f.field(&input));
        let agg_fields = aggr_expr.iter().map(|f| f.field(&input));

        Ok(Self {
            schema: Arc::new(Schema::new(group_fields.chain(agg_fields).collect::<Result<Vec<_>>>()?)),
            input: Box::new(input),
            group_expr,
            aggr_expr,
        })
    }

    pub fn schema(&self) -> SchemaRef {
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
