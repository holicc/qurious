use arrow::datatypes::SchemaRef;

use super::LogicalPlan;
use crate::logical::expr::{self, LogicalExpr};
use std::fmt::Display;

#[derive(Debug, Clone)]
pub struct Aggregate {
    /// The schema description of the aggregate output
    schema: SchemaRef,
    input: Box<LogicalPlan>,
    group_expr: Vec<LogicalExpr>,
    aggr_expr: Vec<expr::AggregateExpr>,
}

impl Aggregate {
    pub fn new(
        input: LogicalPlan,
        group_expr: Vec<LogicalExpr>,
        aggr_expr: Vec<expr::AggregateExpr>,
    ) -> Self {
        let mut schema = input.schema().clone();
        schema.fields = group_expr
            .iter()
            .map(|f| f.to_field(&input).unwrap())
            .collect::<Vec<_>>();

        Self {
            schema,
            input: Box::new(input),
            group_expr,
            aggr_expr,
        }
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
            "Aggregate: group_expr={}, aggregat_expr={}",
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
