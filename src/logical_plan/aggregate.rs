use std::{fmt::Display, sync::Arc};

use crate::{
    expr::{self, LogicalExpr},
    types::schema::Schema,
};

use super::LogicalPlan;

pub struct Aggregate {
    /// The schema description of the aggregate output
    schema: Schema,
    input: Arc<dyn LogicalPlan>,
    group_expr: Vec<Box<dyn LogicalExpr>>,
    aggr_expr: Vec<expr::AggregateExpr>,
}

impl LogicalPlan for Aggregate {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn children(&self) -> Option<Vec<&dyn LogicalPlan>> {
        Some(vec![&*self.input])
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
