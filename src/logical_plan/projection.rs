use crate::error::Result;
use crate::{
    expr::LogicalExpr,
    logical_plan::LogicalPlan,
    types::{field::Field, schema::Schema},
};
use std::fmt::Display;

#[derive(Debug, Clone)]
pub struct Projection {
    schema: Schema,
    input: Box<LogicalPlan>,
    exprs: Vec<LogicalExpr>,
}

impl Projection {
    pub fn try_new(input: LogicalPlan, exprs: Vec<LogicalExpr>) -> Result<Self> {
        Ok(Self {
            schema: Schema {
                fields: exprs
                    .iter()
                    .map(|f| f.to_field(&input))
                    .collect::<Result<Vec<Field>>>()?,
            },
            input: Box::new(input),
            exprs,
        })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn children(&self) -> Option<Vec<&LogicalPlan>> {
        Some(vec![&self.input])
    }
}

impl Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Projection: {{ {} }}",
            self.exprs
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<String>>()
                .join(",")
        )
    }
}
