use crate::error::Result;
use crate::{
    expr::LogicalExpr,
    logical_plan::LogicalPlan,
    types::{field::Field, schema::Schema},
};
use std::{fmt::Display, sync::Arc};

pub struct Projection {
    schema: Schema,
    input: Arc<dyn LogicalPlan>,
    exprs: Vec<Box<dyn LogicalExpr>>,
}

impl Projection {
    pub fn try_new(input: Arc<dyn LogicalPlan>, exprs: Vec<Box<dyn LogicalExpr>>) -> Result<Self> {
        Ok(Self {
            schema: Schema {
                fields: exprs
                    .iter()
                    .map(|f| f.to_field(&*input))
                    .collect::<Result<Vec<Field>>>()?,
            },
            input,
            exprs,
        })
    }
}

impl LogicalPlan for Projection {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn children(&self) -> Option<Vec<&dyn LogicalPlan>> {
        Some(vec![&*self.input])
    }
}

impl Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Projection: ${{ {} }}",
            self.exprs
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<String>>()
                .join(",")
        )
    }
}
