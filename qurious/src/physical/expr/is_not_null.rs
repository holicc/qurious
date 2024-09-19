use std::{fmt::Display, sync::Arc};

use crate::{
    arrow_err,
    error::{Error, Result},
};
use arrow::{
    array::{ArrayRef, RecordBatch},
    compute,
};

use super::PhysicalExpr;

#[derive(Debug)]
pub struct IsNotNull {
    pub expr: Arc<dyn PhysicalExpr>,
}

impl IsNotNull {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl PhysicalExpr for IsNotNull {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef> {
        let array = self.expr.evaluate(input)?;

        compute::is_not_null(&array)
            .map_err(|e| arrow_err!(e))
            .map(|v| Arc::new(v) as ArrayRef)
    }
}

impl Display for IsNotNull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IsNotNull({})", self.expr)
    }
}
