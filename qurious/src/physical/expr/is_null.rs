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
pub struct IsNull {
    pub expr: Arc<dyn PhysicalExpr>,
}

impl IsNull {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl PhysicalExpr for IsNull {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef> {
        let array = self.expr.evaluate(input)?;

        compute::is_null(&array)
            .map_err(|e| arrow_err!(e))
            .map(|v| Arc::new(v) as ArrayRef)
    }
}

impl Display for IsNull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IsNull({})", self.expr)
    }
}
