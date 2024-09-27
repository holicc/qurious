use std::{fmt::Display, sync::Arc};

use super::PhysicalExpr;
use crate::arrow_err;
use crate::error::{Error, Result};
use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::compute::kernels::numeric::neg_wrapping;

#[derive(Debug)]
pub struct Negative {
    pub expr: Arc<dyn PhysicalExpr>,
}

impl Negative {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl Display for Negative {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "- {}", self.expr)
    }
}

impl PhysicalExpr for Negative {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef> {
        let array = self.expr.evaluate(input)?;

        neg_wrapping(&array).map_err(|e| arrow_err!(e))
    }
}
