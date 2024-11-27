use std::{fmt::Display, sync::Arc};

use super::PhysicalExpr;
use crate::{
    arrow_err,
    error::{Error, Result},
};
use arrow::{
    array::{Array, ArrayRef},
    compute,
    record_batch::RecordBatch,
};

#[derive(Debug)]
pub struct Like {
    negated: bool,
    expr: Arc<dyn PhysicalExpr>,
    pattern: Arc<dyn PhysicalExpr>,
}

impl Like {
    pub fn new(negated: bool, expr: Arc<dyn PhysicalExpr>, pattern: Arc<dyn PhysicalExpr>) -> Self {
        Self { negated, expr, pattern }
    }
}

impl PhysicalExpr for Like {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef> {
        let expr = self.expr.evaluate(input)?;
        let pattern = self.pattern.evaluate(input)?;

        if self.negated {
            compute::nlike(&expr, &pattern)
                .map_err(|e| arrow_err!(e))
                .map(|a| Arc::new(a) as Arc<dyn Array>)
        } else {
            compute::like(&expr, &pattern)
                .map_err(|e| arrow_err!(e))
                .map(|a| Arc::new(a) as Arc<dyn Array>)
        }
    }
}

impl Display for Like {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.negated {
            write!(f, "{} NOT LIKE {}", self.expr, self.pattern)
        } else {
            write!(f, "{} LIKE {}", self.expr, self.pattern)
        }
    }
}
