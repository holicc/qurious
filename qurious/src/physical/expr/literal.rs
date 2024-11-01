use std::fmt::Display;

use arrow::{array::ArrayRef, record_batch::RecordBatch};

use super::PhysicalExpr;
use crate::{datatypes::scalar::ScalarValue, error::Result};

#[derive(Debug)]
pub struct Literal {
    value: ScalarValue,
}

impl Literal {
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
    }
}

impl PhysicalExpr for Literal {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef> {
        self.value.to_array(input.num_rows())
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}
