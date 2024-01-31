use std::fmt::Display;

use super::PhysicalExpr;
use crate::error::Result;
use crate::types::columnar::ColumnarValue;
use crate::types::{batch::RecordBatch, scalar::ScalarValue};

#[derive(Debug)]
pub struct Literal {
    value: ScalarValue,
}

impl PhysicalExpr for Literal {
    fn evaluate(&self, _input: &RecordBatch) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(self.value.clone()))
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}
