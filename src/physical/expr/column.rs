use std::fmt::Display;

use arrow::record_batch::RecordBatch;

use super::PhysicalExpr;
use crate::error::Result;
use crate::types::columnar::ColumnarValue;

#[derive(Debug)]
pub struct Column {
    index: usize,
}

impl PhysicalExpr for Column {
    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnarValue> {
        Ok(input.column(self.index).clone().into())
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.index)
    }
}
