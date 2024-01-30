use std::fmt::Display;

use super::PhysicalExpr;
use crate::error::Result;
use crate::types::columnar::ColumnarValue;
use crate::types::{batch::RecordBatch, vector::ColumnarVectorRef};

pub struct Column {
    index: usize,
}

impl PhysicalExpr for Column {
    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnarValue> {
        Ok(input.field(self.index))
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.index)
    }
}
