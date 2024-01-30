use super::PhysicalExpr;
use crate::error::Result;
use crate::types::columnar::ColumnarValue;
use crate::types::{batch::RecordBatch, scalar::ScalarValue};

pub struct Literal {
    value: ScalarValue,
}

impl PhysicalExpr for Literal {
    fn evaluate(&self, _input: &RecordBatch) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(self.value.clone()))
    }
}
