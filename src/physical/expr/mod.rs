pub mod aggregate;
pub mod binary;
pub mod column;
pub mod literal;

use std::fmt::{Debug, Display};

use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::types::columnar::ColumnarValue;

pub trait PhysicalExpr: Debug + Display {
    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnarValue>;
}
