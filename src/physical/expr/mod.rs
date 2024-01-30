pub mod column;
pub mod literal;
pub mod binary;

use crate::error::Result;
use crate::types::batch::RecordBatch;
use crate::types::columnar::ColumnarValue;

pub trait PhysicalExpr {
    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnarValue>;
}
