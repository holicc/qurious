pub mod aggregate;
pub mod binary;
pub mod column;
pub mod literal;

use std::fmt::{Debug, Display};

use arrow::{array::ArrayRef, record_batch::RecordBatch};

use crate::error::Result;

pub trait PhysicalExpr: Debug + Display {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef>;
}
