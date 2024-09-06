mod aggregate;
mod binary;
mod cast;
mod column;
mod literal;

pub use aggregate::{count::*, max::*, min::*, sum::*, Accumulator, AggregateExpr};
pub use binary::BinaryExpr;
pub use cast::CastExpr;
pub use column::Column;
pub use literal::Literal;

use std::fmt::{Debug, Display};

use arrow::{array::ArrayRef, record_batch::RecordBatch};

use crate::error::Result;

pub trait PhysicalExpr: Debug + Display {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef>;
}
