mod aggregate;
mod binary;
mod cast;
mod column;
mod literal;

pub use aggregate::{
    max::{MaxAccumulator, MaxAggregateExpr},
    sum::*,
    Accumulator, AggregateExpr,
};
pub use binary::BinaryExpr;
pub use column::Column;
pub use literal::Literal;
pub use cast::CastExpr;

use std::fmt::{Debug, Display};

use arrow::{array::ArrayRef, record_batch::RecordBatch};

use crate::error::Result;

pub trait PhysicalExpr: Debug + Display {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef>;
}
