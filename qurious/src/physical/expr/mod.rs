mod aggregate;
mod binary;
mod cast;
mod column;
mod function;
mod is_not_null;
mod is_null;
mod literal;

pub use aggregate::{count::*, max::*, min::*, sum::*, Accumulator, AggregateExpr};
pub use binary::BinaryExpr;
pub use cast::CastExpr;
pub use column::Column;
pub use function::*;
pub use is_not_null::*;
pub use is_null::*;
pub use literal::Literal;

use std::fmt::{Debug, Display};

use arrow::{array::ArrayRef, record_batch::RecordBatch};

use crate::error::Result;

pub trait PhysicalExpr: Debug + Display {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef>;
}
