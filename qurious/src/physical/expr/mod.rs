mod aggregate;
mod binary;
mod case;
mod cast;
mod column;
mod function;
mod is_not_null;
mod is_null;
mod like;
mod literal;
mod negative;
mod subquery;

pub use aggregate::{avg::*, count::*, max::*, min::*, sum::*, Accumulator, AggregateExpr};
pub use binary::BinaryExpr;
pub use case::CaseExpr;
pub use cast::CastExpr;
pub use column::Column;
pub use function::*;
pub use is_not_null::*;
pub use is_null::*;
pub use like::*;
pub use literal::Literal;
pub use negative::*;
pub use subquery::SubQuery;

use std::fmt::{Debug, Display};

use arrow::{array::ArrayRef, record_batch::RecordBatch};

use crate::error::Result;

pub trait PhysicalExpr: Debug + Display {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef>;
}
