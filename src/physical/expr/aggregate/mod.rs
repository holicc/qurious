pub mod max;

use arrow::array::ArrayRef;

use crate::datatypes::scalar::ScalarValue;
use crate::error::Result;
use std::fmt::Debug;
use std::sync::Arc;

use super::PhysicalExpr;

pub trait AggregateExpr: Debug {
    fn expression(&self) -> &Arc<dyn PhysicalExpr>;
    fn create_accumulator(&self) -> Box<dyn Accumulator>;
}

pub trait Accumulator: Debug {
    /// Updates the aggregate with the provided value.
    fn accumluate(&mut self, value: &ArrayRef) -> Result<()>;
    /// Returns the final aggregate value.
    fn evaluate(&mut self) -> Result<ScalarValue>;
}
