use std::{fmt::Display, sync::Arc};

use arrow::array::{ArrayRef, AsArray};
use arrow::compute;
use arrow::datatypes::Int32Type;

use super::{Accumulator, AggregateExpr};
use crate::datatypes::scalar::ScalarValue;
use crate::error::{Error, Result};
use crate::physical::expr::PhysicalExpr;

#[derive(Debug)]
pub struct MaxAggregateExpr {
    pub expr: Arc<dyn PhysicalExpr>,
}

impl MaxAggregateExpr {}

impl MaxAggregateExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl AggregateExpr for MaxAggregateExpr {
    fn expression(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MaxAccumulator::default())
    }
}

impl Display for MaxAggregateExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MAX({})", self.expr)
    }
}

#[derive(Debug, Default)]
pub struct MaxAccumulator {
    max: Option<ScalarValue>,
}

impl Accumulator for MaxAccumulator {
    fn accumluate(&mut self, value: &ArrayRef) -> Result<()> {
        let value: &arrow::array::PrimitiveArray<Int32Type> = value.as_primitive();

        let max = compute::max(value);

        self.max = max.map(|a| ScalarValue::from(a));

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.max
            .clone()
            .ok_or(Error::InternalError(format!("failed get max accumulator results!")))
    }
}
