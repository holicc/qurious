use std::{fmt::Display, sync::Arc};

use super::{Accumulator, AggregateExpr};
use crate::error::Result;
use crate::{datatypes::scalar::ScalarValue, physical::expr::PhysicalExpr};
use arrow::array::ArrayRef;

#[derive(Debug)]
pub struct CountAggregateExpr {
    pub expr: Arc<dyn PhysicalExpr>,
}

impl CountAggregateExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl Display for CountAggregateExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "COUNT({})", self.expr)
    }
}

impl AggregateExpr for CountAggregateExpr {
    fn expression(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CountAccumulator::default())
    }
}

#[derive(Debug, Default)]
pub struct CountAccumulator {
    count: i64,
}

impl Accumulator for CountAccumulator {
    fn accumluate(&mut self, values: &ArrayRef) -> Result<()> {
        self.count += (values.len() - values.null_count()) as i64;
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }
}
