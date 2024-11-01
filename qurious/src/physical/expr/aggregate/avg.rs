use arrow::array::{Array, ArrayRef, ArrowNativeTypeOp, AsArray};
use arrow::compute;
use arrow::datatypes::Float64Type;
use std::sync::Arc;

use crate::physical::expr::PhysicalExpr;
use crate::{datatypes::scalar::ScalarValue, error::Result};

use super::{Accumulator, AggregateExpr};

#[derive(Debug)]
pub struct AvgAggregateExpr {
    pub expr: Arc<dyn PhysicalExpr>,
}
impl AvgAggregateExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

impl AggregateExpr for AvgAggregateExpr {
    fn expression(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(AvgAccumulator::default())
    }
}

#[derive(Debug, Default)]
struct AvgAccumulator {
    sum: Option<f64>,
    count: u64,
}

impl Accumulator for AvgAccumulator {
    fn accumluate(&mut self, value: &ArrayRef) -> Result<()> {
        let values = value.as_primitive::<Float64Type>();
        self.count += (values.len() - values.null_count()) as u64;

        if let Some(sum) = compute::sum(values) {
            let s = self.sum.get_or_insert(0.);
            *s = s.add_wrapping(sum);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(self.sum.map(|v| v / self.count as f64)))
    }
}
