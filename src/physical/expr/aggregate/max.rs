use std::{fmt::Display, sync::Arc};

use super::{Accumulator, AggregateExpr};
use crate::datatypes::scalar::ScalarValue;
use crate::error::Result;
use crate::physical::expr::PhysicalExpr;

#[derive(Debug)]
pub struct MaxAggregateExpr {
    expr: Arc<dyn PhysicalExpr>,
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
    fn accumluate(&mut self, value: &ScalarValue) -> Result<()> {
        if self.max.is_none() {
            self.max = Some(value.clone())
        } else if let Some(max) = &mut self.max {
            if value > max {
                *max = value.clone();
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.max.take().unwrap_or_else(|| ScalarValue::Null))
    }
}
