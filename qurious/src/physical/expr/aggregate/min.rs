use crate::error::Result;
use arrow::datatypes::DataType;
use std::sync::Arc;

use super::Accumulator;
use super::AggregateExpr;
use super::PrimitiveAccumulator;
use crate::make_accumulator;
use crate::physical::expr::PhysicalExpr;

#[macro_export]
macro_rules! make_min_accumulator {
    ($DATA_TYPE:ident, $NATIVE:ident, $PRIMTYPE:ident) => {{
        Box::new(PrimitiveAccumulator::<$PRIMTYPE, _>::new(
            $DATA_TYPE,
            |cur, array| {
                if let Some(new) = arrow::compute::min(array) {
                    if cur > &new {
                        return Ok(new);
                    }
                }

                Ok(*cur)
            },
            $NATIVE::MAX,
        ))
    }};
}

#[derive(Debug)]
pub struct MinAggregateExpr {
    pub return_type: DataType,
    pub expr: Arc<dyn PhysicalExpr>,
}

impl MinAggregateExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>, return_type: DataType) -> Self {
        Self { expr, return_type }
    }
}

impl std::fmt::Display for MinAggregateExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MIN({})", self.expr)
    }
}

impl AggregateExpr for MinAggregateExpr {
    fn expression(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let data_type = self.return_type.clone();
        Ok(make_accumulator!(data_type, make_min_accumulator))
    }
}
