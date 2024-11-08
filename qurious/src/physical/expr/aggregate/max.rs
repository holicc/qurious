use std::{fmt::Display, sync::Arc};

use arrow::datatypes::DataType;

use super::PrimitiveAccumulator;
use super::{Accumulator, AggregateExpr};
use crate::error::Result;
use crate::make_accumulator;
use crate::physical::expr::PhysicalExpr;

#[macro_export]
macro_rules! make_max_accumulator {
    ($DATA_TYPE:ident, $NATIVE:ident, $PRIMTYPE:ident) => {{
        Box::new(PrimitiveAccumulator::<$PRIMTYPE, _>::new(
            $DATA_TYPE,
            |cur, array| {
                if let Some(new) = arrow::compute::max(array) {
                    if cur < &new {
                        return Ok(new);
                    }
                }

                Ok(*cur)
            },
            $NATIVE::MIN,
        ))
    }};
}

#[derive(Debug)]
pub struct MaxAggregateExpr {
    pub return_type: DataType,
    pub expr: Arc<dyn PhysicalExpr>,
}

impl MaxAggregateExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>, return_type: DataType) -> Self {
        Self { expr, return_type }
    }
}

impl AggregateExpr for MaxAggregateExpr {
    fn expression(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let data_type = self.return_type.clone();
        Ok(make_accumulator!(data_type, make_max_accumulator))
    }
}

impl Display for MaxAggregateExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MAX({})", self.expr)
    }
}
