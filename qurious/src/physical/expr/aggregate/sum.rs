use std::fmt::Debug;
use std::{fmt::Display, sync::Arc};

use arrow::array::{ArrayRef, ArrowNativeTypeOp, ArrowNumericType, AsArray};
use arrow::compute;
use arrow::datatypes::{ArrowNativeType, DataType, Decimal128Type, Decimal256Type, Float64Type, Int64Type, UInt64Type};

use super::{Accumulator, AggregateExpr};
use crate::error::{Error, Result};
use crate::{datatypes::scalar::ScalarValue, physical::expr::PhysicalExpr};

#[derive(Debug)]
pub struct SumAggregateExpr {
    pub expr: Arc<dyn PhysicalExpr>,
    pub return_type: DataType,
}

impl SumAggregateExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>, return_type: DataType) -> Self {
        Self { expr, return_type }
    }
}

impl Display for SumAggregateExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SUM({})", self.expr)
    }
}

impl AggregateExpr for SumAggregateExpr {
    fn expression(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        match self.return_type {
            DataType::UInt64 => Box::new(SumAccumulator::<UInt64Type>::new()),
            DataType::Int64 => Box::new(SumAccumulator::<Int64Type>::new()),
            DataType::Float64 => Box::new(SumAccumulator::<Float64Type>::new()),
            DataType::Decimal128(_, _) => Box::new(SumAccumulator::<Decimal128Type>::new()),
            DataType::Decimal256(_, _) => Box::new(SumAccumulator::<Decimal256Type>::new()),
            _ => {
                unimplemented!("Sum not supported for {}: {}", self.expr, self.return_type)
            }
        }
    }
}

struct SumAccumulator<T: ArrowNumericType> {
    sum: Option<T::Native>,
}

impl<T: ArrowNumericType> SumAccumulator<T> {
    pub fn new() -> Self {
        Self { sum: None }
    }
}

impl<T: ArrowNumericType> Debug for SumAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SumAccumulator")
    }
}

impl<T: ArrowNumericType> Accumulator for SumAccumulator<T> {
    fn accumluate(&mut self, value: &ArrayRef) -> Result<()> {
        let values = value.as_primitive::<T>();

        if let Some(x) = compute::sum(values) {
            let v = self.sum.get_or_insert(T::Native::default());
            *v = v.add_wrapping(x);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let val = self
            .sum
            .ok_or(Error::InternalError("No value to evaluate".to_string()))?;

        match T::DATA_TYPE {
            DataType::UInt64 => Ok(ScalarValue::UInt64(val.to_usize().map(|x| x as u64))),
            DataType::Int64 => Ok(ScalarValue::Int64(val.to_usize().map(|x| x as i64))),
            // DataType::Float64 => Ok(ScalarValue::Float64(val)),
            // DataType::Decimal128(_, _) => Box::new(SumAccumulator::<Decimal128Type>::new()),
            // DataType::Decimal256(_, _) => Box::new(SumAccumulator::<Decimal256Type>::new()),
            _ => Err(Error::InternalError(format!("Sum not supported for {}", T::DATA_TYPE))),
        }
    }
}
