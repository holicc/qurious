use std::fmt::Debug;
use std::{fmt::Display, sync::Arc};

use arrow::array::{ArrayRef, ArrowNativeTypeOp, ArrowNumericType, AsArray, PrimitiveArray};
use arrow::compute;
use arrow::datatypes::{DataType, Decimal128Type, Decimal256Type, Float64Type, Int64Type, UInt64Type};

use super::{Accumulator, AggregateExpr};
use crate::error::{Error, Result};
use crate::{cast_and_get_scalar, internal_err};
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
        match self.sum {
            Some(v) => {
                let array = PrimitiveArray::<T>::new(vec![v].into(), None);

                match T::DATA_TYPE {
                    DataType::Null => Ok(ScalarValue::Null),
                    DataType::Boolean => cast_and_get_scalar!(array, BooleanArray, 0, Boolean),
                    DataType::Int8 => cast_and_get_scalar!(array, Int8Array, 0, Int8),
                    DataType::Int16 => cast_and_get_scalar!(array, Int16Array, 0, Int16),
                    DataType::Int32 => cast_and_get_scalar!(array, Int32Array, 0, Int32),
                    DataType::Int64 => cast_and_get_scalar!(array, Int64Array, 0, Int64),
                    DataType::Float64 => cast_and_get_scalar!(array, Float64Array, 0, Float64),
                    _ => internal_err!("Unsupported data type: {}", T::DATA_TYPE),
                }
            }
            None => ScalarValue::try_from(T::DATA_TYPE),
        }
    }
}
