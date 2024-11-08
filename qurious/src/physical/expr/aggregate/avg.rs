use arrow::array::{Array, ArrayRef, ArrowNativeTypeOp, ArrowNumericType, AsArray};
use arrow::compute;
use arrow::datatypes::{ArrowNativeType, DataType, Decimal128Type, Decimal256Type, DecimalType, Float64Type};
use std::sync::Arc;

use crate::internal_err;
use crate::physical::expr::PhysicalExpr;
use crate::{
    datatypes::scalar::ScalarValue,
    error::{Error, Result},
};

use super::{Accumulator, AggregateExpr};

#[derive(Debug)]
pub struct AvgAggregateExpr {
    expr: Arc<dyn PhysicalExpr>,
    expr_data_type: DataType,
    return_type: DataType,
}
impl AvgAggregateExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>, expr_data_type: DataType, return_type: DataType) -> Self {
        Self {
            expr,
            expr_data_type,
            return_type,
        }
    }
}

impl AggregateExpr for AvgAggregateExpr {
    fn expression(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        match (&self.expr_data_type, &self.return_type) {
            (DataType::Decimal128(_, sum_scale), DataType::Decimal128(target_precision, target_scale)) => {
                Ok(Box::new(DecimalAvgAccumulator::<Decimal128Type> {
                    sum: None,
                    count: 0,
                    sum_scale: *sum_scale,
                    target_precision: *target_precision,
                    target_scale: *target_scale,
                }))
            }
            (DataType::Decimal128(_, sum_scale), DataType::Decimal256(target_precision, target_scale)) => {
                Ok(Box::new(DecimalAvgAccumulator::<Decimal256Type> {
                    sum: None,
                    count: 0,
                    sum_scale: *sum_scale,
                    target_precision: *target_precision,
                    target_scale: *target_scale,
                }))
            }
            (_, DataType::Float64) => Ok(Box::new(AvgAccumulator::default())),
            _ => internal_err!("Unsupported data type [{}] for AVG aggregate", self.return_type),
        }
    }
}

#[derive(Debug, Default)]
struct AvgAccumulator {
    sum: Option<f64>,
    count: u64,
}

impl Accumulator for AvgAccumulator {
    fn accumluate(&mut self, value: &ArrayRef) -> Result<()> {
        accumluate::<Float64Type>(&mut self.sum, &mut self.count, value)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(self.sum.map(|v| v / self.count as f64)))
    }
}

struct DecimalAvgAccumulator<T: DecimalType + ArrowNumericType> {
    sum: Option<T::Native>,
    count: u64,
    sum_scale: i8,
    target_precision: u8,
    target_scale: i8,
}

impl<T: DecimalType + ArrowNumericType> Accumulator for DecimalAvgAccumulator<T> {
    fn accumluate(&mut self, value: &ArrayRef) -> Result<()> {
        accumluate::<T>(&mut self.sum, &mut self.count, value)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if let Some(sum) = self.sum {
            let count = T::Native::from_usize(self.count as usize).unwrap();
            let sum_mul = T::Native::from_usize(10_usize)
                .map(|b| b.pow_wrapping(self.sum_scale as u32))
                .ok_or(Error::InternalError("Failed to compute sum_mul".to_string()))?;
            let target_mul = T::Native::from_usize(10_usize)
                .map(|b| b.pow_wrapping(self.target_scale as u32))
                .ok_or(Error::InternalError("Failed to compute target_mul".to_string()))?;

            if target_mul < sum_mul {
                return internal_err!("Arithmetic Overflow in DecimalAvgAccumulator");
            }

            if let Ok(value) = sum.mul_checked(target_mul.div_wrapping(sum_mul)) {
                let new_value = value.div_wrapping(count);
                if T::validate_decimal_precision(value, self.target_precision).is_ok() {
                    return ScalarValue::new_primitive::<T>(
                        Some(new_value),
                        &T::TYPE_CONSTRUCTOR(self.target_precision, self.target_scale),
                    );
                }
            }
        }

        ScalarValue::try_from(T::DATA_TYPE)
    }
}

fn accumluate<T: ArrowNumericType>(sum: &mut Option<T::Native>, count: &mut u64, value: &ArrayRef) -> Result<()> {
    let values = value.as_primitive::<T>();
    *count += (values.len() - values.null_count()) as u64;

    if let Some(x) = compute::sum(values) {
        let v = sum.get_or_insert(T::Native::default());
        *sum = Some(v.add_wrapping(x));
    }

    Ok(())
}
