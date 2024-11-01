pub mod avg;
pub mod count;
pub mod max;
pub mod min;
pub mod sum;

use arrow::array::{ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray};
use arrow::datatypes::DataType;

use super::PhysicalExpr;
use crate::datatypes::scalar::ScalarValue;
use crate::error::Result;
use std::fmt::Debug;
use std::sync::Arc;

pub trait AggregateExpr: Debug {
    fn expression(&self) -> &Arc<dyn PhysicalExpr>;
    fn create_accumulator(&self) -> Box<dyn Accumulator>;
}

pub trait Accumulator {
    /// Updates the aggregate with the provided value.
    fn accumluate(&mut self, value: &ArrayRef) -> Result<()>;
    /// Returns the final aggregate value.
    fn evaluate(&mut self) -> Result<ScalarValue>;
}

#[derive(Debug)]
pub struct PrimitiveAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&T::Native, &PrimitiveArray<T>) -> Result<T::Native> + Send + Sync,
{
    /// The output data type of the aggregation.
    data_type: DataType,
    /// The initial value of the aggregation.
    starting_value: T::Native,
    /// The result of the aggregation.
    result: Option<T::Native>,
    /// The function to perform the aggregation.
    prim_fn: F,
}

impl<T, F> PrimitiveAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&T::Native, &PrimitiveArray<T>) -> Result<T::Native> + Send + Sync,
{
    pub fn new(data_type: DataType, prim_fn: F, starting_value: T::Native) -> Self {
        Self {
            data_type,
            starting_value,
            result: None,
            prim_fn,
        }
    }
}

impl<T, F> Accumulator for PrimitiveAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&T::Native, &PrimitiveArray<T>) -> Result<T::Native> + Send + Sync,
{
    fn accumluate(&mut self, value: &ArrayRef) -> Result<()> {
        let value = value.as_primitive::<T>();

        if let Some(v) = &mut self.result {
            *v = (self.prim_fn)(v, value)?;
        } else {
            self.result = Some((self.prim_fn)(&self.starting_value, value)?);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if let Some(v) = self.result {
            let array = PrimitiveArray::<T>::new(vec![v].into(), None).with_data_type(self.data_type.clone());
            return ScalarValue::try_from_array(&array, 0);
        }

        Ok(ScalarValue::Null)
    }
}

#[macro_export]
macro_rules! make_accumulator {
    ($DATA_TYPE: ident, $HELPER: ident) => {{
        use arrow::datatypes::*;

        match $DATA_TYPE {
            DataType::Int8 => $HELPER!($DATA_TYPE, i8, Int8Type),
            DataType::Int16 => $HELPER!($DATA_TYPE, i16, Int16Type),
            DataType::Int32 => $HELPER!($DATA_TYPE, i32, Int32Type),
            DataType::Int64 => $HELPER!($DATA_TYPE, i64, Int64Type),
            DataType::UInt8 => $HELPER!($DATA_TYPE, u8, UInt8Type),
            DataType::UInt16 => $HELPER!($DATA_TYPE, u16, UInt16Type),
            DataType::UInt32 => $HELPER!($DATA_TYPE, u32, UInt32Type),
            DataType::UInt64 => $HELPER!($DATA_TYPE, u64, UInt64Type),
            DataType::Float32 => $HELPER!($DATA_TYPE, f32, Float32Type),
            DataType::Float64 => $HELPER!($DATA_TYPE, f64, Float64Type),
            DataType::Date32 => $HELPER!($DATA_TYPE, i32, Date32Type),
            DataType::Date64 => $HELPER!($DATA_TYPE, i64, Date64Type),
            DataType::Time32(TimeUnit::Second) => $HELPER!($DATA_TYPE, i32, Time32SecondType),
            DataType::Time32(TimeUnit::Millisecond) => $HELPER!($DATA_TYPE, i32, Time32MillisecondType),
            DataType::Time64(TimeUnit::Microsecond) => $HELPER!($DATA_TYPE, i64, Time64MicrosecondType),
            DataType::Time64(TimeUnit::Nanosecond) => $HELPER!($DATA_TYPE, i64, Time64NanosecondType),
            DataType::Timestamp(TimeUnit::Second, _) => $HELPER!($DATA_TYPE, i64, TimestampSecondType),
            DataType::Timestamp(TimeUnit::Millisecond, _) => $HELPER!($DATA_TYPE, i64, TimestampMillisecondType),
            DataType::Timestamp(TimeUnit::Microsecond, _) => $HELPER!($DATA_TYPE, i64, TimestampMicrosecondType),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => $HELPER!($DATA_TYPE, i64, TimestampNanosecondType),
            DataType::Decimal128(_, _) => $HELPER!($DATA_TYPE, i128, Decimal128Type),
            DataType::Decimal256(_, _) => $HELPER!($DATA_TYPE, i256, Decimal256Type),
            _ => unimplemented!("PrimitiveAccumulator not supported for datatype: {}", $DATA_TYPE),
        }
    }};
}
