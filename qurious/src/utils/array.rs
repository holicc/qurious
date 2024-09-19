use crate::error::{Error, Result};
use arrow::{
    array::{
        new_null_array, Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float16Array, Float32Array,
        Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray, Time32MillisecondArray,
        Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array,
        UInt64Array, UInt8Array,
    },
    datatypes::TimeUnit,
};
use std::sync::Arc;

#[macro_export]
macro_rules! build_primary_array {
    ($ARRY_TYPE:ident, $ary:expr, $index:expr, $size:expr) => {{
        use std::any::type_name;

        let pary = $ary
            .as_any()
            .downcast_ref::<arrow::array::$ARRY_TYPE>()
            .ok_or(Error::InternalError(format!(
                "could not cast value to {}",
                type_name::<$ARRY_TYPE>()
            )))?;
        if pary.is_null($index) {
            return Ok(new_null_array(pary.data_type(), $size));
        }
        let val = pary.value($index);
        Ok(Arc::new($ARRY_TYPE::from(vec![val; $size])))
    }};
}

#[macro_export]
macro_rules! build_timestamp_array {
    ($ARRY_TYPE:ident, $ary:expr, $index:expr, $size:expr, $tz:expr) => {{
        use std::any::type_name;

        let pary = $ary
            .as_any()
            .downcast_ref::<arrow::array::$ARRY_TYPE>()
            .ok_or(Error::InternalError(format!(
                "could not cast value to {}",
                type_name::<$ARRY_TYPE>()
            )))?;
        if pary.is_null($index) {
            return Ok(new_null_array(pary.data_type(), $size));
        }
        let val = pary.value($index);
        Ok(Arc::new($ARRY_TYPE::from_value(val, $size).with_timezone_opt($tz)))
    }};
}
pub fn repeat_array(ary: &ArrayRef, index: usize, size: usize) -> Result<ArrayRef> {
    match ary.data_type() {
        arrow::datatypes::DataType::Null => Ok(new_null_array(ary.data_type(), size)),
        arrow::datatypes::DataType::Boolean => build_primary_array!(BooleanArray, ary, index, size),
        arrow::datatypes::DataType::Utf8 => build_primary_array!(StringArray, ary, index, size),
        arrow::datatypes::DataType::Int8 => build_primary_array!(Int8Array, ary, index, size),
        arrow::datatypes::DataType::Int16 => build_primary_array!(Int16Array, ary, index, size),
        arrow::datatypes::DataType::Int32 => build_primary_array!(Int32Array, ary, index, size),
        arrow::datatypes::DataType::Int64 => build_primary_array!(Int64Array, ary, index, size),
        arrow::datatypes::DataType::UInt8 => build_primary_array!(UInt8Array, ary, index, size),
        arrow::datatypes::DataType::UInt16 => build_primary_array!(UInt16Array, ary, index, size),
        arrow::datatypes::DataType::UInt32 => build_primary_array!(UInt32Array, ary, index, size),
        arrow::datatypes::DataType::UInt64 => build_primary_array!(UInt64Array, ary, index, size),
        arrow::datatypes::DataType::Float16 => build_primary_array!(Float16Array, ary, index, size),
        arrow::datatypes::DataType::Float32 => build_primary_array!(Float32Array, ary, index, size),
        arrow::datatypes::DataType::Float64 => build_primary_array!(Float64Array, ary, index, size),
        arrow::datatypes::DataType::Date32 => build_primary_array!(Date32Array, ary, index, size),
        arrow::datatypes::DataType::Date64 => build_primary_array!(Date64Array, ary, index, size),
        arrow::datatypes::DataType::Time32(TimeUnit::Second) => {
            build_primary_array!(Time32SecondArray, ary, index, size)
        }
        arrow::datatypes::DataType::Time32(TimeUnit::Millisecond) => {
            build_primary_array!(Time32MillisecondArray, ary, index, size)
        }
        arrow::datatypes::DataType::Time64(TimeUnit::Microsecond) => {
            build_primary_array!(Time64MicrosecondArray, ary, index, size)
        }
        arrow::datatypes::DataType::Time64(TimeUnit::Nanosecond) => {
            build_primary_array!(Time64NanosecondArray, ary, index, size)
        }
        arrow::datatypes::DataType::Timestamp(TimeUnit::Second, tz) => {
            build_timestamp_array!(TimestampSecondArray, ary, index, size, tz.clone())
        }
        arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            build_timestamp_array!(TimestampMillisecondArray, ary, index, size, tz.clone())
        }
        arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            build_timestamp_array!(TimestampMicrosecondArray, ary, index, size, tz.clone())
        }
        arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            build_timestamp_array!(TimestampNanosecondArray, ary, index, size, tz.clone())
        }
        _ => todo!(),
    }
}
