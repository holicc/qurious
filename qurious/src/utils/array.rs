use crate::{
    arrow_err,
    error::{Error, Result},
    internal_err,
};
use arrow::{
    array::{
        new_null_array, Array, ArrayRef, AsArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
        Decimal256Array, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        StringArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Decimal128Type, Decimal256Type, DecimalType, TimeUnit},
};
use std::sync::Arc;

#[macro_export]
macro_rules! hash_array {
    ($ARRAY: ident,$VALUES: expr,$HASHER_MAP: expr) => {{
        let group_values = $VALUES
            .as_any()
            .downcast_ref::<$ARRAY>()
            .ok_or(Error::InternalError("Failed to downcast to StringArray".to_string()))?;

        for (i, v) in group_values.iter().enumerate() {
            if let Some(val) = v {
                let mut hasher = $HASHER_MAP.entry(i).or_insert(DefaultHasher::new());
                val.hash(&mut hasher);
            }
        }
    }};
}

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

#[macro_export]
macro_rules! cast_and_get_scalar {
    ($array: expr, $ARRAYTYPE: ident, $index: expr,$SCLARA: ident ) => {{
        use arrow::array::Array;
        let pary = $array
            .as_any()
            .downcast_ref::<arrow::array::$ARRAYTYPE>()
            .ok_or(Error::InternalError(format!(
                "could not cast value to {}",
                stringify!($ARRAYTYPE)
            )))?;
        if pary.is_null($index) {
            return Ok(ScalarValue::$SCLARA(None));
        }
        Ok(ScalarValue::$SCLARA(Some(pary.value($index))))
    }};
}

#[macro_export]
macro_rules! cast_and_get_decimal {
    ($array: expr, $ARRAYTYPE: ident, $index: expr,$SCLARA: ident, $precision:expr, $scale:expr) => {{
        use arrow::array::Array;
        let pary = $array
            .as_any()
            .downcast_ref::<arrow::array::$ARRAYTYPE>()
            .ok_or(Error::InternalError(format!(
                "could not cast value to {}",
                stringify!($ARRAYTYPE)
            )))?;
        if pary.is_null($index) {
            return Ok(ScalarValue::$SCLARA(None, $precision, $scale));
        }
        Ok(ScalarValue::$SCLARA(Some(pary.value($index)), $precision, $scale))
    }};
}

pub fn repeat_array(ary: &ArrayRef, index: usize, size: usize) -> Result<ArrayRef> {
    match ary.data_type() {
        DataType::Null => Ok(new_null_array(ary.data_type(), size)),
        DataType::Boolean => build_primary_array!(BooleanArray, ary, index, size),
        DataType::Utf8 => build_primary_array!(StringArray, ary, index, size),
        DataType::Int8 => build_primary_array!(Int8Array, ary, index, size),
        DataType::Int16 => build_primary_array!(Int16Array, ary, index, size),
        DataType::Int32 => build_primary_array!(Int32Array, ary, index, size),
        DataType::Int64 => build_primary_array!(Int64Array, ary, index, size),
        DataType::UInt8 => build_primary_array!(UInt8Array, ary, index, size),
        DataType::UInt16 => build_primary_array!(UInt16Array, ary, index, size),
        DataType::UInt32 => build_primary_array!(UInt32Array, ary, index, size),
        DataType::UInt64 => build_primary_array!(UInt64Array, ary, index, size),
        DataType::Float16 => build_primary_array!(Float16Array, ary, index, size),
        DataType::Float32 => build_primary_array!(Float32Array, ary, index, size),
        DataType::Float64 => build_primary_array!(Float64Array, ary, index, size),
        DataType::Date32 => build_primary_array!(Date32Array, ary, index, size),
        DataType::Date64 => build_primary_array!(Date64Array, ary, index, size),
        DataType::Time32(TimeUnit::Second) => {
            build_primary_array!(Time32SecondArray, ary, index, size)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            build_primary_array!(Time32MillisecondArray, ary, index, size)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            build_primary_array!(Time64MicrosecondArray, ary, index, size)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            build_primary_array!(Time64NanosecondArray, ary, index, size)
        }
        DataType::Timestamp(TimeUnit::Second, tz) => {
            build_timestamp_array!(TimestampSecondArray, ary, index, size, tz.clone())
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            build_timestamp_array!(TimestampMillisecondArray, ary, index, size, tz.clone())
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            build_timestamp_array!(TimestampMicrosecondArray, ary, index, size, tz.clone())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            build_timestamp_array!(TimestampNanosecondArray, ary, index, size, tz.clone())
        }
        DataType::Decimal128(precision, scale) => {
            let array = ary.as_primitive::<Decimal128Type>();

            if array.is_null(index) {
                return Ok(new_null_array(
                    &Decimal128Type::TYPE_CONSTRUCTOR(*precision, *scale),
                    size,
                ));
            }

            Decimal128Array::from_iter_values(vec![array.value(index); size])
                .with_precision_and_scale(*precision, *scale)
                .map(|v| Arc::new(v) as Arc<dyn Array>)
                .map_err(|e| arrow_err!(e))
        }
        DataType::Decimal256(precision, scale) => {
            let array = ary.as_primitive::<Decimal256Type>();

            if array.is_null(index) {
                return Ok(new_null_array(
                    &Decimal256Type::TYPE_CONSTRUCTOR(*precision, *scale),
                    size,
                ));
            }

            Decimal256Array::from_iter_values(vec![array.value(index); size])
                .with_precision_and_scale(*precision, *scale)
                .map(|v| Arc::new(v) as Arc<dyn Array>)
                .map_err(|e| arrow_err!(e))
        }
        _ => internal_err!("Unsupported data type {}", ary.data_type()),
    }
}
