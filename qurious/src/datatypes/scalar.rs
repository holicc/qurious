use crate::error::{Error, Result};
use arrow::{
    array::{
        new_null_array, Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, LargeStringArray, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Field},
};
use std::{fmt::Display, sync::Arc};

macro_rules! typed_cast {
    ($array:expr, $index:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        use std::any::type_name;
        let array = $array
            .as_any()
            .downcast_ref::<$ARRAYTYPE>()
            .ok_or_else(|| Error::InternalError(format!("could not cast value to {}", type_name::<$ARRAYTYPE>())))?;
        Ok::<ScalarValue, Error>(ScalarValue::$SCALAR(match array.is_null($index) {
            true => None,
            false => Some(array.value($index).into()),
        }))
    }};
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum ScalarValue {
    Null,
    Boolean(Option<bool>),
    Int64(Option<i64>),
    Int32(Option<i32>),
    Int16(Option<i16>),
    Int8(Option<i8>),
    UInt64(Option<u64>),
    UInt32(Option<u32>),
    UInt16(Option<u16>),
    UInt8(Option<u8>),
    Float64(Option<f64>),
    Float32(Option<f32>),
    Utf8(Option<String>),
}

impl ScalarValue {
    pub fn to_field(&self) -> Field {
        match self {
            ScalarValue::Null => Field::new("null", DataType::Null, true),
            ScalarValue::Boolean(_) => Field::new("bool", DataType::Boolean, true),
            ScalarValue::Int64(_) => Field::new("i64", DataType::Int64, true),
            ScalarValue::Int32(_) => Field::new("i32", DataType::Int32, true),
            ScalarValue::Int16(_) => Field::new("i16", DataType::Int16, true),
            ScalarValue::Int8(_) => Field::new("i8", DataType::Int8, true),
            ScalarValue::UInt64(_) => Field::new("u64", DataType::UInt64, true),
            ScalarValue::UInt32(_) => Field::new("u32", DataType::UInt32, true),
            ScalarValue::UInt16(_) => Field::new("u16", DataType::UInt16, true),
            ScalarValue::UInt8(_) => Field::new("u8", DataType::UInt8, true),
            ScalarValue::Float64(_) => Field::new("f64", DataType::Float64, true),
            ScalarValue::Float32(_) => Field::new("f32", DataType::Float32, true),
            ScalarValue::Utf8(_) => Field::new("utf8", DataType::Utf8, true),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Null => DataType::Null,
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Utf8(_) => DataType::Utf8,
        }
    }

    pub fn to_array(&self, num_row: usize) -> ArrayRef {
        match self {
            ScalarValue::Null => new_null_array(&DataType::Null, num_row),
            ScalarValue::Boolean(b) => Arc::new(BooleanArray::from(vec![*b; num_row])) as ArrayRef,
            ScalarValue::Int64(i) => Arc::new(Int64Array::from(vec![*i; num_row])) as ArrayRef,
            ScalarValue::Int32(i) => Arc::new(Int32Array::from(vec![*i; num_row])) as ArrayRef,
            ScalarValue::Int16(i) => Arc::new(Int16Array::from(vec![*i; num_row])) as ArrayRef,
            ScalarValue::Int8(i) => Arc::new(Int8Array::from(vec![*i; num_row])) as ArrayRef,
            ScalarValue::UInt64(i) => Arc::new(UInt64Array::from(vec![*i; num_row])) as ArrayRef,
            ScalarValue::UInt32(i) => Arc::new(UInt32Array::from(vec![*i; num_row])) as ArrayRef,
            ScalarValue::UInt16(i) => Arc::new(UInt16Array::from(vec![*i; num_row])) as ArrayRef,
            ScalarValue::UInt8(i) => Arc::new(UInt8Array::from(vec![*i; num_row])) as ArrayRef,
            ScalarValue::Float64(f) => Arc::new(Float64Array::from(vec![*f; num_row])) as ArrayRef,
            ScalarValue::Float32(f) => Arc::new(Float32Array::from(vec![*f; num_row])) as ArrayRef,
            ScalarValue::Utf8(s) => Arc::new(StringArray::from(vec![s.clone(); num_row])) as ArrayRef,
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            ScalarValue::Null => true,
            _ => false,
        }
    }

    pub fn to_value_string(&self) -> String {
        match self {
            ScalarValue::Null => "null".to_string(),
            ScalarValue::Boolean(Some(v)) => v.to_string(),
            ScalarValue::Boolean(None) => "null".to_string(),
            ScalarValue::Int64(Some(v)) => v.to_string(),
            ScalarValue::Int64(None) => "null".to_string(),
            ScalarValue::Int32(Some(v)) => v.to_string(),
            ScalarValue::Int32(None) => "null".to_string(),
            ScalarValue::Int16(Some(v)) => v.to_string(),
            ScalarValue::Int16(None) => "null".to_string(),
            ScalarValue::Int8(Some(v)) => v.to_string(),
            ScalarValue::Int8(None) => "null".to_string(),
            ScalarValue::UInt64(Some(v)) => v.to_string(),
            ScalarValue::UInt64(None) => "null".to_string(),
            ScalarValue::UInt32(Some(v)) => v.to_string(),
            ScalarValue::UInt32(None) => "null".to_string(),
            ScalarValue::UInt16(Some(v)) => v.to_string(),
            ScalarValue::UInt16(None) => "null".to_string(),
            ScalarValue::UInt8(Some(v)) => v.to_string(),
            ScalarValue::UInt8(None) => "null".to_string(),
            ScalarValue::Float64(Some(v)) => v.to_string(),
            ScalarValue::Float64(None) => "null".to_string(),
            ScalarValue::Float32(Some(v)) => v.to_string(),
            ScalarValue::Float32(None) => "null".to_string(),
            ScalarValue::Utf8(Some(v)) => v.to_string(),
            ScalarValue::Utf8(None) => "null".to_string(),
        }
    }

    pub fn try_from_array(array: &dyn Array, index: usize) -> Result<Self> {
        // handle NULL value
        if !array.is_valid(index) {
            return array.data_type().try_into();
        }

        match array.data_type() {
            DataType::Null => Ok(ScalarValue::Null),
            DataType::Boolean => typed_cast!(array, index, BooleanArray, Boolean),
            DataType::Int8 => typed_cast!(array, index, Int8Array, Int8),
            DataType::Int16 => typed_cast!(array, index, Int16Array, Int16),
            DataType::Int32 => typed_cast!(array, index, Int32Array, Int32),
            DataType::Int64 => typed_cast!(array, index, Int64Array, Int64),
            DataType::UInt8 => typed_cast!(array, index, UInt8Array, UInt8),
            DataType::UInt16 => typed_cast!(array, index, UInt16Array, UInt16),
            DataType::UInt32 => typed_cast!(array, index, UInt32Array, UInt32),
            DataType::UInt64 => typed_cast!(array, index, UInt64Array, UInt64),
            DataType::Float32 => typed_cast!(array, index, Float32Array, Float32),
            DataType::Float64 => typed_cast!(array, index, Float64Array, Float64),
            DataType::Utf8 => typed_cast!(array, index, StringArray, Utf8),
            DataType::LargeUtf8 => typed_cast!(array, index, LargeStringArray, Utf8),
            _ => unimplemented!("data type {} not supported", array.data_type()),
        }
    }
}

impl From<&str> for ScalarValue {
    fn from(s: &str) -> Self {
        ScalarValue::Utf8(Some(s.to_string()))
    }
}

impl From<ScalarValue> for Field {
    fn from(value: ScalarValue) -> Self {
        value.to_field()
    }
}

impl TryFrom<DataType> for ScalarValue {
    type Error = crate::error::Error;

    fn try_from(value: DataType) -> std::result::Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<&DataType> for ScalarValue {
    type Error = crate::error::Error;

    fn try_from(value: &DataType) -> std::result::Result<Self, Self::Error> {
        match value {
            DataType::Null => Ok(ScalarValue::Null),
            DataType::Boolean => Ok(ScalarValue::Boolean(None)),
            DataType::Int8 => Ok(ScalarValue::Int8(None)),
            DataType::Int16 => Ok(ScalarValue::Int16(None)),
            DataType::Int32 => Ok(ScalarValue::Int32(None)),
            DataType::Int64 => Ok(ScalarValue::Int64(None)),
            DataType::UInt8 => Ok(ScalarValue::UInt8(None)),
            DataType::UInt16 => Ok(ScalarValue::UInt16(None)),
            DataType::UInt32 => Ok(ScalarValue::UInt32(None)),
            DataType::UInt64 => Ok(ScalarValue::UInt64(None)),
            DataType::Float32 => Ok(ScalarValue::Float32(None)),
            DataType::Float64 => Ok(ScalarValue::Float64(None)),
            DataType::Utf8 => Ok(ScalarValue::Utf8(None)),
            DataType::LargeUtf8 => Ok(ScalarValue::Utf8(None)),
            _ => unimplemented!("data type {} not supported", value),
        }
    }
}

impl Eq for ScalarValue {}

impl std::hash::Hash for ScalarValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.to_value_string().hash(state);
    }
}

impl Display for ScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarValue::Null => write!(f, "null"),
            ScalarValue::Boolean(Some(v)) => write!(f, "{}", v),
            ScalarValue::Boolean(None) => write!(f, "null"),
            ScalarValue::Int64(Some(v)) => write!(f, "Int64({})", v),
            ScalarValue::Int64(None) => write!(f, "null"),
            ScalarValue::Int32(Some(v)) => write!(f, "Int32({})", v),
            ScalarValue::Int32(None) => write!(f, "null"),
            ScalarValue::Int16(Some(v)) => write!(f, "Int16({})", v),
            ScalarValue::Int16(None) => write!(f, "null"),
            ScalarValue::Int8(Some(v)) => write!(f, "Int8({})", v),
            ScalarValue::Int8(None) => write!(f, "null"),
            ScalarValue::UInt64(Some(v)) => write!(f, "UInt64({})", v),
            ScalarValue::UInt64(None) => write!(f, "null"),
            ScalarValue::UInt32(Some(v)) => write!(f, "UInt32({})", v),
            ScalarValue::UInt32(None) => write!(f, "null"),
            ScalarValue::UInt16(Some(v)) => write!(f, "UInt16({})", v),
            ScalarValue::UInt16(None) => write!(f, "null"),
            ScalarValue::UInt8(Some(v)) => write!(f, "UInt8({})", v),
            ScalarValue::UInt8(None) => write!(f, "null"),
            ScalarValue::Float64(Some(v)) => write!(f, "Float64({})", v),
            ScalarValue::Float64(None) => write!(f, "null"),
            ScalarValue::Float32(Some(v)) => write!(f, "Float32({})", v),
            ScalarValue::Float32(None) => write!(f, "null"),
            ScalarValue::Utf8(Some(v)) => write!(f, "Utf8('{}')", v),
            ScalarValue::Utf8(None) => write!(f, "null"),
        }
    }
}
