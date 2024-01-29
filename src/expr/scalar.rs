use crate::expr::LogicalExpr;
use crate::types::{datatype::DataType, field::Field};
use std::fmt::Display;

#[derive(Debug, Clone)]
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
            ScalarValue::Null => Field::new("null", DataType::Null),
            ScalarValue::Boolean(_) => Field::new("bool", DataType::Boolean),
            ScalarValue::Int64(_) => Field::new("i64", DataType::Int64),
            ScalarValue::Int32(_) => Field::new("i32", DataType::Int32),
            ScalarValue::Int16(_) => Field::new("i16", DataType::Int16),
            ScalarValue::Int8(_) => Field::new("i8", DataType::Int8),
            ScalarValue::UInt64(_) => Field::new("u64", DataType::UInt64),
            ScalarValue::UInt32(_) => Field::new("u32", DataType::UInt32),
            ScalarValue::UInt16(_) => Field::new("u16", DataType::UInt16),
            ScalarValue::UInt8(_) => Field::new("u8", DataType::UInt8),
            ScalarValue::Float64(_) => Field::new("f64", DataType::Float64),
            ScalarValue::Float32(_) => Field::new("f32", DataType::Float32),
            ScalarValue::Utf8(_) => Field::new("utf8", DataType::Utf8),
        }
    }
}

impl Display for ScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarValue::Null => write!(f, "null"),
            ScalarValue::Boolean(Some(v)) => write!(f, "{}", v),
            ScalarValue::Boolean(None) => write!(f, "null"),
            ScalarValue::Int64(Some(v)) => write!(f, "{}", v),
            ScalarValue::Int64(None) => write!(f, "null"),
            ScalarValue::Int32(Some(v)) => write!(f, "{}", v),
            ScalarValue::Int32(None) => write!(f, "null"),
            ScalarValue::Int16(Some(v)) => write!(f, "{}", v),
            ScalarValue::Int16(None) => write!(f, "null"),
            ScalarValue::Int8(Some(v)) => write!(f, "{}", v),
            ScalarValue::Int8(None) => write!(f, "null"),
            ScalarValue::UInt64(Some(v)) => write!(f, "{}", v),
            ScalarValue::UInt64(None) => write!(f, "null"),
            ScalarValue::UInt32(Some(v)) => write!(f, "{}", v),
            ScalarValue::UInt32(None) => write!(f, "null"),
            ScalarValue::UInt16(Some(v)) => write!(f, "{}", v),
            ScalarValue::UInt16(None) => write!(f, "null"),
            ScalarValue::UInt8(Some(v)) => write!(f, "{}", v),
            ScalarValue::UInt8(None) => write!(f, "null"),
            ScalarValue::Float64(Some(v)) => write!(f, "{}", v),
            ScalarValue::Float64(None) => write!(f, "null"),
            ScalarValue::Float32(Some(v)) => write!(f, "{}", v),
            ScalarValue::Float32(None) => write!(f, "null"),
            ScalarValue::Utf8(Some(v)) => write!(f, "{}", v),
            ScalarValue::Utf8(None) => write!(f, "null"),
        }
    }
}

macro_rules! impl_from {
    ($ty: ty, $scalar: tt) => {
        impl From<$ty> for ScalarValue {
            fn from(value: $ty) -> Self {
                ScalarValue::$scalar(Some(value))
            }
        }

        impl From<Option<$ty>> for ScalarValue {
            fn from(value: Option<$ty>) -> Self {
                ScalarValue::$scalar(value)
            }
        }
    };
}

impl_from!(bool, Boolean);
impl_from!(i64, Int64);
impl_from!(i32, Int32);
impl_from!(i16, Int16);
impl_from!(i8, Int8);
impl_from!(u64, UInt64);
impl_from!(u32, UInt32);
impl_from!(u16, UInt16);
impl_from!(u8, UInt8);
impl_from!(f64, Float64);
impl_from!(f32, Float32);
impl_from!(String, Utf8);

pub trait Literal {
    fn literal(&self) -> LogicalExpr;
}

pub fn literal<T: Literal>(value: T) -> LogicalExpr {
    value.literal()
}

impl Literal for &str {
    fn literal(&self) -> LogicalExpr {
        LogicalExpr::Literal(ScalarValue::Utf8(Some(self.to_string())))
    }
}

impl Literal for String {
    fn literal(&self) -> LogicalExpr {
        LogicalExpr::Literal(ScalarValue::Utf8(Some(self.clone())))
    }
}

macro_rules! make_literal {
    ($ty: ty, $scalar: ident) => {
        impl Literal for $ty {
            fn literal(&self) -> LogicalExpr {
                LogicalExpr::Literal(ScalarValue::from(*self))
            }
        }
    };
}

make_literal!(bool, Boolean);
make_literal!(i64, Int64);
make_literal!(i32, Int32);
make_literal!(i16, Int16);
make_literal!(i8, Int8);
make_literal!(u64, UInt64);
make_literal!(u32, UInt32);
make_literal!(u16, UInt16);
make_literal!(u8, UInt8);
make_literal!(f64, Float64);
make_literal!(f32, Float32);
