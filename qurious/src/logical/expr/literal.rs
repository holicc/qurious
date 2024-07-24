use crate::datatypes::scalar::ScalarValue;
use crate::logical::expr::LogicalExpr;

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
