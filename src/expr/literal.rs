use super::LogicalExpr;
use crate::error::Result;
use crate::logical_plan::LogicalPlan;
use crate::types::{datatype::DataType, field::Field};
use std::fmt::Display;

macro_rules! make_literal {
    ($scalar:ident, $ty:ty, $dt:ident) => {
        #[derive(Debug)]
        pub struct $scalar {
            pub value: $ty,
        }

        impl LogicalExpr for $scalar {
            fn to_field(&self, _plan: &dyn LogicalPlan) -> Result<Field> {
                Ok(Field {
                    name: self.value.to_string(),
                    datatype: DataType::$dt,
                })
            }
        }

        impl Display for $scalar {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.value)
            }
        }
    };
}

make_literal!(LiteralBoolean, bool, Bool);
make_literal!(LiteralFloat32, f32, Float32);
make_literal!(LiteralFloat64, f64, FLoat64);
make_literal!(LiteralInt8, i8, Int8);
make_literal!(LiteralInt16, i16, Int16);
make_literal!(LiteralInt32, i32, Int32);
make_literal!(LiteralInt64, i64, Int64);
make_literal!(LiteralUInt8, u8, UInt8);
make_literal!(LiteralUInt16, u16, UInt16);
make_literal!(LiteralUInt32, u32, UInt32);
make_literal!(LiteralUInt64, u64, UInt64);

pub struct LiteralString {
    pub value: String,
}

impl LogicalExpr for LiteralString {
    fn to_field(&self, _plan: &dyn LogicalPlan) -> Result<Field> {
        Ok(Field {
            name: self.value.to_string(),
            datatype: DataType::String,
        })
    }
}

impl Display for LiteralString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'", self.value)
    }
}
