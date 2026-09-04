pub mod conditional;
pub mod datetime;
pub mod math;
pub mod string;

use crate::error::{Error, Result};
use crate::internal_err;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use conditional::coalesce::Coalesce;
use conditional::nullif::NullIf;
use datetime::extract::DatetimeExtract;
use math::numeric::{Abs, Ceil, Floor, Round, Sqrt};
use std::fmt::Debug;
use std::sync::Arc;
use string::substring::Substring;
use string::text::{Length, Lower, Ltrim, Rtrim, Trim, Upper};

pub trait UserDefinedFunction: Debug + Send + Sync {
    /// the name of the function
    fn name(&self) -> &str;
    /// The type this call returns, given the types of its arguments.
    ///
    /// The arguments are needed because most functions are polymorphic in them -- `abs` returns
    /// whatever it was given, `coalesce` the common type of its branches -- and this is also where
    /// a wrong argument count or type is reported.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;
    /// whether the function can return null
    fn is_nullable(&self) -> bool {
        true
    }
    /// evaluate the function
    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef>;
}

pub fn all_builtin_functions() -> Vec<Arc<dyn UserDefinedFunction>> {
    vec![
        Arc::new(DatetimeExtract),
        Arc::new(Substring),
        Arc::new(Coalesce),
        Arc::new(NullIf),
        Arc::new(Upper),
        Arc::new(Lower),
        Arc::new(Trim),
        Arc::new(Ltrim),
        Arc::new(Rtrim),
        Arc::new(Length::new("length")),
        Arc::new(Length::new("char_length")),
        Arc::new(Abs),
        Arc::new(Round),
        Arc::new(Ceil),
        Arc::new(Floor),
        Arc::new(Sqrt),
    ]
}

/// The type several arguments have in common, so they can be compared or chosen between.
///
/// `NULL` carries no type and takes on whatever the others have. Numeric arguments are widened to
/// the larger of the two rather than rejected, since `coalesce(int_col, 0)` and friends are the
/// common case; anything else has to match exactly, and the caller is told to cast if it does not.
pub fn common_argument_type(name: &str, arg_types: &[DataType]) -> Result<DataType> {
    let mut common: Option<DataType> = None;

    for arg_type in arg_types {
        if arg_type == &DataType::Null {
            continue;
        }

        common = match common {
            None => Some(arg_type.clone()),
            Some(current) if &current == arg_type => Some(current),
            Some(current) => Some(widen(name, &current, arg_type)?),
        };
    }

    // Every argument was NULL, which is a legitimate call with a NULL result.
    Ok(common.unwrap_or(DataType::Null))
}

fn widen(name: &str, lhs: &DataType, rhs: &DataType) -> Result<DataType> {
    use DataType::*;

    let widened = match (lhs, rhs) {
        (Float64, other) | (other, Float64) if other.is_numeric() => Float64,
        (Float32, other) | (other, Float32) if other.is_numeric() => Float64,
        (Decimal128(p1, s1), Decimal128(p2, s2)) => {
            let scale = *s1.max(s2);
            let integral = (*p1 as i16 - *s1 as i16).max(*p2 as i16 - *s2 as i16);
            Decimal128((integral + scale as i16).clamp(1, 38) as u8, scale)
        }
        (Int64, other) | (other, Int64) if other.is_integer() => Int64,
        (Int32, other) | (other, Int32) if other.is_integer() => Int32,
        (Utf8, LargeUtf8) | (LargeUtf8, Utf8) => LargeUtf8,
        _ => {
            return internal_err!(
                "{name} cannot combine arguments of type {lhs} and {rhs}; cast one of them explicitly"
            )
        }
    };

    Ok(widened)
}
