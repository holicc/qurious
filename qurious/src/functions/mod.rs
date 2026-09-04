pub mod conditional;
pub mod datetime;
pub mod math;
pub mod string;

use crate::error::{Error, Result};
use crate::internal_err;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use conditional::coalesce::Coalesce;
use conditional::extremum::Extremum;
use conditional::nullif::NullIf;
use datetime::extract::DatetimeExtract;
use math::numeric::{Abs, Ceil, Exp, Floor, Ln, Log, Log10, Modulo, Power, Round, Sign, Sqrt, Trunc};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use string::concat::Concat;
use string::manipulate::{Repeat, Replace, Reverse, Side, StartsWith, Strpos};
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
    /// Other names this same function answers to, e.g. `substr` for `substring`.
    ///
    /// Aliases are registered alongside `name()`, so a call by either name reaches this impl.
    fn aliases(&self) -> &[&str] {
        &[]
    }
    /// whether the function can return null
    fn is_nullable(&self) -> bool {
        true
    }
    /// evaluate the function
    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef>;
}

/// The builtin functions keyed by every name they can be called by, upper-cased.
///
/// Lookup is case-insensitive, so callers must upper-case the name they look up.
pub fn builtin_function_registry() -> HashMap<String, Arc<dyn UserDefinedFunction>> {
    let mut registry = HashMap::new();

    for udf in all_builtin_functions() {
        for name in std::iter::once(udf.name()).chain(udf.aliases().iter().copied()) {
            let name = name.to_uppercase();
            if let Some(existing) = registry.insert(name.clone(), udf.clone()) {
                // A collision would make one of the two unreachable depending on iteration order,
                // which is exactly the kind of bug that only shows up as a wrong answer.
                panic!("builtin function `{name}` is registered twice: {existing:?} and {udf:?}");
            }
        }
    }

    registry
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
        Arc::new(Ln),
        Arc::new(Log10),
        Arc::new(Log),
        Arc::new(Exp),
        Arc::new(Sign),
        Arc::new(Trunc),
        Arc::new(Power),
        Arc::new(Modulo),
        Arc::new(Concat),
        Arc::new(Extremum::greatest()),
        Arc::new(Extremum::least()),
        Arc::new(Reverse),
        Arc::new(Replace),
        Arc::new(Strpos),
        Arc::new(StartsWith),
        Arc::new(Repeat),
        Arc::new(Side::left()),
        Arc::new(Side::right()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_holds_every_name_and_alias() {
        let registry = builtin_function_registry();

        // Every function is reachable by its own name, and names are keyed upper-cased.
        for udf in all_builtin_functions() {
            let key = udf.name().to_uppercase();
            assert!(registry.contains_key(&key), "{} is not registered", udf.name());
        }

        // Aliases reach the same implementation as the canonical name.
        for (alias, canonical) in [("SUBSTR", "substring"), ("POW", "power"), ("BTRIM", "trim")] {
            let udf = registry
                .get(alias)
                .unwrap_or_else(|| panic!("alias {alias} is not registered"));
            assert_eq!(udf.name(), canonical);
        }
    }
}
