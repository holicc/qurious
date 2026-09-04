use arrow::array::{Array, ArrayRef, AsArray, Float64Builder, PrimitiveArray};
use arrow::compute::kernels::cast::cast;
use arrow::datatypes::{DataType, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::functions::UserDefinedFunction;
use crate::{arrow_err, internal_err};

fn one_numeric_argument(name: &str, arg_types: &[DataType]) -> Result<DataType> {
    match arg_types {
        [DataType::Null] => Ok(DataType::Null),
        [arg_type] if arg_type.is_numeric() => Ok(arg_type.clone()),
        [other] => internal_err!("{name} expects a numeric argument, got {other}"),
        _ => internal_err!("{name} requires 1 argument, got {}", arg_types.len()),
    }
}

/// `ABS(x)` -- keeps the argument's own type, as postgres does, rather than widening to a float.
#[derive(Debug)]
pub struct Abs;

impl UserDefinedFunction for Abs {
    fn name(&self) -> &str {
        "abs"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        one_numeric_argument("abs", arg_types)
    }

    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
        let [arg] = args.as_slice() else {
            return internal_err!("abs requires 1 argument, got {}", args.len());
        };
        one_numeric_argument("abs", &[arg.data_type().clone()])?;

        // Preserves the data type, which matters for decimals: `unary` would otherwise drop the
        // precision and scale and leave arrow's placeholder behind.
        macro_rules! unsigned_abs {
            ($t:ty) => {{
                let values: PrimitiveArray<$t> = arg.as_primitive::<$t>().unary(|v| v.abs());
                Ok(Arc::new(values.with_data_type(arg.data_type().clone())))
            }};
        }

        match arg.data_type() {
            DataType::Int8 => unsigned_abs!(Int8Type),
            DataType::Int16 => unsigned_abs!(Int16Type),
            DataType::Int32 => unsigned_abs!(Int32Type),
            DataType::Int64 => unsigned_abs!(Int64Type),
            DataType::Float32 => unsigned_abs!(Float32Type),
            DataType::Float64 => unsigned_abs!(Float64Type),
            DataType::Decimal128(_, _) => unsigned_abs!(Decimal128Type),
            // Unsigned values are already their own magnitude, and NULL stays NULL.
            DataType::Null | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                Ok(Arc::clone(arg))
            }
            other => internal_err!("abs is not supported for {other}"),
        }
    }
}

/// Applies `f` to each value as an f64, which is the type `ROUND`/`CEIL`/`FLOOR`/`SQRT` return.
fn map_f64(args: Vec<ArrayRef>, name: &str, f: impl Fn(f64) -> f64) -> Result<ArrayRef> {
    let [arg] = args.as_slice() else {
        return internal_err!("{name} requires 1 argument, got {}", args.len());
    };
    one_numeric_argument(name, &[arg.data_type().clone()])?;

    let values = cast(arg.as_ref(), &DataType::Float64).map_err(|e| arrow_err!(e))?;
    let values = values.as_primitive::<Float64Type>();

    let mut builder = Float64Builder::with_capacity(values.len());
    for row in 0..values.len() {
        if values.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(f(values.value(row)));
        }
    }

    Ok(Arc::new(builder.finish()))
}

macro_rules! f64_function {
    ($ty:ident, $sql_name:literal, $doc:literal, $body:expr) => {
        #[doc = $doc]
        #[derive(Debug)]
        pub struct $ty;

        impl UserDefinedFunction for $ty {
            fn name(&self) -> &str {
                $sql_name
            }

            fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
                one_numeric_argument($sql_name, arg_types)?;
                Ok(DataType::Float64)
            }

            fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
                map_f64(args, $sql_name, $body)
            }
        }
    };
}

f64_function!(
    Round,
    "round",
    "`ROUND(x)` -- to the nearest integer, halves away from zero.",
    |v| v.round()
);
f64_function!(Ceil, "ceil", "`CEIL(x)`.", |v| v.ceil());
f64_function!(Floor, "floor", "`FLOOR(x)`.", |v| v.floor());
f64_function!(
    Sqrt,
    "sqrt",
    "`SQRT(x)` -- NaN for a negative argument, following f64.",
    |v| v.sqrt()
);
