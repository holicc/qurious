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

f64_function!(Ln, "ln", "`LN(x)` -- the natural logarithm.", |v| v.ln());
f64_function!(Log10, "log10", "`LOG10(x)`.", |v| v.log10());
f64_function!(Exp, "exp", "`EXP(x)`.", |v| v.exp());
f64_function!(
    Sign,
    "sign",
    "`SIGN(x)` -- -1, 0 or 1; NaN for a NaN argument, following f64.",
    |v: f64| if v == 0.0 { 0.0 } else { v.signum() }
);
f64_function!(Trunc, "trunc", "`TRUNC(x)` -- towards zero.", |v| v.trunc());

/// Applies `f` to two arguments as f64.
fn map_f64_pair(args: Vec<ArrayRef>, name: &str, f: impl Fn(f64, f64) -> f64) -> Result<ArrayRef> {
    let [lhs, rhs] = args.as_slice() else {
        return internal_err!("{name} requires 2 arguments, got {}", args.len());
    };
    one_numeric_argument(name, &[lhs.data_type().clone()])?;
    one_numeric_argument(name, &[rhs.data_type().clone()])?;

    let lhs = cast(lhs.as_ref(), &DataType::Float64).map_err(|e| arrow_err!(e))?;
    let rhs = cast(rhs.as_ref(), &DataType::Float64).map_err(|e| arrow_err!(e))?;
    let (lhs, rhs) = (lhs.as_primitive::<Float64Type>(), rhs.as_primitive::<Float64Type>());

    let rows = lhs.len().max(rhs.len());
    let mut builder = Float64Builder::with_capacity(rows);

    for row in 0..rows {
        // A literal argument arrives as a single-row array while the columns are longer.
        let left = (if lhs.len() == 1 { 0 } else { row }, lhs);
        let right = (if rhs.len() == 1 { 0 } else { row }, rhs);

        if left.1.is_null(left.0) || right.1.is_null(right.0) {
            builder.append_null();
        } else {
            builder.append_value(f(left.1.value(left.0), right.1.value(right.0)));
        }
    }

    Ok(Arc::new(builder.finish()))
}

macro_rules! f64_pair_function {
    ($ty:ident, $sql_name:literal, $doc:literal, $body:expr $(, aliases: [$($alias:literal),+])?) => {
        #[doc = $doc]
        #[derive(Debug)]
        pub struct $ty;

        impl UserDefinedFunction for $ty {
            fn name(&self) -> &str {
                $sql_name
            }

            fn aliases(&self) -> &[&str] {
                &[$($($alias),+)?]
            }

            fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
                if arg_types.len() != 2 {
                    return internal_err!("{} requires 2 arguments, got {}", $sql_name, arg_types.len());
                }
                one_numeric_argument($sql_name, &arg_types[..1])?;
                one_numeric_argument($sql_name, &arg_types[1..])?;

                Ok(DataType::Float64)
            }

            fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
                map_f64_pair(args, $sql_name, $body)
            }
        }
    };
}

f64_pair_function!(Power, "power", "`POWER(a, b)`.", |a: f64, b: f64| a.powf(b), aliases: ["pow"]);
f64_pair_function!(
    Modulo,
    "mod",
    "`MOD(a, b)` -- the remainder, taking the sign of the dividend as in postgres; NaN when b is 0.",
    |a: f64, b: f64| if b == 0.0 { f64::NAN } else { a % b }
);
f64_pair_function!(
    Log,
    "log",
    "`LOG(base, x)` -- the logarithm of x in the given base, argument order as in postgres.",
    |base: f64, value: f64| value.log(base)
);
