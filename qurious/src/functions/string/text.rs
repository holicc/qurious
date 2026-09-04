use arrow::array::{Array, ArrayRef, AsArray, Int64Builder, StringBuilder};
use arrow::compute::kernels::cast::cast;
use arrow::datatypes::DataType;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::functions::UserDefinedFunction;
use crate::{arrow_err, internal_err};

/// Cast a single string argument to `Utf8` so the implementations below only handle one layout.
fn string_argument(name: &str, args: &[ArrayRef]) -> Result<ArrayRef> {
    let [arg] = args else {
        return internal_err!("{name} requires 1 argument, got {}", args.len());
    };

    match arg.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            cast(arg.as_ref(), &DataType::Utf8).map_err(|e| arrow_err!(e))
        }
        other => internal_err!("{name} expects a string argument, got {other}"),
    }
}

fn one_string_argument(name: &str, arg_types: &[DataType]) -> Result<()> {
    match arg_types {
        [DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View] => Ok(()),
        [other] => internal_err!("{name} expects a string argument, got {other}"),
        _ => internal_err!("{name} requires 1 argument, got {}", arg_types.len()),
    }
}

/// Maps each string through `f`, preserving nulls.
fn map_strings(args: Vec<ArrayRef>, name: &str, f: impl Fn(&str) -> String) -> Result<ArrayRef> {
    let values = string_argument(name, &args)?;
    let values = values.as_string::<i32>();

    let mut builder = StringBuilder::with_capacity(values.len(), values.value_data().len());
    for row in 0..values.len() {
        if values.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(f(values.value(row)));
        }
    }

    Ok(Arc::new(builder.finish()))
}

macro_rules! string_to_string {
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
                one_string_argument($sql_name, arg_types)?;
                Ok(DataType::Utf8)
            }

            fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
                map_strings(args, $sql_name, $body)
            }
        }
    };
}

string_to_string!(Upper, "upper", "`UPPER(s)`.", |s| s.to_uppercase());
string_to_string!(Lower, "lower", "`LOWER(s)`.", |s| s.to_lowercase());
string_to_string!(
    Trim,
    "trim",
    "`TRIM(s)` -- whitespace from both ends.",
    |s| s.trim().to_owned(),
    aliases: ["btrim"]
);
string_to_string!(Ltrim, "ltrim", "`LTRIM(s)` -- whitespace from the start.", |s| s
    .trim_start()
    .to_owned());
string_to_string!(Rtrim, "rtrim", "`RTRIM(s)` -- whitespace from the end.", |s| s
    .trim_end()
    .to_owned());

/// `LENGTH(s)` / `CHAR_LENGTH(s)` -- the number of characters, not bytes.
#[derive(Debug)]
pub struct Length {
    name: &'static str,
}

impl Length {
    pub fn new(name: &'static str) -> Self {
        Self { name }
    }
}

impl UserDefinedFunction for Length {
    fn name(&self) -> &str {
        self.name
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        one_string_argument(self.name, arg_types)?;
        Ok(DataType::Int64)
    }

    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
        let values = string_argument(self.name, &args)?;
        let values = values.as_string::<i32>();

        let mut builder = Int64Builder::with_capacity(values.len());
        for row in 0..values.len() {
            if values.is_null(row) {
                builder.append_null();
            } else {
                // Characters rather than bytes, so multi-byte text counts the way SQL expects.
                builder.append_value(values.value(row).chars().count() as i64);
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}
