use arrow::array::{Array, ArrayRef, AsArray, BooleanBuilder, Int64Builder, StringBuilder};
use arrow::compute::kernels::cast::cast;
use arrow::datatypes::{DataType, Int64Type};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::functions::UserDefinedFunction;
use crate::{arrow_err, internal_err};

fn as_utf8(name: &str, arg: &ArrayRef) -> Result<ArrayRef> {
    match arg.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            cast(arg.as_ref(), &DataType::Utf8).map_err(|e| arrow_err!(e))
        }
        other => internal_err!("{name} expects a string argument, got {other}"),
    }
}

fn check_string_arity(name: &str, arg_types: &[DataType], strings: usize, total: usize) -> Result<()> {
    if arg_types.len() != total {
        return internal_err!("{name} requires {total} arguments, got {}", arg_types.len());
    }

    for arg_type in &arg_types[..strings] {
        if !matches!(
            arg_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
        ) {
            return internal_err!("{name} expects a string argument, got {arg_type}");
        }
    }

    Ok(())
}

/// Row `row` of `column`, treating a single-row array as a literal repeated for every row.
fn value_at<'a>(column: &'a arrow::array::StringArray, row: usize) -> Option<&'a str> {
    let index = if column.len() == 1 { 0 } else { row };
    column.is_valid(index).then(|| column.value(index))
}

fn int_at(column: &arrow::array::PrimitiveArray<Int64Type>, row: usize) -> Option<i64> {
    let index = if column.len() == 1 { 0 } else { row };
    column.is_valid(index).then(|| column.value(index))
}

/// `REVERSE(s)` -- by characters, so multi-byte text is not corrupted.
#[derive(Debug)]
pub struct Reverse;

impl UserDefinedFunction for Reverse {
    fn name(&self) -> &str {
        "reverse"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        check_string_arity("reverse", arg_types, 1, 1)?;
        Ok(DataType::Utf8)
    }

    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
        let [arg] = args.as_slice() else {
            return internal_err!("reverse requires 1 argument, got {}", args.len());
        };

        let values = as_utf8("reverse", arg)?;
        let values = values.as_string::<i32>();

        let mut builder = StringBuilder::with_capacity(values.len(), values.value_data().len());
        for row in 0..values.len() {
            match value_at(values, row) {
                Some(value) => builder.append_value(value.chars().rev().collect::<String>()),
                None => builder.append_null(),
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

/// `REPLACE(s, from, to)` -- every occurrence.
#[derive(Debug)]
pub struct Replace;

impl UserDefinedFunction for Replace {
    fn name(&self) -> &str {
        "replace"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        check_string_arity("replace", arg_types, 3, 3)?;
        Ok(DataType::Utf8)
    }

    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
        let [subject, from, to] = args.as_slice() else {
            return internal_err!("replace requires 3 arguments, got {}", args.len());
        };

        let subject = as_utf8("replace", subject)?;
        let from = as_utf8("replace", from)?;
        let to = as_utf8("replace", to)?;
        let (subject, from, to) = (
            subject.as_string::<i32>(),
            from.as_string::<i32>(),
            to.as_string::<i32>(),
        );

        let rows = subject.len().max(from.len()).max(to.len());
        let mut builder = StringBuilder::with_capacity(rows, rows * 16);

        for row in 0..rows {
            match value_at(subject, row)
                .zip(value_at(from, row))
                .zip(value_at(to, row))
                .map(|((subject, from), to)| (subject, from, to))
            {
                // An empty `from` has nothing to find, and replacing it would loop forever in some
                // implementations; leave the subject alone.
                Some((subject, from, to)) if !from.is_empty() => builder.append_value(subject.replace(from, to)),
                Some((subject, _, _)) => builder.append_value(subject),
                None => builder.append_null(),
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

/// `STRPOS(s, substring)` -- the 1-based character position, or 0 when absent.
#[derive(Debug)]
pub struct Strpos;

impl UserDefinedFunction for Strpos {
    fn name(&self) -> &str {
        "strpos"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        check_string_arity("strpos", arg_types, 2, 2)?;
        Ok(DataType::Int64)
    }

    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
        let [subject, needle] = args.as_slice() else {
            return internal_err!("strpos requires 2 arguments, got {}", args.len());
        };

        let subject = as_utf8("strpos", subject)?;
        let needle = as_utf8("strpos", needle)?;
        let (subject, needle) = (subject.as_string::<i32>(), needle.as_string::<i32>());

        let rows = subject.len().max(needle.len());
        let mut builder = Int64Builder::with_capacity(rows);

        for row in 0..rows {
            match value_at(subject, row).zip(value_at(needle, row)) {
                Some((subject, needle)) => builder.append_value(match subject.find(needle) {
                    // `find` gives a byte offset; the answer is in characters.
                    Some(offset) => subject[..offset].chars().count() as i64 + 1,
                    None => 0,
                }),
                None => builder.append_null(),
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

/// `STARTS_WITH(s, prefix)`.
#[derive(Debug)]
pub struct StartsWith;

impl UserDefinedFunction for StartsWith {
    fn name(&self) -> &str {
        "starts_with"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        check_string_arity("starts_with", arg_types, 2, 2)?;
        Ok(DataType::Boolean)
    }

    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
        let [subject, prefix] = args.as_slice() else {
            return internal_err!("starts_with requires 2 arguments, got {}", args.len());
        };

        let subject = as_utf8("starts_with", subject)?;
        let prefix = as_utf8("starts_with", prefix)?;
        let (subject, prefix) = (subject.as_string::<i32>(), prefix.as_string::<i32>());

        let rows = subject.len().max(prefix.len());
        let mut builder = BooleanBuilder::with_capacity(rows);

        for row in 0..rows {
            match value_at(subject, row).zip(value_at(prefix, row)) {
                Some((subject, prefix)) => builder.append_value(subject.starts_with(prefix)),
                None => builder.append_null(),
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

/// `REPEAT(s, n)` -- an empty string for a count of zero or less.
#[derive(Debug)]
pub struct Repeat;

impl UserDefinedFunction for Repeat {
    fn name(&self) -> &str {
        "repeat"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        check_string_arity("repeat", arg_types, 1, 2)?;
        Ok(DataType::Utf8)
    }

    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
        let [subject, count] = args.as_slice() else {
            return internal_err!("repeat requires 2 arguments, got {}", args.len());
        };

        let subject = as_utf8("repeat", subject)?;
        let subject = subject.as_string::<i32>();
        let count = cast(count.as_ref(), &DataType::Int64).map_err(|e| arrow_err!(e))?;
        let count = count.as_primitive::<Int64Type>();

        let rows = subject.len().max(count.len());
        let mut builder = StringBuilder::with_capacity(rows, rows * 16);

        for row in 0..rows {
            match value_at(subject, row).zip(int_at(count, row)) {
                Some((subject, count)) if count > 0 => {
                    // Guard the multiplication: a large count would otherwise try to allocate
                    // without bound.
                    let total = subject.len().saturating_mul(count as usize);
                    if total > MAX_REPEAT_BYTES {
                        return internal_err!(
                            "repeat would produce {total} bytes, more than the {MAX_REPEAT_BYTES} allowed"
                        );
                    }
                    builder.append_value(subject.repeat(count as usize))
                }
                Some((_, _)) => builder.append_value(""),
                None => builder.append_null(),
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

/// A repeated string beyond this is treated as a mistake rather than an allocation request.
const MAX_REPEAT_BYTES: usize = 1 << 20;

/// `LEFT(s, n)` / `RIGHT(s, n)`.
///
/// A negative count removes that many characters from the far end, as in postgres.
#[derive(Debug)]
pub struct Side {
    name: &'static str,
    from_left: bool,
}

impl Side {
    pub fn left() -> Self {
        Self {
            name: "left",
            from_left: true,
        }
    }

    pub fn right() -> Self {
        Self {
            name: "right",
            from_left: false,
        }
    }
}

impl UserDefinedFunction for Side {
    fn name(&self) -> &str {
        self.name
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        check_string_arity(self.name, arg_types, 1, 2)?;
        Ok(DataType::Utf8)
    }

    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
        let [subject, count] = args.as_slice() else {
            return internal_err!("{} requires 2 arguments, got {}", self.name, args.len());
        };

        let subject = as_utf8(self.name, subject)?;
        let subject = subject.as_string::<i32>();
        let count = cast(count.as_ref(), &DataType::Int64).map_err(|e| arrow_err!(e))?;
        let count = count.as_primitive::<Int64Type>();

        let rows = subject.len().max(count.len());
        let mut builder = StringBuilder::with_capacity(rows, subject.value_data().len());

        for row in 0..rows {
            let Some((value, count)) = value_at(subject, row).zip(int_at(count, row)) else {
                builder.append_null();
                continue;
            };

            let total = value.chars().count();
            // How many characters to keep, counted from `self.from_left`.
            let keep = if count >= 0 {
                (count as usize).min(total)
            } else {
                total.saturating_sub(count.unsigned_abs() as usize)
            };

            let taken: String = if self.from_left {
                value.chars().take(keep).collect()
            } else {
                value.chars().skip(total - keep).collect()
            };

            builder.append_value(taken);
        }

        Ok(Arc::new(builder.finish()))
    }
}
