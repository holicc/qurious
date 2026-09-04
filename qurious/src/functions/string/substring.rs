use arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Int64Type};

use crate::error::{Error, Result};
use crate::functions::UserDefinedFunction;
use crate::{arrow_err, internal_err};

/// SQL `SUBSTRING(<string> FROM <start> [FOR <length>])`.
///
/// Follows the SQL standard rather than sqlite's `substr`:
/// - positions are 1-based;
/// - `length` counts from `start` even when `start` is below 1, so
///   `SUBSTRING('abcdef' FROM -1 FOR 4)` covers positions -1..2 and yields `ab`;
/// - a negative `start` never means "from the end of the string";
/// - a NULL in any argument yields NULL, and a negative `length` is an error.
///
/// Positions count characters, not bytes, so multi-byte input is handled correctly.
#[derive(Debug)]
pub struct Substring;

impl UserDefinedFunction for Substring {
    fn name(&self) -> &str {
        "substring"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !matches!(arg_types.len(), 2 | 3) {
            return Err(Error::InvalidArgumentError(format!(
                "substring requires 2 or 3 arguments, got {}",
                arg_types.len()
            )));
        }

        Ok(DataType::Utf8)
    }

    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
        if !matches!(args.len(), 2 | 3) {
            return Err(Error::InvalidArgumentError(format!(
                "substring requires 2 or 3 arguments, got {}",
                args.len()
            )));
        }

        let values = match args[0].data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                cast(args[0].as_ref(), &DataType::Utf8).map_err(|e| arrow_err!(e))?
            }
            other => return internal_err!("substring expects a string as its first argument, got {other}"),
        };
        let values = values.as_string::<i32>();

        let starts = cast(args[1].as_ref(), &DataType::Int64).map_err(|e| arrow_err!(e))?;
        let starts = starts.as_primitive::<Int64Type>();

        let lengths = args
            .get(2)
            .map(|arg| cast(arg.as_ref(), &DataType::Int64).map_err(|e| arrow_err!(e)))
            .transpose()?;
        let lengths = lengths.as_ref().map(|arg| arg.as_primitive::<Int64Type>());

        let mut builder = StringBuilder::with_capacity(values.len(), values.value_data().len());

        for row in 0..values.len() {
            let length_is_null = lengths.is_some_and(|lengths| lengths.is_null(row));
            if values.is_null(row) || starts.is_null(row) || length_is_null {
                builder.append_null();
                continue;
            }

            let start = starts.value(row);
            // Exclusive 1-based end position, or None for "to the end of the string".
            let end = match lengths.map(|lengths| lengths.value(row)) {
                Some(length) if length < 0 => {
                    return Err(Error::InvalidArgumentError(format!(
                        "negative substring length not allowed: {length}"
                    )))
                }
                Some(length) => Some(start.saturating_add(length)),
                None => None,
            };
            let first = start.max(1);

            let mut out = String::new();
            for (offset, ch) in values.value(row).chars().enumerate() {
                let position = offset as i64 + 1;
                if position < first {
                    continue;
                }
                if end.is_some_and(|end| position >= end) {
                    break;
                }
                out.push(ch);
            }
            builder.append_value(out);
        }

        Ok(std::sync::Arc::new(builder.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{new_empty_array, Int64Array, StringArray};
    use std::sync::Arc;

    fn eval(
        input: Vec<Option<&str>>,
        starts: Vec<Option<i64>>,
        lengths: Option<Vec<Option<i64>>>,
    ) -> Vec<Option<String>> {
        let mut args: Vec<ArrayRef> = vec![Arc::new(StringArray::from(input)), Arc::new(Int64Array::from(starts))];
        if let Some(lengths) = lengths {
            args.push(Arc::new(Int64Array::from(lengths)));
        }

        let out = Substring.eval(args).unwrap();
        let out = out.as_string::<i32>();
        (0..out.len())
            .map(|i| out.is_valid(i).then(|| out.value(i).to_owned()))
            .collect()
    }

    fn one(s: &str, start: i64, length: Option<i64>) -> String {
        eval(vec![Some(s)], vec![Some(start)], length.map(|l| vec![Some(l)]))
            .remove(0)
            .unwrap()
    }

    #[test]
    fn follows_sql_standard_positions() {
        assert_eq!(one("abcdef", 2, Some(3)), "bcd");
        assert_eq!(one("abcdef", 1, Some(2)), "ab");
        assert_eq!(one("abcdef", 2, None), "bcdef");
        // `length` counts from `start`, so positions before 1 consume part of it.
        assert_eq!(one("abcdef", 0, Some(2)), "a");
        assert_eq!(one("abcdef", -1, Some(4)), "ab");
        // a negative start is not "from the end", unlike sqlite's substr
        assert_eq!(one("abcdef", -10, Some(3)), "");
        // running past the end simply truncates
        assert_eq!(one("abcdef", 4, Some(10)), "def");
        assert_eq!(one("abcdef", 10, Some(2)), "");
        assert_eq!(one("abcdef", 1, Some(0)), "");
    }

    #[test]
    fn counts_characters_not_bytes() {
        assert_eq!(one("中文abc", 1, Some(2)), "中文");
        assert_eq!(one("中文abc", 3, Some(3)), "abc");
    }

    #[test]
    fn propagates_nulls_from_every_argument() {
        assert_eq!(eval(vec![None], vec![Some(1)], Some(vec![Some(2)])), vec![None]);
        assert_eq!(eval(vec![Some("abc")], vec![None], Some(vec![Some(2)])), vec![None]);
        assert_eq!(eval(vec![Some("abc")], vec![Some(1)], Some(vec![None])), vec![None]);
    }

    #[test]
    fn accepts_per_row_start_and_length() {
        assert_eq!(
            eval(
                vec![Some("abcdef"), Some("abcdef"), Some("abcdef")],
                vec![Some(1), Some(2), Some(3)],
                Some(vec![Some(1), Some(2), Some(3)]),
            ),
            vec![Some("a".to_owned()), Some("bc".to_owned()), Some("cde".to_owned())]
        );
    }

    #[test]
    fn rejects_negative_length() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some("abc")])),
            Arc::new(Int64Array::from(vec![Some(1)])),
            Arc::new(Int64Array::from(vec![Some(-1)])),
        ];
        let err = Substring.eval(args).unwrap_err().to_string();
        assert!(err.contains("negative substring length"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_wrong_arity_and_non_string_input() {
        let single: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![Some("abc")]))];
        assert!(Substring
            .eval(single)
            .unwrap_err()
            .to_string()
            .contains("2 or 3 arguments"));

        let non_string: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(1)])),
            Arc::new(Int64Array::from(vec![Some(1)])),
        ];
        assert!(Substring.eval(non_string).unwrap_err().to_string().contains("string"));
    }

    #[test]
    fn handles_empty_batches() {
        // Projections can be evaluated on empty batches (e.g. after a filter matches nothing).
        let args: Vec<ArrayRef> = vec![
            new_empty_array(&DataType::Utf8),
            new_empty_array(&DataType::Int64),
            new_empty_array(&DataType::Int64),
        ];
        let out = Substring.eval(args).unwrap();
        assert_eq!(out.len(), 0);
        assert_eq!(out.data_type(), &DataType::Utf8);
    }
}
