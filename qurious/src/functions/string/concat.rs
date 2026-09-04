use arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use arrow::compute::kernels::cast::cast;
use arrow::datatypes::DataType;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::functions::UserDefinedFunction;
use crate::{arrow_err, internal_err};

/// `CONCAT(...)`.
///
/// A NULL argument contributes nothing, as in postgres, so the result is NULL only when there is
/// nothing to concatenate at all. This is deliberately unlike `||`, which propagates NULL.
#[derive(Debug)]
pub struct Concat;

impl UserDefinedFunction for Concat {
    fn name(&self) -> &str {
        "concat"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return internal_err!("concat requires at least one argument");
        }

        Ok(DataType::Utf8)
    }

    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
        if args.is_empty() {
            return internal_err!("concat requires at least one argument");
        }

        // Anything is concatenable once rendered as text, which is what postgres does too.
        let columns = args
            .iter()
            .map(|arg| cast(arg.as_ref(), &DataType::Utf8).map_err(|e| arrow_err!(e)))
            .collect::<Result<Vec<_>>>()?;

        let rows = columns.iter().map(|column| column.len()).max().unwrap_or(0);
        let mut builder = StringBuilder::with_capacity(rows, rows * 16);

        for row in 0..rows {
            let mut out = String::new();
            let mut any = false;

            for column in &columns {
                // A literal argument arrives as a single-row array while the columns are longer.
                let index = if column.len() == 1 { 0 } else { row };
                let values = column.as_string::<i32>();

                if values.is_valid(index) {
                    out.push_str(values.value(index));
                    any = true;
                }
            }

            if any {
                builder.append_value(out);
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}
