use arrow::array::{Array, ArrayRef};
use arrow::compute::kernels::boolean::is_not_null;
use arrow::compute::kernels::cast::cast;
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::DataType;

use crate::error::{Error, Result};
use crate::functions::{common_argument_type, UserDefinedFunction};
use crate::{arrow_err, internal_err};

/// `COALESCE(a, b, ...)` -- the first argument that is not NULL, or NULL if all of them are.
#[derive(Debug)]
pub struct Coalesce;

impl UserDefinedFunction for Coalesce {
    fn name(&self) -> &str {
        "coalesce"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return internal_err!("coalesce requires at least one argument");
        }

        common_argument_type("coalesce", arg_types)
    }

    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
        let Some(first) = args.first() else {
            return internal_err!("coalesce requires at least one argument");
        };

        let arg_types = args.iter().map(|arg| arg.data_type().clone()).collect::<Vec<_>>();
        let target = self.return_type(&arg_types)?;

        // Every branch has to have the same type before they can be combined, and the caller may
        // well have passed a mix (`coalesce(int_col, 0)`).
        let mut result = cast(first.as_ref(), &target).map_err(|e| arrow_err!(e))?;

        for arg in args.iter().skip(1) {
            if result.null_count() == 0 {
                break;
            }

            let fallback = cast(arg.as_ref(), &target).map_err(|e| arrow_err!(e))?;
            let taken = is_not_null(result.as_ref()).map_err(|e| arrow_err!(e))?;

            result = zip(&taken, &result, &fallback).map_err(|e| arrow_err!(e))?;
        }

        Ok(result)
    }
}
