use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute::kernels::boolean::{and, is_not_null, is_null, or};
use arrow::compute::kernels::cast::cast;
use arrow::compute::kernels::cmp::{gt, lt};
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::DataType;

use crate::error::{Error, Result};
use crate::functions::{common_argument_type, UserDefinedFunction};
use crate::{arrow_err, internal_err};

/// A comparison mask with its nulls read as false.
///
/// Comparing against a NULL yields NULL, and `zip` needs to be told which side to take for every
/// row, so an unknown comparison has to become a definite "keep what we have".
fn nulls_as_false(mask: BooleanArray) -> BooleanArray {
    mask.iter().map(|value| Some(value.unwrap_or(false))).collect()
}

/// `GREATEST(...)` / `LEAST(...)`.
///
/// NULL arguments are ignored, as in postgres, so the result is NULL only when every argument is.
/// sqlite's scalar `max`/`min` instead return NULL if any argument is NULL.
#[derive(Debug)]
pub struct Extremum {
    name: &'static str,
    greatest: bool,
}

impl Extremum {
    pub fn greatest() -> Self {
        Self {
            name: "greatest",
            greatest: true,
        }
    }

    pub fn least() -> Self {
        Self {
            name: "least",
            greatest: false,
        }
    }
}

impl UserDefinedFunction for Extremum {
    fn name(&self) -> &str {
        self.name
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return internal_err!("{} requires at least one argument", self.name);
        }

        common_argument_type(self.name, arg_types)
    }

    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
        let Some(first) = args.first() else {
            return internal_err!("{} requires at least one argument", self.name);
        };

        let arg_types = args.iter().map(|arg| arg.data_type().clone()).collect::<Vec<_>>();
        let target = self.return_type(&arg_types)?;

        let mut result = cast(first.as_ref(), &target).map_err(|e| arrow_err!(e))?;

        for arg in args.iter().skip(1) {
            let candidate = cast(arg.as_ref(), &target).map_err(|e| arrow_err!(e))?;

            let wins = if self.greatest {
                gt(&candidate, &result)
            } else {
                lt(&candidate, &result)
            }
            .map_err(|e| arrow_err!(e))?;

            // Take the candidate when it compares better, and also when what we have is NULL and
            // the candidate is not -- that is what "ignore NULLs" amounts to.
            let fills_a_gap = and(
                &is_null(result.as_ref()).map_err(|e| arrow_err!(e))?,
                &is_not_null(candidate.as_ref()).map_err(|e| arrow_err!(e))?,
            )
            .map_err(|e| arrow_err!(e))?;

            let take = or(&nulls_as_false(wins), &fills_a_gap).map_err(|e| arrow_err!(e))?;

            result = zip(&take, &candidate, &result).map_err(|e| arrow_err!(e))?;
        }

        Ok(result)
    }
}
