use arrow::array::ArrayRef;
use arrow::compute::kernels::cast::cast;
use arrow::compute::kernels::cmp::eq;
use arrow::compute::kernels::nullif::nullif;
use arrow::datatypes::DataType;

use crate::error::{Error, Result};
use crate::functions::{common_argument_type, UserDefinedFunction};
use crate::{arrow_err, internal_err};

/// `NULLIF(a, b)` -- NULL when the two are equal, otherwise `a`.
#[derive(Debug)]
pub struct NullIf;

impl UserDefinedFunction for NullIf {
    fn name(&self) -> &str {
        "nullif"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return internal_err!("nullif requires 2 arguments, got {}", arg_types.len());
        }

        // The result is always the first argument or NULL, but the two still have to be
        // comparable, which `common_argument_type` is what decides.
        common_argument_type("nullif", arg_types)?;

        Ok(arg_types[0].clone())
    }

    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef> {
        let [lhs, rhs] = args.as_slice() else {
            return internal_err!("nullif requires 2 arguments, got {}", args.len());
        };

        let comparable = common_argument_type("nullif", &[lhs.data_type().clone(), rhs.data_type().clone()])?;
        let left = cast(lhs.as_ref(), &comparable).map_err(|e| arrow_err!(e))?;
        let right = cast(rhs.as_ref(), &comparable).map_err(|e| arrow_err!(e))?;

        let equal = eq(&left, &right).map_err(|e| arrow_err!(e))?;

        // Back to the first argument's own type: `nullif` returns it unchanged, not the widened one.
        let original = cast(left.as_ref(), lhs.data_type()).map_err(|e| arrow_err!(e))?;

        nullif(original.as_ref(), &equal).map_err(|e| arrow_err!(e))
    }
}
