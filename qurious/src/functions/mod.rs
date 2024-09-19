pub mod datetime;

use crate::error::Result;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datetime::extract::DatetimeExtract;
use std::fmt::Debug;
use std::sync::Arc;

pub trait UserDefinedFunction: Debug + Send + Sync {
    /// the name of the function
    fn name(&self) -> &str;
    /// the return type of the function
    fn return_type(&self) -> DataType;
    /// whether the function can return null
    fn is_nullable(&self) -> bool {
        true
    }
    /// evaluate the function
    fn eval(&self, args: Vec<ArrayRef>) -> Result<ArrayRef>;
}

pub fn all_builtin_functions() -> Vec<Arc<dyn UserDefinedFunction>> {
    vec![Arc::new(DatetimeExtract)]
}
