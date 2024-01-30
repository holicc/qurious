use crate::types::datatype::DataType;
use std::fmt::Debug;

use super::scalar::ScalarValue;

#[derive(Debug, Clone)]
pub enum ColumnarValue {
    Scalar(ScalarValue),
}

impl ColumnarValue {
    fn data_type(&self) -> DataType {
        match self {
            ColumnarValue::Scalar(a) => a.data_type(),
        }
    }
}
