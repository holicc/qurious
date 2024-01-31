use crate::types::datatype::DataType;
use std::fmt::{Debug, Display};

use super::scalar::ScalarValue;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum ColumnarValue {
    Scalar(ScalarValue),
}

impl ColumnarValue {
    pub fn data_type(&self) -> DataType {
        match self {
            ColumnarValue::Scalar(a) => a.data_type(),
        }
    }
}