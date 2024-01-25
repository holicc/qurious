use crate::types::{datatype::DataType, value::Value};
use std::fmt::Debug;

pub trait ColumnVector: Debug {
    fn data_type(&self) -> &DataType;

    fn get_value(&self, index: usize) -> Option<&Value>;

    fn size(&self) -> usize;
}
