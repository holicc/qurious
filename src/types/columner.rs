use crate::types::{datatype::DataType, value::Value};

pub trait ColumnVector {
    fn data_type(&self) -> &DataType;

    fn get_value(&self, index: usize) -> Option<&Value>;

    fn size(&self) -> usize;
}
