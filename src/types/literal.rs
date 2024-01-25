use crate::types::{columner::ColumnVector, datatype::DataType, value::Value};

#[derive(Debug, Clone)]
pub struct LiteralColumnVector {
    value: Value,
    size: usize,
    data_type: DataType,
}

impl ColumnVector for LiteralColumnVector {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn get_value(&self, index: usize) -> Option<&Value> {
        if index < self.size {
            Some(&self.value)
        } else {
            None
        }
    }

    fn size(&self) -> usize {
        self.size
    }
}
