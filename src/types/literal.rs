use super::{datatype::DataType, scalar::ScalarValue, vector::ColumnVector};

#[derive(Debug, Clone)]
pub struct Literal {
    value: ScalarValue,
    size: usize,
}

impl Literal {
    pub fn new(value: ScalarValue, size: usize) -> Self {
        Self { value, size }
    }
}

impl ColumnVector for Literal {
    fn data_type(&self) -> DataType {
        self.value.data_type()
    }

    fn get_value(&self, _index: usize) -> ScalarValue {
        self.value.clone()
    }

    fn size(&self) -> usize {
        self.size
    }
}
