use crate::types::datatype::DataType;

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub datatype: DataType,
}

impl Field {
    pub fn new(name: &str, datatype: DataType) -> Self {
        Self {
            name: name.to_string(),
            datatype,
        }
    }
}