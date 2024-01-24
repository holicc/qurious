use crate::types::datatype::DataType;

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub datatype: DataType,
}
