use std::fmt::Display;

/// A datatype
#[derive(Clone, Copy, Debug, Hash, PartialEq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Boolean => write!(f, "Boolean"),
            DataType::Integer => write!(f, "Integer"),
            DataType::Float => write!(f, "Float"),
            DataType::String => write!(f, "String"),
        }
    }
}
