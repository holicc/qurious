use std::fmt::Display;

/// A datatype
#[derive(Clone, Copy, Debug, Hash, PartialEq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
    Date,
    Timestamp,
    Decimal(Option<u8>, Option<i8>),
    Int16,
    Int64,
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Boolean => write!(f, "Boolean"),
            DataType::Integer => write!(f, "Integer"),
            DataType::Float => write!(f, "Float"),
            DataType::String => write!(f, "String"),
            DataType::Date => write!(f, "Date"),
            DataType::Timestamp => write!(f, "Timestamp"),
            DataType::Int16 => write!(f, "Int16"),
            DataType::Decimal(precision, scale) => {
                write!(f, "Decimal({:?}, {:?})", precision, scale)
            }
            DataType::Int64 => write!(f, "Int64"),
        }
    }
}

pub struct Number {}
