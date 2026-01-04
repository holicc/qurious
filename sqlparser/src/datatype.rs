use std::fmt::Display;

use crate::{
    error::{Error, Result},
    token::{Keyword, Token, TokenType},
};

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

#[derive(Clone, Copy, Debug, Hash, PartialEq)]
pub enum IntervalFields {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

impl Display for IntervalFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IntervalFields::Year => write!(f, "Year"),
            IntervalFields::Month => write!(f, "Month"),
            IntervalFields::Day => write!(f, "Day"),
            IntervalFields::Hour => write!(f, "Hour"),
            IntervalFields::Minute => write!(f, "Minute"),
            IntervalFields::Second => write!(f, "Second"),
        }
    }
}

impl TryInto<IntervalFields> for Token {
    type Error = Error;

    fn try_into(self) -> Result<IntervalFields> {
        match self.token_type {
            TokenType::Keyword(Keyword::Year) => Ok(IntervalFields::Year),
            TokenType::Keyword(Keyword::Month) => Ok(IntervalFields::Month),
            TokenType::Keyword(Keyword::Day) => Ok(IntervalFields::Day),
            TokenType::Keyword(Keyword::Hour) => Ok(IntervalFields::Hour),
            TokenType::Keyword(Keyword::Minute) => Ok(IntervalFields::Minute),
            TokenType::Keyword(Keyword::Second) => Ok(IntervalFields::Second),
            _ => Err(Error::ParserError(format!(
                "invalid interval field: {:?}",
                self.token_type
            ))),
        }
    }
}
