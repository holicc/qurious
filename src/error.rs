use std::fmt::Display;

use arrow::error::ArrowError;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    InternalError(String),
    ColumnNotFound(String),
    DuplicateColumn(String),
    CompareError(String),
    ComputeError(String),
    ArrowError(ArrowError),
    SQLParseError(sqlparser::error::Error),
    PlanError(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InternalError(e) => write!(f, "Internal Error: {}", e),
            Error::ColumnNotFound(e) => write!(f, "Column Not Found: {}", e),
            Error::CompareError(e) => write!(f, "Compare Error: {}", e),
            Error::ComputeError(e) => write!(f, "Compute Error: {}", e),
            Error::ArrowError(e) => write!(f, "Arrow Error: {}", e),
            Error::SQLParseError(e) => write!(f, "SQL Parse Error: {}", e),
            Error::PlanError(e) => write!(f, "Plan Error: {}", e),
            Error::DuplicateColumn(c) => write!(f, "Duplicate column: {}", c),
        }
    }
}
