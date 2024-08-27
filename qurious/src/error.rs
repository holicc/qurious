use std::{
    backtrace::{Backtrace, BacktraceStatus},
    fmt::Display,
};

use arrow::error::ArrowError;
use parquet::errors::ParquetError;

macro_rules! impl_from_error {
    ($error_type:ty) => {
        impl From<$error_type> for Error {
            fn from(value: $error_type) -> Self {
                Error::InternalError(value.to_string())
            }
        }
    };
}

#[macro_export]
macro_rules! arrow_err {
    ($ERR:expr) => {
        Error::ArrowError($ERR, Some(Error::get_back_trace()))
    };
}

impl_from_error!(std::io::Error);
impl_from_error!(ParquetError);
impl_from_error!(std::num::ParseIntError);
impl_from_error!(std::num::ParseFloatError);
impl_from_error!(std::str::ParseBoolError);

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    InternalError(String),
    ColumnNotFound(String),
    DuplicateColumn(String),
    CompareError(String),
    ComputeError(String),
    ArrowError(ArrowError, Option<String>),
    SQLParseError(sqlparser::error::Error),
    PlanError(String),
    TableNotFound(String),
}

impl Error {
    #[inline(always)]
    pub fn get_back_trace() -> String {
        let back_trace = Backtrace::capture();
        if back_trace.status() == BacktraceStatus::Captured {
            return format!("{}{}", "\n\nbacktrace: ", back_trace);
        }

        "".to_owned()
    }
}

impl std::error::Error for Error {}

impl From<ArrowError> for Error {
    fn from(e: ArrowError) -> Self {
        Error::ArrowError(e, None)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InternalError(e) => write!(f, "Internal Error: {}", e),
            Error::ColumnNotFound(e) => write!(f, "Column Not Found: {}", e),
            Error::CompareError(e) => write!(f, "Compare Error: {}", e),
            Error::ComputeError(e) => write!(f, "Compute Error: {}", e),
            Error::ArrowError(e, msg) => write!(f, "Arrow Error: {}, msg: {}", e, msg.clone().unwrap_or_default()),
            Error::SQLParseError(e) => write!(f, "SQL Parse Error: {}", e),
            Error::PlanError(e) => write!(f, "Plan Error: {}", e),
            Error::DuplicateColumn(c) => write!(f, "Duplicate column: {}", c),
            Error::TableNotFound(e) => write!(f, "Table Not Found: {}", e),
        }
    }
}
