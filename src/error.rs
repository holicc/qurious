use arrow::error::ArrowError;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    InternalError(String),
    ColumnNotFound(String),
    CompareError(String),
    ComputeError(String),
    ArrowError(ArrowError),
    SQLParseError(sqlparser::error::Error),
    PlanError(String),
}
