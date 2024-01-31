pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ColumnNotFound(String),
    CompareError(String),
    ComputeError(String),
}
