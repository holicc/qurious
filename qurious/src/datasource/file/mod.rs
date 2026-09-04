pub mod csv;
pub mod json;
pub mod parquet;

use std::fs::{self};
use url::Url;

use crate::error::{Error, Result};

pub trait DataFilePath {
    fn to_url(self) -> Result<Url>;
}

impl DataFilePath for &String {
    fn to_url(self) -> Result<Url> {
        parse_path(self)
    }
}

impl DataFilePath for String {
    fn to_url(self) -> Result<Url> {
        parse_path(self)
    }
}

impl DataFilePath for &str {
    fn to_url(self) -> Result<Url> {
        parse_path(self)
    }
}

pub fn parse_path<S: AsRef<str>>(path: S) -> Result<Url> {
    match path.as_ref().parse::<Url>() {
        Ok(url) => Ok(url),
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            let absolute = fs::canonicalize(path.as_ref())
                .map_err(|e| Error::InternalError(format!("file path: {}, err: {}", path.as_ref(), e)))?;

            // `from_file_path` rejects a path that is not absolute or cannot be encoded; that is a
            // bad argument, not a reason to bring the process down.
            Url::from_file_path(&absolute)
                .map_err(|_| Error::InternalError(format!("file path is not usable as a URL: {}", absolute.display())))
        }
        Err(e) => Err(Error::InternalError(e.to_string())),
    }
}
