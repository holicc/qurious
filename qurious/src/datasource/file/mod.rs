pub mod csv;
pub mod json;
pub mod parquet;

use std::fs::{self, File};

use arrow::{csv::reader::BufReader, datatypes::SchemaRef};
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
        Err(url::ParseError::RelativeUrlWithoutBase) => fs::canonicalize(path.as_ref())
            .and_then(|absolute| Ok(Url::from_file_path(absolute).unwrap()))
            .map_err(|e| Error::InternalError(format!("file path: {}, err: {}", path.as_ref(), e.to_string()))),
        Err(e) => Err(Error::InternalError(e.to_string())),
    }
}

/// FileSource is a data source for reading data from a file
/// different file formats have different Readers
/// all readers from arrows
#[derive(Debug)]
pub struct FileSource<R> {
    schema: SchemaRef,
    reader: R,
}

impl FileSource<BufReader<File>> {
    pub fn new(schema: SchemaRef, reader: BufReader<File>) -> Self {
        Self { schema, reader }
    }
}
