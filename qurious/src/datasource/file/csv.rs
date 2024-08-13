use std::fs::File;
use std::io::Seek;
use std::sync::Arc;

use arrow::csv::reader::Format;
use arrow::csv::ReaderBuilder;

use crate::datasource::memory::MemoryTable;
use crate::error::{Error, Result};
use crate::provider::table::TableProvider;

use super::DataFilePath;

pub struct CsvReadOptions {
    pub has_header: bool,
    pub delimiter: u8,
    pub quote: Option<u8>,
    pub escape: Option<u8>,
}

impl Default for CsvReadOptions {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            quote: None,
            escape: None,
        }
    }
}

pub fn read_csv<T: DataFilePath>(path: T, options: CsvReadOptions) -> Result<Arc<dyn TableProvider>> {
    let url = path.to_url()?;

    match url.scheme() {
        "file" => {
            // FIXME this may not ok when csv file is too big to read into memory
            let mut file = File::open(url.path()).map_err(|e| Error::InternalError(e.to_string()))?;

            let mut format = Format::default()
                .with_header(options.has_header)
                .with_delimiter(options.delimiter);

            if let Some(quote) = options.quote {
                format = format.with_quote(quote);
            }
            if let Some(escape) = options.escape {
                format = format.with_escape(escape);
            }

            // max records set 2 means we only read the first 2 records to infer the schema
            // first line is header
            // second line is data to infer the data type
            let (schema, _) = format.infer_schema(&mut file, None).map_err(|e| Error::ArrowError(e))?;

            // rewind the file to the beginning because the schema inference
            file.rewind().unwrap();

            let schema = Arc::new(schema);

            ReaderBuilder::new(schema.clone())
                .with_format(format)
                .build(file)
                .and_then(|reader| reader.into_iter().collect())
                .map(|data| Arc::new(MemoryTable::new(schema, data)) as Arc<dyn TableProvider>)
                .map_err(|e| Error::ArrowError(e))
        }
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use arrow::util;

    use super::*;

    #[test]
    fn test_read_csv() {
        let options = CsvReadOptions::default();

        let source = read_csv("tests/testdata/file/case1.csv", options).unwrap();

        println!(
            "{}",
            util::pretty::pretty_format_batches(&source.scan(None, &vec![]).unwrap()).unwrap()
        );
    }
}
