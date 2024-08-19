use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use arrow::json::reader::infer_json_schema_from_seekable;
use arrow::json::ReaderBuilder;

use crate::datasource::file::DataFilePath;
use crate::datasource::memory::MemoryTable;
use crate::error::{Error, Result};
use crate::provider::table::TableProvider;

#[derive(Default)]
pub struct JsonReadOptions {}

pub fn read_json<T: DataFilePath>(path: T) -> Result<Arc<dyn TableProvider>> {
    let url = path.to_url()?;
    let file = File::open(url.path())?;
    let mut reader = BufReader::new(file);
    let (schema, _) = infer_json_schema_from_seekable(&mut reader, None)?;

    let schema = Arc::new(schema);
    ReaderBuilder::new(schema.clone())
        .build(reader)
        .and_then(|builder| builder.into_iter().collect())
        .map_err(|e| Error::ArrowError(e))
        .and_then(|data| MemoryTable::try_new(schema, data).map(|v| Arc::new(v) as Arc<dyn TableProvider>))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::util;

    #[test]
    fn test_read_json() {
        let source = read_json("tests/testdata/file/case1.json").unwrap();

        println!(
            "{}",
            util::pretty::pretty_format_batches(&source.scan(None, &vec![]).unwrap()).unwrap()
        );
    }
}
