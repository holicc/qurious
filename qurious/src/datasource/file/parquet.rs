use crate::datasource::file::DataFilePath;
use crate::datasource::memory::MemoryTable;
use crate::error::Result;
use crate::provider::table::TableProvider;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::sync::Arc;

pub fn read_parquet<T: DataFilePath>(path: T) -> Result<Arc<dyn TableProvider>> {
    let url = path.to_url()?;
    let file = File::open(url.path())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let data = builder.build()?.collect::<Result<Vec<_>, arrow::error::ArrowError>>()?;

    MemoryTable::try_new(schema, data).map(|v| Arc::new(v) as Arc<dyn TableProvider>)
}

#[cfg(test)]
mod tests {
    use crate::datasource::file::parquet::read_parquet;

    #[test]
    fn test_read_parquet() {
        let source = read_parquet("tests/testdata/file/case1.parquet").unwrap();

        println!(
            "{}",
            arrow::util::pretty::pretty_format_batches(&source.scan(None, None).unwrap()).unwrap()
        );
    }
}
