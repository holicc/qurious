pub mod csv;
pub mod json;
pub mod memory;
pub mod parquet;

use std::fmt::Debug;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

use crate::error::Result;

pub trait DataSource: Debug {
    fn schema(&self) -> SchemaRef;

    fn scan(&self, projection: Option<Vec<String>>) -> Result<Vec<RecordBatch>>;
}
