pub mod db;
pub mod file;
pub mod memory;

use crate::error::Result;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use std::fmt::Debug;

pub trait DataSource: Debug {
    fn schema(&self) -> SchemaRef;

    fn scan(&self, projection: Option<Vec<String>>) -> Result<Vec<RecordBatch>>;
}
