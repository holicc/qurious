pub mod memory;

use std::fmt::Debug;

use crate::{
    error::Result,
    types::{batch::RecordBatch, schema::Schema},
};

pub trait DataSource: Debug {
    fn schema(&self) -> &Schema;

    fn scan(&self, projection: Option<Vec<String>>) -> Result<Vec<RecordBatch>>;
}
