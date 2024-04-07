use std::sync::Arc;

use arrow::{
    array::RecordBatch,
    datatypes::{Fields, Schema},
};

use crate::datasource::{memory::MemoryDataSource, DataSource};

pub(crate) fn build_mem_datasource(
    fields: impl Into<Fields>,
    data: Vec<RecordBatch>,
) -> Arc<dyn DataSource> {
    let schema = Schema::new(fields.into());
    Arc::new(MemoryDataSource::new(Arc::new(schema), data))
}
