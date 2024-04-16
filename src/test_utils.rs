use std::sync::Arc;

use arrow::{
    array::RecordBatch,
    datatypes::{Fields, Schema},
};

use crate::datasource::{memory::MemoryDataSource, DataSource};

#[macro_export]
macro_rules! build_schema {
    ( $(($field_name:expr,$data_type:expr)),* ) => {
        Schema::new(vec![
            $(
                arrow::datatypes::Field::new($field_name, $data_type, false),
            )*
        ])
    };

    ( $(($field_name:expr,$data_type:expr, $nullable:expr)),* ) => {
        Schema::new(vec![
            $(
                arrow::datatypes::Field::new($field_name, $data_type, $nullable),
            )*
        ])
    };
}

pub(crate) fn build_mem_datasource(fields: impl Into<Fields>, data: Vec<RecordBatch>) -> Arc<dyn DataSource> {
    let schema = Schema::new(fields.into());
    Arc::new(MemoryDataSource::new(Arc::new(schema), data))
}
