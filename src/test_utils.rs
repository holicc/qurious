use std::sync::Arc;

use arrow::{
    array::{Array, Int32Array, RecordBatch},
    datatypes::{Fields, Schema, SchemaRef},
    util,
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

pub(crate) fn assert_batch_eq(actual: &[RecordBatch], except: Vec<&str>) {
    let str = util::pretty::pretty_format_batches(actual).unwrap().to_string();
    let actual = str.split('\n').collect::<Vec<&str>>();

    assert_eq!(actual, except, "\nexcepe:\n[ \n\t{}\n ]\nactual:\n[ \n\t{} \n]\n", except.join("\n\t"), actual.join("\n\t"));
}

pub(crate) fn build_mem_datasource(fields: impl Into<Fields>, data: Vec<RecordBatch>) -> Arc<dyn DataSource> {
    let schema = Schema::new(fields.into());
    Arc::new(MemoryDataSource::new(Arc::new(schema), data))
}

pub(crate) fn build_record_i32(schema: SchemaRef, ary: Vec<Vec<i32>>) -> Vec<RecordBatch> {
    ary.into_iter()
        .map(|v| {
            let columns = v
                .into_iter()
                .map(|v| Arc::new(Int32Array::from(vec![v])) as Arc<dyn Array>)
                .collect::<Vec<_>>();
            RecordBatch::try_new(schema.clone(), columns).unwrap()
        })
        .collect()
}
