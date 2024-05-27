use std::sync::Arc;

use arrow::{
    array::{make_array, make_builder, Array, Int32Array, PrimitiveArray, RecordBatch},
    datatypes::{Fields, Int32Type, Schema, SchemaRef},
    util,
};

use crate::datasource::{memory::MemoryDataSource, DataSource};

#[macro_export]
macro_rules! build_schema {
    ( $(($field_name:expr,$data_type:expr)),+$(,)?) => {
        arrow::datatypes::Schema::new(vec![
            $(
                arrow::datatypes::Field::new($field_name, $data_type, false),
            )*
        ])
    };

    ( $(($field_name:expr,$data_type:expr, $nullable:expr)),+$(,)? ) => {
        Schema::new(vec![
            $(
                arrow::datatypes::Field::new($field_name, $data_type, $nullable),
            )*
        ])
    };
}

#[macro_export]
macro_rules! build_table_scan {
    ( $(($column: expr, $data_type: ty, $f_dy: expr, $data: expr)),+$(,)? ) => {
       {
        use crate::datasource::memory::MemoryDataSource;
        use crate::physical::plan::Scan;
        use arrow::array::{Array,RecordBatch, PrimitiveArray};
        use arrow::datatypes::*;
        use std::sync::Arc;

        let schema = Schema::new(vec![
            $(
                Field::new($column, $f_dy, false),
            )*
        ]);

        let columns = vec![
            $(
                Arc::new(PrimitiveArray::<$data_type>::from( $data )) as Arc<dyn Array>,
            )*
        ];

        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns).unwrap();

        let source = MemoryDataSource::new(Arc::new(schema.clone()), vec![batch]);

        Arc::new(Scan::new(Arc::new(schema), Arc::new(source), None))
       }
    };
}

pub(crate) fn assert_batch_eq(actual: &[RecordBatch], except: Vec<&str>) {
    let str = util::pretty::pretty_format_batches(actual).unwrap().to_string();
    let actual = str.split('\n').collect::<Vec<&str>>();

    assert_eq!(
        actual,
        except,
        "\nexcepe:\n[ \n\t{}\n ]\nactual:\n[ \n\t{} \n]\n",
        except.join("\n\t"),
        actual.join("\n\t")
    );
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
