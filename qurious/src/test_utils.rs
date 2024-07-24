use std::sync::Arc;

use arrow::{
    array::{Array, Int32Array, RecordBatch},
    datatypes::SchemaRef,
    util,
};

#[macro_export]
macro_rules! build_mem_datasource {
    ( $(($field_name: expr,$data_type: expr, $nullable: expr)),+$(,)? ) => {
        build_mem_datasource!(
            HashMap::new(),
            [$(($field_name, $data_type, $nullable, new_empty_array(&$data_type))),+]
        )
    };

    ( $default_values: expr, [$(($field_name: expr,$data_type: expr, $nullable: expr)),+$(,)?] ) => {
        build_mem_datasource!(
            $default_values,
            [$(($field_name, $data_type, $nullable, new_empty_array(&$data_type))),+]
        )
    };

    ( $default_values: expr, [$(($field_name: expr,$data_type: expr, $nullable: expr, $array: expr)),+$(,)?] ) => {
        {
            use arrow::datatypes::*;
            use arrow::array::*;
            use std::sync::Arc;
            use crate::datasource::DataSource;
            use crate::datasource::memory::MemoryDataSource;

            let schema = Arc::new(Schema::new(vec![
                $(
                        arrow::datatypes::Field::new($field_name, $data_type, $nullable),
                )*
            ]));
            let data = vec![RecordBatch::try_new(schema.clone(), vec![
                $(
                    $array,
                )*
            ]).unwrap()];

            Arc::new(
                MemoryDataSource::new(schema, data)
                    .with_default_values($default_values)
            ) as Arc<dyn DataSource>
        }
    };
}

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
