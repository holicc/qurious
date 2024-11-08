use std::sync::Arc;

use arrow::{
    array::{Array, Int32Array, Int32Builder, RecordBatch},
    datatypes::{DataType, Field, Schema, SchemaRef},
    util,
};

use crate::{
    datasource::memory::MemoryTable,
    physical::plan::{PhysicalPlan, Scan},
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
            use crate::provider::table::TableProvider;
            use crate::datasource::memory::MemoryTable;

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
                MemoryTable::try_new(schema, data).unwrap()
                    .with_default_values($default_values)
            ) as Arc<dyn TableProvider>
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
        arrow::datatypes::Schema::new(vec![
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
        use crate::datasource::memory::MemoryTable;
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

        let source = MemoryTable::try_new(Arc::new(schema.clone()), vec![batch]).unwrap();

        Arc::new(Scan::new(Arc::new(schema), Arc::new(source), None))
       }
    };
}

pub fn assert_batch_eq(actual: &[RecordBatch], except: Vec<&str>) {
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

pub fn build_record_i32(schema: SchemaRef, columns: Vec<Vec<i32>>) -> RecordBatch {
    let mut builders = (0..columns.len()).map(|_| Int32Builder::new()).collect::<Vec<_>>();

    for (i, array) in columns.into_iter().enumerate() {
        let size = array.len();
        builders[i].append_values(&array, &vec![true; size]);
    }

    let columns = builders
        .into_iter()
        .map(|mut b| Arc::new(b.finish()) as Arc<dyn Array>)
        .collect::<Vec<_>>();

    RecordBatch::try_new(schema, columns).unwrap()
}

pub fn build_table_scan_i32(fields: Vec<(&str, Vec<i32>)>) -> Arc<dyn PhysicalPlan> {
    let schema = Schema::new(
        fields
            .iter()
            .map(|(name, _)| Field::new(name.to_string(), DataType::Int32, true))
            .collect::<Vec<_>>(),
    );

    let columns = fields
        .iter()
        .map(|(_, v)| Arc::new(Int32Array::from(v.clone())) as Arc<dyn Array>)
        .collect::<Vec<_>>();

    let record_batch = RecordBatch::try_new(Arc::new(schema.clone()), columns).unwrap();

    let datasource = MemoryTable::try_new(Arc::new(schema.clone()), vec![record_batch]).unwrap();

    Arc::new(Scan::new(Arc::new(schema), Arc::new(datasource), None))
}
