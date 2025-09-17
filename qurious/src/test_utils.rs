use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Array, Int32Array, Int32Builder, RecordBatch},
    datatypes::{DataType, Field, Schema, SchemaRef},
    util,
};
use sqlparser::parser::Parser;

use crate::{
    common::table_relation::TableRelation,
    datasource::memory::MemoryTable,
    logical::plan::LogicalPlan,
    optimizer::{
        rule::{OptimizerRule, RuleBaseOptimizer},
        Optimizer,
    },
    physical::plan::{PhysicalPlan, Scan},
    planner::sql::SqlQueryPlanner,
    provider::table::TableProvider,
    utils,
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
macro_rules! build_table_schema {
    ( $(($qualified_field:expr,$data_type:expr)),+$(,)?) => {
        {
            use crate::common::table_schema::TableSchema;
            use crate::common::table_relation::TableRelation;
            use arrow::datatypes::Field;
            use std::sync::Arc;

            let qualified_fields = vec![
                $(
                    {
                        let (relation, field_name) = $qualified_field;
                        let relation = if relation.is_empty() {
                            None
                        } else {
                            Some(TableRelation::from(relation))
                        };
                        let field = Arc::new(Field::new(field_name, $data_type, false));
                        (relation, field)
                    },
                )*
            ];

            Arc::new(TableSchema::try_new(qualified_fields).unwrap())
        }
    };

    ( $(($qualified_field:expr,$data_type:expr, $nullable:expr)),+$(,)? ) => {
        {
            use crate::common::table_schema::TableSchema;
            use crate::common::table_relation::TableRelation;
            use arrow::datatypes::Field;
            use std::sync::Arc;

            let qualified_fields = vec![
                $(
                    {
                        let (relation, field_name) = $qualified_field;
                        let relation = if relation.is_empty() {
                            None
                        } else {
                            Some(TableRelation::from(relation))
                        };
                        let field = Arc::new(Field::new(field_name, $data_type, $nullable));
                        (relation, field)
                    },
                )*
            ];

            Arc::new(TableSchema::try_new(qualified_fields).unwrap())
        }
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

        Arc::new(Scan::new(Arc::new(schema), Arc::new(source), None, None))
       }
    };
}

pub fn assert_after_optimizer(sql: &str, rules: Vec<Box<dyn OptimizerRule>>, expected: Vec<&str>) {
    let plan = sql_to_plan(sql);
    let rule_optimizer = RuleBaseOptimizer::with_rules(rules);
    let plan = rule_optimizer.optimize(&plan).unwrap();
    let actual = utils::format(&plan, 0);
    let actual = actual.trim().lines().collect::<Vec<_>>();

    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );
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

    Arc::new(Scan::new(Arc::new(schema), Arc::new(datasource), None, None))
}

pub fn sql_to_plan(sql: &str) -> LogicalPlan {
    let mut tables = HashMap::new();

    // Add test tables
    tables.insert(
        "users".into(),
        build_mem_datasource!(
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, false),
            ("email", DataType::Utf8, false)
        ),
    );

    tables.insert(
        "repos".into(),
        build_mem_datasource!(
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, false),
            ("owner_id", DataType::Int64, false)
        ),
    );

    tables.insert(
        "commits".into(),
        build_mem_datasource!(
            ("id", DataType::Int64, false),
            ("repo_id", DataType::Int64, false),
            ("user_id", DataType::Int64, false),
            ("time", DataType::Date32, false),
            ("message", DataType::Utf8, true)
        ),
    );

    tables.extend(create_tpch_tables());

    let stmt = Parser::new(sql).parse().unwrap();
    let udsf = HashMap::default();
    SqlQueryPlanner::create_logical_plan(stmt, tables, &udsf).unwrap()
}

pub fn create_tpch_tables() -> HashMap<TableRelation, Arc<dyn TableProvider>> {
    let mut tables = HashMap::new();

    tables.insert(
        TableRelation::from("supplier"),
        build_mem_datasource!(
            ("s_suppkey", DataType::Int64, false),
            ("s_name", DataType::Utf8, false),
            ("s_address", DataType::Utf8, false),
            ("s_nationkey", DataType::Int64, false),
            ("s_phone", DataType::Utf8, false),
            ("s_acctbal", DataType::Decimal128(15, 2), false),
            ("s_comment", DataType::Utf8, false),
            ("s_rev", DataType::Utf8, false)
        ),
    );

    tables.insert(
        TableRelation::from("part"),
        build_mem_datasource!(
            ("p_partkey", DataType::Int64, false),
            ("p_name", DataType::Utf8, false),
            ("p_mfgr", DataType::Utf8, false),
            ("p_brand", DataType::Utf8, false),
            ("p_type", DataType::Utf8, false),
            ("p_size", DataType::Int32, false),
            ("p_container", DataType::Utf8, false),
            ("p_retailprice", DataType::Decimal128(15, 2), false),
            ("p_comment", DataType::Utf8, false),
            ("p_rev", DataType::Utf8, false)
        ),
    );

    tables.insert(
        TableRelation::from("partsupp"),
        build_mem_datasource!(
            ("ps_partkey", DataType::Int64, false),
            ("ps_suppkey", DataType::Int64, false),
            ("ps_availqty", DataType::Int32, false),
            ("ps_supplycost", DataType::Decimal128(15, 2), false),
            ("ps_comment", DataType::Utf8, false),
            ("ps_rev", DataType::Utf8, false)
        ),
    );

    tables.insert(
        TableRelation::from("customer"),
        build_mem_datasource!(
            ("c_custkey", DataType::Int64, false),
            ("c_name", DataType::Utf8, false),
            ("c_address", DataType::Utf8, false),
            ("c_nationkey", DataType::Int64, false),
            ("c_phone", DataType::Utf8, false),
            ("c_acctbal", DataType::Decimal128(15, 2), false),
            ("c_mktsegment", DataType::Utf8, false),
            ("c_comment", DataType::Utf8, false),
            ("c_rev", DataType::Utf8, false)
        ),
    );

    tables.insert(
        TableRelation::from("orders"),
        build_mem_datasource!(
            ("o_orderkey", DataType::Int64, false),
            ("o_custkey", DataType::Int64, false),
            ("o_orderstatus", DataType::Utf8, false),
            ("o_totalprice", DataType::Decimal128(15, 2), false),
            ("o_orderdate", DataType::Date32, false),
            ("o_orderpriority", DataType::Utf8, false),
            ("o_clerk", DataType::Utf8, false),
            ("o_shippriority", DataType::Int32, false),
            ("o_comment", DataType::Utf8, false),
            ("o_rev", DataType::Utf8, false)
        ),
    );

    tables.insert(
        TableRelation::from("lineitem"),
        build_mem_datasource!(
            ("l_orderkey", DataType::Int64, false),
            ("l_partkey", DataType::Int64, false),
            ("l_suppkey", DataType::Int64, false),
            ("l_linenumber", DataType::Int32, false),
            ("l_quantity", DataType::Decimal128(15, 2), false),
            ("l_extendedprice", DataType::Decimal128(15, 2), false),
            ("l_discount", DataType::Decimal128(15, 2), false),
            ("l_tax", DataType::Decimal128(15, 2), false),
            ("l_returnflag", DataType::Utf8, false),
            ("l_linestatus", DataType::Utf8, false),
            ("l_shipdate", DataType::Date32, false),
            ("l_commitdate", DataType::Date32, false),
            ("l_receiptdate", DataType::Date32, false),
            ("l_shipinstruct", DataType::Utf8, false),
            ("l_shipmode", DataType::Utf8, false),
            ("l_comment", DataType::Utf8, false),
            ("l_rev", DataType::Utf8, false)
        ),
    );

    tables.insert(
        TableRelation::from("nation"),
        build_mem_datasource!(
            ("n_nationkey", DataType::Int64, false),
            ("n_name", DataType::Utf8, false),
            ("n_regionkey", DataType::Int64, false),
            ("n_comment", DataType::Utf8, false),
            ("n_rev", DataType::Utf8, false)
        ),
    );

    tables.insert(
        TableRelation::from("region"),
        build_mem_datasource!(
            ("r_regionkey", DataType::Int64, false),
            ("r_name", DataType::Utf8, false),
            ("r_comment", DataType::Utf8, false),
            ("r_rev", DataType::Utf8, false)
        ),
    );

    tables
}
