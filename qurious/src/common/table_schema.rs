use std::sync::Arc;

use crate::common::table_relation::TableRelation;
use arrow::datatypes::Schema;

pub type TableSchemaRef = Arc<TableSchema>;

pub struct TableSchema {
    schema: Schema,
    relation: TableRelation,
}
