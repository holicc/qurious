use std::sync::Arc;

use super::table_relation::TableRelation;
use arrow::datatypes::SchemaRef;

pub type TableSchemaRef = Arc<TableSchema>;

pub struct TableSchema {
    pub schema: SchemaRef,
    pub qualified_name: Vec<Option<TableRelation>>,
}
