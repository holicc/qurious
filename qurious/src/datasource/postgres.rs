use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::common::table_relation::TableRelation;
use crate::error::Result;
use crate::logical::expr::LogicalExpr;
use crate::provider::table::TableProvider;

#[derive(Debug)]
pub struct DynamicPostgresSource {
    table: TableRelation,
}

impl DynamicPostgresSource {
    pub fn try_new(table: TableRelation) -> Result<Self> {
        todo!()
    }
}

impl TableProvider for DynamicPostgresSource {
    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn scan(&self, projection: Option<Vec<String>>, filters: &[LogicalExpr]) -> crate::error::Result<Vec<RecordBatch>> {
        todo!()
    }
}
