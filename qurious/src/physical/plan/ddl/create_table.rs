use std::sync::Arc;

use crate::common::table_relation::TableRelation;
use crate::error::Result;
use crate::physical::plan::PhysicalPlan;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

pub struct CreateTable {
    table: TableRelation,
    schema: SchemaRef,
    source: Arc<dyn PhysicalPlan>,
}

impl CreateTable {
    pub fn new(table: String, schema: SchemaRef, source: Arc<dyn PhysicalPlan>) -> Self {
        Self {
            table: table.into(),
            schema,
            source,
        }
    }
}

impl PhysicalPlan for CreateTable {
    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        todo!()
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        todo!()
    }
}
