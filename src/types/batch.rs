use crate::types::{columner::ColumnVector, schema::Schema};
use std::sync::Arc;

pub type ColumnVectorRef = Arc<dyn ColumnVector>;

#[derive(Debug, Clone)]
pub struct RecordBatch {
    schema: Schema,
    columns: Vec<ColumnVectorRef>,
}

impl RecordBatch {
    pub fn new(schema: Schema, columns: Vec<ColumnVectorRef>) -> Self {
        Self { schema, columns }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn row_count(&self) -> usize {
        self.columns[0].size()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn field(&self, index: usize) -> &ColumnVectorRef {
        &self.columns[index]
    }
}
