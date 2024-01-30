use super::columnar::ColumnarValue;
use crate::types::schema::Schema;

#[derive(Debug, Clone)]
pub struct RecordBatch {
    schema: Schema,
    row_count: usize,
    columns: Vec<ColumnarValue>,
}

impl RecordBatch {
    pub fn new(schema: Schema, columns: Vec<ColumnarValue>, row_count: usize) -> Self {
        Self {
            schema,
            columns,
            row_count,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn field(&self, index: usize) -> ColumnarValue {
        self.columns[index].clone()
    }
}
