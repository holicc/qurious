use std::sync::Arc;

use arrow2::{array::Array, datatypes::Schema, scalar::Scalar};

use crate::columner::ColumnVector;

pub struct RecordBatch {
    schema: Schema,
    columns: Vec<Arc<dyn ColumnVector<Array>>>,
}

impl<Column> RecordBatch<Column>
where
    Column: ColumnVector<>,
{
    pub fn new(schema: Schema, records: Vec<Column>) -> Self {
        Self { schema, records }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn row_count(&self) -> usize {
        self.records[0].size()
    }

    pub fn column_count(&self) -> usize {
        self.records.len()
    }

    pub fn field(&self, index: usize) -> &Column {
        &self.records[index]
    }
}
