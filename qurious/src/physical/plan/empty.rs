use std::sync::Arc;

use crate::error::Result;
use crate::physical::plan::PhysicalPlan;
use arrow::array::{make_array, ArrayData, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::datatypes::SchemaRef;

pub struct EmptyRelation {
    produce_one_row: bool,
    schema: SchemaRef,
}

impl EmptyRelation {
    pub fn new(schema: SchemaRef, produce_one_row: bool) -> Self {
        Self {
            schema,
            produce_one_row,
        }
    }

    fn data(&self) -> Result<Vec<RecordBatch>> {
        Ok({
            vec![RecordBatch::try_new_with_options(
                self.schema.clone(),
                self.schema
                    .flattened_fields()
                    .iter()
                    .map(|field| Arc::new(make_array(ArrayData::new_null(field.data_type(), 1))) as ArrayRef)
                    .collect(),
                // Even if column number is empty we can generate single row.
                &RecordBatchOptions::new().with_row_count(Some(1)),
            )?]
        })
    }
}

impl PhysicalPlan for EmptyRelation {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        if self.produce_one_row {
            self.data()
        } else {
            Ok(vec![])
        }
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        None
    }
}
