use std::sync::Arc;

use crate::error::Result;
use crate::physical::plan::PhysicalPlan;
use arrow::array::{ArrayRef, NullArray, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};

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
            let n_field = self.schema.fields.len();
            vec![RecordBatch::try_new_with_options(
                Arc::new(Schema::new(
                    (0..n_field)
                        .map(|i| Field::new(format!("placeholder_{i}"), DataType::Null, true))
                        .collect::<Fields>(),
                )),
                (0..n_field)
                    .map(|_i| {
                        let ret: ArrayRef = Arc::new(NullArray::new(1));
                        ret
                    })
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
