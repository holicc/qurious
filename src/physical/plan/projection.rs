use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use super::PhysicalPlan;
use crate::error::Result;
use crate::types::columnar::ColumnarValue;
use crate::{physical::expr::PhysicalExpr, types::schema::Schema};

pub struct Projection {
    input: Arc<dyn PhysicalPlan>,
    schema: Schema,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl PhysicalPlan for Projection {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        self.input.execute().and_then(|batches| {
            batches
                .into_iter()
                .map(|batch| {
                    self.exprs
                        .iter()
                        .map(|expr| expr.evaluate(&batch))
                        .collect::<Result<Vec<ColumnarValue>>>()
                        .map(|columns| RecordBatch::try_new(schema, columns))
                })
                .collect()
        })
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        Some(vec![self.input.clone()])
    }
}
