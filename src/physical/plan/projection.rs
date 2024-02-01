use std::sync::Arc;

use super::PhysicalPlan;
use crate::error::Result;
use crate::types::batch::RecordBatch;
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
                        .map(|columns| {
                            RecordBatch::new(self.schema.clone(), columns, batch.row_count())
                        })
                })
                .collect()
        })
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        Some(vec![self.input.clone()])
    }
}
