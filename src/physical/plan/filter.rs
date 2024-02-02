use std::sync::Arc;

use arrow::array::AsArray;
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;

use super::PhysicalPlan;
use crate::error::Result;
use crate::{physical::expr::PhysicalExpr, types::schema::Schema};

pub struct Filter {
    input: Arc<dyn PhysicalPlan>,
    predicate: Arc<dyn PhysicalExpr>,
}

impl PhysicalPlan for Filter {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let batches = self.input.execute()?;

        for batch in batches {
            let new_barch = self.predicate.evaluate(&batch).and_then(|v| {
                filter_record_batch(&batch.try_into()?, v.to_array().as_boolean()).try_into()
            });
        }

        Ok(vec![])
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        Some(vec![self.input.clone()])
    }
}
