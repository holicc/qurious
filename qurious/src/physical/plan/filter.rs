use std::sync::Arc;

use arrow::array::AsArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::PhysicalPlan;
use crate::error::{Error, Result};
use crate::physical::expr::PhysicalExpr;

pub struct Filter {
    input: Arc<dyn PhysicalPlan>,
    predicate: Arc<dyn PhysicalExpr>,
}

impl Filter {
    pub fn new(input: Arc<dyn PhysicalPlan>, predicate: Arc<dyn PhysicalExpr>) -> Self {
        Self { input, predicate }
    }
}

impl PhysicalPlan for Filter {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        self.input
            .execute()?
            .into_iter()
            .map(|batch| {
                let ary = self.predicate.evaluate(&batch)?;
                filter_record_batch(&batch, &ary.as_boolean()).map_err(|e| {
                    Error::ArrowError(
                        e,
                        Some(format!(
                            "physical::plan::filter.rs: Filter::execute: filter_record_batch error"
                        )),
                    )
                })
            })
            .collect()
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        Some(vec![self.input.clone()])
    }
}
