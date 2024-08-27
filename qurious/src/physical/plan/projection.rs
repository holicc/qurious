use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_columns;

use super::PhysicalPlan;
use crate::error::{Error, Result};
use crate::physical::expr::PhysicalExpr;

pub struct Projection {
    input: Arc<dyn PhysicalPlan>,
    schema: SchemaRef,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl Projection {
    pub fn new(schema: SchemaRef, input: Arc<dyn PhysicalPlan>, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self { input, schema, exprs }
    }
}

impl PhysicalPlan for Projection {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        self.input
            .execute()?
            .into_iter()
            .map(|batch| {
                let mut columns = vec![];
                for expr in &self.exprs {
                    columns.push(expr.evaluate(&batch)?);
                }
                RecordBatch::try_new(self.schema(), columns).map_err(|e| {
                    Error::ArrowError(
                        e,
                        Some(format!(
                            "physical::plan::projection.rs: Projection::execute: RecordBatch::try_new error"
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
