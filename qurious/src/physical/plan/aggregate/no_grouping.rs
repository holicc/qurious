use crate::arrow_err;
use crate::error::{Error, Result};
use crate::physical::expr::AggregateExpr;
use crate::physical::plan::PhysicalPlan;
use arrow::array::RecordBatch;
use arrow::compute::concat;
use arrow::datatypes::SchemaRef;
use std::sync::Arc;

pub struct NoGroupingAggregate {
    schema: SchemaRef,
    input: Arc<dyn PhysicalPlan>,
    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
}

impl NoGroupingAggregate {
    pub(crate) fn new(schema: SchemaRef, input: Arc<dyn PhysicalPlan>, aggr_expr: Vec<Arc<dyn AggregateExpr>>) -> Self {
        Self {
            schema,
            input,
            aggr_expr,
        }
    }
}

impl PhysicalPlan for NoGroupingAggregate {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let batches = self.input.execute()?;
        let mut accums = self
            .aggr_expr
            .iter()
            .map(|expr| expr.create_accumulator())
            .collect::<Vec<_>>();
        let values = self
            .aggr_expr
            .iter()
            .map(|expr| {
                batches
                    .iter()
                    .map(|batch| expr.expression().evaluate(&batch))
                    .collect::<Result<Vec<_>>>()
                    .and_then(|array| {
                        let array_ref = array.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
                        concat(&array_ref).map_err(|e| arrow_err!(e))
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        for (accum, value) in accums.iter_mut().zip(values.iter()) {
            accum.accumluate(value)?;
        }

        let columns = accums
            .into_iter()
            .map(|mut accum| accum.evaluate().map(|v| v.to_array(1)))
            .collect::<Result<Vec<_>>>()?;

        RecordBatch::try_new(self.schema.clone(), columns)
            .map(|b| vec![b])
            .map_err(|e| arrow_err!(e))
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        None
    }
}
