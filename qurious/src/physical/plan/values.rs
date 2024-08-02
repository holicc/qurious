use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::error::Result;
use crate::physical::expr::PhysicalExpr;
use crate::physical::plan::PhysicalPlan;

pub struct Values {
    schema: SchemaRef,
    exprs: Vec<Vec<Arc<dyn PhysicalExpr>>>,
}

impl Values {
    pub fn new(schema: SchemaRef, exprs: Vec<Vec<Arc<dyn PhysicalExpr>>>) -> Self {
        Self { schema, exprs }
    }
}

impl PhysicalPlan for Values {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        todo!()
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        None
    }
}
