use std::sync::Arc;

use super::PhysicalPlan;
use crate::error::Result;
use crate::types::batch::RecordBatch;
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
            batch.
        }
        
        
    }

    fn children(&self) -> Option<Vec<Arc<dyn PhysicalPlan>>> {
        Some(vec![self.input.clone()])
    }
}
