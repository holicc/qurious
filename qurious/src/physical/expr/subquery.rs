use crate::error::Result;
use arrow::array::{ArrayRef, RecordBatch};
use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use crate::physical::plan::PhysicalPlan;

use super::PhysicalExpr;

pub struct SubQuery {
    pub plan: Arc<dyn PhysicalPlan>,
}

impl PhysicalExpr for SubQuery {
    fn evaluate(&self, _input: &RecordBatch) -> Result<ArrayRef> {
        let batches = self.plan.execute()?;
        Ok(batches[0].column(0).clone())
    }
}

impl Debug for SubQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SubQuery")
    }
}

impl Display for SubQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SubQuery")
    }
}
