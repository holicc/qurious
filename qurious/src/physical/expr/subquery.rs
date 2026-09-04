use crate::error::{Error, Result};
use crate::internal_err;
use arrow::array::{new_null_array, ArrayRef, RecordBatch};
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
        let schema = self.plan.schema();

        let Some(field) = schema.fields().first() else {
            return internal_err!("a scalar subquery must produce a column");
        };

        let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        match rows {
            // SQL says a scalar subquery that selects no rows is NULL. This used to index the
            // first batch unconditionally and panic, taking the process down.
            0 => Ok(new_null_array(field.data_type(), 1)),
            1 => batches
                .iter()
                .find(|batch| batch.num_rows() == 1)
                .map(|batch| batch.column(0).clone())
                .ok_or_else(|| Error::InternalError("scalar subquery row is not in any batch".to_string())),
            rows => internal_err!("a scalar subquery returned {rows} rows, but at most one is allowed"),
        }
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
