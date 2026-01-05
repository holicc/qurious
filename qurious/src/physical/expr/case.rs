use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use arrow::compute::kernels::zip::zip;
use arrow::record_batch::RecordBatch;

use crate::arrow_err;
use crate::error::{Error, Result};

use super::PhysicalExpr;

/// Physical CASE expression implemented as nested `zip(mask, truthy, falsy)`.
#[derive(Debug)]
pub struct CaseExpr {
    /// For searched CASE: each `when` must evaluate to boolean.
    pub when_then: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
    pub else_expr: Arc<dyn PhysicalExpr>,
}

impl CaseExpr {
    pub fn new(
        when_then: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
        else_expr: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self { when_then, else_expr }
    }
}

impl PhysicalExpr for CaseExpr {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef> {
        // Start with ELSE branch and fold WHENs from the end:
        // CASE WHEN c1 THEN v1 WHEN c2 THEN v2 ELSE e END
        // => if(c1, v1, if(c2, v2, e))
        let mut acc = self.else_expr.evaluate(input)?;
        for (when, then) in self.when_then.iter().rev() {
            let cond = when.evaluate(input)?;
            let cond = cond
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| Error::InternalError("CASE WHEN must be boolean".to_string()))?;
            let then_val = then.evaluate(input)?;
            acc = zip(cond, &then_val.as_ref(), &acc.as_ref()).map_err(|e| arrow_err!(e))?;
        }
        Ok(acc)
    }
}

impl Display for CaseExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CASE")?;
        for (w, t) in &self.when_then {
            write!(f, " WHEN {w} THEN {t}")?;
        }
        write!(f, " ELSE {} END", self.else_expr)
    }
}
