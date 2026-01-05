use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Schema};

use crate::datatypes::scalar::ScalarValue;
use crate::error::Result;
use crate::logical::plan::LogicalPlan;

use super::LogicalExpr;

/// SQL CASE expression.
///
/// Supports both:
/// - searched CASE: `CASE WHEN <cond> THEN <value> ... ELSE <value> END`
/// - simple CASE: `CASE <operand> WHEN <expr> THEN <value> ... ELSE <value> END`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CaseExpr {
    pub operand: Option<Box<LogicalExpr>>,
    pub when_then: Vec<(LogicalExpr, LogicalExpr)>,
    pub else_expr: Box<LogicalExpr>,
}

impl CaseExpr {
    pub fn field(&self, plan: &LogicalPlan) -> Result<FieldRef> {
        // Best-effort: use first THEN type (or ELSE) as the output type.
        // The optimizer's type coercion rule will cast branches to a common type later.
        let schema = plan.schema();
        let dt = self.data_type(&schema)?;
        Ok(Arc::new(Field::new(format!("{}", self), dt, true)))
    }

    pub fn data_type(&self, schema: &Arc<Schema>) -> Result<DataType> {
        for (_, then_expr) in &self.when_then {
            let dt = then_expr.data_type(schema)?;
            if dt != DataType::Null {
                return Ok(dt);
            }
        }
        self.else_expr.data_type(schema)
    }
}

impl Display for CaseExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CASE")?;
        if let Some(op) = &self.operand {
            write!(f, " {op}")?;
        }
        for (w, t) in &self.when_then {
            write!(f, " WHEN {w} THEN {t}")?;
        }
        // Always print ELSE for determinism (planner fills missing ELSE with NULL).
        write!(f, " ELSE {} END", self.else_expr)
    }
}

impl From<ScalarValue> for CaseExpr {
    fn from(value: ScalarValue) -> Self {
        CaseExpr {
            operand: None,
            when_then: vec![],
            else_expr: Box::new(LogicalExpr::Literal(value)),
        }
    }
}


