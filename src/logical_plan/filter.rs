use std::fmt::Display;
use std::sync::Arc;

use crate::expr::LogicalExpr;
use crate::logical_plan::LogicalPlan;
use crate::types::schema::Schema;

pub struct Filter {
    input: Arc<dyn LogicalPlan>,
    expr: Box<dyn LogicalExpr>,
}

impl LogicalPlan for Filter {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn children(&self) -> Option<Vec<&dyn LogicalPlan>> {
        Some(vec![&*self.input])
    }
}

impl Display for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Filter: {}", self.expr)
    }
}
