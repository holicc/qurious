use std::fmt::{Debug, Display};
use std::hash::Hasher;
use std::sync::Arc;

use crate::error::Result;
use crate::functions::UserDefinedFunction;
use crate::logical::expr::LogicalExpr;
use crate::logical::plan::LogicalPlan;
use arrow::datatypes::{Field, FieldRef};

#[derive(Debug, Clone)]
pub struct Function {
    pub func: Arc<dyn UserDefinedFunction>,
    pub args: Vec<LogicalExpr>,
}

impl Function {
    pub fn field(&self, _plan: &LogicalPlan) -> Result<FieldRef> {
        let return_type = self.func.return_type();
        let field = Field::new(self.to_string(), return_type, self.func.is_nullable());
        Ok(Arc::new(field))
    }
}

impl Display for Function {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({})",
            self.func.name(),
            self.args
                .iter()
                .map(|arg| arg.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl PartialEq for Function {
    fn eq(&self, other: &Self) -> bool {
        self.func.name() == other.func.name() && self.args == other.args
    }
}

impl Eq for Function {}

impl std::hash::Hash for Function {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.func.name().hash(state);
        self.args.hash(state);
    }
}
