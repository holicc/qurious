use std::fmt::Display;
use std::sync::Arc;

use super::PhysicalExpr;
use crate::error::Result;
use crate::functions::UserDefinedFunction;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use std::fmt::Debug;

pub struct Function {
    pub func: Arc<dyn UserDefinedFunction>,
    pub args: Vec<Arc<dyn PhysicalExpr>>,
}

impl Function {
    pub fn new(func: Arc<dyn UserDefinedFunction>, args: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self { func, args }
    }
}

impl PhysicalExpr for Function {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef> {
        let inputs = self
            .args
            .iter()
            .map(|arg| arg.evaluate(input))
            .collect::<Result<Vec<_>>>()?;
        self.func.eval(inputs)
    }
}

impl Display for Function {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.func.name())
    }
}

impl Debug for Function {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.func.name())
    }
}
