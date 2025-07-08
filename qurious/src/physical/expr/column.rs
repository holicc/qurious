use std::fmt::Display;

use arrow::{array::ArrayRef, record_batch::RecordBatch};

use super::PhysicalExpr;
use crate::error::{Error, Result};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Column {
    name: String,
    index: usize,
}

impl Column {
    pub fn new(name: &str, index: usize) -> Self {
        Self {
            name: name.to_owned(),
            index,
        }
    }
}

impl PhysicalExpr for Column {
    fn evaluate(&self, input: &RecordBatch) -> Result<ArrayRef> {
        let input_schema = input.schema();
        if self.index >= input_schema.fields.len() {
            return Err(Error::InternalError(format!("PhysicalExpr Column references column '{}' at index {} (zero-based) but input schema only has {} columns: {:?}",
                self.name,
                self.index, input_schema.fields.len(), input_schema.fields().iter().map(|f| f.name().clone()).collect::<Vec<String>>())));
        }

        Ok(input.column(self.index).clone())
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.name, self.index)
    }
}
