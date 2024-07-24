use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::datatypes::scalar::ScalarValue;
use crate::error::Error;
use crate::error::Result;
use crate::logical::expr::LogicalExpr;

use super::DataSource;

#[derive(Clone, Debug)]
pub struct MemoryDataSource {
    schema: SchemaRef,
    data: Vec<RecordBatch>,
    column_defaults: HashMap<String, ScalarValue>,
}

impl MemoryDataSource {
    pub fn new(schema: SchemaRef, data: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            data,
            column_defaults: HashMap::new(),
        }
    }

    pub fn with_default_values(self, columns_defaults: HashMap<String, ScalarValue>) -> Self {
        Self {
            column_defaults: columns_defaults,
            ..self
        }
    }
}

impl Default for MemoryDataSource {
    fn default() -> Self {
        Self {
            schema: Arc::new(Schema::empty()),
            data: vec![],
            column_defaults: HashMap::new(),
        }
    }
}

impl DataSource for MemoryDataSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, projection: Option<Vec<String>>, _filters: &[LogicalExpr]) -> Result<Vec<RecordBatch>> {
        if let Some(projection) = projection {
            let mut r = vec![];
            for p in projection {
                let index = self
                    .schema
                    .fields
                    .iter()
                    .position(|f| f.name() == &p)
                    .ok_or(Error::ColumnNotFound(p))?;
                r.push(self.data[index].clone());
            }
            Ok(r)
        } else {
            Ok(self.data.clone())
        }
    }

    fn get_column_default(&self, column: &str) -> Option<&ScalarValue> {
        self.column_defaults.get(column)
    }
}
