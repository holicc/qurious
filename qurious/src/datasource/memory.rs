use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::datatypes::scalar::ScalarValue;
use crate::error::Error;
use crate::error::Result;
use crate::logical::expr::LogicalExpr;
use crate::physical::plan::PhysicalPlan;
use crate::provider::table::TableProvider;

#[derive(Clone, Debug)]
pub struct MemoryTable {
    schema: SchemaRef,
    data: Arc<RwLock<Vec<RecordBatch>>>,
    column_defaults: HashMap<String, ScalarValue>,
}

impl MemoryTable {
    pub fn try_new(schema: SchemaRef, data: Vec<RecordBatch>) -> Result<Self> {
        Ok(Self {
            schema,
            data: Arc::new(RwLock::new(data)),
            column_defaults: HashMap::new(),
        })
    }

    pub fn with_default_values(self, columns_defaults: HashMap<String, ScalarValue>) -> Self {
        Self {
            column_defaults: columns_defaults,
            ..self
        }
    }
}

impl Default for MemoryTable {
    fn default() -> Self {
        Self {
            schema: Arc::new(Schema::empty()),
            data: Arc::new(RwLock::new(vec![])),
            column_defaults: HashMap::new(),
        }
    }
}

impl TableProvider for MemoryTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, projection: Option<Vec<String>>, _filters: &[LogicalExpr]) -> Result<Vec<RecordBatch>> {
        let batches = self.data.read().map_err(|e| Error::InternalError(e.to_string()))?;

        if let Some(projection) = projection {
            let indices = projection
                .iter()
                .map(|name| self.schema.index_of(name).map_err(|e| Error::ArrowError(e)))
                .collect::<Result<Vec<_>>>()?;

            batches
                .iter()
                .map(|batch| batch.project(&indices).map_err(|e| Error::ArrowError(e)))
                .collect()
        } else {
            Ok(batches.clone())
        }
    }

    fn get_column_default(&self, column: &str) -> Option<ScalarValue> {
        self.column_defaults.get(column).map(|v| v.clone())
    }

    fn insert(&self, input: Arc<dyn PhysicalPlan>) -> Result<u64> {
        let mut batces = self.data.write().map_err(|e| Error::InternalError(e.to_string()))?;
        let mut input_batch = input.execute()?;

        batces.append(&mut input_batch);

        Ok(input_batch.iter().map(|batch| batch.num_rows()).sum::<usize>() as u64)
    }
}
