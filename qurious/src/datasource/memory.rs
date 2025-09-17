use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use arrow::array::AsArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::arrow_err;
use crate::datatypes::scalar::ScalarValue;
use crate::error::Error;
use crate::error::Result;
use crate::physical::expr::PhysicalExpr;
use crate::physical::plan::PhysicalPlan;
use crate::provider::table::{TableProvider, TableType};
use std::fmt::{self, Debug, Formatter};

#[derive(Clone)]
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

impl Debug for MemoryTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryTable")
            .field("schema", &self.schema)
            .field("column_defaults", &self.column_defaults)
            .field("data", &"[ ... ]")
            .finish()
    }
}

impl TableProvider for MemoryTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(
        &self,
        projection: Option<Vec<String>>,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Vec<RecordBatch>> {
        self.data
            .read()
            .map_err(|e| Error::InternalError(e.to_string()))?
            .iter()
            .map(|batch| {
                let batch = if let Some(projection) = &projection {
                    let indices = projection
                        .iter()
                        .map(|name| self.schema.index_of(name).map_err(|e| arrow_err!(e)))
                        .collect::<Result<Vec<_>>>()?;

                    batch.project(&indices).map_err(|e| arrow_err!(e))?
                } else {
                    batch.clone()
                };

                if let Some(filters) = filters {
                    let mask = filters.evaluate(&batch)?;
                    filter_record_batch(&batch, &mask.as_boolean()).map_err(|e| arrow_err!(e))
                } else {
                    Ok(batch)
                }
            })
            .collect::<Result<Vec<RecordBatch>>>()
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

    fn delete(&self, filter: Option<Arc<dyn PhysicalExpr>>) -> Result<u64> {
        let mut data = self
            .data
            .write()
            .map_err(|e| Error::InternalError(format!("delete error: {}", e)))?;

        if let Some(predicate) = filter {
            let new_batch = data
                .iter()
                .map(|batch| {
                    let mask = predicate.evaluate(batch)?;
                    let mask = arrow::compute::not(mask.as_boolean())?;
                    let filtered_batch = filter_record_batch(batch, &mask)?;
                    Ok(filtered_batch)
                })
                .collect::<Result<Vec<RecordBatch>>>()?;

            data.clear();
            data.extend(new_batch);

            Ok(data.iter().map(|batch| batch.num_rows()).sum::<usize>() as u64)
        } else {
            let row_effected = data.iter().map(|batch| batch.num_rows()).sum::<usize>() as u64;
            data.clear();

            Ok(row_effected)
        }
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}
