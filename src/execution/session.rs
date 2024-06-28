use std::sync::{Arc, RwLock};

use arrow::array::RecordBatch;

use super::registry::TableRegistry;
use crate::datasource::DataSource;
use crate::error::Error;
use crate::execution::registry::HashMapTableRegistry;
use crate::logical::plan::LogicalPlan;
use crate::planner::sql::SqlQueryPlanner;
use crate::planner::QueryPlanner;
use crate::{error::Result, planner::DefaultQueryPlanner};

pub struct ExecuteSession {
    tables: Arc<RwLock<dyn TableRegistry>>,
    query_planner: Box<dyn QueryPlanner>,
}

impl Default for ExecuteSession {
    fn default() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMapTableRegistry::default())),
            query_planner: Box::new(DefaultQueryPlanner),
        }
    }
}

impl ExecuteSession {
    pub fn sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        SqlQueryPlanner::create_logical_plan(self.tables.clone(), sql)
            .and_then(|logical_plan| self.execute_logical_plan(&logical_plan))
    }

    pub fn execute_logical_plan(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>> {
        self.query_planner.create_physical_plan(plan)?.execute()
    }

    pub fn register_table(&mut self, name: &str, table: Arc<dyn DataSource>) -> Result<()> {
        self.tables
            .write()
            .map_err(|e| Error::InternalError(e.to_string()))?
            .register_table(name, table)
    }

    pub(crate) fn get_tables(&self) -> Arc<RwLock<dyn TableRegistry>> {
        self.tables.clone()
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{Field, Fields, Schema},
    };

    use crate::datasource::memory::MemoryDataSource;

    use super::*;

    #[test]
    fn test_execute_sql() -> Result<()> {
        let mut session = ExecuteSession::default();

        let schema = Arc::new(Schema::new(Fields::from_iter(vec![
            Field::new("id", arrow::datatypes::DataType::Int32, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ])));

        let data = vec![RecordBatch::try_new(
            Arc::new(Schema::new(Fields::from_iter(vec![
                Field::new("id", arrow::datatypes::DataType::Int32, false),
                Field::new("name", arrow::datatypes::DataType::Utf8, false),
            ]))),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            ],
        )
        .unwrap()];

        let datasource = MemoryDataSource::new(schema, data.clone());

        session.register_table("t", Arc::new(datasource))?;

        let batch = session.sql("SELECT a.* FROM t as a")?;

        assert_eq!(data, batch);

        Ok(())
    }
}
