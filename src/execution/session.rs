use std::rc::Rc;
use std::sync::Arc;

use arrow::array::RecordBatch;

use crate::datasource::DataSource;
use crate::planner::sql::SqlQueryPlanner;
use crate::planner::QueryPlanner;
use crate::{error::Result, planner::DefaultQueryPlanner};

use crate::execution::registry::HashMapTableRegistry;

use super::registry::TableRegistry;

pub struct ExecuteSession {
    tables: Rc<dyn TableRegistry>,
    query_planner: Box<dyn QueryPlanner>,
}

impl Default for ExecuteSession {
    fn default() -> Self {
        Self {
            tables: Rc::new(HashMapTableRegistry::default()),
            query_planner: Box::new(DefaultQueryPlanner),
        }
    }
}

impl ExecuteSession {
    pub fn sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let sql_planner = SqlQueryPlanner::new(self.tables.clone());
        let plan = sql_planner.create_logical_plan(sql)?;
        let plan = self.query_planner.create_physical_plan(&plan)?;

        plan.execute()
    }

    pub fn register_table(&mut self, name: &str, table: Arc<dyn DataSource>) -> Result<()> {
        self.tables.register_table(name, table)
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
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            ],
        )
        .unwrap()];

        let datasource = MemoryDataSource::new(schema, data.clone());

        session.register_table("t", Arc::new(datasource))?;

        let batch = session.sql("SELECT * FROM t")?;

        assert_eq!(data, batch);

        Ok(())
    }
}
