use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow::array::RecordBatch;
use sqlparser::parser::Parser;

use super::registry::TableRegistry;
use crate::common::table_relation::TableRelation;
use crate::datasource::DataSource;
use crate::error::Error;
use crate::execution::registry::DefaultTableRegistry;
use crate::logical::plan::LogicalPlan;
use crate::planner::sql::SqlQueryPlanner;
use crate::planner::QueryPlanner;
use crate::{error::Result, planner::DefaultQueryPlanner};

pub type TableRegistryRef = Arc<RwLock<dyn TableRegistry>>;

pub struct ExecuteSession {
    planner: Arc<dyn QueryPlanner>,
    table_registry: TableRegistryRef,
}

impl Default for ExecuteSession {
    fn default() -> Self {
        let table_registry = Arc::new(RwLock::new(DefaultTableRegistry::default()));

        Self {
            planner: Arc::new(DefaultQueryPlanner::new(table_registry.clone())),
            table_registry,
        }
    }
}

impl ExecuteSession {
    pub fn sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        // parse sql collect tables
        let mut parser = Parser::new(sql);
        let stmt = parser.parse().map_err(|e| Error::SQLParseError(e))?;
        // register tables for statement if there are any file source tables to be registered
        let relations = self.resolve_tables(parser.relations)?;
        // create logical plan
        SqlQueryPlanner::create_logical_plan(stmt, relations)
            .and_then(|logical_plan| self.execute_logical_plan(&logical_plan))
    }

    pub fn execute_logical_plan(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>> {
        // let plan = self.optimizer.optimize(plan)?;
        self.planner.create_physical_plan(&plan)?.execute()
    }

    pub fn register_table(&mut self, name: &str, table: Arc<dyn DataSource>) -> Result<()> {
        self.table_registry
            .write()
            .map_err(|e| Error::InternalError(e.to_string()))?
            .register_table(name, table)
    }

    pub(crate) fn resolve_tables(&self, tables: Vec<String>) -> Result<HashMap<TableRelation, Arc<dyn DataSource>>> {
        tables
            .into_iter()
            .map(|r| {
                let relation: TableRelation = r.into();
                self.table_registry
                    .write()
                    .map_err(|e| Error::InternalError(e.to_string()))?
                    .get_table_source(&relation.to_quanlify_name())
                    .map(|table| (relation, table))
            })
            .collect::<Result<_>>()
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
