pub mod list_file_factory;

use crate::datasource::file::csv::CsvReadOptions;
use crate::datasource::file::json::JsonReadOptions;
use crate::datasource::{file, DataSource};
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

pub trait TableRegistry: Debug + Sync + Send {
    fn register_table(&mut self, name: &str, table: Arc<dyn DataSource>) -> Result<()>;

    fn deregister_table(&mut self, name: &str) -> Result<()>;

    fn get_table_source(&self, name: &str) -> Result<Arc<dyn DataSource>>;
}

pub trait TableSourceFactory: Debug + Sync + Send {
    fn create(&self, name: &str) -> Result<Arc<dyn DataSource>>;
}


#[derive(Debug)]
pub struct DefaultTableRegistry {
    tables: HashMap<String, Arc<dyn DataSource>>,
    factory: Arc<dyn TableSourceFactory>,
}

impl Default for DefaultTableRegistry {
    fn default() -> Self {
        Self {
            tables: HashMap::new(),
            factory: Arc::new(DefaultTableSourceFactory),
        }
    }
}

impl DefaultTableRegistry {
    pub fn new(tables: HashMap<String, Arc<dyn DataSource>>) -> Self {
        Self {
            tables,
            factory: Arc::new(DefaultTableSourceFactory),
        }
    }
}

impl TableRegistry for DefaultTableRegistry {
    fn register_table(&mut self, name: &str, table: Arc<dyn DataSource>) -> Result<()> {
        self.tables.insert(name.to_string(), table);
        Ok(())
    }

    fn deregister_table(&mut self, name: &str) -> Result<()> {
        self.tables
            .remove(name)
            .ok_or_else(|| Error::PlanError(format!("No table named '{}' to deregister", name)))?;
        Ok(())
    }

    fn get_table_source(&self, name: &str) -> Result<Arc<dyn DataSource>> {
        if let Some(table) = self.tables.get(name) {
            return Ok(table.clone());
        }
        // try to create a dynamic table
        self.factory.create(name)
    }
}