use crate::datasource::DataSource;
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::sync::Arc;

pub trait TableRegistry {
    fn register_table(&mut self, name: &str, table: Arc<dyn DataSource>) -> Result<()>;

    fn get_table_source(&self, name: &str) -> Result<Arc<dyn DataSource>>;
}

pub struct HashMapTableRegistry {
    tables: HashMap<String, Arc<dyn DataSource>>,
}

impl Default for HashMapTableRegistry {
    fn default() -> Self {
        Self { tables: HashMap::new() }
    }
}

impl HashMapTableRegistry {
    pub fn new(tables: HashMap<String, Arc<dyn DataSource>>) -> Self {
        Self { tables }
    }
}

impl TableRegistry for HashMapTableRegistry {
    fn register_table(&mut self, name: &str, table: Arc<dyn DataSource>) -> Result<()> {
        self.tables.insert(name.to_string(), table);
        Ok(())
    }

    fn get_table_source(&self, name: &str) -> Result<Arc<dyn DataSource>> {
        match self.tables.get(name) {
            Some(table) => Ok(table.clone()),
            None => Err(Error::PlanError(format!("No table named '{}'", name))),
        }
    }
}
