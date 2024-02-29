use crate::datasource::DataSource;
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::sync::Arc;

pub trait TableRegistry {
    fn register_table(&mut self, name: &str, table: Arc<dyn DataSource>) -> Result<()>;

    fn get_table_source(&self, name: &str) -> Result<Arc<dyn DataSource>>;
}

pub struct ImmutableHashMapTableRegistry {
    tables: HashMap<String, Arc<dyn DataSource>>,
}

impl ImmutableHashMapTableRegistry {
    pub fn new(tables: HashMap<String, Arc<dyn DataSource>>) -> Self {
        Self { tables }
    }
}

impl TableRegistry for ImmutableHashMapTableRegistry {
    fn register_table(&mut self, _name: &str, _table: Arc<dyn DataSource>) -> Result<()> {
        Err(Error::InternalError(
            "Cannot register table in ImmutableHashMapTableRegistry".to_string(),
        ))
    }

    fn get_table_source(&self, name: &str) -> Result<Arc<dyn DataSource>> {
        match self.tables.get(name) {
            Some(table) => Ok(table.clone()),
            None => Err(Error::PlanError(format!("No table named '{}'", name))),
        }
    }
}
