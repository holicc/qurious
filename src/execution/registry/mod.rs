use crate::datasource::DataSource;
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

pub trait TableRegistry {
    fn register_table(&self, name: &str, table: Arc<dyn DataSource>) -> Result<()>;

    fn get_table_source(&self, name: &str) -> Result<Arc<dyn DataSource>>;
}

pub struct HashMapTableRegistry {
    tables: Rc<Mutex<HashMap<String, Arc<dyn DataSource>>>>,
}

impl Default for HashMapTableRegistry {
    fn default() -> Self {
        Self {
            tables: Rc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl HashMapTableRegistry {
    pub fn new(tables: HashMap<String, Arc<dyn DataSource>>) -> Self {
        Self {
            tables: Rc::new(Mutex::new(tables)),
        }
    }
}

impl TableRegistry for HashMapTableRegistry {
    fn register_table(&self, name: &str, table: Arc<dyn DataSource>) -> Result<()> {
        self.tables
            .lock()
            .expect("Cannot lock mutex for table registry")
            .insert(name.to_string(), table);
        Ok(())
    }

    fn get_table_source(&self, name: &str) -> Result<Arc<dyn DataSource>> {
        match self
            .tables
            .lock()
            .expect("Cannot lock mutex for table registry")
            .get(name)
        {
            Some(table) => Ok(table.clone()),
            None => Err(Error::PlanError(format!("No table named '{}'", name))),
        }
    }
}
