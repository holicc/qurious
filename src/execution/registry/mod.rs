use crate::datasource::DataSource;
use crate::error::Result;
use std::sync::Arc;

pub trait TableRegistry {
    fn register_table(&mut self, name: &str, table: Arc<dyn DataSource>) -> Result<()>;

    fn get_table_source(&self, name: &str) -> Result<Arc<dyn DataSource>>;
}
