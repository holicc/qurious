use std::fmt::Debug;
use std::sync::Arc;

use crate::error::Result;
use crate::provider::table::TableProvider;

pub trait SchemaProvider: Debug + Send + Sync {
    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>>;

    fn register_table(&self, _name: String, _table: Arc<dyn TableProvider>) -> Result<Option<Arc<dyn TableProvider>>> {
        unimplemented!("schema provider does not support registering tables")
    }

    fn deregister_table(&self, _name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        unimplemented!("schema provider does not support deregistering tables")
    }

    fn table_names(&self) -> Vec<String>;
}
