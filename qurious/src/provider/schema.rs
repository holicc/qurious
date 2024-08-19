use std::fmt::Debug;
use std::sync::Arc;

use crate::error::Result;
use crate::provider::table::TableProvider;

pub trait SchemaProvider: Debug + Send + Sync {
    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>>;

    fn register_table(&self, name: String, table: Arc<dyn TableProvider>) -> Result<Option<Arc<dyn TableProvider>>> {
        unimplemented!("schema provider does not support registering tables")
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        unimplemented!("schema provider does not support deregistering tables")
    }
}
