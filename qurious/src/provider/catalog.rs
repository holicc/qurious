use std::sync::Arc;

use crate::error::Result;
use crate::provider::schema::SchemaProvider;
use std::fmt::Debug;

pub trait CatalogProvider: Debug + Send + Sync {
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>;

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        unimplemented!("Registering new schemas is not supported")
    }

    fn deregister_schema(&self, _name: &str, _cascade: bool) -> Result<Option<Arc<dyn SchemaProvider>>> {
        unimplemented!("Deregistering new schemas is not supported")
    }

    fn schema_names(&self) -> Vec<String>;
}
