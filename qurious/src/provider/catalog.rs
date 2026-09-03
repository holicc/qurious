use std::sync::Arc;

use crate::error::{Error, Result};
use crate::internal_err;
use crate::provider::schema::SchemaProvider;
use std::fmt::Debug;

pub trait CatalogProvider: Debug + Send + Sync {
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>;

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        internal_err!("this catalog provider does not support registering schemas")
    }

    fn deregister_schema(&self, _name: &str, _cascade: bool) -> Result<Option<Arc<dyn SchemaProvider>>> {
        internal_err!("this catalog provider does not support deregistering schemas")
    }

    fn schema_names(&self) -> Vec<String>;
}
