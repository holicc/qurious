use crate::{
    datasource::file::{self, csv::CsvReadOptions},
    provider::{catalog::CatalogProvider, schema::SchemaProvider, table::TableProvider},
};
use dashmap::DashMap;
use std::sync::Arc;

use crate::error::Result;

pub struct CatalogProviderList {
    catalogs: DashMap<String, Arc<dyn CatalogProvider>>,
}

impl Default for CatalogProviderList {
    fn default() -> Self {
        CatalogProviderList {
            catalogs: DashMap::default(),
        }
    }
}

impl CatalogProviderList {
    pub fn register_catalog(
        &self,
        name: &str,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Result<Option<Arc<dyn CatalogProvider>>> {
        Ok(self.catalogs.insert(name.to_owned(), catalog))
    }

    pub fn deregister_catalog(&self, name: &str) -> Result<Option<Arc<dyn CatalogProvider>>> {
        Ok(self.catalogs.remove(name).map(|(_, v)| v))
    }

    pub fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.get(name).map(|v| v.value().clone())
    }
}

#[derive(Default)]
pub struct MemoryCatalogProvider {
    schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl CatalogProvider for MemoryCatalogProvider {
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).map(|v| v.value().clone())
    }

    fn register_schema(&self, name: &str, schema: Arc<dyn SchemaProvider>) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.insert(name.to_owned(), schema))
    }

    fn deregister_schema(&self, name: &str, _cascade: bool) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.remove(name).map(|(_, v)| v))
    }
}

#[derive(Default, Debug)]
pub struct MemorySchemaProvider {
    tables: DashMap<String, Arc<dyn TableProvider>>,
}

impl SchemaProvider for MemorySchemaProvider {
    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.get(name).map(|v| v.value().clone())
    }

    fn register_table(&self, name: String, table: Arc<dyn TableProvider>) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.remove(name).map(|(_, v)| v))
    }
}

pub struct DefaultTableFactory {}

impl DefaultTableFactory {
    pub fn new() -> Self {
        DefaultTableFactory {}
    }

    pub fn create_csv_table(&self, path: &str, opts: CsvReadOptions) -> Result<Arc<dyn TableProvider>> {
        file::csv::read_csv(path, opts)
    }

    pub fn create_parquet_table(&self, path: &str) -> Result<Arc<dyn TableProvider>> {
        file::parquet::read_parquet(path)
    }

    pub fn create_json_table(&self, path: &str) -> Result<Arc<dyn TableProvider>> {
        file::json::read_json(path)
    }
}
