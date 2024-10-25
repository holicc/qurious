use arrow::array::StringBuilder;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::datasource::memory::MemoryTable;
use crate::provider::table::TableType;
use crate::provider::{schema::SchemaProvider, table::TableProvider};

use super::providers::CatalogProviderList;

pub(crate) const INFORMATION_SCHEMA: &str = "information_schema";
pub(crate) const TABLES: &str = "tables";
pub(crate) const VIEWS: &str = "views";
pub(crate) const COLUMNS: &str = "columns";
pub(crate) const DF_SETTINGS: &str = "df_settings";
pub(crate) const SCHEMATA: &str = "schemata";

/// All information schema tables
pub const INFORMATION_SCHEMA_TABLES: &[&str] = &[TABLES, VIEWS, COLUMNS, DF_SETTINGS, SCHEMATA];

#[derive(Debug)]
pub struct InformationSchemaProvider {
    catalog_list: Arc<CatalogProviderList>,
}

impl InformationSchemaProvider {
    pub fn new(catalog_list: Arc<CatalogProviderList>) -> Self {
        Self { catalog_list }
    }
}

impl SchemaProvider for InformationSchemaProvider {
    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        match name {
            TABLES => Some(self.build_tables()),
            _ => None,
        }
    }

    fn table_names(&self) -> Vec<String> {
        INFORMATION_SCHEMA_TABLES.iter().map(|&s| s.to_string()).collect()
    }
}

impl InformationSchemaProvider {
    fn build_tables(&self) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]));
        let mut builder = TablesBuilder::new(schema.clone());

        for catalog_name in self.catalog_list.catalog_names() {
            if let Some(catalog) = self.catalog_list.catalog(&catalog_name) {
                for schema_name in catalog.schema_names() {
                    if schema_name != INFORMATION_SCHEMA {
                        if let Some(schema) = catalog.schema(&schema_name) {
                            for table_name in schema.table_names() {
                                if let Some(table) = schema.table(&table_name) {
                                    builder.append(
                                        &catalog_name,
                                        &schema_name,
                                        &table_name,
                                        &table.table_type().to_string(),
                                    );
                                }
                            }
                        }
                    }
                }
            }

            // Add information schema tables
            for &table_name in INFORMATION_SCHEMA_TABLES {
                builder.append(
                    &catalog_name,
                    INFORMATION_SCHEMA,
                    table_name,
                    &TableType::View.to_string(),
                );
            }
        }

        Arc::new(MemoryTable::try_new(schema, vec![builder.build()]).unwrap())
    }
}

struct TablesBuilder {
    schema: SchemaRef,
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    table_types: StringBuilder,
}

impl TablesBuilder {
    fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            catalog_names: StringBuilder::new(),
            schema_names: StringBuilder::new(),
            table_names: StringBuilder::new(),
            table_types: StringBuilder::new(),
        }
    }

    fn append(&mut self, catalog: &str, schema: &str, table: &str, table_type: &str) {
        self.catalog_names.append_value(catalog);
        self.schema_names.append_value(schema);
        self.table_names.append_value(table);
        self.table_types.append_value(table_type);
    }

    fn build(mut self) -> RecordBatch {
        RecordBatch::try_new(
            self.schema,
            vec![
                Arc::new(self.catalog_names.finish()),
                Arc::new(self.schema_names.finish()),
                Arc::new(self.table_names.finish()),
                Arc::new(self.table_types.finish()),
            ],
        )
        .unwrap()
    }
}
