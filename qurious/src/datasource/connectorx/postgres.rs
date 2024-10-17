use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{as_string_array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use connectorx::prelude::{SourceConn, SourceType};
use dashmap::DashMap;
use itertools::multizip;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::common::table_relation::TableRelation;
use crate::datatypes::scalar::ScalarValue;
use crate::error::{Error, Result};
use crate::logical::expr::LogicalExpr;
use crate::provider::catalog::CatalogProvider;
use crate::provider::schema::SchemaProvider;
use crate::provider::table::TableProvider;

use super::query_batchs;

#[derive(Debug)]
pub struct PostgresCatalogProvider {
    schemas: dashmap::DashMap<String, Arc<dyn SchemaProvider>>,
}

impl PostgresCatalogProvider {
    pub fn try_new(url: &str) -> Result<Self> {
        let source = SourceConn::try_from(url).map_err(|e| Error::InternalError(e.to_string()))?;
        let config: postgres::Config = url
            .parse()
            .map_err(|e: postgres::Error| Error::InternalError(e.to_string()))?;
        match source.ty {
            SourceType::Postgres => {}
            _ => return Err(Error::InternalError("Invalid source type".to_string())),
        }
        let db_name = config
            .get_dbname()
            .ok_or(Error::InternalError("No database name".to_string()))?;
        let schemas: DashMap<String, Arc<dyn SchemaProvider>> = DashMap::new();

        for batch in query_batchs(&source, "select schema_name from information_schema.schemata")? {
            let array = as_string_array(batch.column(0));
            let values = array
                .iter()
                .filter_map(|x| x.map(|x| x.to_string()))
                .collect::<Vec<_>>();

            values
                .into_par_iter()
                .map(|schema| PostgresSchemaProvider::try_new(source.clone(), db_name.to_owned(), schema).map(Arc::new))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .for_each(|provider| {
                    schemas.insert(provider.schema.clone(), provider);
                });
        }

        Ok(Self { schemas })
    }
}

impl CatalogProvider for PostgresCatalogProvider {
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).map(|x| x.value().clone())
    }
}

#[derive(Debug)]
pub struct PostgresSchemaProvider {
    schema: String,
    tables: DashMap<String, Arc<dyn TableProvider>>,
}

impl PostgresSchemaProvider {
    pub fn try_new(source: SourceConn, db_name: String, schema: String) -> Result<Self> {
        let sql = format!(
            "select table_name from information_schema.tables where table_schema = '{}'",
            schema
        );
        let tables: DashMap<String, Arc<dyn TableProvider>> = DashMap::new();
        for batch in query_batchs(&source, &sql)? {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("Failed to downcast");
            let values = array
                .iter()
                .filter_map(|x| x.map(|x| x.to_string()))
                .collect::<Vec<_>>();

            values
                .into_par_iter()
                .map(|table| {
                    PostgresTableProvider::try_new(source.clone(), format!("{}.{}.{}", db_name, schema, table).into())
                        .map(Arc::new)
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .for_each(|table_provider| {
                    tables.insert(table_provider.table.table().to_owned(), table_provider);
                });
        }

        Ok(Self { schema, tables })
    }
}

impl SchemaProvider for PostgresSchemaProvider {
    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.get(name).map(|x| x.value().clone())
    }
}

#[derive(Debug)]
pub struct PostgresTableProvider {
    schema: SchemaRef,
    table: TableRelation,
    source: SourceConn,
    default_values: DashMap<String, ScalarValue>,
}

impl PostgresTableProvider {
    pub fn try_new(source: SourceConn, table: TableRelation) -> Result<Self> {
        let sql = format!(
            r#"
                select 
                    column_name,
                    column_default,
                    is_nullable,
                    data_type 
                from 
                    information_schema.columns 
                where table_catalog = '{}' and table_schema = '{}' and table_name = '{}'"#,
            table.catalog().ok_or(Error::InternalError("No catalog".to_string()))?,
            table.schema().ok_or(Error::InternalError("No schema".to_string()))?,
            table.table()
        );
        let default_values: DashMap<String, ScalarValue> = DashMap::new();
        let mut fields = vec![];
        for batch in query_batchs(&source, &sql)? {
            let column_names = as_string_array(batch.column(0)).into_iter();
            let column_defaults = as_string_array(batch.column(1)).into_iter();
            let column_nullables = as_string_array(batch.column(2)).into_iter();
            let column_types = as_string_array(batch.column(3)).into_iter();
            // zip the columns
            let rows = multizip((column_names, column_defaults, column_nullables, column_types)).collect::<Vec<_>>();
            for (col_name, default_val, nullable, col_type) in rows {
                let col_name = col_name.expect("Failed to get column name").to_owned();

                if let Some(default_val) = default_val {
                    default_values.insert(
                        col_name.clone(),
                        to_default_value(
                            &to_arrow_type(col_type.expect("Failed to get column type")),
                            &default_val,
                        )?,
                    );
                }

                fields.push(Field::new(
                    col_name,
                    to_arrow_type(col_type.expect("Failed to get column type")),
                    nullable.map(|v| v == "YES").unwrap_or_default(),
                ));
            }
        }

        Ok(Self {
            default_values,
            schema: Arc::new(Schema::new(fields)),
            source,
            table,
        })
    }
}

impl TableProvider for PostgresTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, projection: Option<Vec<String>>, filters: &[LogicalExpr]) -> Result<Vec<RecordBatch>> {
        let projection = projection.unwrap_or_else(|| self.schema.fields().iter().map(|x| x.name().clone()).collect());
        let mut sql = format!("select {} from {}", projection.join(","), self.table);

        if !filters.is_empty() {
            let filters = filters.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(" and ");
            sql = format!("{} where {}", sql, filters);
        }

        query_batchs(&self.source, &sql)
    }

    fn get_column_default(&self, _column: &str) -> Option<ScalarValue> {
        self.default_values.get(_column).map(|x| x.value().clone())
    }
}

fn to_arrow_type(col_type: &str) -> DataType {
    match col_type {
        "bigint" | "integer" => DataType::Int64,
        "smallint" => DataType::Int16,
        "character varying" => DataType::Utf8,
        "character" => DataType::Utf8,
        "text" => DataType::Utf8,
        "timestamp without time zone" => DataType::Timestamp(TimeUnit::Second, None),
        "timestamp with time zone" => DataType::Timestamp(TimeUnit::Second, None),
        "date" => DataType::Date32,
        "boolean" => DataType::Boolean,
        "real" => DataType::Float32,
        "double precision" => DataType::Float64,
        "numeric" => DataType::Float64,
        _ => DataType::Utf8,
    }
}

fn to_default_value(data_type: &DataType, default_value: &str) -> Result<ScalarValue> {
    match data_type {
        DataType::Int64 => Ok(ScalarValue::Int64(Some(default_value.parse()?))),
        DataType::Int32 => Ok(ScalarValue::Int32(Some(default_value.parse()?))),
        DataType::Int16 => Ok(ScalarValue::Int16(Some(default_value.parse()?))),
        DataType::Utf8 => Ok(ScalarValue::Utf8(Some(default_value.to_string()))),
        DataType::Boolean => {
            // Parse the default value as a boolean
            let boolean = default_value.parse()?;
            Ok(ScalarValue::Boolean(Some(boolean)))
        }
        DataType::Float32 => Ok(ScalarValue::Float32(Some(default_value.parse()?))),
        DataType::Float64 => Ok(ScalarValue::Float64(Some(default_value.parse()?))),
        _ => Err(Error::InternalError("Unsupported data type".to_string())),
    }
}

#[cfg(test)]
mod tests {

    use arrow::util::pretty::print_batches;

    use super::*;

    #[test]
    fn test_postgres_catalog_provider() {
        let url = "postgresql://root:root@localhost:5433/qurious";
        let catalog = PostgresCatalogProvider::try_new(url).unwrap();

        let schema = catalog.schema("public").unwrap();

        let table = schema.table("schools").unwrap();

        let batch = table.scan(None, &vec![]).unwrap();

        print_batches(&batch).unwrap();
    }
}
