use std::sync::Arc;

use crate::datasource::db::get_record_batch;
use crate::datasource::DataSource;
use crate::error::{Error, Result};
use arrow::array::{as_string_array, RecordBatch};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use connectorx::prelude::*;

pub struct PostgresSourceOptions {
    pub filter_schemas: Option<Vec<String>>,
}

pub struct PostgresSource {
    conn: SourceConn,
    tables: Vec<PostgresTableSource>,
    config: PostgresSourceOptions,
}

impl PostgresSource {
    /// Create a Postgres connection and map each table to a PostgresTableSource
    pub fn try_new_with_config(
        url: &str,
        config: PostgresSourceOptions,
    ) -> Result<Vec<PostgresTableSource>> {
        let conn = SourceConn::try_from(url).map_err(|e| Error::InternalError(e.to_string()))?;

        // get all tables, eg: schema.table_name
        let schemas = Self::get_schema_tables(&conn, &config)?
            .into_iter()
            .map(|(table, schema)| PostgresTableSource {
                schema: Arc::new(schema),
                conn: Arc::new(conn.clone()),
                table,
            })
            .collect();
        // query select * from schema.table_name limit 1 to get schema

        Ok(schemas)
    }

    /// return the schema of all tables, eg: schema.table_name -> schema
    fn get_schema_tables(
        conn: &SourceConn,
        config: &PostgresSourceOptions,
    ) -> Result<Vec<(String, Schema)>> {
        let mut sql = String::from("SELECT schema_name, table_name FROM information_schema.schemata JOIN information_schema.tables ON schemata.schema_name = tables.table_schema");

        if let Some(filter_schemas) = &config.filter_schemas {
            let filter_schemas = filter_schemas.join("','");
            sql = format!(
                "{} WHERE schemata.schema_name IN ('{}')",
                sql, filter_schemas
            );
        }

        let batch = get_record_batch(conn, None, &sql)?;
        let schemas = as_string_array(batch.column(0))
            .iter()
            .filter_map(|a| a)
            .collect::<Vec<&str>>();
        let tables = as_string_array(batch.column(1))
            .iter()
            .filter_map(|a| a)
            .collect::<Vec<&str>>();
        // iterate over the record batches and get the table schema
        // zip the schema and table names
        let mut results = vec![];
        for (schema, table) in schemas.iter().zip(tables.iter()) {
            let sql = format!("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '{}' AND table_schema = '{}'", table, schema);
            let batch = get_record_batch(conn, None, &sql)?;

            let column_name = as_string_array(batch.column(0))
                .iter()
                .filter_map(|a| a)
                .collect::<Vec<&str>>();
            let data_type = as_string_array(batch.column(1))
                .iter()
                .filter_map(|a| a)
                .map(parse_data_type)
                .collect::<Result<Vec<DataType>>>()?;
            let is_nullable = as_string_array(batch.column(2))
                .iter()
                .filter_map(|a| a.map(parse_is_nullable))
                .collect::<Vec<bool>>();

            let fields = column_name
                .iter()
                .zip(data_type.into_iter().zip(is_nullable.into_iter()))
                .map(|(col_name, (dt, nullable))| Field::new(*col_name, dt, nullable));
            results.push((
                format!("{}.{}", schema, table),
                Schema::new(Fields::from_iter(fields)),
            ));
        }

        Ok(results)
    }

    pub fn tables(&self) -> Vec<PostgresTableSource> {
        todo!()
    }

    pub fn refresh(&mut self) -> Result<()> {
        todo!()
    }
}

fn parse_data_type(data_type: &str) -> Result<DataType> {
    match data_type.to_lowercase().as_str() {
        // Numeric types
        "smallint" | "int2" => Ok(DataType::Int16),
        "integer" | "int" | "int4" => Ok(DataType::Int32),
        "int8" | "bigint" => Ok(DataType::Int64),
        "oid" => Ok(DataType::UInt64),
        "float4" => Ok(DataType::Float32),
        "float8" | "real" => Ok(DataType::Float64),
        // Character types
        "char" | "varchar" | "jsonb" | "json" | "character varying" => Ok(DataType::Utf8),
        // Boolean type
        "bool" | "boolean" => Ok(DataType::Boolean),
        // Date/time types
        "timestamp with time zone" | "timestamp" => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        // TODO: Handle other date/time types (DATE, TIME)

        // Unsupported types
        _ => Err(Error::InternalError(format!(
            "Unsupported PostgreSQL data type: {}",
            data_type
        ))),
    }
}

fn parse_is_nullable(is_nullable: &str) -> bool {
    is_nullable == "YES"
}

#[derive(Debug)]
pub struct PostgresTableSource {
    schema: SchemaRef,
    conn: Arc<SourceConn>,
    table: String,
}

impl PostgresTableSource {
    pub fn table_name(&self) -> String {
        self.table.to_string()
    }
}

impl DataSource for PostgresTableSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, projection: Option<Vec<String>>) -> Result<Vec<RecordBatch>> {
        let sql = match projection {
            Some(p) => format!("SELECT {} FROM {}", p.join(","), self.table_name()),
            None => format!("SELECT * FROM {}", self.table_name()),
        };
        let batch = get_record_batch(&self.conn, Some(self.schema()), &sql)?;
        Ok(vec![batch])
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{Field, Fields, Schema};

    use crate::datasource::{
        db::postgres::{PostgresSource, PostgresSourceOptions},
        DataSource,
    };
    use std::{sync::Arc, vec};

    #[test]
    fn test_postgres_source() {
        let sources = PostgresSource::try_new_with_config(
            "postgres://root:root@localhost:5433/qurious",
            PostgresSourceOptions {
                filter_schemas: Some(vec!["public".to_string()]),
            },
        )
        .unwrap();

        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].table_name(), "public.schools".to_string());
        assert_eq!(
            sources[0].schema(),
            Arc::new(Schema::new(Fields::from_iter(vec![
                Field::new("id", arrow::datatypes::DataType::Int32, false),
                Field::new("name", arrow::datatypes::DataType::Utf8, true),
            ])))
        );
    }
}
