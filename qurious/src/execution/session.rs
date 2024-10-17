use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::vec;

use arrow::array::RecordBatch;
use sqlparser::parser::{Parser, TableInfo};

use crate::common::table_relation::TableRelation;
use crate::datasource::memory::MemoryTable;
use crate::error::Error;
use crate::functions::{all_builtin_functions, UserDefinedFunction};
use crate::internal_err;
use crate::logical::plan::{
    CreateMemoryTable, DdlStatement, DmlOperator, DmlStatement, DropTable, Filter, LogicalPlan,
};
use crate::optimizer::Optimizer;
use crate::planner::sql::{parse_csv_options, parse_file_path, SqlQueryPlanner};
use crate::planner::QueryPlanner;
use crate::provider::catalog::CatalogProvider;
use crate::provider::schema::SchemaProvider;
use crate::provider::table::TableProvider;
use crate::utils::batch::make_count_batch;
use crate::{error::Result, planner::DefaultQueryPlanner};

use crate::execution::providers::CatalogProviderList;

use super::config::SessionConfig;
use super::providers::{DefaultTableFactory, MemoryCatalogProvider, MemorySchemaProvider};

pub struct ExecuteSession {
    config: SessionConfig,
    planner: Arc<dyn QueryPlanner>,
    table_factory: DefaultTableFactory,
    catalog_list: CatalogProviderList,
    optimizer: Optimizer,
    udfs: RwLock<HashMap<String, Arc<dyn UserDefinedFunction>>>,
}

impl ExecuteSession {
    pub fn new() -> Result<Self> {
        Self::new_with_config(SessionConfig::default())
    }

    pub fn new_with_config(config: SessionConfig) -> Result<Self> {
        let catalog_list = CatalogProviderList::default();
        let catalog: Arc<dyn CatalogProvider> = Arc::new(MemoryCatalogProvider::default());
        let udfs = RwLock::new(
            all_builtin_functions()
                .into_iter()
                .map(|udf| (udf.name().to_uppercase().to_string(), udf))
                .collect(),
        );

        catalog.register_schema(&config.default_schema, Arc::new(MemorySchemaProvider::default()))?;
        catalog_list.register_catalog(&config.default_catalog, catalog)?;

        Ok(Self {
            config,
            planner: Arc::new(DefaultQueryPlanner::default()),
            catalog_list,
            table_factory: DefaultTableFactory::new(),
            optimizer: Optimizer::new(),
            udfs,
        })
    }

    pub fn sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        // parse sql collect tables
        let mut parser = Parser::new(sql);
        let stmt = parser.parse().map_err(|e| Error::SQLParseError(e))?;
        // register tables for statement if there are any file source tables to be registered
        let relations = self.resolve_tables(parser.tables)?;
        let udfs = &self
            .udfs
            .read()
            .map_err(|e| Error::InternalError(format!("failed to get udfs: {}", e)))?;
        // create logical plan
        SqlQueryPlanner::create_logical_plan(stmt, relations, udfs)
            .and_then(|logical_plan| self.execute_logical_plan(&logical_plan))
    }

    pub fn execute_logical_plan(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>> {
        let plan = self.optimizer.optimize(plan)?;
        match &plan {
            LogicalPlan::Ddl(ddl) => self.execute_ddl(ddl),
            LogicalPlan::Dml(stmt) => self.execute_dml(stmt),
            plan => self.planner.create_physical_plan(plan)?.execute(),
        }
    }

    pub fn register_table(&self, name: &str, table_provider: Arc<dyn TableProvider>) -> Result<()> {
        let table = TableRelation::from(name);
        let schema_provider = self.find_schema_provider(&table)?;
        schema_provider
            .register_table(table.table().to_owned(), table_provider)
            .map(|_| ())
    }

    pub fn register_catalog(&self, name: &str, catalog_provider: Arc<dyn CatalogProvider>) -> Result<()> {
        self.catalog_list.register_catalog(name, catalog_provider).map(|_| ())
    }
    pub fn register_udf(&self, name: &str, udf: Arc<dyn UserDefinedFunction>) -> Result<()> {
        let mut udfs = self
            .udfs
            .write()
            .map_err(|e| Error::InternalError(format!("failed to register udf: {}", e)))?;
        udfs.insert(name.to_string(), udf);
        Ok(())
    }
}

impl ExecuteSession {
    fn execute_dml(&self, stmt: &DmlStatement) -> Result<Vec<RecordBatch>> {
        let source = self.find_table_provider(&stmt.relation)?;
        let rows_affected = match stmt.op {
            DmlOperator::Insert => self.execute_insert(source, &stmt.input),
            DmlOperator::Delete => self.execute_delete(source, &stmt.input),
            _ => internal_err!("Unsupported DML {} operation", stmt.op),
        }?;

        Ok(vec![make_count_batch(rows_affected)])
    }

    fn execute_delete(&self, source: Arc<dyn TableProvider>, input: &LogicalPlan) -> Result<u64> {
        let predicate = if let LogicalPlan::Filter(Filter { input, expr }) = input {
            Some(self.planner.create_physical_expr(&input.schema(), expr)?)
        } else {
            None
        };
        source.delete(predicate)
    }

    fn execute_insert(&self, source: Arc<dyn TableProvider>, input: &LogicalPlan) -> Result<u64> {
        let physical_plan = self.planner.create_physical_plan(input)?;
        source.insert(physical_plan)
    }
    /// Resolve tables from the table registry
    /// If the table is not found in the registry, an error is returned
    /// Inspire by Datafusion implementation, but more simpllify. We decided separate table into a normal database schema, eg: catalog.schema.table
    /// Catalog like a data source factory. For example, When we create a Postgres source we want to create multiple tables in the same connection.
    /// Default Catalog factory is `DefaultCatalog` each table will try to qualify with the catalog name if it's not provided.
    ///  eg: table name `person` will be qualified as `qurious.public.person`
    /// take a sql as a example:
    /// ```sql
    /// SELECT * FROM person a LEFT JOIN db_school.public.school b ON a.id = b.person_id
    /// ```
    /// `person` will be resolved as `qurious.public.person` and try to get the table from the default table registry.
    /// `db_school.public.school` {db_name}.{schema}.{table_name} try to get the table from the Postgres table registry.
    ///
    fn resolve_tables(&self, tables: Vec<TableInfo>) -> Result<HashMap<TableRelation, Arc<dyn TableProvider>>> {
        tables.into_iter().map(|t| self.resolve_table(t)).collect()
    }

    fn resolve_table(&self, mut table: TableInfo) -> Result<(TableRelation, Arc<dyn TableProvider>)> {
        if table.args.is_empty() {
            let relation = table.name.into();
            self.find_table_provider(&relation).map(|provider| (relation, provider))
        } else {
            let path = parse_file_path(&mut table.args)?;

            match table.name.to_lowercase().as_str() {
                "read_csv" => self
                    .table_factory
                    .create_csv_table(&path, parse_csv_options(table.args)?)
                    .map(|provider| (TableRelation::parse_file_path(&path), provider)),
                "read_json" => self
                    .table_factory
                    .create_json_table(&path)
                    .map(|provider| (TableRelation::parse_file_path(&path), provider)),
                "read_parquet" => self
                    .table_factory
                    .create_parquet_table(&path)
                    .map(|provider| (TableRelation::parse_file_path(&path), provider)),
                _ => unimplemented!("not support table function: {}", table.name),
            }
        }
    }

    fn find_table_provider(&self, table: &TableRelation) -> Result<Arc<dyn TableProvider>> {
        self.find_schema_provider(table)?
            .table(table.table())
            .ok_or(Error::InternalError(format!(
                "failed to resolve table: {}",
                table.to_quanlify_name()
            )))
    }

    fn find_schema_provider(&self, table: &TableRelation) -> Result<Arc<dyn SchemaProvider>> {
        self.catalog_list
            .catalog(table.catalog().unwrap_or(&self.config.default_catalog))
            .ok_or(Error::InternalError(format!(
                "failed to resolve catalog: {}",
                table.to_quanlify_name()
            )))?
            .schema(table.schema().unwrap_or(&self.config.default_schema))
            .ok_or(Error::PlanError(format!(
                "failed to resolve schema: {}",
                table.to_quanlify_name()
            )))
    }

    fn execute_ddl(&self, ddl: &DdlStatement) -> Result<Vec<RecordBatch>> {
        match ddl {
            DdlStatement::CreateMemoryTable(CreateMemoryTable { schema, name, input }) => {
                let table: TableRelation = name.to_ascii_lowercase().into();
                let schema_provider = self.find_schema_provider(&table)?;
                let batch = self.execute_logical_plan(input)?;

                schema_provider
                    .register_table(
                        table.table().to_owned(),
                        Arc::new(MemoryTable::try_new(schema.clone(), batch)?),
                    )
                    .map(|_| vec![])
            }
            DdlStatement::DropTable(DropTable { name, if_exists }) => {
                let table: TableRelation = name.to_ascii_lowercase().into();
                let schema_provider = self.find_schema_provider(&table)?;
                let provider = schema_provider.deregister_table(table.table())?;

                if provider.is_some() || *if_exists {
                    Ok(vec![])
                } else {
                    Err(Error::PlanError(format!(
                        "Drop table failed, table not found: {}",
                        table.to_quanlify_name()
                    )))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{build_schema, datasource::memory::MemoryTable, test_utils::assert_batch_eq};
    use arrow::{
        array::{Int32Array, StringArray},
        util::pretty::print_batches,
    };

    use super::*;

    fn execute_and_assert(sql: &str, expected: Vec<&str>) {
        let session = ExecuteSession::new().unwrap();

        let batch = session.sql(sql).unwrap();

        assert_batch_eq(&batch, expected);
    }

    #[test]
    fn test_create_table() -> Result<()> {
        let session = ExecuteSession::new()?;
        session.sql("create table t (v1 int, v2 int);")?;
        // session.sql("create table b(v1 int, v2 float);")?;
        // session.sql("create table t(v1 int not null, v2 int not null, v3 double not null)")?;

        // session.sql("create table x(a int, b int);")?;
        // session.sql("create table y(c int, d int);")?;

        session.sql("insert into t values (1, 0), ( 2, 2), (3, 15), (2, 12), (3, 9), (1, 5);")?;
        // session.sql("insert into b select v1, v2 from a;")?;
        // session.sql("INSERT INTO test VALUES (1, 1), (2, 2), (3, 3), (3, 5), (NULL, NULL);")?;
        // session.sql("select a, b, c, d from x join y on a = c")?;
        println!("++++++++++++++");
        let batch = session.sql("select v1, v2 from t order by v1 asc, v2 desc")?;

        print_batches(&batch)?;

        Ok(())
    }

    #[test]
    fn test_execute_sql() -> Result<()> {
        let session = ExecuteSession::new()?;

        let schema = Arc::new(build_schema!(
            ("id", arrow::datatypes::DataType::Int32, false),
            ("name", arrow::datatypes::DataType::Utf8, false),
        ));
        let data = vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            ],
        )?];
        let datasource = MemoryTable::try_new(schema, data.clone())?;

        session.register_table("t", Arc::new(datasource))?;

        let batch = session.sql("SELECT a.* FROM t as a")?;
        assert_eq!(data, batch);

        Ok(())
    }

    #[test]
    fn test_read_csv_sql() {
        execute_and_assert(
            "SELECT * FROM read_csv('./tests/testdata/file/case1.csv')",
            vec![
                "+----+---------------+--------------------+",
                "| id | localtion     | name               |",
                "+----+---------------+--------------------+",
                "| 1  | China BeiJing | BeiJing University |",
                "+----+---------------+--------------------+",
            ],
        );
    }

    #[test]
    fn test_read_json_sql() {
        execute_and_assert(
            "SELECT * FROM read_json('./tests/testdata/file/case1.json')",
            vec![
                "+----+-----------+",
                "| id | name      |",
                "+----+-----------+",
                "| 1  | BeiJing   |",
                "| 2  | ChengDu   |",
                "| 3  | ChongQing |",
                "+----+-----------+",
            ],
        );
    }

    #[test]
    fn test_read_parquet_sql() {
        execute_and_assert(
            "SELECT * FROM read_parquet('./tests/testdata/file/case2.parquet') limit 1",
            vec![
                "+------------+----------+--------+-------+",
                "| counter_id | currency | market | type  |",
                "+------------+----------+--------+-------+",
                "| ST/SZ/001  | HKD      | SZ     | STOCK |",
                "+------------+----------+--------+-------+",
            ],
        );
    }

    #[test]
    #[cfg(feature = "connectorx")]
    fn test_postgres() {
        use crate::datasource::connectorx::postgres::PostgresCatalogProvider;

        let session = ExecuteSession::new().unwrap();
        let catalog = PostgresCatalogProvider::try_new("postgresql://root:root@localhost:5433/qurious").unwrap();

        session.register_catalog("qurious", Arc::new(catalog)).unwrap();

        let data = session
            .sql("SELECT * FROM qurious.public.schools WHERE id = 1")
            .unwrap();

        assert_batch_eq(
            &data,
            vec![
                "+----+--------------------+",
                "| id | name               |",
                "+----+--------------------+",
                "| 1  | BeiJing University |",
                "+----+--------------------+",
            ],
        );
    }
}
