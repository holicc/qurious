use std::sync::Arc;

use crate::error::Result;
use adbc_core::{
    driver_manager::ManagedDriver,
    options::{AdbcVersion, OptionDatabase},
    Connection, Database, Driver, Statement,
};

use super::{memory::MemoryDataSource, DataSource};

pub enum AdbcDriver {
    Postgres,
}

impl AdbcDriver {
    pub fn get_dirver(&self) -> Result<ManagedDriver, adbc_core::error::Error> {
        ManagedDriver::load_dynamic_from_name("adbc_driver_postgresql", None, AdbcVersion::V110)
    }
}

pub struct PostgresOption {
    driver: AdbcDriver,
    url: String,
    user: Option<String>,
    password: Option<String>,
}

impl PostgresOption {
    pub fn url(url: &str) -> PostgresOption {
        PostgresOption {
            driver: AdbcDriver::Postgres,
            url: url.into(),
            user: None,
            password: None,
        }
    }
}

pub fn read_postgres(sql: &str, ops: PostgresOption) -> Result<Arc<dyn DataSource>> {
    let mut driver = ops.driver.get_dirver()?;
    let mut opts = vec![];
    // add url
    opts.push((OptionDatabase::Uri, ops.url.into()));
    // add user
    if let Some(user) = ops.user {
        opts.push((OptionDatabase::Username, user.into()));
    }
    // add password
    if let Some(password) = ops.password {
        opts.push((OptionDatabase::Password, password.into()));
    }
    let mut db = driver.new_database_with_opts(opts)?;
    let mut conn = db.new_connection()?;
    let mut stmt = conn.new_statement()?;
    
    stmt.set_sql_query(sql)?;

    let schema = stmt.execute_schema()?;
    stmt.execute().map(|reader| {
        let batch = reader.collect::<Result<Vec<_>, arrow::error::ArrowError>>()?;
        Ok(Arc::new(MemoryDataSource::new(Arc::new(schema), batch)) as Arc<dyn DataSource>)
    })?
}

#[cfg(test)]
mod tests {
    use crate::datasource::adbc::{read_postgres, PostgresOption};

    #[test]
    fn test_read_postgres() {
        let sql = "SELECT * FROM public.schools";
        let source = read_postgres(sql, PostgresOption::url("postgres://root:root@localhost:5433/qurious")).unwrap();

        let schema = source.schema();

        let batch = source.scan(None, &[]).unwrap();

        println!("{}", arrow::util::pretty::pretty_format_batches(&batch).unwrap());

        assert_eq!(schema.fields().len(), 2);
    }
}
