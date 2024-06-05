use crate::server::postgresql;

pub struct Server {
    postgres: postgresql::PostgresqlServer,
}
