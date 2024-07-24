use crate::error::Result;
use crate::execution::session::ExecuteSession;

#[cfg(feature = "postgresql")]
use crate::server::postgresql;


pub struct Server {
    session: ExecuteSession,
    #[cfg(feature = "postgresql")]
    postgres: postgresql::PostgresqlServer,
}

impl Server {
    pub fn new() -> Result<Self> {
        // let (tx, mut rx) = mpsc::channel(1024);

        todo!()
    }
}
