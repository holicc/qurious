use crate::error::Result;
use crate::execution::session::ExecuteSession;
use arrow::array::RecordBatch;
use tokio::sync::{mpsc, oneshot};

#[cfg(feature = "postgresql")]
use crate::server::postgresql;

pub(crate) type Responder<T> = oneshot::Sender<Result<T>>;

pub(crate) enum Message {
    Query {
        sql: String,
        resp: Responder<Vec<RecordBatch>>,
    },
}

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
