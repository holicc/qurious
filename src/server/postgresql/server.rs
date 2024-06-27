use crate::error::{Error, Result};
use crate::execution::session::ExecuteSession;
use crate::server::server::Message;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::mpsc::{self, Sender};

use super::handler::HandlerFactory;
use super::PostgresqlHandler;

pub struct PostgresqlServer {
    session: Arc<ExecuteSession>,
    tx: Sender<Message>,
    addr: SocketAddr,
}

impl PostgresqlServer {
    pub fn try_new(session: Arc<ExecuteSession>, tx: Sender<Message>, svr_addr: SocketAddr) -> Result<Self> {
        Ok(PostgresqlServer {
            session,
            tx,
            addr: svr_addr,
        })
    }

    // async fn connect_pg_backend(url: &str) -> Result<Client> {
    //     let (cli, connection) = tokio_postgres::connect(url, NoTls)
    //         .await
    //         .map_err(|e| Error::InternalError(e.to_string()))?;

    //     // The connection object performs the actual communication with the database,
    //     // so spawn it off to run on its own.
    //     tokio::spawn(async move {
    //         if let Err(e) = connection.await {
    //             eprintln!("connection error: {}", e);
    //         }
    //     });

    //     Ok(cli)
    // }
}

impl PostgresqlServer {
    pub async fn start(&self) -> Result<()> {
        // tokio::spawn();
        Self::listen(self.session.clone(), self.tx.clone(), self.addr).await;
        Ok(())
    }

    pub fn shutdown(&self) -> Result<()> {
        todo!("Implement PostgresqlServer::shutdown()")
    }

    async fn listen(session: Arc<ExecuteSession>, tx: mpsc::Sender<Message>, addr: SocketAddr) {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .unwrap_or_else(|e| panic!("PostgreSQL Server bind fail. err: {}", e));

        let processor = Arc::new(HandlerFactory(Arc::new(PostgresqlHandler { session })));

        loop {
            tokio::select! {
                peer = listener.accept() => {
                    match peer {
                        Ok((socket, _)) => {
                            println!("Accept new connection from {:?}", socket.peer_addr().unwrap());
                            let p_ref = processor.clone();

                            tokio::spawn(pgwire::tokio::process_socket(
                                socket,
                                None,
                                p_ref,
                            ));
                        }
                        Err(e) => {
                            println!("PostgreSQL Server accept new connection fail. err: {}", e);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::execution::session::ExecuteSession;

    use super::*;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use tokio::select;
    use tokio::sync::mpsc;

    fn mock_session() -> mpsc::Sender<Message> {
        let session = ExecuteSession::default();
        let (tx, mut rx) = mpsc::channel(10);

        tokio::spawn(async move {
            loop {
                select! {
                    Some(msg) = rx.recv() => {
                        match msg {
                            Message::Query { sql, resp } => {
                                println!("======> {sql}");

                                resp.send(session.sql(&sql)).unwrap();
                            }
                        }
                    }
                }
            }
        });

        tx
    }

    // #[tokio::test(flavor = "multi_thread")]
    // async fn test_postgresql_server() {
    //     let addr = SocketAddr::from_str("0.0.0.0:5434").unwrap();
    //     let tx = mock_session();
    //     let server = PostgresqlServer::try_new(tx, addr).unwrap();

    //     server.start().await.unwrap();
    // }
}
