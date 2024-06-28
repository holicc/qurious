use crate::error::Result;
use crate::execution::session::ExecuteSession;
use std::{net::SocketAddr, sync::Arc};

use super::handler::HandlerFactory;
use super::PostgresqlHandler;

pub struct PostgresqlServer {
    session: Arc<ExecuteSession>,
    addr: SocketAddr,
}

impl PostgresqlServer {
    pub fn try_new(session: Arc<ExecuteSession>, svr_addr: SocketAddr) -> Result<Self> {
        Ok(PostgresqlServer {
            session,
            addr: svr_addr,
        })
    }
}

impl PostgresqlServer {
    pub async fn start(&self) -> Result<()> {
        // tokio::spawn();
        Self::listen(self.session.clone(), self.addr).await;
        Ok(())
    }

    pub fn shutdown(&self) -> Result<()> {
        todo!("Implement PostgresqlServer::shutdown()")
    }

    async fn listen(session: Arc<ExecuteSession>, addr: SocketAddr) {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .unwrap_or_else(|e| panic!("PostgreSQL Server bind fail. err: {}", e));

        let processor = Arc::new(HandlerFactory(Arc::new(PostgresqlHandler { session })));

        loop {
            tokio::select! {
                peer = listener.accept() => {
                    match peer {
                        Ok((socket, _)) => {
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
    use std::str::FromStr;

    use crate::execution::session::ExecuteSession;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_postgresql_server() {
        let addr = SocketAddr::from_str("0.0.0.0:5434").unwrap();
        let session = Arc::new(ExecuteSession::default());
        let server = PostgresqlServer::try_new(session, addr).unwrap();

        server.start().await.unwrap();
    }
}
