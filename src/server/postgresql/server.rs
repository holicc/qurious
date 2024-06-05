use crate::error::Result;
use log::error;
use pgwire::api::MakeHandler;
use pgwire::api::{auth::noop::NoopStartupHandler, query::PlaceholderExtendedQueryHandler, StatelessMakeHandler};
use std::{net::SocketAddr, sync::Arc};

use super::PostgresqlHandler;

pub struct PostgresqlServer {
    addr: SocketAddr,
}

impl PostgresqlServer {
    pub fn new(addr: SocketAddr) -> Self {
        PostgresqlServer { addr }
    }
}

impl PostgresqlServer {
    pub async fn start(&self) -> Result<()> {
        tokio::spawn(Self::listen(self.addr));

        Ok(())
    }

    pub fn shutdown(&self) -> Result<()> {
        todo!("Implement PostgresqlServer::shutdown()")
    }

    async fn listen(addr: SocketAddr) {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .unwrap_or_else(|e| panic!("PostgreSQL Server bind fail. err: {}", e));

        let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));
        let processor = Arc::new(StatelessMakeHandler::new(Arc::new(PostgresqlHandler)));
        let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(PlaceholderExtendedQueryHandler)));

        loop {
            tokio::select! {
                peer = listener.accept() => {
                    match peer {
                        Ok((socket, _)) => {
                            tokio::spawn(pgwire::tokio::process_socket(
                                socket,
                                None,
                                authenticator.make(),
                                processor.make(),
                                placeholder.make(),
                            ));
                        }
                        Err(e) => {
                            error!("PostgreSQL Server accept new connection fail. err: {}", e);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_postgresql_server() {
        let addr = SocketAddr::from_str("127.0.0.1:5434").unwrap();
        let server = PostgresqlServer::new(addr);
        server.start().await.unwrap();

        // wait
        tokio::time::sleep(tokio::time::Duration::from_secs(10000000)).await;
    }
}
