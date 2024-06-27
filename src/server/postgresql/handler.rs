use std::{
    sync::{Arc, Mutex, RwLock},
    vec,
};

use crate::{
    error::{Error, Result},
    execution::{registry::TableRegistry, session::ExecuteSession},
    logical::plan::LogicalPlan,
    planner::sql::SqlQueryPlanner,
};
use async_trait::async_trait;
use pgwire::{
    api::{
        auth::noop::NoopStartupHandler,
        copy::NoopCopyHandler,
        portal::{Format, Portal},
        query::{ExtendedQueryHandler, PlaceholderExtendedQueryHandler, SimpleQueryHandler},
        results::{DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse, Response},
        stmt::{NoopQueryParser, QueryParser, StoredStatement},
        ClientInfo, PgWireHandlerFactory, Type,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
};
use tokio::sync::{mpsc::Sender, oneshot};

use crate::server::server::Message;

use super::datatypes::into_pg_reponse;

pub struct HandlerFactory(pub Arc<PostgresqlHandler>);

impl PgWireHandlerFactory for HandlerFactory {
    type StartupHandler = NoopStartupHandler;
    type SimpleQueryHandler = PostgresqlHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.0.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(NoopStartupHandler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}

pub struct PostgresqlHandler {
    pub(crate) session: Arc<ExecuteSession>,
}

#[async_trait]
impl SimpleQueryHandler for PostgresqlHandler {
    async fn do_query<'a, C>(&self, _client: &mut C, sql: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        self.session
            .sql(sql)
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "FATAL".to_owned(),
                    "28P01".to_owned(),
                    e.to_string(),
                )))
            })
            .and_then(into_pg_reponse)
            .map(|v| vec![v])
    }
}

#[async_trait]
impl ExtendedQueryHandler for PostgresqlHandler {
    type Statement = LogicalPlan;

    type QueryParser = Parser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(Parser(self.session.get_tables()))
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse> {
        todo!()
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse> {
        println!("{:?}", target.statement.statement);

        Ok(DescribePortalResponse::new(vec![]))
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // send_and_receive(self.tx.clone(), portal.statement.statement.clone())
        //     .await
        //     .map_err(|e| {
        //         PgWireError::UserError(Box::new(ErrorInfo::new(
        //             "FATAL".to_owned(),
        //             "28P01".to_owned(),
        //             e.to_string(),
        //         )))
        //     })
        //     .and_then(|batch| into_pg_reponse(batch))
        todo!()
    }
}

pub struct Parser(Arc<RwLock<dyn TableRegistry>>);

#[async_trait]
impl QueryParser for Parser {
    type Statement = LogicalPlan;

    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        SqlQueryPlanner::create_logical_plan(self.0.clone(), sql).map_err(|e| PgWireError::ApiError(Box::new(e)))
    }
}
