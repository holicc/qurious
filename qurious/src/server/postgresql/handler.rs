use std::sync::Arc;

use crate::{execution::session::ExecuteSession, logical::plan::LogicalPlan, planner::sql::SqlQueryPlanner};
use async_trait::async_trait;
use pgwire::{
    api::{
        auth::noop::NoopStartupHandler,
        copy::NoopCopyHandler,
        portal::Portal,
        query::{ExtendedQueryHandler, SimpleQueryHandler},
        results::{DescribePortalResponse, DescribeStatementResponse, Response},
        stmt::{QueryParser, StoredStatement},
        ClientInfo, PgWireHandlerFactory, Type,
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
};

use super::datatypes::{into_pg_reponse, into_pg_type};

pub struct HandlerFactory(pub Arc<PostgresqlHandler>);

impl PgWireHandlerFactory for HandlerFactory {
    type StartupHandler = NoopStartupHandler;
    type SimpleQueryHandler = PostgresqlHandler;
    type ExtendedQueryHandler = PostgresqlHandler;
    type CopyHandler = NoopCopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.0.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.0.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(NoopStartupHandler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}

pub struct Parser {
    session: Arc<ExecuteSession>,
}

#[async_trait]
impl QueryParser for Parser {
    type Statement = LogicalPlan;

    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        let mut parse = sqlparser::parser::Parser::new(sql);
        let stmt = parse.parse();
        let relations = self.session.resolve_tables(parse.relations);

        stmt.map_err(|e| crate::error::Error::SQLParseError(e))
            .and_then(|stmt| SqlQueryPlanner::create_logical_plan(stmt, relations?))
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "FATAL".to_owned(),
                    "28P01".to_owned(),
                    e.to_string(),
                )))
            })
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
        Arc::new(Parser {
            session: self.session.clone(),
        })
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse> {
        let plan = &target.statement;
        let schema = plan.schema();
        let fields = schema
            .fields()
            .iter()
            .map(|v| into_pg_type(v.as_ref()))
            .collect::<PgWireResult<_>>()?;

        Ok(DescribeStatementResponse::new(vec![], fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse> {
        let plan = &target.statement.statement;

        println!("[do_describe_portal] ====> {}", plan);

        let schema = plan.schema();
        let fields = schema
            .fields()
            .iter()
            .map(|v| into_pg_type(v.as_ref()))
            .collect::<PgWireResult<_>>()?;

        Ok(DescribePortalResponse::new(fields))
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
        let plan = &portal.statement.statement;

        println!("[do_query] ====> {}", plan);

        self.session
            .execute_logical_plan(plan)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))
            .and_then(|batch| into_pg_reponse(batch))
    }
}
