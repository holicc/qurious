use std::sync::Arc;

use sqlparser::{
    ast::{From, Statement},
    parser::Parser,
};

use crate::{
    error::{Error, Result},
    execution::registry::TableRegistry,
    logical::{plan::LogicalPlan, LogicalPlanBuilder},
};

pub struct SqlQueryPlanner {
    table_registry: Arc<dyn TableRegistry>,
}

impl SqlQueryPlanner {
    pub fn new(table_registry: Arc<dyn TableRegistry>) -> Self {
        SqlQueryPlanner { table_registry }
    }

    pub fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let stmts = Parser::new(sql)
            .parse()
            .map_err(|e| Error::SQLParseError(e))?;

        match stmts {
            Statement::CreateTable {
                table,
                check_exists,
                columns,
            } => todo!(),
            Statement::CreateSchema {
                schema,
                check_exists,
            } => todo!(),
            Statement::DropTable {
                table,
                check_exists,
            } => todo!(),
            Statement::DropSchema {
                schema,
                check_exists,
            } => todo!(),
            Statement::Select {
                distinct,
                columns,
                from,
                r#where,
                group_by,
                having,
                order_by,
                limit,
                offset,
            } => {
                // process `from` clause
                let plan = if let Some(f) = from {
                    self.table_scan_to_plan(f)?
                } else {
                    LogicalPlanBuilder::empty().build()
                };

                // TODO distinct

                todo!()
            }
            Statement::Insert {
                table,
                columns,
                values,
                on_conflict,
                returning,
            } => todo!(),
            Statement::Update {
                table,
                assignments,
                r#where,
            } => todo!(),
            Statement::Delete { table, r#where } => todo!(),
        }
    }

    fn create_logical_expr(&self) -> Result<LogicalPlan> {
        todo!()
    }

    fn select_to_plan(&self) -> Result<LogicalPlan> {
        todo!()
    }

    fn table_scan_to_plan(&self, from: From) -> Result<LogicalPlan> {
        match from {
            From::Table { name, alias } => {
                self.table_registry
                    .get_table_source(&name)
                    .map(|table_source| LogicalPlanBuilder::scan(&name, table_source).build())

                // TODO process table alias
            }
            From::TableFunction { name, args, alias } => todo!(),
            From::SubQuery { query, alias } => todo!(),
            From::Join {
                left,
                right,
                on,
                join_type,
            } => todo!(),
        }
    }
}
