use std::sync::Arc;

use sqlparser::{
    ast::{Expression, From, Statement},
    parser::Parser,
};

use crate::{
    error::{Error, Result},
    execution::registry::TableRegistry,
    logical::{
        expr::{column, Column, LogicalExpr},
        plan::LogicalPlan,
        LogicalPlanBuilder,
    },
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

                let column_exprs = columns
                    .into_iter()
                    .map(|(col, alias)| match col {
                        Expression::Literal(lit) => {
                            // normalize column name with table name
                            let column_name = lit.to_string();
                            plan.schema()
                                .field_with_name(&column_name)
                                .map_err(|e| Error::ArrowError(e))
                                .and(Ok(LogicalExpr::Column(Column::new(column_name, Some("t")))))
                        }
                        _ => todo!(),
                    })
                    .collect::<Result<Vec<_>>>()?;
                // process the SELECT expressions
                LogicalPlanBuilder::project(plan, column_exprs)
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
                let builder = self
                    .table_registry
                    .get_table_source(&name)
                    .map(|table_source| LogicalPlanBuilder::scan(&name, table_source))?;

                if let Some(alias) = alias {
                    self.apply_table_alias(builder.build(), alias)
                } else {
                    Ok(builder.build())
                }
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

    fn apply_table_alias(&self, plan: LogicalPlan, alias: String) -> Result<LogicalPlan> {
        todo!("apply_table_alias")
    }

    fn project(
        plan: LogicalPlan,
        columns: Vec<(Expression, Option<String>)>,
    ) -> Result<LogicalPlan> {
        let fields = columns.into_iter().map(|(expr, alias)| {
            LogicalExpr::Column(Column {
                name: expr.to_string(),
                relation: None,
            })
        });

        LogicalPlanBuilder::project(plan, fields)
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder};

    use crate::{
        datasource::{memory::MemoryDataSource, DataSource},
        error::Result,
        execution::registry::TableRegistry,
        utils,
    };

    use super::SqlQueryPlanner;

    struct TestTableRegistry {
        tables: HashMap<String, Box<dyn DataSource>>,
    }

    impl TestTableRegistry {
        fn new() -> Self {
            TestTableRegistry {
                tables: HashMap::new(),
            }
        }
    }

    impl TableRegistry for TestTableRegistry {
        fn register_table(&mut self, _name: &str, _table: Arc<dyn DataSource>) -> Result<()> {
            Ok(())
        }

        fn get_table_source(&self, _name: &str) -> Result<Arc<dyn DataSource>> {
            let mut schema = SchemaBuilder::default();

            schema.push(Field::new("id", DataType::Int32, false));
            schema.push(Field::new("name", DataType::Utf8, false));

            Ok(Arc::new(MemoryDataSource::new(
                Arc::new(schema.finish()),
                vec![],
            )))
        }
    }

    #[test]
    fn test_project_plan() -> Result<()> {
        let planner = SqlQueryPlanner::new(Arc::new(TestTableRegistry::new()));
        let plan = planner.create_logical_plan("SELECT id, name FROM t")?;

        println!("{}", utils::format(&plan, 0));

        Ok(())
    }
}
